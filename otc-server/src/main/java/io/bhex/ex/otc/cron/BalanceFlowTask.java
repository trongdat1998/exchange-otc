package io.bhex.ex.otc.cron;

import io.bhex.ex.otc.entity.OtcBrokerRiskBalanceConfig;
import io.bhex.ex.otc.service.OtcBrokerRiskBalanceConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import io.bhex.base.account.ReleaseRiskOtcRequest;
import io.bhex.base.account.ReleaseRiskOtcResponse;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.ex.otc.config.GrpcClientFactory;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.service.OtcBalanceFlowService;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.util.LockUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 动账流水定时任务
 *
 * @author lizhen
 * @date 2018-11-09
 */
@Slf4j
@Component
public class BalanceFlowTask {

    @Autowired
    private OtcBalanceFlowService otcBalanceFlowService;

    @Autowired
    private GrpcServerService grpcServerService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private OtcOrderService otcOrderService;

    @Autowired
    private GrpcClientFactory grpcClientFactory;

    @Autowired
    OtcBrokerRiskBalanceConfigService otcBrokerRiskBalanceConfigService;

    private static final String BALANCE_FLOW_TASK_LOCK = "OTC::BALANCE_FLOW::TASK::LOCK";

    private static final String RELEASE_RISK_FUND_TASK = "RELEASE::RISK::FUND::TASK";

    private static final Long RISK_FUND_TIME = 24L;

    private static final String RELEASE_TIME_KEY = "RELEASE::TIME::KEY";

    /**
     * 每隔30s扫描一次未修改成功的动账流水，重新执行
     */
    @Scheduled(cron = "4/30 * * * * ?")
    public void balanceFlowSchedule() {
        Boolean locked = LockUtil.tryLock(redisTemplate, BALANCE_FLOW_TASK_LOCK, 30 * 1000);
        if (!locked) {
            return;
        }
        // 开定时任务扫描未处理的动账流水，发送给bh-server处理
        List<OtcBalanceFlow> balanceFlowList = otcBalanceFlowService.getTimeoutOtcBalanceFlow();
        if (CollectionUtils.isEmpty(balanceFlowList)) {
            return;
        }
        balanceFlowList.forEach(grpcServerService::sendBalanceChangeRequest);
    }

    /**
     * 每隔10分钟扫描订单已完成超过24小时 买方 状态为未释放风险资金的OTC订单
     */
    @Scheduled(cron = "0 5/10 * * * ?")
    public void releaseRiskFunds() {
        Boolean locked = LockUtil.tryLock(redisTemplate, RELEASE_RISK_FUND_TASK, 10 * 60 * 1000);
        if (!locked) {
            return;
        }
        Long riskTime;
        Object object = redisTemplate.opsForValue().get(RELEASE_TIME_KEY);
        log.info("value {}", object != null ? Long.parseLong(String.valueOf(object)) : null);
        if (object != null) {
            riskTime = Long.parseLong(String.valueOf(object));
        } else {
            riskTime = RISK_FUND_TIME;
        }

        log.info("OTC release risk time : {}", riskTime);
        log.info("OTC release risk funds task start");
        int index = 1;
        int limit = 100;
        while (true) {
            //获取未释放的风险订单 进行释放，freed值 1：共享券商未释放 3:非共享券商未释放
            List<OtcOrder> orders = otcOrderService.selectOtcOrderForFreed(index, limit);
            if (CollectionUtils.isEmpty(orders)) {
                break;
            }

            if (orders != null && orders.size() > 0) {
                orders.forEach(order -> {
                    //是否超过24小时 如果超过24小时调用风险资金释放接口 transfer_date
                    if (order.getTransferDate() != null) {
                        LocalDateTime transferDate = LocalDateTime.ofInstant(order.getTransferDate().toInstant(), ZoneId.systemDefault());
                        LocalDateTime now = LocalDateTime.ofInstant(new Date().toInstant(), ZoneId.systemDefault());

                        //风险控制时间默认取risktime的值
                        long currRiskTime = riskTime;

                        //对于3:非共享券商未释放 取券商的具体配置来确定风险资产释放的时间配置
                        if (order.getFreed() == 3) {
                            //默认为0
                            currRiskTime = 0l;
                            OtcBrokerRiskBalanceConfig otcBrokerRiskBalanceConfig = otcBrokerRiskBalanceConfigService.getOtcBrokerRiskBalanceConfig(order.getOrgId());
                            //有配置记录且状态有效，取hours配置；如果没有配置或者状态无效则按0处理
                            if (otcBrokerRiskBalanceConfig != null && otcBrokerRiskBalanceConfig.getStatus() == 1){
                                currRiskTime = otcBrokerRiskBalanceConfig.getHours();
                            }
                        }

                        if (Duration.between(transferDate, now).toHours() >= currRiskTime) {
                            OtcBalanceFlow otcBalanceFlow
                                    = this.otcBalanceFlowService.queryByOrderId(order.getId(), order.getAccountId());
                            if (otcBalanceFlow == null) {
                                return;
                            }
                            ReleaseRiskOtcRequest releaseRiskOtcRequest = ReleaseRiskOtcRequest
                                    .newBuilder()
                                    .setAccountId(order.getAccountId())
                                    .setOrgId(order.getOrgId())
                                    .setAmount(DecimalUtil.fromBigDecimal(order.getQuantity()))
                                    .setBusinessId(otcBalanceFlow.getId())
                                    .build();
                            try {
                                ReleaseRiskOtcResponse response = grpcClientFactory.balanceChangeBlockingStub().releaseRiskOtc(releaseRiskOtcRequest);
                                if (response.getCode() == ReleaseRiskOtcResponse.ResponseCode.SUCCESS) {
                                    //状态修改为已经解冻
                                    otcOrderService.updateOtcOrderFreed(order.getId());
                                } else {
                                    log.warn("OTC release risk funds failed  orderId:{}, accountId:{}, orgId {}, quantity:{}", order.getId(),
                                            order.getAccountId(), order.getOrgId(), order.getQuantity());
                                }
                            } catch (Exception ex) {
                                log.warn("OTC release risk funds failed  orderId:{}, accountId:{}, orgId {}, quantity:{}", order.getId(),
                                        order.getAccountId(), order.getOrgId(), order.getQuantity());
                            }
                        }
                    }
                });
            }
            index++;
        }
        log.info("OTC release risk funds task end");
    }
}