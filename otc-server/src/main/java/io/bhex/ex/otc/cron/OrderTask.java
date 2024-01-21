package io.bhex.ex.otc.cron;

import io.bhex.ex.otc.entity.BrokerExt;
import io.bhex.ex.otc.entity.OtcDepthShareBrokerWhiteList;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.enums.AppealType;
import io.bhex.ex.otc.exception.BusinessException;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.service.config.OtcConfigService;
import io.bhex.ex.otc.util.LockUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;

/**
 * 订单处理定时任务
 *
 * @author lizhen
 * @date 2018-11-09
 */
@Slf4j
@Component
public class OrderTask {

    @Autowired
    private OtcOrderService otcOrderService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private OtcConfigService otcConfigService;

    private static final String ORDER_PAY_OVERTIME_TASK_LOCK = "OTC::ORDER::PAY_OVERTIME_TASK::LOCK";

    private static final String ORDER_FINISH_OVERTIME_TASK_LOCK = "OTC::ORDER::FINISH_OVERTIME_TASK::LOCK";

    private static final int EXPIRE_TIME = 15 * 60 * 1000;

    /**
     * 每隔10s扫描一次超时未支付订单，并进行撤销
     */
    @Scheduled(cron = "8/10 * * * * ?")
    public void orderPayOvertimeSchedule() {
        Boolean locked = LockUtil.tryLock(redisTemplate, ORDER_PAY_OVERTIME_TASK_LOCK, 10 * 1000);
        if (!locked) {
            return;
        }
        log.info("new orderPayOvertimeSchedule got lock");
        // 开定时任务扫描超时未支付的订单，并进行撤单操作
        long startTime = System.currentTimeMillis();
        try {
            List<OtcOrder> orderList = otcOrderService.listUnpayTimeoutOrder(new Date(System.currentTimeMillis() - (15 * 60 * 1000)));
            if (CollectionUtils.isEmpty(orderList)) {
                log.info("cancel pay-overtime-order empty list");
                return;
            }
            orderList.forEach(otcOrder -> {
                //获取当前券商的申诉时间配置(内存获取) 如果大于的话直接进入申诉状态
                BrokerExt brokerExt = this.otcConfigService.getBrokerExtFromCache(otcOrder.getOrgId());
                //当前券商是否是白名单券商
                OtcDepthShareBrokerWhiteList brokerWhite = this.otcConfigService.getOtcDepthShareBrokerWhiteListFromCache(otcOrder.getOrgId());
                try {
                    if (brokerWhite == null && brokerExt != null) {
                        if (brokerExt.getCancelTime() != null && brokerExt.getCancelTime() > 0) {
                            if (otcOrder.getCreateDate() != null && otcOrder.getCreateDate().getTime() <= (System.currentTimeMillis() - (brokerExt.getCancelTime() * 60 * 1000))) {
                                otcOrderService.cancelOrder(otcOrder, true);
                                log.info("cancel pay-overtime-order success, orderId={}", otcOrder.getId());
                            }
                        } else {
                            //判断是否超过15分钟
                            if (otcOrder.getCreateDate() != null && otcOrder.getCreateDate().getTime() <= (System.currentTimeMillis() - (15 * 60 * 1000))) {
                                otcOrderService.cancelOrder(otcOrder, true);
                                log.info("cancel pay-overtime-order success, orderId={}", otcOrder.getId());
                            }
                        }
                    } else {
                        if (otcOrder.getCreateDate() != null && otcOrder.getCreateDate().getTime() <= (System.currentTimeMillis() - (15 * 60 * 1000))) {
                            otcOrderService.cancelOrder(otcOrder, true);
                            log.info("cancel pay-overtime-order success, orderId={}", otcOrder.getId());
                        }
                    }
                } catch (BusinessException e) {
                    log.info("cancel pay-overtime-order failed, orderId={}", otcOrder.getId(), e);
                } catch (Exception e) {
                    log.error("cancel pay-overtime-order failed, orderId={}", otcOrder.getId(), e);
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("use time : ", (System.currentTimeMillis() - startTime));
    }

    /**
     * 每隔30s扫描一次已支付超过30min订单，并进行
     */
    @Scheduled(cron = "6/30 * * * * ?")
    public void orderFinishOvertimeSchedule() {
        Boolean locked = LockUtil.tryLock(redisTemplate, ORDER_FINISH_OVERTIME_TASK_LOCK, 30 * 1000);
        if (!locked) {
            return;
        }
        log.info("new orderFinishOvertimeSchedule got lock");
        // 开定时任务扫描超时未确认放币的订单，并进行申诉操作
        try {
            List<OtcOrder> orderList = otcOrderService.getFinishOvertimeOrderList();
            if (CollectionUtils.isEmpty(orderList)) {
                return;
            }
            orderList.forEach(otcOrder -> {
                //获取当前券商的申诉时间配置(内存获取) 如果大于的话直接进入申诉状态
                BrokerExt brokerExt = this.otcConfigService.getBrokerExtFromCache(otcOrder.getOrgId());
                //当前券商是否是白名单券商
                OtcDepthShareBrokerWhiteList brokerWhite = this.otcConfigService.getOtcDepthShareBrokerWhiteListFromCache(otcOrder.getOrgId());
                try {
                    //非共享商家才能走特殊的时间配置
                    if (brokerWhite == null && brokerExt != null) {
                        if (brokerExt.getAppealTime() != null && brokerExt.getAppealTime() > 0) {
                            if (otcOrder.getTransferDate() != null && otcOrder.getTransferDate().getTime() <= (System.currentTimeMillis() - (brokerExt.getAppealTime() * 60 * 1000))) {
                                otcOrder.setAppealType(AppealType.OTHER.getType());
                                otcOrder.setAppealContent("overtime auto appeal");
                                otcOrderService.appealOrder(otcOrder, 0, true);
                            }
                        } else {
                            //判断是否超过30分钟
                            if (otcOrder.getTransferDate() != null && otcOrder.getTransferDate().getTime() <= (System.currentTimeMillis() - (30 * 60 * 1000))) {
                                otcOrder.setAppealType(AppealType.OTHER.getType());
                                otcOrder.setAppealContent("overtime auto appeal");
                                otcOrderService.appealOrder(otcOrder, 0, true);
                            }
                        }
                    } else {
                        if (otcOrder.getTransferDate() != null && otcOrder.getTransferDate().getTime() <= (System.currentTimeMillis() - (30 * 60 * 1000))) {
                            otcOrder.setAppealType(AppealType.OTHER.getType());
                            otcOrder.setAppealContent("overtime auto appeal");
                            otcOrderService.appealOrder(otcOrder, 0, true);
                        }
                    }
                    log.info("appeal finish-overtime-order success, orderId={}", otcOrder.getId());
                } catch (BusinessException e) {
                    log.info("appeal finish-overtime-order failed, orderId={}", otcOrder.getId(), e);
                } catch (Exception e) {
                    log.error("appeal finish-overtime-order failed, orderId={}", otcOrder.getId(), e);
                }
            });
        } catch (Exception e) {
            log.error("select finish-overtime-orders failed", e);
        }
    }
}