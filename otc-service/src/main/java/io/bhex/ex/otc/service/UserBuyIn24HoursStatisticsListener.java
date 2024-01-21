package io.bhex.ex.otc.service;

import com.google.api.client.util.Sets;
import com.google.common.base.Splitter;
import io.bhex.ex.otc.OTCOrderStatusEnum;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.UserExt;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.mappper.UserExtMapper;
import io.bhex.ex.proto.OrderSideEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UserBuyIn24HoursStatisticsListener extends OtcBaseMessageListener {


    private final static String TOPIC = "/redis/otc/user/statistics";

    private final static String USER_STATISTICS = "OTC_USER_STATISTICS";

    private final static String USDT = "USDT";

    @Resource
    private GrpcServerService grpcServerService;

    @Resource
    private UserExtMapper userExtMapper;

    private final long exchangeId = 301L;

    @Override
    public void doWork(Object object) {

        String accountIdStr = "";
        boolean rollback = true;
        try {
            while (true) {
                accountIdStr = stringRedisTemplate.opsForList().rightPop(USER_STATISTICS);

                if (StringUtils.isBlank(accountIdStr)) {
                    log.info("Hasn't any accountId");
                    break;
                }

                List<String> list = Splitter.on("*").splitToList(accountIdStr);
                if (list.size() < 3) {
                    log.error("Statistics of user buy error,accountId={}", accountIdStr);
                    continue;
                }

                long accountId = Long.parseLong(list.get(0));
                Date start = new Date(Long.parseLong(list.get(1)));
                Date end = new Date(Long.parseLong(list.get(2)));
                long userId = 0;
                if (Objects.nonNull(list.get(3))) {
                    userId = Long.parseLong(list.get(3));
                }

                boolean success = this.doStatistics(accountId, start, end, userId);
                if (success) {
                    rollback = false;
                }
            }
        } catch (Exception e) {
            log.error("Error,param=[{}]" + accountIdStr, e);
            if (rollback == true) {
                try {
                    stringRedisTemplate.opsForList().leftPush(USER_STATISTICS, accountIdStr);
                } catch (Exception ex) {
                    log.error("Rollback fail,accountId={}", accountIdStr);
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public String getTopic() {
        return TOPIC;
    }

    private boolean doStatistics(Long accountId, Date start, Date end, long userId) {

        Example exp = new Example(OtcOrder.class);
        exp.selectProperties("userId", "accountId", "tokenId", "quantity");
        exp.createCriteria()
                .andEqualTo("accountId", accountId)
                .andEqualTo("side", OrderSideEnum.BUY.getNumber())
                .andEqualTo("status", OTCOrderStatusEnum.OTC_ORDER_FINISH.getNumber())
                .andBetween("createDate", start, end);

        List<OtcOrder> orders = otcOrderMapper.selectByExample(exp);
        if (CollectionUtils.isEmpty(orders)) {

            UserExt.Builder builder = UserExt.builder()
                    .userId(userId)
                    .updatedAt(new Date())
                    .usdtValue24HoursBuy(BigDecimal.ZERO);

            userExtMapper.updateByPrimaryKeySelective(builder.build());
            return true;
        }

        userId = orders.get(0).getUserId();

        //按照token分组，对quantity进行加和
        Map<String, Double> buyMap = orders.stream().collect(Collectors.groupingBy
                (OtcOrder::getTokenId, Collectors.summingDouble(i -> i.getQuantity().doubleValue())));

        Set<BigDecimal> amountSet = Sets.newHashSet();
        buyMap.forEach((token, quantity) -> {
            Map<String, BigDecimal> rateMap = grpcServerService.getLastPrice(exchangeId, token);
            BigDecimal rate = rateMap.getOrDefault(USDT, BigDecimal.ZERO);
            BigDecimal amount = rate.multiply(BigDecimal.valueOf(quantity));
            amountSet.add(amount);
        });

        double totalAmount = amountSet.stream().mapToDouble(i -> i.doubleValue()).sum();

        UserExt.Builder builder = UserExt.builder()
                .userId(userId)
                .updatedAt(new Date())
                .usdtValue24HoursBuy(BigDecimal.valueOf(totalAmount));

        UserExt exist = userExtMapper.selectByPrimaryKey(userId);
        if (Objects.isNull(exist)) {
            builder.accountId(accountId).createdAt(new Date());
            userExtMapper.insertSelective(builder.build());
        } else {
            userExtMapper.updateByPrimaryKeySelective(builder.build());
        }

        return true;
    }

}
