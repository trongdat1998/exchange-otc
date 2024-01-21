package io.bhex.ex.otc.service;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.OtcStatisticData;
import io.bhex.ex.otc.enums.OrderStatus;
import io.bhex.ex.otc.enums.Side;
import io.bhex.ex.otc.enums.StatisticType;
import io.bhex.ex.otc.mappper.OtcItemMapper;
import io.bhex.ex.otc.mappper.OtcOrderMapper;
import io.bhex.ex.otc.mappper.OtcStatisticDataMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;

/**
 * @author lizhen
 * @date 2018-12-06
 */
@Slf4j
@Service
public class OtcStatisticDataService {

    private static final int SIZE = 200;

    @Resource
    private OtcItemMapper otcItemMapper;

    @Resource
    private OtcOrderMapper otcOrderMapper;

    @Resource
    private OtcStatisticDataMapper otcStatisticDataMapper;

    public void executeStatistic(String statisticDate) {
        log.info("begin statistic data, date: {}", statisticDate);
        Date now = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date beginTime, endTime;
        try {
            beginTime = format.parse(statisticDate + "000000");
            endTime = format.parse(statisticDate + "235959");
        } catch (ParseException e) {
            log.info("format date error, date: {}", statisticDate, e);
            return;
        }
        Example example = Example.builder(OtcOrder.class).build();
        example.createCriteria().andIn("status", Lists.newArrayList(
                OrderStatus.APPEAL.getStatus()
                , OrderStatus.CANCEL.getStatus()
                , OrderStatus.FINISH.getStatus()
        ))
                .andBetween("updateDate", beginTime, endTime);
/*        example.createCriteria()
            .andNotEqualTo("status", OrderStatus.DELETE.getStatus())
            .andNotEqualTo("status", OrderStatus.INIT.getStatus())
            .andBetween("updateDate", beginTime, endTime);*/

        Map<String, Integer> finishOrderCount = Maps.newHashMap();
        Map<String, Integer> cancelOrderCount = Maps.newHashMap();
        Map<String, Integer> appealOrderCount = Maps.newHashMap();

        Map<String, Set<Long>> finishOrderUserCount = Maps.newHashMap();
        Map<String, Set<Long>> cancelOrderUserCount = Maps.newHashMap();
        Map<String, Set<Long>> appealOrderUserCount = Maps.newHashMap();

        Set<Long> finishOrderTotalUserCount = Sets.newHashSet();
        Set<Long> cancelOrderTotalUserCount = Sets.newHashSet();
        Set<Long> appealOrderTotalUserCount = Sets.newHashSet();

        Map<String, BigDecimal> tokenTradeAmount = Maps.newHashMap();
        Map<String, BigDecimal> currencyTradeAmount = Maps.newHashMap();

        for (int i = 0; ; ++i) {

            PageHelper.startPage((i + 1), SIZE);
            List<OtcOrder> orderList = otcOrderMapper.selectByExample(example);

/*            List<OtcOrder> orderList = otcOrderMapper.selectByExampleAndRowBounds(example,
                new RowBounds(i * SIZE, SIZE));*/
            if (CollectionUtils.isEmpty(orderList)) {
                break;
            }
            for (OtcOrder order : orderList) {
                String symbolKey = order.getTokenId() + order.getCurrencyId();
                if (order.getAppealType() != null) {
                    Integer orderCount = appealOrderCount.get(symbolKey);
                    if (orderCount == null) {
                        orderCount = 0;
                    }
                    appealOrderCount.put(symbolKey, ++orderCount);

                    Set<Long> userCount = appealOrderUserCount.get(symbolKey);
                    if (userCount == null) {
                        userCount = Sets.newHashSet();
                        appealOrderUserCount.put(symbolKey, userCount);
                    }

                    if (order.getAppealAccountId() != null && order.getAppealAccountId() > 0) {
                        userCount.add(order.getAppealAccountId());
                        appealOrderTotalUserCount.add(order.getAppealAccountId());
                    }
                }

                if (order.getStatus().equals(OrderStatus.FINISH.getStatus())) {
                    Integer orderCount = finishOrderCount.get(symbolKey);
                    if (orderCount == null) {
                        orderCount = 0;
                    }
                    finishOrderCount.put(symbolKey, ++orderCount);

                    Set<Long> userCount = finishOrderUserCount.get(symbolKey);
                    if (userCount == null) {
                        userCount = Sets.newHashSet();
                        finishOrderUserCount.put(symbolKey, userCount);
                    }
                    userCount.add(order.getAccountId());
                    userCount.add(order.getTargetAccountId());

                    finishOrderTotalUserCount.add(order.getAccountId());
                    finishOrderTotalUserCount.add(order.getTargetAccountId());

                    BigDecimal tokenAmount = tokenTradeAmount.get(symbolKey);
                    if (tokenAmount == null) {
                        tokenAmount = BigDecimal.ZERO;
                    }
                    tokenTradeAmount.put(symbolKey, tokenAmount.add(order.getQuantity()));

                    BigDecimal currencyAmount = currencyTradeAmount.get(symbolKey);
                    if (currencyAmount == null) {
                        currencyAmount = BigDecimal.ZERO;
                    }
                    currencyTradeAmount.put(symbolKey, currencyAmount.add(order.getAmount()));
                } else if (order.getStatus().equals(OrderStatus.CANCEL.getStatus())) {
                    Integer orderCount = cancelOrderCount.get(symbolKey);
                    if (orderCount == null) {
                        orderCount = 0;
                    }
                    cancelOrderCount.put(symbolKey, ++orderCount);

                    Set<Long> userCount = cancelOrderUserCount.get(symbolKey);
                    if (userCount == null) {
                        userCount = Sets.newHashSet();
                        cancelOrderUserCount.put(symbolKey, userCount);
                    }

                    long cancelAccountId = order.getSide().equals(Side.BUY.getCode()) ?
                            order.getAccountId() : order.getTargetAccountId();
                    userCount.add(cancelAccountId);

                    cancelOrderTotalUserCount.add(cancelAccountId);
                }
            }
        }

        List<OtcStatisticData> statisticDataList = Lists.newArrayList();
        statisticDataList.addAll(finishOrderCount.entrySet().stream().map(
                finishOrder -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.FINISH_ORDER_COUNT.getType())
                        .statisticDetail(finishOrder.getKey())
                        .amount(new BigDecimal(finishOrder.getValue()))
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));
        statisticDataList.addAll(cancelOrderCount.entrySet().stream().map(
                cancelOrder -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.CANCEL_ORDER_COUNT.getType())
                        .statisticDetail(cancelOrder.getKey())
                        .amount(new BigDecimal(cancelOrder.getValue()))
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));
        statisticDataList.addAll(appealOrderCount.entrySet().stream().map(
                appealOrder -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.APPEAL_ORDER_COUNT.getType())
                        .statisticDetail(appealOrder.getKey())
                        .amount(new BigDecimal(appealOrder.getValue()))
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));

        statisticDataList.addAll(finishOrderUserCount.entrySet().stream().map(
                finishOrder -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.FINISH_ORDER_USER_COUNT.getType())
                        .statisticDetail(finishOrder.getKey())
                        .amount(new BigDecimal(finishOrder.getValue().size()))
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));
        statisticDataList.addAll(cancelOrderUserCount.entrySet().stream().map(
                cancelOrder -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.CANCEL_ORDER_USER_COUNT.getType())
                        .statisticDetail(cancelOrder.getKey())
                        .amount(new BigDecimal(cancelOrder.getValue().size()))
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));
        statisticDataList.addAll(appealOrderUserCount.entrySet().stream().map(
                appealOrder -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.APPEAL_ORDER_USER_COUNT.getType())
                        .statisticDetail(appealOrder.getKey())
                        .amount(new BigDecimal(appealOrder.getValue().size()))
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));

        statisticDataList.add(OtcStatisticData.builder()
                .orgId(6002L)
                .statisticDate(statisticDate)
                .type(StatisticType.FINISH_ORDER_TOTAL_USER_COUNT.getType())
                .statisticDetail("")
                .amount(new BigDecimal(finishOrderTotalUserCount.size()))
                .createDate(now)
                .build());
        statisticDataList.add(OtcStatisticData.builder()
                .orgId(6002L)
                .statisticDate(statisticDate)
                .type(StatisticType.CANCEL_ORDER_TOTAL_USER_COUNT.getType())
                .statisticDetail("")
                .amount(new BigDecimal(cancelOrderTotalUserCount.size()))
                .createDate(now)
                .build());
        statisticDataList.add(OtcStatisticData.builder()
                .orgId(6002L)
                .statisticDate(statisticDate)
                .type(StatisticType.APPEAL_ORDER_TOTAL_USER_COUNT.getType())
                .statisticDetail("")
                .amount(new BigDecimal(appealOrderTotalUserCount.size()))
                .createDate(now)
                .build());

        statisticDataList.addAll(tokenTradeAmount.entrySet().stream().map(
                tokenTrade -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.TOKEN_TRADE_AMOUNT.getType())
                        .statisticDetail(tokenTrade.getKey())
                        .amount(tokenTrade.getValue())
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));
        statisticDataList.addAll(currencyTradeAmount.entrySet().stream().map(
                currencyTrade -> OtcStatisticData.builder()
                        .orgId(6002L)
                        .statisticDate(statisticDate)
                        .type(StatisticType.CURRENCY_TRADE_AMOUNT.getType())
                        .statisticDetail(currencyTrade.getKey())
                        .amount(currencyTrade.getValue())
                        .createDate(now)
                        .build()
        ).collect(Collectors.toList()));

        List<Map<String, Object>> itemStatisticList = otcItemMapper.selectOtcItemStatistic(beginTime, endTime);
        if (!CollectionUtils.isEmpty(itemStatisticList)) {
            statisticDataList.addAll(itemStatisticList.stream().map(
                    itemStatistic -> OtcStatisticData.builder()
                            .orgId(6002L)
                            .statisticDate(statisticDate)
                            .type(StatisticType.CREATE_ITEM_COUNT.getType())
                            .statisticDetail(itemStatistic.get("token_id").toString())
                            .amount(new BigDecimal(itemStatistic.get("item_count").toString()))
                            .createDate(now)
                            .build()
            ).collect(Collectors.toList()));
        }

        //存在更新，不存在则插入
        try {

            for (OtcStatisticData data : statisticDataList) {

                Example exp = new Example(OtcStatisticData.class);
                exp.createCriteria()
                        .andEqualTo("orgId", data.getOrgId())
                        .andEqualTo("statisticDate", data.getStatisticDate())
                        .andEqualTo("type", data.getType())
                        .andEqualTo("statisticDetail", data.getStatisticDetail());

                OtcStatisticData exist = otcStatisticDataMapper.selectOneByExample(exp);
                if (Objects.isNull(exist)) {
                    otcStatisticDataMapper.insert(data);
                    continue;
                }

                OtcStatisticData update = new OtcStatisticData();
                update.setId(exist.getId());
                update.setAmount(data.getAmount().add(exist.getAmount()));
                update.setCreateDate(data.getCreateDate());

                otcStatisticDataMapper.updateByPrimaryKeySelective(update);
            }

            //otcStatisticDataMapper.batchInsertStatisticData(statisticDataList);
        } catch (Exception e) {
            log.info("batch insert statistic data failed, date={}, maybe repeat", statisticDate, e);
        }
        log.info("statistic data success, date={}, count={}", statisticDate, statisticDataList.size());
    }

    public List<OtcStatisticData> getDataList(Long orgId, Integer type, String date) {
        OtcStatisticData example = OtcStatisticData.builder().build();
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        if (type != null && type > 0) {
            example.setType(type);
        }
        if (StringUtils.isNotBlank(date)) {
            example.setStatisticDate(date);
        }

        return otcStatisticDataMapper.select(example);
    }

}