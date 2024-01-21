package io.bhex.ex.otc.service.item;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.collect.Maps;
import io.bhex.ex.otc.util.CommonUtil;
import io.bhex.ex.otc.entity.OtcBrokerCurrency;
import io.bhex.ex.otc.entity.OtcBrokerToken;
import io.bhex.ex.otc.entity.OtcItem;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.enums.PriceType;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lizhen
 * @date 2018-10-04
 */
@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class OtcItemOnline implements Serializable {

    private Set<Long> bidIds = Sets.newHashSet();
    private Set<Long> askIds = Sets.newHashSet();

    private Map<String, PriceLevel> bidCache = Maps.newHashMap();
    private Map<String, PriceLevel> askCache = Maps.newHashMap();

    private BigDecimal buyPrice;
    private BigDecimal sellPrice;

    /**
     * 数字货币币种
     */
    private String tokenId;
    /**
     * 法币币种
     */
    private String currencyId;
    /**
     * 买方广告单列表
     */
    private List<OtcItem> bidList;
    /**
     * 卖方广告单列表
     */
    private List<OtcItem> askList;
    /**
     * 买深度
     */
    private List<PriceLevel> bids;
    /**
     * 卖深度
     */
    private List<PriceLevel> asks;
    /**
     * 最新价
     */
    //private BigDecimal lastPrice;
    /**
     * 用户信息
     */
    private Map<Long, OtcUserInfo> userInfoMap;
    /**
     * 付款方式
     */
    private Map<Long, List<Integer>> paymentTermMap;

    /**
     * token配置
     */
    private Map<String, OtcBrokerToken> tokenConfig;

    /**
     * 货币配置
     */
    private Map<String, Map<String, OtcBrokerCurrency>> currencyConfig;
    /**
     * 刷新时间
     */
    private long time;

    public void setBidList(List<OtcItem> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        this.bidList = list;
        this.bidIds = list.stream().map(i -> i.getId()).collect(Collectors.toSet());
    }

    public void setAskList(List<OtcItem> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        this.askList = list;
        this.askIds = list.stream().map(i -> i.getId()).collect(Collectors.toSet());
    }

    public void cacheBids(List<PriceLevel> list) {

        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        this.bids = list;
        this.bidCache = list.stream().collect(Collectors.toMap(i -> CommonUtil.BigDecimalToString(i.getPrice()), i -> i));
    }

    public void cacheAsks(List<PriceLevel> list) {

        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        this.asks = list;
        this.askCache = list.stream().collect(Collectors.toMap(i -> CommonUtil.BigDecimalToString(i.getPrice()), i -> i));
    }


    public void mergeBidList(List<OtcItem> list) {

        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        if (Objects.isNull(bidList)) {
            bidList = Lists.newArrayList(list);
            return;
        }

        for (OtcItem oi : list) {
            if (bidIds.contains(oi.getId())) {
                continue;
            }

            bidList.add(oi);

            //合并深度信息
            PriceLevel pl = bidCache.get(CommonUtil.BigDecimalToString(oi.getPrice()));
            if (Objects.nonNull(pl)) {
                pl.addQuantity(oi.getLastQuantity());
                pl.addSize(1);
            } else {
                PriceLevel priceLevel = buildPriceLevel(oi);
                if (CollectionUtils.isEmpty(this.bids)) {
                    this.bids = Lists.newArrayList();
                }
                this.bids.add(priceLevel);
            }
        }

    }

    private PriceLevel buildPriceLevel(OtcItem otcItem) {
        return PriceLevel.builder()
                .price(otcItem.getPrice())
                .quantity(otcItem.getLastQuantity())
                .size(1).build();
    }

    public void mergeAskList(List<OtcItem> list) {

        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        if (Objects.isNull(askList)) {
            askList = Lists.newArrayList(list);
            return;
        }

        for (OtcItem oi : list) {
            if (askIds.contains(oi.getId())) {
                continue;
            }

            askList.add(oi);
            //合并深度信息
            PriceLevel pl = askCache.get(CommonUtil.BigDecimalToString(oi.getPrice()));
            if (Objects.nonNull(pl)) {
                pl.addQuantity(oi.getLastQuantity());
                pl.addSize(1);
            } else {
                PriceLevel priceLevel = buildPriceLevel(oi);
                if (CollectionUtils.isEmpty(this.asks)) {
                    this.asks = Lists.newArrayList();
                }
                this.asks.add(priceLevel);
            }
        }

    }

/*    public void appendBids(List<PriceLevel> list){

        if(CollectionUtils.isEmpty(list)){
            return;
        }

        if(CollectionUtils.isEmpty(bids)){
            bids= Lists.newArrayList(list);
            return;
        }

        bids.addAll(list);
    }

    public void appendAsks(List<PriceLevel> list){

        if(CollectionUtils.isEmpty(list)){
            return;
        }

        if(CollectionUtils.isEmpty(asks)){
            asks= Lists.newArrayList(list);
            return;
        }

        asks.addAll(list);
    }*/

    public void mergeUserInfo(Map<Long, OtcUserInfo> userInfos) {

        if (Objects.isNull(userInfos) || userInfos.isEmpty()) {
            return;
        }

        if (Objects.isNull(userInfoMap)) {
            userInfoMap = Maps.newHashMap();
            userInfoMap.putAll(userInfos);
            return;
        }

        userInfoMap.putAll(userInfos);
    }

    public void mergePaymentTerm(Map<Long, List<Integer>> paymentTerms) {

        if (Objects.isNull(paymentTerms) || paymentTerms.isEmpty()) {
            return;
        }

        if (Objects.isNull(paymentTermMap)) {
            paymentTermMap = Maps.newHashMap();
            paymentTermMap.putAll(paymentTerms);
            return;
        }

        paymentTermMap.putAll(paymentTerms);
    }

/*    public void updateLastPrice(BigDecimal price, Long refreshTime) {
        if (Objects.isNull(price) || Objects.isNull(refreshTime)) {
            return;
        }

        if (Objects.isNull(this.lastPrice) || Objects.isNull(this.time)) {
            this.lastPrice = price;
            this.time = refreshTime;
            return;
        }

        if (refreshTime.longValue() > time) {
            this.lastPrice = price;
        }
    }*/

    public OtcItemOnline deepCopy() {
        return SerializationUtils.clone(this);
    }

    public void addTokenConfig(List<OtcBrokerToken> tokenList) {

        if (MapUtils.isEmpty(tokenConfig)) {
            this.tokenConfig = Maps.newHashMap();
        }

        tokenList.forEach(token -> {
            String key = token.getOrgId().toString() + "-" + token.getTokenId();
            if (tokenConfig.containsKey(key)) {
                return;
            }

            tokenConfig.put(key, token);
        });
    }

    public OtcBrokerToken findTokenConfig(Long brokerId, String tokenId) {
        String key = brokerId.toString() + "-" + tokenId;
        return tokenConfig.get(key);
    }

    public void addCurrencyConfig(List<OtcBrokerCurrency> currencies) {

        if (MapUtils.isEmpty(currencyConfig)) {
            this.currencyConfig = Maps.newHashMap();
        }

        currencies.forEach(currency -> {
            String key = currency.getOrgId().toString() + "-" + currency.getCode();
            Map<String, OtcBrokerCurrency> subMap = currencyConfig.get(key);
            if (MapUtils.isEmpty(subMap)) {
                subMap = Maps.newHashMap();
                subMap.put(currency.getLanguage(), currency);
                currencyConfig.put(key, subMap);
                return;
            }

            if (!subMap.containsKey(currency.getLanguage())) {
                subMap.put(currency.getLanguage(), currency);
            }
        });

        if (CollectionUtils.isEmpty(currencies)) {
            return;
        }

        //浮动价格=指数（按找配置精度进行舍位）*浮动率
        Map<Long, Integer> currencyScaleMap = currencies.stream().collect(Collectors.toMap(i -> i.getOrgId(), i -> i.getScale(), (o, n) -> o));

        if (CollectionUtils.isNotEmpty(bidList)) {
            bidList.stream().filter(i -> i.getPriceType().equals(PriceType.PRICE_FLOATING.getType())).forEach(i -> {
                Integer scale = currencyScaleMap.getOrDefault(i.getOrgId(), 8);
                BigDecimal price = i.getPremium().multiply(buyPrice.setScale(scale, BigDecimal.ROUND_DOWN))
                        .divide(new BigDecimal(100), 8, BigDecimal.ROUND_DOWN);
                i.setPrice(price);
            });
        }


        if (CollectionUtils.isNotEmpty(askList)) {
            askList.stream().filter(i -> i.getPriceType().equals(PriceType.PRICE_FLOATING.getType())).forEach(i -> {
                Integer scale = currencyScaleMap.getOrDefault(i.getOrgId(), 8);
                BigDecimal price = i.getPremium().multiply(sellPrice.setScale(scale, BigDecimal.ROUND_DOWN))
                        .divide(new BigDecimal(100), 8, BigDecimal.ROUND_DOWN);
                i.setPrice(price);
            });
        }
    }

    public List<OtcBrokerCurrency> findCurrencyConfig(Long brokerId, String currencyId) {

        String key = brokerId.toString() + "-" + currencyId;
        Map<String, OtcBrokerCurrency> map = currencyConfig.get(key);
        if (MapUtils.isEmpty(map)) {
            return Lists.newArrayList();
        }

        return Lists.newArrayList(map.values());
    }

    public void mergeTokenConfig(Map<String, OtcBrokerToken> config) {

        if (MapUtils.isEmpty(config)) {
            return;
        }

        if (Objects.isNull(this.tokenConfig)) {
            this.tokenConfig = Maps.newHashMap();
        }

        this.tokenConfig.putAll(config);

    }

    public void mergeCurrencyConfig(Map<String, Map<String, OtcBrokerCurrency>> config) {

        if (MapUtils.isEmpty(config)) {
            return;
        }

        if (MapUtils.isEmpty(this.currencyConfig)) {
            this.currencyConfig = Maps.newHashMap();
        }

        config.forEach((k, v) -> {
            if (!this.currencyConfig.containsKey(k)) {
                this.currencyConfig.put(k, v);
                return;
            }

            Map<String, OtcBrokerCurrency> map = this.currencyConfig.get(k);
            v.forEach((vk, vv) -> {
                if (!map.containsKey(vk)) {
                    map.put(vk, vv);
                }
            });
        });
    }

/*    public void sortAskList() {

        if (CollectionUtils.isEmpty(askList)) {
            return;
        }


        askList.forEach(item -> {
            OtcUserInfo oui = userInfoMap.get(item.getAccountId());
            if (Objects.nonNull(oui)) {
                item.setCompeteRate(oui.getCompleteRateDay30Safe());
                item.setFinishOrderNumber(oui.getOrderFinishNumberDay30Safe());
            } else {
                item.setCompeteRate(BigDecimal.ZERO);
                item.setFinishOrderNumber(Integer.valueOf(0));
            }
        });

        //广告时间
        Comparator<OtcItem> createTime = (o1, o2) ->
                o1.getCreateDate().compareTo(o2.getCreateDate());

        //30日成单率
        Comparator<OtcItem> rateDesc = (o1, o2) ->
                o2.getCompeteRate().setScale(2, RoundingMode.DOWN).compareTo(o1.getCompeteRate().setScale(2, RoundingMode.DOWN));

        //30日成单数
        Comparator<OtcItem> finishItemDesc = (o1, o2) ->
                o2.getFinishOrderNumber().compareTo(o1.getFinishOrderNumber());

        //取最大法币精度配置
        int tmp_scale = 2;
        Set<Integer> scales = Sets.newHashSet();
        scales.add(tmp_scale);
        if (MapUtils.isNotEmpty(this.currencyConfig)) {
            for (Map<String, OtcBrokerCurrency> value : this.currencyConfig.values()) {
                value.forEach((k, v) -> {
                    scales.add(v.getScaleSafe());
                });
            }

*//*            Map<String,OtcBrokerCurrency> map=this.currencyConfig.values().stream().findFirst().orElse(Maps.newHashMap());
            tmp_scale=map.values().stream().map(i->i.getScale()).findFirst().orElse(2);*//*
        }

        final int scale = scales.stream().mapToInt(i -> i).max().getAsInt();

*//*        Comparator<OtcItem> finishItemDesc=(o1,o2)->
                o2.getFinishNumSafe().compareTo(o1.getFinishNumSafe());*//*

        //ask按照价格降序,成交率倒序排列
        try {
            askList = askList.stream().sorted(Comparator.comparing(OtcItem::getPrice,
                    (p1, p2) -> p1.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)
                            .compareTo(p2.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)))
                    .thenComparing(rateDesc)
                    .thenComparing(createTime)
                    .thenComparing(finishItemDesc))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }*/

    /**
     * 广告排序算法，按照成交率98%分组，分组后的广告，按照价格和时间排序
     */
    public void sortAskList(BigDecimal lastPrice) {

        if (CollectionUtils.isEmpty(askList)) {
            return;
        }

        if (CollectionUtils.isNotEmpty(askList) &&
                Objects.nonNull(lastPrice)) {
            askList.stream().filter(i -> i.getPriceType().equals(PriceType.PRICE_FLOATING.getType())).forEach(i -> {

                String key = i.getOrgId().toString() + "-" + i.getCurrencyId();
                Map<String, OtcBrokerCurrency> map = currencyConfig.getOrDefault(key, Maps.newHashMap());
                OtcBrokerCurrency currency = map.values().stream().findFirst().orElse(null);

                int scale = 8;
                if (Objects.nonNull(currency)) {
                    scale = currency.getScaleSafe();
                }
                BigDecimal price = i.getPremium().multiply(lastPrice.setScale(scale, BigDecimal.ROUND_DOWN))
                        .divide(new BigDecimal(100), 8, BigDecimal.ROUND_DOWN);
                i.setPrice(price);
            });
        }


        askList.forEach(item -> {
            OtcUserInfo oui = userInfoMap.get(item.getAccountId());

            if (Objects.nonNull(oui)) {
                item.setCompeteRate(oui.getCompleteRateDay30Safe());
                item.setFinishOrderNumber(oui.getOrderFinishNumberDay30Safe());
            } else {
                item.setCompeteRate(BigDecimal.ZERO);
                item.setFinishOrderNumber(Integer.valueOf(0));
            }
        });

        Map<Boolean, List<OtcItem>> group = askList.stream().collect(Collectors.groupingBy(
                i -> i.getCompeteRate().compareTo(new BigDecimal("0.98")) >= 0));

        List<OtcItem> group1 = group.getOrDefault(Boolean.TRUE, Lists.newArrayList());
        List<OtcItem> group2 = group.getOrDefault(Boolean.FALSE, Lists.newArrayList());

        //广告时间
        Comparator<OtcItem> createTime = (o1, o2) ->
                o1.getCreateDate().compareTo(o2.getCreateDate());

        //取最大法币精度配置
        int tmp_scale = 2;
        Set<Integer> scales = Sets.newHashSet();
        scales.add(tmp_scale);
        if (MapUtils.isNotEmpty(this.currencyConfig)) {
            for (Map<String, OtcBrokerCurrency> value : this.currencyConfig.values()) {
                value.forEach((k, v) -> {
                    scales.add(v.getScaleSafe());
                });
            }
        }

        final int scale = scales.stream().mapToInt(i -> i).max().getAsInt();

        //ask按照价格降序,成交率倒序排列
        try {
            group1 = group1.stream().sorted(Comparator.comparing(OtcItem::getPrice,
                    (p1, p2) -> p1.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)
                            .compareTo(p2.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)))
                    .thenComparing(createTime))
                    .collect(Collectors.toList());

            group2 = group2.stream().sorted(Comparator.comparing(OtcItem::getPrice,
                    (p1, p2) -> p1.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)
                            .compareTo(p2.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)))
                    .thenComparing(createTime))
                    .collect(Collectors.toList());

            askList.clear();
            askList.addAll(group1);
            askList.addAll(group2);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    /**
     * 广告排序算法，按照成交率98%分组，分组后的广告，按照价格和时间排序
     */
    public void sortBidList(BigDecimal lastPrice) {
        if (CollectionUtils.isEmpty(bidList)) {
            return;
        }

        if (CollectionUtils.isNotEmpty(bidList) &&
                Objects.nonNull(lastPrice)) {
            bidList.stream().filter(i -> i.getPriceType().equals(PriceType.PRICE_FLOATING.getType())).forEach(i -> {

                String key = i.getOrgId().toString() + "-" + i.getCurrencyId();
                Map<String, OtcBrokerCurrency> map = currencyConfig.getOrDefault(key, Maps.newHashMap());
                OtcBrokerCurrency currency = map.values().stream().findFirst().orElse(null);

                int scale = 8;
                if (Objects.nonNull(currency)) {
                    scale = currency.getScaleSafe();
                }
                BigDecimal price = i.getPremium().multiply(lastPrice.setScale(scale, BigDecimal.ROUND_DOWN))
                        .divide(new BigDecimal(100), 8, BigDecimal.ROUND_DOWN);
                i.setPrice(price);
            });
        }

        bidList.forEach(item -> {
            OtcUserInfo oui = userInfoMap.get(item.getAccountId());
            if (Objects.nonNull(oui)) {
                item.setCompeteRate(oui.getCompleteRateDay30Safe());
                item.setFinishOrderNumber(oui.getOrderFinishNumberDay30Safe());
            } else {
                item.setCompeteRate(BigDecimal.ZERO);
                item.setFinishOrderNumber(Integer.valueOf(0));
            }
        });

        Map<Boolean, List<OtcItem>> group = bidList.stream().collect(Collectors.groupingBy(
                i -> i.getCompeteRate().compareTo(new BigDecimal("0.98")) >= 0));

        List<OtcItem> group1 = group.getOrDefault(Boolean.TRUE, Lists.newArrayList());
        List<OtcItem> group2 = group.getOrDefault(Boolean.FALSE, Lists.newArrayList());

        //广告时间
        Comparator<OtcItem> createTime = (o1, o2) ->
                o1.getCreateDate().compareTo(o2.getCreateDate());

        //取最大法币精度配置
        int tmp_scale = 2;
        Set<Integer> scales = Sets.newHashSet();
        scales.add(tmp_scale);
        if (MapUtils.isNotEmpty(this.currencyConfig)) {
            for (Map<String, OtcBrokerCurrency> value : this.currencyConfig.values()) {
                value.forEach((k, v) -> {
                    scales.add(v.getScaleSafe());
                });
            }
        }

        final int scale = scales.stream().mapToInt(i -> i).max().getAsInt();

        //bid按照价格降序,创建时间升序排列
        try {
            group1 = group1.stream().sorted(Comparator.comparing(OtcItem::getPrice,
                    (p1, p2) -> p1.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)
                            .compareTo(p2.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)))
                    .reversed()
                    .thenComparing(createTime))
                    .collect(Collectors.toList());

            group2 = group2.stream().sorted(Comparator.comparing(OtcItem::getPrice,
                    (p1, p2) -> p1.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)
                            .compareTo(p2.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)))
                    .reversed()
                    .thenComparing(createTime))
                    .collect(Collectors.toList());

            bidList.clear();
            bidList.addAll(group1);
            bidList.addAll(group2);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    /*public void sortBidList() {
        if (CollectionUtils.isEmpty(bidList)) {
            return;
        }

        bidList.forEach(item -> {
            OtcUserInfo oui = userInfoMap.get(item.getAccountId());
            if (Objects.nonNull(oui)) {
                item.setCompeteRate(oui.getCompleteRateDay30Safe());
                item.setFinishOrderNumber(oui.getOrderFinishNumberDay30Safe());
            } else {
                item.setCompeteRate(BigDecimal.ZERO);
                item.setFinishOrderNumber(Integer.valueOf(0));
            }
        });

        //广告时间
        Comparator<OtcItem> createTime = (o1, o2) ->
                o1.getCreateDate().compareTo(o2.getCreateDate());

        //30日成单率
        Comparator<OtcItem> rateDesc = (o1, o2) ->
                o2.getCompeteRate().setScale(2, RoundingMode.DOWN).compareTo(o1.getCompeteRate().setScale(2, RoundingMode.DOWN));

        //30日成单数
        Comparator<OtcItem> finishItemDesc = (o1, o2) ->
                o2.getFinishOrderNumber().compareTo(o1.getFinishOrderNumber());

        //取最大法币精度配置
        int tmp_scale = 2;
        Set<Integer> scales = Sets.newHashSet();
        scales.add(tmp_scale);
        if (MapUtils.isNotEmpty(this.currencyConfig)) {
            for (Map<String, OtcBrokerCurrency> value : this.currencyConfig.values()) {
                value.forEach((k, v) -> {
                    scales.add(v.getScaleSafe());
                });
            }

*//*            Map<String,OtcBrokerCurrency> map=this.currencyConfig.values().stream().findFirst().orElse(Maps.newHashMap());
            tmp_scale=map.values().stream().map(i->i.getScale()).findFirst().orElse(2);*//*
        }

        final int scale = scales.stream().mapToInt(i -> i).max().getAsInt();
        //final int scale=tmp_scale;
*//*      Comparator<OtcItem> finishItemDesc=(o1,o2)->
        o2.getFinishNumSafe().compareTo(o1.getFinishNumSafe());*//*

        //bid按照价格升序,成交率降序排列
        try {
            bidList = bidList.stream().sorted(Comparator.comparing(OtcItem::getPrice,
                    (p1, p2) -> p1.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)
                            .compareTo(p2.setScale(scale > 0 ? scale : 2, RoundingMode.DOWN)))
                    .reversed()
                    .thenComparing(rateDesc)
                    .thenComparing(createTime)
                    .thenComparing(finishItemDesc))
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }*/
}