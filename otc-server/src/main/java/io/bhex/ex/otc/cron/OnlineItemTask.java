package io.bhex.ex.otc.cron;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.alibaba.fastjson.JSON;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import io.bhex.base.quote.OtcSide;
import io.bhex.ex.otc.dto.RefreshItemEvent;
import io.bhex.ex.otc.dto.SymbolDto;
import io.bhex.ex.otc.entity.OtcBrokerToken;
import io.bhex.ex.otc.entity.OtcItem;
import io.bhex.ex.otc.entity.OtcSymbol;
import io.bhex.ex.otc.enums.MsgCode;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.service.OtcItemService;
import io.bhex.ex.otc.service.item.OtcItemOnline;
import io.bhex.ex.otc.util.LockUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 广告列表定时任务
 *
 * @author lizhen
 * @date 2018-11-09
 */
@Slf4j
@Component
public class OnlineItemTask {

    @Autowired
    private OtcItemService otcItemService;

    @Autowired
    private GrpcServerService grpcServerService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private Set<String> symbolSet = Sets.newConcurrentHashSet();

    private final static String REFRESH_ITEM_PREFIX = "refresh_item_%s_%s_%s";

    public static final String OTC_ITEM_AUTO_OFFLINE_SMALL_ITEM = "OTC_ITEM_AUTO_OFFLINE_SMALL_ITEM:%s";

    private ScheduledExecutorService itemRefreshExecutor = Executors.newScheduledThreadPool(1);

    @PostConstruct
    public void init() {
        this.itemSchedule();
    }


    /**
     * 自动下架额度较小的广告 5秒执行一次
     */
    @Scheduled(fixedRate = 5 * 1000)
    public void autoOfflineSmallItem() {
        List<OtcSymbol> symbolList = otcItemService.getAllSymbol();
        for (OtcSymbol symbol : symbolList) {

            OtcItemOnline itemOnline = otcItemService.getOtcItemOnline(symbol.getExchangeId(), symbol.getTokenId(), symbol.getCurrencyId());
            if (itemOnline == null) {
                continue;
            }

            List<OtcItem> itemList = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(itemOnline.getAskList())) {
                itemList.addAll(itemOnline.getAskList());
            }
            if (!CollectionUtils.isEmpty(itemOnline.getBidList())) {
                itemList.addAll(itemOnline.getBidList());
            }

            if (CollectionUtils.isEmpty(itemList)) {
                continue;
            }

            for (OtcItem item : itemList) {

                boolean offline = false;
                OtcBrokerToken tokenConfig = item.getTokenConfig();
                BigDecimal available = item.getLastQuantity().add(item.getFrozenQuantitySafe());
                if (Objects.nonNull(tokenConfig)) {
                    offline = available.compareTo(tokenConfig.getMinQuote()) < 0;
                }

                if (!offline) {
                    // 剩余数量 * 价格  如果大于最小值， 则忽略， 如果小于最小值，则自动下架
                    BigDecimal lastAmount = available.multiply(item.getPrice()).setScale(8, RoundingMode.DOWN);
                    if (lastAmount.compareTo(item.getMinAmount()) >= 0) {
                        continue;
                    }
                }

                try {
                    String lockKey = String.format(OTC_ITEM_AUTO_OFFLINE_SMALL_ITEM, item.getId());
//                    LockUtil.getLockOnce(redisTemplate, lockKey, 5);
                    Boolean locked = LockUtil.tryLock(redisTemplate, lockKey, 5 * 1000);
                    if (!locked) {
                        continue;
                    }

                    // 自动下架
                    otcItemService.cancelItem(item.getAccountId(), item.getId(), item.getExchangeId());

                    // 发送消息
                    otcItemService.createSysMessage(item, MsgCode.ITEM_AUTO_OFFLINE_SMALL_QUANTITY);
                    //刷新广告列表
                    refreshItem(item.getExchangeId(), item.getTokenId(), item.getCurrencyId());
                    log.info(" auto offline small item :{} ", JSON.toJSON(item));
                } catch (ItemNotExistException e) {
                    log.warn("auto offline small item fail", e);
                } catch (Exception e) {
                    log.error(" auto offline small item exception:{}", JSON.toJSONString(item), e);
                }

            }

        }
    }


    /**
     * 每隔60s刷新一次币对列表，决定是否重启任务
     */
/*    @Scheduled(fixedRate = 60 * 1000)
    public void itemSchedule() {
        List<OtcSymbol> symbolList = otcItemService.getAllSymbol();
        if (!isChange(symbolList)) {
            return;
        }

        // 当币对有更新，重启任务
        itemRefreshExecutor.shutdownNow();
        try {
            //等待上个线程池任务完成
            Thread.sleep(2000);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        if (itemRefreshExecutor.isTerminated()) {
            itemRefreshExecutor = Executors.newScheduledThreadPool(symbolList.size(),
                    new ThreadFactoryBuilder().setNameFormat("itemrefresh-pool-%d")
                            .setDaemon(false).build());
            startTask(symbolList);
            symbolSet = Sets.newHashSet();
            symbolSet.addAll(symbolList.stream().map(symbol ->
                    otcItemService.getOnlineKey(symbol.getExchangeId(), symbol.getTokenId(), symbol.getCurrencyId())
            ).collect(Collectors.toList()));
        }

    }*/


    //发送币到redis消息队列
    @Scheduled(cron = "5/10 * * * * ?")
    public void sendRefreshItemMessage() {
        String key = "OTC::REFRESH::ITEM::LOCK";
        Boolean locked = LockUtil.tryLock(redisTemplate, key, 10 * 1000);
        if (!locked) {
            return;
        }

        try {
            symbolSet.forEach(symbol -> {
                List<String> list = Splitter.on("_").splitToList(symbol);
                SymbolDto dto = new SymbolDto();
                dto.setExchangeId(Long.valueOf(list.get(0)));
                dto.setTokenId(list.get(1));
                dto.setCurrencyId(list.get(2));
                redisTemplate.convertAndSend("/redis/otc-item/refreshItem", JSON.toJSONString(dto));
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            redisTemplate.delete(key);
        }
    }

    @Scheduled(fixedRate = 60 * 1000)
    public void itemSchedule() {
        List<OtcSymbol> symbolList = otcItemService.getAllSymbol();
        if (!isChange(symbolList)) {
            return;
        }

        symbolSet.addAll(symbolList.stream().map(symbol ->
                otcItemService.getOnlineKey(symbol.getExchangeId(), symbol.getTokenId(), symbol.getCurrencyId())
        ).collect(Collectors.toList()));

        //币对变化，发送更新缓存消息
        sendRefreshItemMessage();
    }


/*    @Scheduled(fixedRate = 60 * 1000)
    public void itemSchedule() {
        List<OtcSymbol> symbolList = otcItemService.getAllSymbol();
        if (!isChange(symbolList)) {
            return;
        }

        symbolSet.addAll(symbolList.stream().map(symbol ->
                otcItemService.getOnlineKey(symbol.getExchangeId(), symbol.getTokenId(), symbol.getCurrencyId())
        ).collect(Collectors.toList()));

        // 当币对有更新，重启任务
        itemRefreshExecutor.shutdown();
        if (itemRefreshExecutor.isTerminated()) {
            itemRefreshExecutor = Executors.newScheduledThreadPool(symbolList.size());
            startTask(symbolList);
        }

    }*/

/*    private void startTask(List<OtcSymbol> symbolList) {
        symbolList.forEach(otcSymbol -> itemRefreshExecutor.scheduleWithFixedDelay(() -> {
            // 调行情获取最新价格
            Map<String, BigDecimal> lastPriceMap = grpcServerService.getLastPrice(otcSymbol.getExchangeId(), otcSymbol.getTokenId());
            // 刷新广告列表
            String onlineKey = otcItemService.getOnlineKey(otcSymbol.getExchangeId(), otcSymbol.getTokenId(),
                    otcSymbol.getCurrencyId());

            //USDT指数分买卖方向
            BigDecimal buyPrice = BigDecimal.ZERO;
            BigDecimal sellPrice = BigDecimal.ZERO;
            if ("USDT".equalsIgnoreCase(otcSymbol.getTokenId()) && "CNY".equals(otcSymbol.getCurrencyId())) {
                buyPrice = grpcServerService.getOTCUsdtIndex(otcSymbol.getTokenId(), otcSymbol.getCurrencyId(), OtcSide.BUY);
                sellPrice = grpcServerService.getOTCUsdtIndex(otcSymbol.getTokenId(), otcSymbol.getCurrencyId(), OtcSide.SELL);
                log.debug("buy price={}", buyPrice.stripTrailingZeros().toPlainString());
                log.debug("sell price={}", sellPrice.stripTrailingZeros().toPlainString());
            }

            BigDecimal lastPrice = lastPriceMap.get(otcSymbol.getCurrencyId());
            if (lastPrice == null || lastPrice.compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("get {} lastPrice failed", onlineKey);
                lastPrice = null;
                return;
            }
            try {
                otcItemService.refreshOnlineList(otcSymbol.getExchangeId(), otcSymbol.getTokenId(),
                        otcSymbol.getCurrencyId(), lastPrice, buyPrice, sellPrice);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                log.error("refresh {} failed ", onlineKey, e);
            }
        }, 0, 1, TimeUnit.SECONDS));
    }*/


/*    private void startTask(List<OtcSymbol> symbolList) {
        symbolList.forEach(otcSymbol -> itemRefreshExecutor.scheduleWithFixedDelay(
                ()->refreshItem(
                        otcSymbol.getExchangeId(),
                        otcSymbol.getTokenId(),
                        otcSymbol.getCurrencyId()),
                0, 5, TimeUnit.SECONDS));
    }*/

    public void refreshItem(Long exchangeId, String tokenId, String currencyId) {

        String key = String.format(REFRESH_ITEM_PREFIX, exchangeId.toString(), tokenId, currencyId);
        boolean holdLock = LockUtil.getLockOnce(redisTemplate, key, 3);
        //未拿到锁不执行刷新
        if (!holdLock) {
            return;
        }
        Stopwatch sw = Stopwatch.createStarted();
        // 刷新广告列表
        String onlineKey = otcItemService.getOnlineKey(exchangeId, tokenId, currencyId);
        try {
            // 调行情获取最新价格
            Map<String, BigDecimal> lastPriceMap = grpcServerService.getLastPrice(exchangeId, tokenId);

            //USDT指数分买卖方向
            BigDecimal buyPrice = BigDecimal.ZERO;
            BigDecimal sellPrice = BigDecimal.ZERO;
            if ("USDT".equalsIgnoreCase(tokenId) && "CNY".equals(currencyId)) {
                buyPrice = grpcServerService.getOTCUsdtIndex(tokenId, currencyId, OtcSide.BUY);
                sellPrice = grpcServerService.getOTCUsdtIndex(tokenId, currencyId, OtcSide.SELL);
                log.debug("buy price={}", buyPrice.stripTrailingZeros().toPlainString());
                log.debug("sell price={}", sellPrice.stripTrailingZeros().toPlainString());
            }

            BigDecimal lastPrice = lastPriceMap.get(currencyId);
            if (lastPrice == null || lastPrice.compareTo(BigDecimal.ZERO) <= 0) {
                log.warn("get {} lastPrice failed", onlineKey);
                lastPrice = null;
                return;
            }
            otcItemService.refreshOnlineList(exchangeId, tokenId,
                    currencyId, lastPrice, buyPrice, sellPrice);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("refresh {} failed ", onlineKey, e);
        } finally {
            sw.stop();
            redisTemplate.delete(key);
        }

    }

    @Async
    @EventListener
    public void refreshItemListener(RefreshItemEvent event) {
        try {
            log.info("receive refresh item message");
            refreshItem(event.getExchangeId(), event.getTokenId(), event.getCurrencyId());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean isChange(List<OtcSymbol> symbolList) {
        int curSize = 0;
        if (!CollectionUtils.isEmpty(symbolList)) {
            curSize = symbolList.size();
        }
        if (curSize != symbolSet.size()) {
            return true;
        }
        if (curSize == 0) {
            return false;
        }
        for (OtcSymbol symbol : symbolList) {
            if (!symbolSet.contains(otcItemService.getOnlineKey(symbol.getExchangeId(), symbol.getTokenId(),
                    symbol.getCurrencyId()))) {
                return true;
            }
        }
        return false;
    }
}