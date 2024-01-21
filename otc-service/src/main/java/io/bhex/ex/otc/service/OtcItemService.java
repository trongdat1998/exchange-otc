package io.bhex.ex.otc.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.alibaba.fastjson.JSON;
import com.github.pagehelper.PageHelper;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.quote.OtcSide;
import io.bhex.base.rc.RcBalanceResponse;
import io.bhex.base.rc.UserRequest;
import io.bhex.ex.otc.OTCGetItemsAdminRequest;
import io.bhex.ex.otc.OTCItemStatusEnum;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcBrokerCurrency;
import io.bhex.ex.otc.entity.OtcBrokerToken;
import io.bhex.ex.otc.entity.OtcItem;
import io.bhex.ex.otc.entity.OtcSymbol;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.enums.FlowStatus;
import io.bhex.ex.otc.enums.FlowType;
import io.bhex.ex.otc.enums.ItemStatus;
import io.bhex.ex.otc.enums.MsgCode;
import io.bhex.ex.otc.enums.PriceType;
import io.bhex.ex.otc.enums.Side;
import io.bhex.ex.otc.exception.BusinessException;
import io.bhex.ex.otc.exception.CurrencyConfigNotFoundException;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.exception.NicknameNotSetException;
import io.bhex.ex.otc.exception.PermissionDeniedException;
import io.bhex.ex.otc.exception.RiskControlInterceptionException;
import io.bhex.ex.otc.exception.UnFinishedItemException;
import io.bhex.ex.otc.grpc.GrpcExchangeRCService;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.mappper.OtcBalanceFlowMapper;
import io.bhex.ex.otc.mappper.OtcBrokerCurrencyMapper;
import io.bhex.ex.otc.mappper.OtcItemMapper;
import io.bhex.ex.otc.mappper.OtcSymbolMapper;
import io.bhex.ex.otc.service.config.OtcConfigService;
import io.bhex.ex.otc.service.item.OtcItemOnline;
import io.bhex.ex.otc.service.item.PriceLevel;
import io.bhex.ex.otc.service.order.OrderMsg;
import io.bhex.ex.proto.OrderSideEnum;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * 商品/广告service
 *
 * @author lizhen
 * @date 2018-09-13
 */
@Slf4j
@Service
public class OtcItemService {

    @Resource
    private OtcItemMapper otcItemMapper;

    @Resource
    private OtcSymbolMapper otcSymbolMapper;

    @Resource
    private OtcBalanceFlowMapper otcBalanceFlowMapper;

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Autowired
    private OtcPaymentTermService otcPaymentTermService;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Resource
    private OtcConfigService otcConfigService;

    private static final String ONLINE_KEY = "%s_%s_%s";

    private static final String USDT = "USDT";

    private static final String CNY = "CNY";

    //private ConcurrentHashMap<String, OtcItemOnline> onlineMap = new ConcurrentHashMap<>();

    private Set<String> symbols;

    @Resource
    private GrpcServerService grpcServerService;

    @Resource
    private GrpcExchangeRCService grpcExchangeRCService;

    @Resource
    private OtcBrokerCurrencyMapper otcBrokerCurrencyMapper;

    @PostConstruct
    public void init() {
        initSymbol();
        this.otcConfigService.setOtcItemService(this);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public long createItem(OtcItem otcItem) {
        if (!symbols.contains(getOnlineKey(otcItem.getExchangeId(), otcItem.getTokenId(), otcItem.getCurrencyId()))) {
            addSymbol(otcItem.getExchangeId(), otcItem.getTokenId(), otcItem.getCurrencyId());
        }
        // 检查是否设置过昵称
        OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfoForUpdate(otcItem.getAccountId());
        if (userInfo == null || StringUtils.isBlank(userInfo.getNickName())) {
            throw new NicknameNotSetException("create item should set nickname first");
        }

        //广告的法币是否在券商配置支持范围
        List<OtcBrokerCurrency> brokerCurrencyList = this.otcBrokerCurrencyMapper.queryBrokerCurrencyConfig(otcItem.getOrgId());
        if (CollectionUtils.isEmpty(brokerCurrencyList)) {
            throw new CurrencyConfigNotFoundException("Otc broker currency not set orgId = " + otcItem.getOrgId());
        }
        List<String> currencyList = brokerCurrencyList.stream().map(OtcBrokerCurrency::getCode).collect(Collectors.toList());
        Set<String> stringSet = new HashSet<>(currencyList);
        if (!stringSet.contains(otcItem.getCurrencyId())) {
            throw new CurrencyConfigNotFoundException("Otc broker currency not set orgId = " + otcItem.getOrgId());
        }
        if (otcItem.getSide().intValue() == Side.SELL.getCode()) {
            try {
                RcBalanceResponse rcBalanceResponse
                        = grpcExchangeRCService.getUserRcBalance(UserRequest.newBuilder().setUserId(userInfo.getUserId()).build());
                log.info("rcBalanceResponse userId {} locked {}", userInfo.getUserId(), rcBalanceResponse.getLocked());
                if (StringUtils.isEmpty(rcBalanceResponse.getLocked())) {
                    throw new RiskControlInterceptionException("RC LIMIT, userId=" + otcItem.getId() + ", locked=" + rcBalanceResponse.getLocked());
                }
                if (new BigDecimal(rcBalanceResponse.getLocked()).compareTo(BigDecimal.ZERO) < 0) {
                    throw new RiskControlInterceptionException("RC LIMIT, userId=" + otcItem.getId() + ", locked=" + rcBalanceResponse.getLocked());
                }
            } catch (RiskControlInterceptionException ex) {
                throw ex;
            } catch (Exception ex) {
                log.error("check UserRcBalance fail orgId {} userId {} error {}", otcItem.getOrgId(), userInfo.getUserId(), ex);
            }
        }

        // 检查是否有未完结的广告单
        int count = otcItemMapper.selectOtcItemUnFinishCount(otcItem.getAccountId(), otcItem.getTokenId(),
                otcItem.getCurrencyId(), otcItem.getSide());
        if (count > 0) {
            throw new UnFinishedItemException("the same side item can only be created one");
        }
        otcItem.setId(sequenceGenerator.getLong());
        otcItem.setUserId(userInfo.getUserId());
        otcItem.setRecommendLevel(0);
        otcItem.setLastQuantity(otcItem.getQuantity());
        otcItem.setFrozenQuantity(BigDecimal.ZERO);
        otcItem.setExecutedQuantity(BigDecimal.ZERO);
        // 15分钟
        otcItem.setPaymentPeriod(60 * 15);
        otcItem.setMarginAmount(BigDecimal.ZERO);
        otcItem.setOnlyHighAuth(0);
        otcItem.setOrderNum(0);
        otcItem.setFinishNum(0);
        Date now = new Date();
        otcItem.setCreateDate(now);
        otcItem.setUpdateDate(now);
        // otc广告入库
        otcItemMapper.insert(otcItem);
        if (otcItem.getId() == null || otcItem.getId() == 0) {
            throw new BusinessException("insert OtcItem failed");
        }
        if (userInfo.getStatus().equals(0)) {
            otcUserInfoService.setTradeFlag(userInfo.getAccountId());
        }
        return otcItem.getId();
    }

    public int updateItemStatusToNormal(long itemId) {
        return otcItemMapper.updateOtcItemStatus(itemId, ItemStatus.NORMAL.getStatus(),
                ItemStatus.INIT.getStatus(), new Date());
    }

    public int updateItemStatusToDelete(long itemId) {
        return otcItemMapper.updateOtcItemStatus(itemId, ItemStatus.DELETE.getStatus(),
                ItemStatus.INIT.getStatus(), new Date());
    }

    public Long getItemIdByClient(long orgId, long clientItemId) {
        return otcItemMapper.selectOtcItemIdByClient(orgId, clientItemId);
    }

    public OtcItem getOtcItemById(long itemId) {
        OtcItem item = otcItemMapper.selectByPrimaryKey(itemId);
        Set<Long> brokerIds = Sets.newHashSet(item.getOrgId());
        if (!CollectionUtils.isEmpty(brokerIds)) {
            List<OtcBrokerToken> tokenConfigs = otcConfigService.listBrokerTokenConfig(brokerIds, item.getTokenId());
            List<OtcBrokerCurrency> currencyConfigs = otcConfigService.listBrokerCurrencyConfig(brokerIds, item.getCurrencyId());

            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(tokenConfigs)) {
                item.addTokenConfig(tokenConfigs.get(0));
            }
            item.addCurrencyConfig(currencyConfigs);
        }

        return item;
    }

    public OtcItem getOtcItemWithoutConfigById(long itemId) {
        return otcItemMapper.selectByPrimaryKey(itemId);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public OtcBalanceFlow cancelItem(long accountId, long itemId, long exchangeId) {
        OtcItem otcItem = checkAndLockItem(accountId, itemId, exchangeId);
        if (!otcItem.getStatus().equals(ItemStatus.NORMAL.getStatus())) {
            throw new ItemNotExistException("item status error, itemId=" + itemId + ", status=" + otcItem.getStatus());
        }
        Date now = new Date();
        // 撤销广告单，修改状态
        otcItemMapper.updateOtcItemStatus(itemId, ItemStatus.CANCEL.getStatus(),
                ItemStatus.NORMAL.getStatus(), now);
        if (otcItem.getSide().equals(Side.BUY.getCode())) {
            return null;
        }
        BigDecimal amount
                = otcItem.getLastQuantity().add(otcItem.getFrozenFee().subtract(otcItem.getFee()));
        // 卖单广告撤单，还要生成返还账户冻结到可用的流水
        OtcBalanceFlow otcBalanceFlow = OtcBalanceFlow.builder()
                .accountId(otcItem.getAccountId())
                .orgId(otcItem.getOrgId())
                .userId(otcItem.getUserId())
                .tokenId(otcItem.getTokenId())
                .amount(amount)
                .flowType(FlowType.BACK_ITEM_FROZEN.getType())
                .objectId(otcItem.getId())
                .status(FlowStatus.WAITING_PROCESS.getStatus())
                .createDate(now)
                .updateDate(now)
                .build();
        otcBalanceFlowMapper.insert(otcBalanceFlow);
        if (otcBalanceFlow.getId() == null || otcBalanceFlow.getId() == 0) {
            throw new BusinessException("insert OtcBalanceFlow failed");
        }
        return otcBalanceFlow;
    }

    public OtcItemOnline getOtcItemOnline(long exchangeId, String tokenId, String currencyId) {

/*        String onlineItemStr=redisTemplate.opsForValue().get(getOnlineKey(exchangeId, tokenId, currencyId));
        OtcItemOnline otcItemOnline=JSON.parseObject(onlineItemStr,OtcItemOnline.class);*/

        //OtcItemOnline otcItemOnline=onlineMap.get(getOnlineKey(exchangeId, tokenId, currencyId));
        OtcItemOnline otcItemOnline = findOtcItemOnline(exchangeId, tokenId, currencyId);
        if (Objects.isNull(otcItemOnline)) {
            return null;
        }

        BigDecimal buyerPrice = findExchangeRate(exchangeId, tokenId, currencyId, OtcSide.BUY);
        BigDecimal sellPrice = findExchangeRate(exchangeId, tokenId, currencyId, OtcSide.SELL);

        otcItemOnline.sortAskList(sellPrice);
        otcItemOnline.sortBidList(buyerPrice);

        return otcItemOnline;
    }

    private BigDecimal findExchangeRate(long exchangeId, String tokenId, String currencyId, OtcSide otcSide) {
        Map<String, BigDecimal> lastPriceMap = grpcServerService.getLastPrice(exchangeId, tokenId);

        //USDT指数分买卖方向
        BigDecimal lastPrice = BigDecimal.ZERO;
        if (USDT.equalsIgnoreCase(tokenId) && CNY.equals(currencyId)) {
            //buyPrice = grpcServerService.getOTCUsdtIndex(tokenId, currencyId, OtcSide.BUY);
            //sellPrice = grpcServerService.getOTCUsdtIndex(tokenId, currencyId, OtcSide.SELL);
            lastPrice = grpcServerService.getOTCUsdtIndex(tokenId, currencyId, otcSide);
        } else {
            lastPrice = lastPriceMap.get(currencyId);
        }

        if (Objects.isNull(lastPrice)) {
            log.error("findExchangeRate is null,exchangeId={},tokenId={},currencyId={},side={}", exchangeId, tokenId, currencyId, otcSide.name());
            return null;
            /*throw new NullPointerException("findExchangeRate is null,exchangeId"+exchangeId+",tokenId="+
                    tokenId+",currencyId="+currencyId+",side="+otcSide.name());*/
        }

        return lastPrice;

    }

    public OtcItemOnline getOtcItemOnlineWithMultiExchange(List<Long> exchangeIds, String tokenId, String currencyId) {

        OtcItemOnline otcItemOnline = null;
        //排重
        List<Long> ids = exchangeIds.stream().distinct().collect(Collectors.toList());

        for (Long exchangeId : ids) {
            //OtcItemOnline tmp=onlineMap.get(getOnlineKey(exchangeId, tokenId, currencyId));
            OtcItemOnline tmp = this.findOtcItemOnline(exchangeId, tokenId, currencyId);

            if (Objects.isNull(tmp)) {
                continue;
            }

            if (Objects.isNull(otcItemOnline)) {
                otcItemOnline = tmp.deepCopy();
                continue;
            }

            //合并深度相关数据
            otcItemOnline.mergeAskList(tmp.getAskList());
            //otcItemOnline.appendAsks(tmp.getAsks());
            otcItemOnline.mergeBidList(tmp.getBidList());
            //otcItemOnline.appendBids(tmp.getBids());
            otcItemOnline.mergeUserInfo(tmp.getUserInfoMap());
            otcItemOnline.mergePaymentTerm(tmp.getPaymentTermMap());
            //otcItemOnline.updateLastPrice(tmp.getLastPrice(), tmp.getTime());
            otcItemOnline.mergeTokenConfig(tmp.getTokenConfig());
            otcItemOnline.mergeCurrencyConfig(tmp.getCurrencyConfig());
        }

        if (Objects.isNull(otcItemOnline)) {
            return otcItemOnline;
        }

        BigDecimal buyerPrice = findExchangeRate(exchangeIds.get(0), tokenId, currencyId, OtcSide.BUY);
        BigDecimal sellPrice = findExchangeRate(exchangeIds.get(0), tokenId, currencyId, OtcSide.SELL);

        otcItemOnline.sortAskList(sellPrice);
        otcItemOnline.sortBidList(buyerPrice);

        return otcItemOnline;
    }

    public OtcItemOnline findOtcItemOnline(long exchangeId, String tokenId, String currencyId) {

        String onlineItemStr = redisTemplate.opsForValue().get(getOnlineKey(exchangeId, tokenId, currencyId));
        if (StringUtils.isBlank(onlineItemStr)) {
            return null;
        }
        OtcItemOnline otcItemOnline = JSON.parseObject(onlineItemStr, OtcItemOnline.class);

        return otcItemOnline;
    }

    public List<OtcItem> getOtcItemList(long accountId, List<Integer> status, Date beginTime, Date endTime,
                                        String tokenId, Integer side, int page, int size) {
        Example example = Example.builder(OtcItem.class)
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria = example.createCriteria().andEqualTo("accountId", accountId);

        if (StringUtils.isNotBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        if (!CollectionUtils.isEmpty(status)) {
            criteria.andIn("status", status);
        }
        if (side != null) {
            criteria.andEqualTo("side", side);
        }
        if (beginTime != null) {
            criteria.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria.andLessThanOrEqualTo("createDate", endTime);
        }

        //PageHelper.startPage(page,size+1);
        //return otcItemMapper.selectByExample(example);
        //todo 需要专门优化
        return otcItemMapper.selectByExampleAndRowBounds(example, new RowBounds((page - 1) * size, size + 1));
    }

    public List<OtcItem> getItemsAdmin(OTCGetItemsAdminRequest request) {
        Example example = Example.builder(OtcItem.class)
                .orderByDesc("id")
                .build();
        PageHelper.startPage(0, request.getSize());
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orgId", request.getOrgId());
        if (StringUtils.isNotBlank(request.getTokenId())) {
            criteria.andEqualTo("tokenId", request.getTokenId());
        }
        if (!CollectionUtils.isEmpty(request.getStatusList())) {
            criteria.andIn("status", request.getStatusValueList());
        }
        if (!CollectionUtils.isEmpty(request.getSideList())) {
            criteria.andIn("side", request.getSideValueList());
        }
        if (request.getBeginTime() > 0) {
            criteria.andGreaterThanOrEqualTo("createDate", request.getBeginTime());
        }
        if (request.getEndTime() > 0) {
            criteria.andLessThanOrEqualTo("createDate", request.getEndTime());
        }
        if (request.getAccountId() > 0) {
            criteria.andEqualTo("accountId", request.getAccountId());
        }
        List<OtcItem> items = otcItemMapper.selectByExample(example);
        if (CollectionUtils.isEmpty(items)) {
            return Lists.newArrayList();
        }

        return items;
    }

    public List<OtcItem> getSortedOnlineItemList(long exchangeId, String tokenId, String currencyId, Side side,
                                                 BigDecimal lastPrice) {
        Example example = Example.builder(OtcItem.class).build();
        example.createCriteria()
                .andEqualTo("exchangeId", exchangeId)
                .andEqualTo("tokenId", tokenId)
                .andEqualTo("currencyId", currencyId)
                .andEqualTo("side", side.getCode())
                .andEqualTo("status", ItemStatus.NORMAL.getStatus())
                .andGreaterThan("lastQuantity", BigDecimal.ZERO);
        List<OtcItem> itemList = otcItemMapper.selectByExample(example);
        List<OtcItem> valuationList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(itemList)) {
            return valuationList;
        }
        // 计算浮动价格广告单的实时价格
        itemList.forEach(otcItem -> {
            if (otcItem.getPriceType().equals(PriceType.PRICE_FLOATING.getType())) {
                if (lastPrice == null) {
                    return;
                }
                BigDecimal price = otcItem.getPremium().multiply(lastPrice)
                        .divide(new BigDecimal(100), 8, BigDecimal.ROUND_DOWN);
                otcItem.setPrice(price);
            }
            valuationList.add(otcItem);
        });
        // 排序，卖单按价格升序，买单按价格降序
/*        Collections.sort(valuationList, (o1, o2) -> side == Side.SELL ? o1.getPrice().compareTo(o2.getPrice())
                : o2.getPrice().compareTo(o1.getPrice()));*/
        return valuationList;
    }

    public void refreshOnlineList(long exchangeId, String tokenId, String currencyId, BigDecimal lastPrice, BigDecimal buyPrice, BigDecimal sellPrice) {
        String onlineKey = getOnlineKey(exchangeId, tokenId, currencyId);
        OtcItemOnline itemOnline = OtcItemOnline.builder()
                .tokenId(tokenId)
                .currencyId(currencyId)
                .time(System.currentTimeMillis())
                //.lastPrice(lastPrice)
                .build();

        Set<Long> brokerIds = Sets.newHashSet();

        List<OtcItem> bidList;
        List<OtcItem> askList;
        if ("USDT".equalsIgnoreCase(tokenId) && "CNY".equals(currencyId)) {
            itemOnline.setBuyPrice(buyPrice);
            itemOnline.setSellPrice(sellPrice);

            bidList = getSortedOnlineItemList(exchangeId, tokenId, currencyId, Side.BUY, buyPrice);
            askList = getSortedOnlineItemList(exchangeId, tokenId, currencyId, Side.SELL, sellPrice);
        } else {
            itemOnline.setBuyPrice(lastPrice);
            itemOnline.setSellPrice(lastPrice);

            bidList = getSortedOnlineItemList(exchangeId, tokenId, currencyId, Side.BUY, lastPrice);
            askList = getSortedOnlineItemList(exchangeId, tokenId, currencyId, Side.SELL, lastPrice);
        }

        itemOnline.setBidList(bidList);
        itemOnline.setAskList(askList);
        itemOnline.cacheBids(buildOrderBook(bidList));
        itemOnline.cacheAsks(buildOrderBook(askList));

        List<Long> accountIdList = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(bidList)) {
            accountIdList.addAll(bidList.stream().map(OtcItem::getAccountId).collect(Collectors.toList()));
        }
        if (!CollectionUtils.isEmpty(askList)) {
            accountIdList.addAll(askList.stream().map(OtcItem::getAccountId).collect(Collectors.toList()));
        }
        if (!CollectionUtils.isEmpty(accountIdList)) {
            Map<Long, List<Integer>> paymentTermMap = otcPaymentTermService.getUserPaymentTerm(accountIdList);
            Map<Long, OtcUserInfo> userInfoMap = otcUserInfoService.getOtcUserInfoMap(accountIdList);
            itemOnline.setUserInfoMap(userInfoMap);
            itemOnline.setPaymentTermMap(paymentTermMap);
        }

        //增加token及货币的配置信息
        if (!CollectionUtils.isEmpty(bidList)) {
            Set<Long> ids1 = bidList.stream().map(i -> i.getOrgId()).collect(Collectors.toSet());
            brokerIds.addAll(ids1);
        }

        if (!CollectionUtils.isEmpty(askList)) {
            Set<Long> ids2 = askList.stream().map(i -> i.getOrgId()).collect(Collectors.toSet());
            brokerIds.addAll(ids2);
        }

        if (!CollectionUtils.isEmpty(brokerIds)) {
            List<OtcBrokerToken> tokenConfigs = otcConfigService.listBrokerTokenConfig(brokerIds, tokenId);
            List<OtcBrokerCurrency> currencyConfigs = otcConfigService.listBrokerCurrencyConfig(brokerIds, currencyId);

            itemOnline.addTokenConfig(tokenConfigs);
            itemOnline.addCurrencyConfig(currencyConfigs);
        }

        //onlineMap.put(onlineKey, itemOnline);
        log.debug("refreshOnlineList,cache items,onlineKey={}", onlineKey);
        redisTemplate.opsForValue().set(onlineKey, JSON.toJSONString(itemOnline));
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public void offlineItem(long accountId, long itemId, long exchangeId) {
        OtcItem otcItem = checkAndLockItem(accountId, itemId, exchangeId);
        // 只能下架交易中广告
        if (!otcItem.getStatus().equals(ItemStatus.NORMAL.getStatus())) {
            throw new ItemNotExistException("item status error, itemId=" + itemId + ", status=" + otcItem.getStatus());
        }
        Date now = new Date();
        // 下架广告单，修改状态
        otcItemMapper.updateOtcItemStatus(itemId, ItemStatus.OFFLINE.getStatus(),
                ItemStatus.NORMAL.getStatus(), now);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public void onlineItem(long accountId, long itemId, long exchangeId) {
        OtcItem otcItem = checkAndLockItem(accountId, itemId, exchangeId);
        // 只能上架已下架广告
        if (!otcItem.getStatus().equals(ItemStatus.OFFLINE.getStatus())) {
            throw new ItemNotExistException("item status error, itemId=" + itemId + ", status=" + otcItem.getStatus());
        }
        Date now = new Date();
        // 上架广告单，修改状态
        otcItemMapper.updateOtcItemStatus(itemId, ItemStatus.NORMAL.getStatus(), ItemStatus.OFFLINE.getStatus(), now);
    }

    public void initSymbol() {
        OtcSymbol example = OtcSymbol.builder().status(OtcSymbol.SYMBOL_STATUS_ON).build();
        List<OtcSymbol> otcSymbolList = otcSymbolMapper.select(example);
        if (CollectionUtils.isEmpty(otcSymbolList)) {
            return;
        }
        symbols = Sets.newHashSet();
        otcSymbolList.forEach(otcSymbol ->
                symbols.add(getOnlineKey(otcSymbol.getExchangeId(), otcSymbol.getTokenId(), otcSymbol.getCurrencyId()))
        );
    }

    public synchronized void addSymbol(long exchangeId, String tokenId, String currencyId) {
        String onlineKey = getOnlineKey(exchangeId, tokenId, currencyId);
        if (symbols.contains(onlineKey)) {
            return;
        }

        Date now = new Date();
        OtcSymbol otcSymbol = OtcSymbol.builder()
                .exchangeId(exchangeId)
                .tokenId(tokenId)
                .currencyId(currencyId)
                .status(OtcSymbol.SYMBOL_STATUS_ON)
                .createDate(now)
                .updateDate(now)
                .build();
        try {
            otcSymbolMapper.insert(otcSymbol);
        } catch (Exception e) {
            log.error("insert symbol failed, symbol: {}", onlineKey, e);
        }
        symbols.add(onlineKey);
    }

    public List<OtcSymbol> getAllSymbol() {
        OtcSymbol example = OtcSymbol.builder().status(OtcSymbol.SYMBOL_STATUS_ON).build();
        return otcSymbolMapper.select(example);
    }

    private OtcItem checkAndLockItem(long accountId, long itemId, long exchangeId) {
        OtcItem otcItem = otcItemMapper.selectOtcItemForUpdate(itemId);
        // 广告不存在，或者状态为不在交易状态（只有正常在线的广告才能被撤销）
        if (otcItem == null) {
            throw new ItemNotExistException("item not exist, itemId=" + itemId);
        }
        // 不能操作非本交易所的单
        //if (!otcItem.getExchangeId().equals(exchangeId)) {
        //    throw new ItemNotExistException("item exchangeId error, itemId=" + itemId + ", exchangeId=" + exchangeId);
        //}
        // 不是本人的单不能被操作
        if (!otcItem.getAccountId().equals(accountId)) {
            throw new PermissionDeniedException("account(" + accountId + ") not item (" + itemId + ") owner");
        }
        return otcItem;
    }

    private List<PriceLevel> buildOrderBook(List<OtcItem> itemList) {
/*        List<PriceLevel> orderBook = Lists.newArrayList();
        if (CollectionUtils.isEmpty(itemList)) {
            return orderBook;
        }
        itemList.forEach(otcItem -> {
            if (orderBook.size() == 0 || !orderBook.get(orderBook.size() - 1).getPriceStr().equals(otcItem.getPriceStr())) {
                PriceLevel priceLevel = PriceLevel.builder()
                        .price(otcItem.getPrice())
                        .quantity(otcItem.getLastQuantity())
                        .size(1).build();
                orderBook.add(priceLevel);
            } else {
                PriceLevel priceLevel = orderBook.get(orderBook.size() - 1);
                priceLevel.setQuantity(priceLevel.getQuantity().add(otcItem.getLastQuantity()));
                priceLevel.setSize(priceLevel.getSize() + 1);
            }
        });
        return orderBook;*/

        Map<String, PriceLevel> orderBook = Maps.newHashMap();
        if (CollectionUtils.isEmpty(itemList)) {
            return Lists.newArrayList();
        }
        itemList.forEach(otcItem -> {
            String key = otcItem.getPriceStr();
            PriceLevel pl = orderBook.get(key);
            if (Objects.nonNull(pl)) {
                pl.setQuantity(pl.getQuantity().add(otcItem.getLastQuantity()));
                pl.setSize(pl.getSize() + 1);
            } else {
                PriceLevel priceLevel = PriceLevel.builder()
                        .price(otcItem.getPrice())
                        .quantity(otcItem.getLastQuantity())
                        .size(1).build();

                orderBook.put(otcItem.getPriceStr(), priceLevel);
            }

        });
        return Lists.newArrayList(orderBook.values());
    }

    public String getOnlineKey(long exchangeId, String tokenId, String currencyId) {
        return String.format(ONLINE_KEY, exchangeId, tokenId, currencyId);
    }

    /**
     * 创建系统消息
     */
    public void createSysMessage(OtcItem item, MsgCode msgCode) {

        OtcUserInfo buyerInfo = otcUserInfoService.getOtcUserInfo(item.getAccountId());

        try {
            OrderMsg buyerMsg = OrderMsg.builder()
                    .msgCode(msgCode.getCode())
                    .orgId(item.getOrgId())
                    .userId(item.getUserId())
                    .buyer(buyerInfo.getNickName())
                    .seller("0")
                    .tokenId(item.getTokenId())
                    .currencyId(item.getCurrencyId())
                    .quantity(item.getLastQuantity().stripTrailingZeros().toPlainString())
                    .amount("0")
                    .side(OrderSideEnum.BUY_VALUE)
                    .build();
            redisTemplate.opsForList().leftPush(String.format(OtcOrderService.OTC_ORDER_ORG_MSG_KEY, buyerInfo.getOrgId()),
                    JSON.toJSONString(buyerMsg));

        } catch (Exception e) {
            log.error("create item msg failed, msgCode:{} item={}", msgCode, JSON.toJSONString(item), e);
        }
    }


    public List<OtcItem> findItemsByIds(List<Long> exchangeIdList, List<Long> itemIdList, List<Long> orgIdList) {

        Example exp = new Example(OtcItem.class);
        exp.createCriteria().andIn("exchangeId", exchangeIdList)
                .andIn("orgId", orgIdList)
                .andIn("id", itemIdList);

        return otcItemMapper.selectByExample(exp);
    }

    public List<OtcItem> listUnFinishItem(Long orgId, String tokenId, String currencyId) {
        Example exp = new Example(OtcItem.class);
        Example.Criteria criteria = exp.createCriteria().andEqualTo("orgId", orgId)
                .andIn("status", Lists.newArrayList(OTCItemStatusEnum.OTC_ITEM_NORMAL.getNumber()
                        , OTCItemStatusEnum.OTC_ITEM_INIT.getNumber()));
        if (StringUtils.isNoneBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        if (StringUtils.isNoneBlank(currencyId)) {
            criteria.andEqualTo("currencyId", currencyId);
        }

        return otcItemMapper.selectByExample(exp);
    }

/*    public OtcItemOnline getOnlineItem(String key){
        return onlineMap.get(key);
    }*/


}