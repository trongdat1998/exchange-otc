package io.bhex.ex.otc.service.config;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.bhex.ex.otc.entity.*;
import io.bhex.ex.otc.exception.BusinessException;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.exception.NotAllowShareTokenException;
import io.bhex.ex.otc.exception.UnFinishedItemException;
import io.bhex.ex.otc.mappper.*;
import io.bhex.ex.otc.service.OtcItemService;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.service.OtcTradeFeeService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Slf4j
@Service
public class OtcConfigService {

    @Resource
    private OtcBrokerSymbolMapper otcBrokerSymbolMapper;

    @Resource
    private OtcSymbolShareMapper otcSymbolShareMapper;

    @Resource
    private OtcBrokerCurrencyMapper otcBrokerCurrencyMapper;

    @Resource
    private OtcBrokerTokenMapper otcBrokerTokenMapper;

    @Resource
    private OtcBankMapper otcBankMapper;

    @Resource
    private BrokerExtMapper brokerExtMapper;

    @Resource
    private OtcDepthShareBrokerWhiteListMapper otcDepthShareBrokerWhiteListMapper;

    @Resource
    private OtcDepthShareExchangeMapper otcDepthShareExchangeMapper;

    @Resource
    private OtcTradeFeeService otcTradeFeeService;

    private static ImmutableMap<Long, BrokerExt> brokerExtImmutableMap = ImmutableMap.of();

    private static ImmutableMap<Long, OtcDepthShareBrokerWhiteList> shareBrokerWhiteListImmutableMap = ImmutableMap.of();

    //不能自动注入，会形成相互依赖
    private OtcOrderService otcOrderService;
    //不能自动注入，会形成相互依赖
    private OtcItemService otcItemService;

    public void setOtcItemService(OtcItemService ois) {
        this.otcItemService = ois;
    }

    public void setOtcOrderService(OtcOrderService oos) {
        this.otcOrderService = oos;
    }

    @PostConstruct
    public void brokerOtcConfigCache() {
        List<BrokerExt> brokerExtList = brokerExtMapper.selectAll();
        List<OtcDepthShareBrokerWhiteList> brokerWhiteList = otcDepthShareBrokerWhiteListMapper.selectAll();
        brokerExtImmutableMap = ImmutableMap.copyOf(brokerExtList.stream().collect(Collectors.toMap(BrokerExt::getBrokerId, broker -> broker)));
        shareBrokerWhiteListImmutableMap = ImmutableMap.copyOf(brokerWhiteList.stream().collect(Collectors.toMap(OtcDepthShareBrokerWhiteList::getBrokerId, broker -> broker)));
    }

    @Scheduled(cron = "0 6/10 * * * ?")
    public void brokerOtcConfigTask() throws Exception {
        try {
            List<BrokerExt> brokerExtList = brokerExtMapper.selectAll();
            List<OtcDepthShareBrokerWhiteList> brokerWhiteList = otcDepthShareBrokerWhiteListMapper.selectAll();
            brokerExtImmutableMap = ImmutableMap.copyOf(brokerExtList.stream().collect(Collectors.toMap(BrokerExt::getBrokerId, broker -> broker)));
            shareBrokerWhiteListImmutableMap = ImmutableMap.copyOf(brokerWhiteList.stream().collect(Collectors.toMap(OtcDepthShareBrokerWhiteList::getBrokerId, broker -> broker)));
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public BrokerExt getBrokerExtFromCache(Long brokerId) {
        return brokerExtImmutableMap.get(brokerId);
    }

    public OtcDepthShareBrokerWhiteList getOtcDepthShareBrokerWhiteListFromCache(Long brokerId) {
        return shareBrokerWhiteListImmutableMap.get(brokerId);
    }

    public List<OtcBrokerToken> getOtcTokenList(Long orgId) {
        if (orgId != null && orgId > 0) {
            List<OtcBrokerToken> list = otcBrokerTokenMapper.queryTokenListByOrgId(orgId);

            List<OtcTradeFeeRate> feeRates = otcTradeFeeService.queryOtcTradeFeeByOrgId(orgId);
            Map<String, OtcTradeFeeRate> map = feeRates.stream().collect(Collectors.toMap(i -> i.getTokenId(), i -> i, (n, o) -> o));
            return list.stream().map(i -> {
                OtcTradeFeeRate fr = map.get(i.getTokenId());
                if (Objects.nonNull(fr)) {
                    i.setFeeRate(OtcBrokerToken.FeeRate.builder()
                            .buyRate(fr.getMakerBuyFeeRate())
                            .sellRate(fr.getMakerSellFeeRate())
                            .build());
                }

                return i;
            }).collect(Collectors.toList());
        }

        List<OtcBrokerToken> list = otcBrokerTokenMapper.queryAllTokenList();
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }

        //不在白名单中的券商不可共享币对
        OtcDepthShareBrokerWhiteList whiteList = otcDepthShareBrokerWhiteListMapper.selectByPrimaryKey(orgId);
        if (Objects.isNull(whiteList)) {
            list = list.stream().map(i -> {
                i.setShareStatus(-1);
                return i;
            }).collect(Collectors.toList());
        }

        return list;

    }

    public List<OtcBrokerCurrency> getOtcCurrencyList(Long orgId) {
        OtcBrokerCurrency example = OtcBrokerCurrency.builder()
                .status(OtcBrokerCurrency.AVAILABLE)
                .build();
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        return otcBrokerCurrencyMapper.select(example);
    }

    public List<OtcBrokerCurrency> listAllCurrencyByOrgId(Long orgId) {
        OtcBrokerCurrency example = OtcBrokerCurrency.builder()
                .orgId(orgId)
                .build();
        return otcBrokerCurrencyMapper.select(example);
    }

    public List<OtcBrokerCurrency> getOtcCurrencyList(@NonNull Long orgId, @NonNull String currencyId, @NonNull String language) {
        OtcBrokerCurrency obj = OtcBrokerCurrency.builder()
                .status(OtcBrokerCurrency.AVAILABLE)
                .language(language)
                .code(currencyId)
                .orgId(orgId)
                .build();

        return otcBrokerCurrencyMapper.select(obj);
    }


    @Transactional(rollbackFor = Throwable.class)
    public void saveBrokerCurrency(OtcBrokerCurrency otcCurrency) {
        OtcBrokerCurrency existCurrency = otcBrokerCurrencyMapper.queryOtcCurrency(
                otcCurrency.getCode(),
                otcCurrency.getLanguage(),
                otcCurrency.getOrgId());

        boolean success = true;
        if (existCurrency == null) {
            otcCurrency.setCreateDate(new Date());
            otcCurrency.setUpdateDate(new Date());
            success = otcBrokerCurrencyMapper.insertSelective(otcCurrency) == 1;
        } else {
            otcCurrency.setUpdateDate(new Date());
            otcCurrency.setId(existCurrency.getId());
            success = otcBrokerCurrencyMapper.updateByPrimaryKeySelective(otcCurrency) == 1;
        }

        if (!success) {
            throw new IllegalStateException("Save currency fail");
        }

        Example tokenExp = new Example(OtcBrokerToken.class);
        tokenExp.createCriteria()
                .andEqualTo("orgId", otcCurrency.getOrgId());

        List<OtcBrokerToken> tokens = otcBrokerTokenMapper.selectByExample(tokenExp);
        if (CollectionUtils.isEmpty(tokens)) {
            throw new IllegalStateException("Hasn't any token");
        }

        //查询币对
        Example symbolExp = new Example(OtcBrokerSymbol.class);
        symbolExp.createCriteria()
                .andEqualTo("orgId", otcCurrency.getOrgId());
        ;

        List<OtcBrokerSymbol> symbols = otcBrokerSymbolMapper.selectByExample(symbolExp);
        if (CollectionUtils.isEmpty(symbols)) {
            throw new IllegalStateException("Hasn't any symbol");
        }

        Set<Long> exchangeIds = symbols.stream().map(i -> i.getExchangeId()).collect(Collectors.toSet());
        if (exchangeIds.size() != 1) {
            throw new IllegalStateException("Symbol's exchangeId count is more than 1");
        }
        Long exchangeId = Lists.newArrayList(exchangeIds).get(0);
        Map<String, OtcBrokerSymbol> symbolMap = symbols.stream().collect(Collectors.toMap(i -> i.getTokenId() + "-" + i.getCurrencyId(), i -> i));

        //构建需要增加的币对
        List<OtcBrokerSymbol> newSymbols = tokens.stream().map(i -> {
            String tokenId = i.getTokenId();
            String key = tokenId + "-" + otcCurrency.getCode();
            if (symbolMap.containsKey(key)) {
                return null;
            }

            int status = otcCurrency.getStatus().intValue() == 1 ? 1 : -1;

            return OtcBrokerSymbol.builder()
                    .createDate(new Date())
                    .updateDate(new Date())
                    .tokenId(i.getTokenId())
                    .currencyId(otcCurrency.getCode())
                    .exchangeId(exchangeId)
                    .orgId(otcCurrency.getOrgId())
                    .status(status)
                    .build();

        }).filter(Objects::nonNull).collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(newSymbols)) {
            success = otcBrokerSymbolMapper.insertList(newSymbols) > 0;
        }

        //构建需要更新的币对
        List<OtcBrokerSymbol> updateSymbols = tokens.stream().map(i -> {
            String tokenId = i.getTokenId();
            String key = tokenId + "-" + otcCurrency.getCode();
            OtcBrokerSymbol symbol = symbolMap.get(key);
            if (Objects.isNull(symbol)) {
                return null;
            }

            int status = otcCurrency.getStatus().intValue() == 1 ? 1 : -1;

            return OtcBrokerSymbol.builder()
                    .updateDate(new Date())
                    .status(status)
                    .tokenId(symbol.getTokenId())
                    .currencyId(symbol.getCurrencyId())
                    .id(symbol.getId())
                    .build();

        }).filter(Objects::nonNull).collect(Collectors.toList());

        updateSymbols.forEach(item -> {

            otcBrokerSymbolMapper.updateByPrimaryKeySelective(item);
        });


        //如果是开启则不执行后续动作
        if (otcCurrency.getStatus().intValue() == OtcBrokerCurrency.AVAILABLE) {
            return;
        }

        //是否有在线广告
        List<OtcItem> otcItems = otcItemService.listUnFinishItem(otcCurrency.getOrgId(), null, otcCurrency.getCode());
        if (CollectionUtils.isEmpty(otcItems)) {
            return;
        }

        //是否有未完成订单
        List<Long> itemIds = otcItems.stream().map(i -> i.getId()).collect(Collectors.toList());
        int count = otcOrderService.countUnfinishOrderByItemId(itemIds);
        if (count > 0) {
            String msg = String.format("Exist unfinish item,orgId=%s,currency=%s",
                    otcCurrency.getOrgId(), otcCurrency.getCode());
            throw new UnFinishedItemException(msg);
        }

        //取消广告
        otcItems.forEach(i -> otcItemService.cancelItem(i.getAccountId(), i.getId(), i.getExchangeId()));
    }

    public List<OtcBrokerSymbol> getOtcBrokerSymbolList(Long exchangeId, Long orgId) {
        OtcBrokerSymbol example = OtcBrokerSymbol.builder()
                .status(OtcBrokerSymbol.AVAILABLE)
                .build();
        if (exchangeId != null && exchangeId > 0) {
            example.setExchangeId(exchangeId);
        }
        if (orgId != null && orgId > 0) {
            example.setOrgId(orgId);
        }
        return otcBrokerSymbolMapper.select(example);
    }

    /**
     * 查询币对是否共享
     *
     * @param brokerId   券商id
     * @param exchangeId 交易所id
     * @param token      代币
     * @param currency   法币
     * @return
     */
    public boolean isShared(Long brokerId, Long exchangeId, String token, String currency) {

        Example exp = new Example(OtcSymbolShare.class);
        exp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andEqualTo("exchangeId", exchangeId)
                .andEqualTo("tokenId", token)
                .andEqualTo("currencyId", currency)
                .andEqualTo("status", OtcSymbolShare.AVAILABLE);

        return otcSymbolShareMapper.selectCountByExample(exp) == 1;

    }

    public List<Long> listExchangeIdByShareSymbol(Long brokerId, Long exchangeId, String token, String currency) {

        boolean isShared = isShared(brokerId, exchangeId, token, currency);
        if (!isShared) {
            return Lists.newArrayList();
        }

        Example exp = new Example(OtcSymbolShare.class);
        exp.selectProperties("exchangeId");
        exp.createCriteria()
                .andEqualTo("tokenId", token)
                .andEqualTo("currencyId", currency)
                .andEqualTo("status", OtcSymbolShare.AVAILABLE);

        return otcSymbolShareMapper.selectByExample(exp)
                .stream().map(i -> i.getExchangeId())
                .collect(Collectors.toList());
    }

    public List<Long> listExchangeIdByShareSymbolV2(Long brokerId, Long exchangeId, String token, String currency) {

        boolean isShared = isShared(brokerId, exchangeId, token, currency);
        if (!isShared) {
            return Lists.newArrayList();
        }

        Example exp = new Example(OtcDepthShareExchange.class);
        exp.selectProperties("exchangeId");
        exp.createCriteria()
                .andEqualTo("status", OtcDepthShareExchange.AVAILABLE);

        Set<Long> exchangeIds = otcDepthShareExchangeMapper.selectByExample(exp)
                .stream().map(i -> i.getExchangeId())
                .collect(Collectors.toSet());
        exchangeIds.add(exchangeId);

        return Lists.newArrayList(exchangeIds);
    }


    public List<OtcBrokerToken> listBrokerTokenConfig(Collection<Long> brokerIds, String tokenId) {

        Example exp = new Example(OtcBrokerToken.class);
        exp.createCriteria().andIn("orgId", brokerIds)
                .andEqualTo("tokenId", tokenId)
                .andEqualTo("status", OtcBrokerToken.AVAILABLE);

        return otcBrokerTokenMapper.selectByExample(exp);
    }

    public List<OtcBrokerCurrency> listBrokerCurrencyConfig(Collection<Long> brokerIds, String currencyId) {

        Example exp = new Example(OtcBrokerCurrency.class);
        exp.createCriteria().andIn("orgId", brokerIds)
                .andEqualTo("code", currencyId)
                .andEqualTo("status", OtcBrokerToken.AVAILABLE);

        return otcBrokerCurrencyMapper.selectByExample(exp);
    }

    public List<OtcBank> listOtcBank() {
        OtcBank example = OtcBank.builder()
                .status(OtcBank.AVAILABLE)
                .build();

        return otcBankMapper.select(example);
    }

    public void addShareSymbol(long exchangeId, long brokerId, String tokenId, String currencyId) {

        Example exp = new Example(OtcSymbolShare.class);
        exp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andEqualTo("exchangeId", exchangeId)
                .andEqualTo("tokenId", tokenId)
                .andEqualTo("currencyId", currencyId);

        int row = otcSymbolShareMapper.selectCountByExample(exp);
        if (row > 0) {
            return;
        }

        OtcSymbolShare obj = OtcSymbolShare.builder()
                .brokerId(brokerId)
                .exchangeId(exchangeId)
                .tokenId(tokenId)
                .currencyId(currencyId)
                .status(OtcSymbolShare.AVAILABLE)
                .createDate(new Date())
                .updateDate(new Date())
                .build();

        otcSymbolShareMapper.insertSelective(obj);
    }

    public void setSymbolShareStatus(long brokerId, String tokenId, String currencyId, int shareStatus) {

        Example exp = new Example(OtcSymbolShare.class);
        exp.createCriteria()
                .andEqualTo("brokerId", brokerId)
                .andEqualTo("tokenId", tokenId)
                .andEqualTo("currencyId", currencyId);

        OtcSymbolShare obj = otcSymbolShareMapper.selectOneByExample(exp);
        if (Objects.isNull(obj)) {
            throw new ItemNotExistException("Symbol not exist");
        }

        OtcSymbolShare tmp = OtcSymbolShare.builder()
                .id(obj.getId())
                .status(shareStatus)
                .updateDate(new Date())
                .build();

        otcSymbolShareMapper.updateByPrimaryKeySelective(tmp);
    }

    public List<OtcSymbolShare> listSharedSymbol(long brokerId) {

        return this.listSharedSymbol(brokerId, null);
    }

    /**
     * 查询券商共享的币对
     *
     * @param brokerId   券商id
     * @param exchangeId 交易所id
     * @return
     */
    public List<OtcSymbolShare> listSharedSymbol(Long brokerId, Long exchangeId) {

        Example exp = new Example(OtcSymbolShare.class);
        Example.Criteria criteria = exp.createCriteria()
                .andEqualTo("brokerId", brokerId);
        if (Objects.nonNull(exchangeId)) {
            criteria.andEqualTo("exchangeId", exchangeId);
        }

        return otcSymbolShareMapper.selectByExample(exp);

    }

    @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_COMMITTED)
    public boolean updateBrokerTokenStatus(long exchangeId, long orgId, String tokenId, int tokenStatus) {

        Example tokenExp = new Example(OtcBrokerToken.class);
        tokenExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", tokenId);

        OtcBrokerToken sample = OtcBrokerToken.builder().status(tokenStatus).updateDate(new Date()).build();

        int rows = otcBrokerTokenMapper.updateByExampleSelective(sample, tokenExp);
        if (rows == 0) {
            return true;
        }
        int shareStatus = -1;
        int symbolStatus = -1;
        if (tokenStatus == OtcBrokerToken.AVAILABLE) {
            shareStatus = OtcBrokerToken.SHAREABLE;
            symbolStatus = OtcBrokerSymbol.AVAILABLE;
        }

        Example symbolExp = new Example(OtcBrokerSymbol.class);
        symbolExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", tokenId);

        OtcBrokerSymbol symbol = OtcBrokerSymbol.builder().status(symbolStatus).updateDate(new Date()).build();
        rows = otcBrokerSymbolMapper.updateByExampleSelective(symbol, symbolExp);

        //禁用需要关闭共享
        if (tokenStatus == -1) {
            this.updateBrokerTokenShareStatus(exchangeId, orgId, tokenId, shareStatus, false);
        }

        //如果是开启则不执行后续动作
        if (symbolStatus == OtcBrokerSymbol.AVAILABLE) {
            return true;
        }

        //是否有在线广告
        List<OtcItem> otcItems = otcItemService.listUnFinishItem(orgId, tokenId, null);
        if (CollectionUtils.isEmpty(otcItems)) {
            return true;
        }

        //是否有未完成订单
        List<Long> itemIds = otcItems.stream().map(i -> i.getId()).collect(Collectors.toList());
        int count = otcOrderService.countUnfinishOrderByItemId(itemIds);
        if (count > 0) {
            String msg = String.format("Exist unfinish item,orgId=%s,tokenId=%s", orgId, tokenId);
            throw new UnFinishedItemException(msg);
        }

        //取消广告
        otcItems.forEach(i -> otcItemService.cancelItem(i.getAccountId(), i.getId(), i.getExchangeId()));
        return true;
    }

    @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_COMMITTED)
    public boolean updateBrokerTokenShareStatus(long exchangeId, long orgId, String tokenId, int shareStatus, boolean checkTokenStatus) {


        //更新token的共享状态
        Example tokenExp = new Example(OtcBrokerToken.class);
        Example.Criteria criteria = tokenExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", tokenId);

        if (checkTokenStatus) {
            criteria.andEqualTo("status", OtcBrokerToken.AVAILABLE);
        }

        OtcBrokerToken sample = OtcBrokerToken.builder().shareStatus(shareStatus).updateDate(new Date()).build();

        int rows = otcBrokerTokenMapper.updateByExampleSelective(sample, tokenExp);
        if (rows == 0) {
            return true;
        }

        //更新共享币对的共享状态
        Example shareExp = new Example(OtcSymbolShare.class);
        shareExp.createCriteria()
                .andEqualTo("brokerId", orgId)
                .andEqualTo("tokenId", tokenId);

        OtcSymbolShare shareSymbol = OtcSymbolShare.builder().status(shareStatus).updateDate(new Date()).build();
        rows = otcSymbolShareMapper.updateByExampleSelective(shareSymbol, shareExp);
        if (shareStatus != OtcBrokerToken.SHAREABLE) {
            return true;
        }

        OtcDepthShareBrokerWhiteList whiteList = otcDepthShareBrokerWhiteListMapper.selectByPrimaryKey(orgId);
        if (Objects.isNull(whiteList)) {
            throw new NotAllowShareTokenException();
        }

        //共享状态，增加新的币对到共享池
        Example symbolExp = new Example(OtcBrokerSymbol.class);
        symbolExp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", tokenId);

        List<OtcBrokerSymbol> symbols = otcBrokerSymbolMapper.selectByExample(symbolExp);
        List<OtcSymbolShare> inSharePool = otcSymbolShareMapper.selectByExample(shareExp);

        Set<String> inShareCurrency = inSharePool.stream().map(i -> i.getCurrencyId().toUpperCase()).collect(Collectors.toSet());
        Map<String, OtcBrokerSymbol> symbolMap = symbols.stream().collect(Collectors.toMap(i -> i.getCurrencyId().toUpperCase(), i -> i));
        Set<String> newCurrencies = Sets.difference(symbolMap.keySet(), inShareCurrency);
        if (CollectionUtils.isNotEmpty(newCurrencies)) {
            newCurrencies.forEach(i -> {
                //OtcBrokerSymbol obs=symbolMap.get(i);
                OtcSymbolShare oss = OtcSymbolShare.builder()
                        .status(OtcSymbolShare.AVAILABLE)
                        .updateDate(new Date())
                        .brokerId(orgId)
                        .currencyId(i)
                        .tokenId(tokenId)
                        .exchangeId(exchangeId)
                        .createDate(new Date())
                        .build();
                otcSymbolShareMapper.insertSelective(oss);
            });
        }
        return true;
    }

/*    public boolean saveBrokerToken(OtcBrokerToken token) {

        Example exp=new Example(OtcBrokerToken.class);
        exp.createCriteria().andEqualTo("orgId",token.getOrgId())
                .andEqualTo("tokenId",token.getOrgId());

        OtcBrokerToken exist=otcBrokerTokenMapper.selectOneByExample(exp);
        if(Objects.isNull(exist)){
            return otcBrokerTokenMapper.insertSelective(token)==1;
        }

        token.setId(exist.getId());
        token.setOrgId(null);
        token.setCreateDate(null);

        return otcBrokerTokenMapper.updateByPrimaryKeySelective(token)==1;
    }*/

    public OtcBrokerToken getBrokerToken(Long orgId, String tokenId) {
        Example exp = new Example(OtcBrokerToken.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", tokenId);
        OtcBrokerToken token = otcBrokerTokenMapper.selectOneByExample(exp);
        if (Objects.isNull(token)) {
            return null;
        }

        OtcTradeFeeRate fr = otcTradeFeeService.queryOtcTradeFeeByTokenId(orgId, tokenId);
        if (Objects.isNull(fr)) {
            return token;
        }

        token.setFeeRate(OtcBrokerToken.FeeRate.builder()
                .sellRate(fr.getMakerSellFeeRate())
                .buyRate(fr.getMakerBuyFeeRate())
                .build());

        return token;

    }

/*    @Transactional(rollbackFor = Exception.class)
    public void saveBrokerCurrency(long brokerId,List<OtcBrokerCurrency> list) {

        //insert into or update
        Example exp=new Example(OtcBrokerCurrency.class);
        exp.createCriteria()
                .andEqualTo("orgId",brokerId);

        List<OtcBrokerCurrency> existList=otcBrokerCurrencyMapper.selectByExample(exp);
        Map<String,OtcBrokerCurrency> existMap=existList.stream().collect(Collectors.toMap(i->i.getCode()+"-"+i.getLanguage(),i->i));
        Map<String,OtcBrokerCurrency> inputMap=list.stream().collect(Collectors.toMap(i->i.getCode()+"-"+i.getLanguage(),i->i));

        inputMap.forEach((k,v)->{
            OtcBrokerCurrency exist=existMap.get(k);
            if(Objects.isNull(exist)){
                v.setCreateDate(new Date());
                v.setUpdateDate(new Date());
                otcBrokerCurrencyMapper.insertSelective(v);
            }else{
                v.setId(exist.getId());
                v.setUpdateDate(new Date());
                otcBrokerCurrencyMapper.updateByPrimaryKeySelective(v);
            }
        });
    }*/

    public List<OtcBrokerCurrency> getOTCBrokerCurrency(long orgId, String code, String lang) {
        Example exp = new Example(OtcBrokerCurrency.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("code", code)
                .andEqualTo("language", lang)
        ;

        return otcBrokerCurrencyMapper.selectByExample(exp);
    }


    public void saveBrokerExt(BrokerExt ext) {

        long brokerId = ext.getBrokerId();
        if (brokerId == 0) {
            throw new BusinessException("brokerId is null");
        }

        long now = System.currentTimeMillis();
        BrokerExt exist = brokerExtMapper.selectByPrimaryKey(brokerId);
        int row = 0;
        if (Objects.isNull(exist)) {
            row = brokerExtMapper.insertSelective(ext);
        } else {
            ext.setCreateAt(null);
            row = brokerExtMapper.updateByPrimaryKeySelective(ext);
        }
    }

    public BrokerExt getBrokerExt(Long brokerId) {

        return brokerExtMapper.selectByPrimaryKey(brokerId);
    }

    public List<BrokerExt> getAllBrokerExt() {
        return brokerExtMapper.selectAll();
    }

    @Transactional(rollbackFor = Throwable.class)
    public boolean saveBrokerToken(OtcBrokerToken token) {

        Example tokenExp = new Example(OtcBrokerToken.class);
        tokenExp.createCriteria()
                .andEqualTo("orgId", token.getOrgId())
                .andEqualTo("tokenId", token.getTokenId());

        OtcBrokerToken exist = otcBrokerTokenMapper.selectOneByExample(tokenExp);

        boolean success = true;
        if (Objects.isNull(exist)) {
            success = otcBrokerTokenMapper.insertSelective(token) == 1;
        } else {
            OtcBrokerToken updateToken = OtcBrokerToken.builder()
                    .id(exist.getId())
                    .updateDate(new Date())
                    .minQuote(token.getMinQuote())
                    .maxQuote(token.getMaxQuote())
                    .scale(token.getScale())
                    .upRange(token.getUpRange())
                    .downRange(token.getDownRange())
                    .build();

            success = otcBrokerTokenMapper.updateByPrimaryKeySelective(updateToken) == 1;
        }

        if (!success) {
            throw new IllegalStateException("Insert token fail,token=" + token.getTokenId());
        }

        //查询法币
        Example currencyExp = new Example(OtcBrokerCurrency.class);
        currencyExp.createCriteria().andEqualTo("orgId", token.getOrgId());
        List<OtcBrokerCurrency> currencies = otcBrokerCurrencyMapper.selectByExample(currencyExp);
        if (CollectionUtils.isEmpty(currencies)) {
            throw new IllegalStateException("Hasn't any currency");
        }

        //查询币对
        Example symbolExp = new Example(OtcBrokerSymbol.class);
        symbolExp.createCriteria()
                .andEqualTo("orgId", token.getOrgId());

        List<OtcBrokerSymbol> symbols = otcBrokerSymbolMapper.selectByExample(symbolExp);
        if (CollectionUtils.isEmpty(symbols)) {
            throw new IllegalStateException("Hasn't any symbol");
        }

        Set<Long> exchangeIds = symbols.stream().map(i -> i.getExchangeId()).collect(Collectors.toSet());
        if (exchangeIds.size() != 1) {
            throw new IllegalStateException("Symbol's exchangeId count is more than 1");
        }
        Long exchangeId = Lists.newArrayList(exchangeIds).get(0);
        Set<String> sets = symbols.stream().map(i -> i.getTokenId() + "-" + i.getCurrencyId()).collect(Collectors.toSet());

        Set<String> currencyCodes = currencies.stream().map(i -> i.getCode()).collect(Collectors.toSet());
        //构建需要增加的币对
        List<OtcBrokerSymbol> newSymbols = currencyCodes.stream().map(i -> {
            String tokenId = token.getTokenId();
            String key = tokenId + "-" + i;
            if (sets.contains(key)) {
                return null;
            }

            return OtcBrokerSymbol.builder()
                    .createDate(new Date())
                    .updateDate(new Date())
                    .tokenId(tokenId)
                    .currencyId(i)
                    .exchangeId(exchangeId)
                    .orgId(token.getOrgId())
                    .status(1)
                    .build();

        }).filter(Objects::nonNull).collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(newSymbols)) {
            success = otcBrokerSymbolMapper.insertList(newSymbols) > 0;
        }

        if (!success) {
            throw new IllegalStateException("Save symbol fail");
        }

        if (Objects.isNull(token.getFeeRate())) {
            return success;
        }

        //交易费率不为空
        OtcTradeFeeRate feeRate = otcTradeFeeService.queryOtcTradeFeeByTokenId(token.getOrgId(), token.getTokenId());
        if (Objects.isNull(feeRate)) {
            success = otcTradeFeeService.addOtcTradeFee(token.getOrgId(), token.getTokenId(),
                    token.getFeeRate().getBuyRate(), token.getFeeRate().getSellRate());
        } else {
            success = otcTradeFeeService.updateOtcTradeFee(feeRate.getId(), token.getFeeRate().getBuyRate(), token.getFeeRate().getSellRate());
        }

        if (!success) {
            throw new IllegalStateException("Save trade fee fail");
        }

        return true;
    }

    @Transactional
    public void cancelBrokerShare(Long orgId) {

        otcDepthShareBrokerWhiteListMapper.deleteByPrimaryKey(orgId);
        Example tokenExp = new Example(OtcBrokerToken.class);
        tokenExp.createCriteria().andEqualTo("orgId", orgId);

        OtcBrokerToken exp = OtcBrokerToken.builder()
                .shareStatus(-1)
                .updateDate(new Date())
                .build();
        otcBrokerTokenMapper.updateByExampleSelective(exp, tokenExp);

        Example shareExp = new Example(OtcSymbolShare.class);
        shareExp.createCriteria().andEqualTo("brokerId", orgId);
        otcSymbolShareMapper.deleteByExample(shareExp);
    }

    public void addSymoblShareBroker(Long orgId) {
        OtcDepthShareBrokerWhiteList obj = otcDepthShareBrokerWhiteListMapper.selectByPrimaryKey(orgId);
        if (Objects.isNull(obj)) {
            obj = OtcDepthShareBrokerWhiteList.builder()
                    .brokerId(orgId).build();
            otcDepthShareBrokerWhiteListMapper.insert(obj);
            log.info("add {} SymoblShareBroker", orgId);
        }
    }


    public List<Long> querySymoblShareBrokers() {
        List<OtcDepthShareBrokerWhiteList> list = otcDepthShareBrokerWhiteListMapper.selectAll();
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        return list.stream().map(c -> c.getBrokerId()).collect(Collectors.toList());
    }

    @Transactional(rollbackFor = Throwable.class)
    public void sortBrokerToken(long brokerId, List<String> tokensList) {

        List<OtcBrokerToken> exist = otcBrokerTokenMapper.queryTokenListByOrgId(brokerId);
        if (CollectionUtils.isEmpty(exist)) {
            return;
        }

        Map<String, Long> tokenMap = exist.stream().collect(Collectors.toMap(i -> i.getTokenId(), i -> i.getId()));

        Stack<Integer> sequence = new Stack<>();
        sequence.push(10000);
        tokensList.forEach(item -> {
            Long id = tokenMap.get(item);
            if (Objects.isNull(id) || id.longValue() < 1L) {
                return;
            }

            Integer seq = sequence.pop();
            OtcBrokerToken token = OtcBrokerToken.builder()
                    .id(id)
                    .sequence(seq)
                    .build();

            otcBrokerTokenMapper.updateByPrimaryKeySelective(token);
            sequence.push(seq.intValue() - 100);
        });


    }
}
