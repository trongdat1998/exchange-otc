package io.bhex.ex.otc.service;


import io.bhex.ex.otc.mappper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Slf4j
@Service
public class OtcUtilService {

    @Resource
    private OtcBrokerCurrencyMapper otcBrokerCurrencyMapper;

    @Resource
    private OtcBrokerSymbolMapper otcBrokerSymbolMapper;

    @Resource
    private OtcBrokerTokenMapper otcBrokerTokenMapper;

    @Resource
    private OtcSymbolMapper otcSymbolMapper;

    @Resource
    private OtcTradeFeeRateMapper otcTradeFeeRateMapper;

    public void createOtc(Long orgId, Long fromOrgId, Long exchangeId, Long fromExchangeId, String token) {
        if (CollectionUtils.isEmpty(otcBrokerCurrencyMapper.queryOtcBrokerCurrencyByOrgId(orgId))) {
            otcBrokerCurrencyMapper.initOtcBrokerCurrency(orgId, fromOrgId);
        }

        if (CollectionUtils.isEmpty(otcBrokerSymbolMapper.queryOtcBrokerSymbolByOrgId(orgId))) {
            otcBrokerSymbolMapper.initOtcBrokerSymbol(orgId, fromOrgId, exchangeId, token);
        }

        if (CollectionUtils.isEmpty(otcBrokerTokenMapper.queryOtcBrokerTokenByOrgId(orgId))) {
            otcBrokerTokenMapper.initOtcBrokerToken(orgId, fromOrgId, token);
        }

        if (CollectionUtils.isEmpty(otcSymbolMapper.queryOtcSymbolByExchangeId(exchangeId))) {
            otcSymbolMapper.initOtcSymbol(exchangeId, fromExchangeId, token);
        }

        if (CollectionUtils.isEmpty(otcTradeFeeRateMapper.queryOtcTradeFeeByOrgId(orgId))) {
            otcTradeFeeRateMapper.initTradeFee(orgId, fromOrgId);
        }
    }
}
