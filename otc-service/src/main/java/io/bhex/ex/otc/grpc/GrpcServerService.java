package io.bhex.ex.otc.grpc;

import com.google.common.collect.Maps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import io.bhex.ex.otc.OTCNewOrderRequest;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import io.bhex.base.account.BalanceChangeRequest;
import io.bhex.base.account.BalanceChangeResponse;
import io.bhex.base.account.BalanceDetailList;
import io.bhex.base.account.BalanceServiceGrpc;
import io.bhex.base.account.GetBalanceDetailRequest;
import io.bhex.base.account.GetPositionRequest;
import io.bhex.base.account.PositionResponseList;
import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.GetLegalCoinRatesReply;
import io.bhex.base.quote.GetRatesRequest;
import io.bhex.base.quote.OtcIndexRequest;
import io.bhex.base.quote.OtcIndexResponse;
import io.bhex.base.quote.OtcSide;
import io.bhex.base.quote.Rate;
import io.bhex.base.quote.Token;
import io.bhex.ex.otc.config.GrpcClientFactory;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.OtcOrderExt;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.service.OtcBalanceFlowService;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.service.OtcUserInfoService;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lizhen
 * @date 2018-09-14
 */
@Slf4j
@Service
public class GrpcServerService {

    @Autowired
    private GrpcClientFactory grpcClientFactory;

    @Autowired
    private OtcBalanceFlowService otcBalanceFlowService;

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private OtcOrderService otcOrderService;

    private final static String OTC_LAST_PRICE_CACHE_PREFIX = "OTC_LAST_PRICE_CACHE_%s_%s";

    private final ExecutorService flowSendExecutor = Executors.newFixedThreadPool(4);

    public Map<String, BigDecimal> getLastPrice(Long exchangeId, String tokenId) {
        try {
            String cacheKey = String.format(OTC_LAST_PRICE_CACHE_PREFIX, exchangeId.toString(), tokenId);
            String content = stringRedisTemplate.opsForValue().get(cacheKey);
            if (StringUtils.isNotBlank(content)) {
                return JSON.parseObject(content, new TypeReference<Map<String, BigDecimal>>() {
                });
            }

            Map<String, BigDecimal> lastPriceMap = ratesV2(exchangeId, tokenId);
            if (MapUtils.isEmpty(lastPriceMap)) {
                return Maps.newHashMap();
            }
            stringRedisTemplate.opsForValue().set(cacheKey, JSON.toJSONString(lastPriceMap), 5, TimeUnit.SECONDS);
            return lastPriceMap;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private Map<String, BigDecimal> ratesV2(Long exchangeId, String tokenId) {
        Map<String, BigDecimal> lastPriceMap = Maps.newHashMap();
        try {
            GetLegalCoinRatesReply getLegalCoinRatesReply = grpcClientFactory.quoteServiceBlockingStub()
                    .getRatesV2(GetRatesRequest.newBuilder().addTokens(Token.newBuilder()
                            .setExchangeId(exchangeId)
                            .setToken(tokenId)
                            .build()
                    ).build());
            //grpcClientFactory.quoteServiceBlockingStub().getOTCIndex();
            Map<String, Rate> rateMap = getLegalCoinRatesReply.getRatesMapMap();
            if (rateMap == null || rateMap.size() == 0) {
            }
            rateMap.keySet().forEach(key -> {
                Rate rate = rateMap.get(key);
                if (rate != null) {
                    Map<String, Decimal> legalMap = rate.getRatesMap();
                    if (legalMap != null && legalMap.size() > 0) {
                        legalMap.keySet().forEach(k -> {
                            lastPriceMap.put(k, new BigDecimal(legalMap.get(k).getStr()));
                        });
                    }
                }
            });
        } catch (Exception e) {
            log.error("[EXCHANGE-OTC ratesV2] Get {} lastPrice failed: {} ", tokenId, e.getMessage());
        }
        return lastPriceMap;
    }

    public BigDecimal getOTCUsdtIndex(String tokenId, String currencyId, OtcSide side) {
        OtcIndexResponse otcIndexResponse = grpcClientFactory.quoteServiceBlockingStub()
                .getOTCIndex(OtcIndexRequest.newBuilder().setSymbol(tokenId + currencyId).setSide(side).build());
        return new BigDecimal((otcIndexResponse.getIndex().getStr()));
    }

    public void sendBalanceChangeRequest(OtcBalanceFlow otcBalanceFlow) {
        flowSendExecutor.execute(() -> {
            try {
                OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfo(otcBalanceFlow.getAccountId());
                OtcOrder otcOrder = this.otcOrderService.findOtcOrderById(otcBalanceFlow.getObjectId());
                OtcOrderExt otcOrderExt = otcOrderService.findOrderExtById(otcBalanceFlow.getObjectId());
                //判断是否是商家身份 不是商家则true
                Boolean isRiskBalance = Boolean.FALSE;
                if (Objects.nonNull(otcOrderExt) && Objects.nonNull(otcOrder)) {
                    OTCNewOrderRequest.RiskBalanceType riskBalanceType = OTCNewOrderRequest.RiskBalanceType.forNumber(otcOrder.getRiskBalanceType());
                    if (riskBalanceType == OTCNewOrderRequest.RiskBalanceType.NOT_RISK_BALANCE) {
                        isRiskBalance = Boolean.FALSE;
                    } else if (riskBalanceType == OTCNewOrderRequest.RiskBalanceType.RISK_BALANCE) {
                        isRiskBalance = Boolean.TRUE;
                    } else if (Objects.nonNull(otcOrderExt) && Objects.nonNull(otcOrder) && otcOrderExt.getIsBusiness() != null) {
                        if (otcBalanceFlow.getUserId().equals(otcOrder.getUserId()) && (otcOrder.getFreed().equals(1) || otcOrder.getFreed().equals(3))) {
                            isRiskBalance = otcOrderExt.getIsBusiness().equals(0) ? true : false;
                        }
                    }
                }

                log.info("flowid:{} account:{} objectId {} isRiskBalance {}", otcBalanceFlow.getId(),
                        otcBalanceFlow.getAccountId(), otcBalanceFlow.getObjectId(), isRiskBalance);
                BalanceChangeRequest.Builder request = BalanceChangeRequest.newBuilder()
                        .setOrgId(userInfo.getOrgId())
                        .setAccountId(otcBalanceFlow.getAccountId())
                        .setTokenId(otcBalanceFlow.getTokenId())
                        .setBusinessId(otcBalanceFlow.getId())
                        .setBusinessTypeValue(otcBalanceFlow.getFlowType())
                        .setIsRiskBalance(isRiskBalance)
                        .setAmount(DecimalUtil.fromBigDecimal(otcBalanceFlow.getAmount()));
                if (otcBalanceFlow.getFee() != null && otcBalanceFlow.getFee().compareTo(BigDecimal.ZERO) > 0) {
                    request.setFee(otcBalanceFlow.getFee().toPlainString());
                    request.setIsMaker(true);
                }
                BalanceChangeResponse response = grpcClientFactory.balanceChangeBlockingStub().changeBalance(request.build());
                if (response.getChangeId() > 0) {
                    otcBalanceFlowService.finishOtcBalanceFlow(otcBalanceFlow.getId());
                } else {
                    log.warn("balance change failed  flowId: {}, response: {}", otcBalanceFlow.getId(),
                            response.getChangeId());
                }
            } catch (Exception e) {
                log.error("balance change exception  ", e);
            }
        });
    }

    public BalanceDetailList getBalanceDetail(GetBalanceDetailRequest request) {
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientFactory.balanceServiceBlockingStub();
        try {
            return stub.getBalanceDetail(request);
        } catch (Exception e) {
            log.error("getBalanceDetail exception  ", e);
            throw e;
        }
    }

    public PositionResponseList getBalancePosition(GetPositionRequest request) {
        BalanceServiceGrpc.BalanceServiceBlockingStub stub = grpcClientFactory.balanceServiceBlockingStub();
        try {
            return stub.getPosition(request);
        } catch (StatusRuntimeException e) {
            log.error("getBalancePosition exception  ", e);
            throw e;
        }
    }
}
