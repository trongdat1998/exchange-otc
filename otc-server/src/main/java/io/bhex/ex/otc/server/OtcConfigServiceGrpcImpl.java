package io.bhex.ex.otc.server;


import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.*;
import io.bhex.ex.otc.util.CommonUtil;
import io.bhex.ex.otc.entity.BrokerExt;
import io.bhex.ex.otc.entity.*;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.exception.NotAllowShareTokenException;
import io.bhex.ex.otc.exception.UnFinishedItemException;
import io.bhex.ex.otc.service.config.OtcConfigService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcConfigServiceGrpcImpl extends OTCConfigServiceGrpc.OTCConfigServiceImplBase {

    @Resource
    private OtcConfigService otcConfigService;

    @Override
    public void getOTCBrokerSymbols(GetOTCSymbolsRequest request,
                                    StreamObserver<GetOTCSymbolsResponse> responseObserver) {

        GetOTCSymbolsResponse.Builder responseBuilder = GetOTCSymbolsResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<OtcBrokerSymbol> symbolList = otcConfigService.getOtcBrokerSymbolList(request.getExchangeId() > 0
                    ? request.getExchangeId() : null, request.getOrgId() > 0 ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(symbolList)) {
                List<OTCSymbol> list = symbolList.stream()
                        .map(otcSymbol -> OTCSymbol.newBuilder()
                                .setId(otcSymbol.getId())
                                .setOrgId(otcSymbol.getOrgId())
                                .setExchangeId(otcSymbol.getExchangeId())
                                .setTokenId(otcSymbol.getTokenId())
                                .setCurrencyId(otcSymbol.getCurrencyId())
                                .build())
                        .collect(Collectors.toList());
                responseBuilder.addAllSymbol(list);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getOTCBrokerSymbols consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void getOTCBrokerTokens(GetOTCTokensRequest request,
                                   StreamObserver<GetOTCTokensResponse> responseObserver) {

        GetOTCTokensResponse.Builder responseBuilder = GetOTCTokensResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<OtcBrokerToken> tokenList = otcConfigService.getOtcTokenList(request.getOrgId() > 0
                    ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(tokenList)) {

                List<OTCToken> list = tokenList.stream()
                        .map(otcToken -> {

                            OTCToken.TokenExt ext = OTCToken.TokenExt.newBuilder().build();
                            if (Objects.nonNull(otcToken.getFeeRate())) {
                                ext = OTCToken.TokenExt.newBuilder()
                                        .setFeeRateBuy(otcToken.getFeeRate().buyRateToString())
                                        .setFeeRateSell(otcToken.getFeeRate().sellRateToString())
                                        .build();
                            }

                            return OTCToken.newBuilder()
                                    .setOrgId(otcToken.getOrgId())
                                    .setTokenId(otcToken.getTokenId())
                                    .setScale(otcToken.getScale())
                                    .setMinQuote(otcToken.getMinQuote().stripTrailingZeros().toPlainString())
                                    .setMaxQuote(otcToken.getMaxQuote().stripTrailingZeros().toPlainString())
                                    .setUpRange(otcToken.getUpRange().stripTrailingZeros().toPlainString())
                                    .setDownRange(otcToken.getDownRange().stripTrailingZeros().toPlainString())
                                    .setStatus(otcToken.getStatus())
                                    .setTokenName(otcToken.getTokenName())
                                    .setShareStatus(otcToken.shareStatusBool() ? ShareStatusEnum.SHARED : ShareStatusEnum.UN_SHARE)
                                    .setExt(ext)
                                    .build();
                        }).collect(Collectors.toList());
                responseBuilder.addAllToken(list);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getOTCBrokerTokens consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * <pre>
     * 查询currency
     * </pre>
     */
/*    @Override
    public void getOTCBrokerCurrencys(GetOTCCurrencysRequest request,
                                      StreamObserver<GetOTCCurrencysResponse> responseObserver) {

        GetOTCCurrencysResponse.Builder responseBuilder = GetOTCCurrencysResponse.newBuilder();
        try {
            List<OtcBrokerCurrency> currencyList = otcConfigService.getOtcCurrencyList(request.getOrgId() > 0
                    ? request.getOrgId() : null);
            if (!CollectionUtils.isEmpty(currencyList)) {
                Map<String, OTCCurrency> currencyMap = Maps.newLinkedHashMap();
                for (OtcBrokerCurrency otcCurrency : currencyList) {
                    String currencyKey = otcCurrency.getCode() + "-" + otcCurrency.getOrgId();
                    OTCCurrency currency = currencyMap.get(currencyKey);
                    if (currency == null) {
                        currency = OTCCurrency.newBuilder()
                                .setOrgId(otcCurrency.getOrgId())
                                .setCode(otcCurrency.getCode())
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcCurrency.getLanguage())
                                        .setName(otcCurrency.getName())
                                        .build())
                                .setMinQuote(otcCurrency.getMinQuote().stripTrailingZeros().toPlainString())
                                .setMaxQuote(otcCurrency.getMaxQuote().stripTrailingZeros().toPlainString())
                                .setScale(otcCurrency.getScale())
                                .setAmountScale(otcCurrency.getAmountScale())
                                .setStatus(otcCurrency.getStatus())
                                .build();
                    } else {
                        currency = currency.toBuilder()
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcCurrency.getLanguage())
                                        .setName(otcCurrency.getName())
                                        .build())
                                .build();
                    }
                    currencyMap.put(currencyKey, currency);
                }
                responseBuilder.addAllCurrency(currencyMap.values());
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }
    }*/
    @Override
    public void getOTCBrokerCurrencys(GetOTCCurrencysRequest request,
                                      StreamObserver<GetOTCCurrencysResponse> responseObserver) {

        GetOTCCurrencysResponse.Builder responseBuilder = GetOTCCurrencysResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<OtcBrokerCurrency> currencyList = otcConfigService.getOtcCurrencyList(request.getOrgId() > 0
                    ? request.getOrgId() : null);

            List<OTCCurrency> list = currencyList.stream().map(i -> {
                return OTCCurrency.newBuilder()
                        .setOrgId(i.getOrgId())
                        .setCode(i.getCode())
                        .setLang(i.getLanguage())
                        .setName(i.getName())
                        .setMinQuote(i.getMinQuote().stripTrailingZeros().toPlainString())
                        .setMaxQuote(i.getMaxQuote().stripTrailingZeros().toPlainString())
                        .setScale(i.getScale())
                        .setAmountScale(i.getAmountScale())
                        .setStatus(i.getStatus())
                        .build();
            }).collect(Collectors.toList());

            responseBuilder.addAllCurrency(list);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getOTCBrokerCurrencys consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void getOTCBrokerCurrencysForAdmin(GetOTCCurrencysRequest request,
                                              StreamObserver<GetOTCCurrencysResponse> responseObserver) {

        GetOTCCurrencysResponse.Builder responseBuilder = GetOTCCurrencysResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<OtcBrokerCurrency> currencyList = otcConfigService.listAllCurrencyByOrgId(request.getOrgId());

            List<OTCCurrency> list = currencyList.stream().map(i -> {
                return OTCCurrency.newBuilder()
                        .setOrgId(i.getOrgId())
                        .setCode(i.getCode())
                        .setLang(i.getLanguage())
                        .setName(i.getName())
                        .setMinQuote(i.getMinQuote().stripTrailingZeros().toPlainString())
                        .setMaxQuote(i.getMaxQuote().stripTrailingZeros().toPlainString())
                        .setScale(i.getScale())
                        .setAmountScale(i.getAmountScale())
                        .setStatus(i.getStatus())
                        .build();
            }).collect(Collectors.toList());

            responseBuilder.addAllCurrency(list);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getOTCBrokerCurrencys consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void listOTCBanks(GetOTCBanksRequest request, StreamObserver<GetOTCBanksResponse> responseObserver) {
        GetOTCBanksResponse.Builder responseBuilder = GetOTCBanksResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<OtcBank> bankList = otcConfigService.listOtcBank();
            if (!CollectionUtils.isEmpty(bankList)) {
                Map<String, OTCBank> bankMap = Maps.newLinkedHashMap();
                for (OtcBank otcBank : bankList) {
                    String bankKey = otcBank.getCode();
                    OTCBank bank = bankMap.get(bankKey);
                    if (bank == null) {
                        bank = OTCBank.newBuilder()
                                .setCode(otcBank.getCode())
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcBank.getLanguage())
                                        .setName(otcBank.getName())
                                        .build())
                                .build();
                    } else {
                        bank = bank.toBuilder()
                                .addLanguage(OTCLanguage.newBuilder()
                                        .setCode(otcBank.getLanguage())
                                        .setName(otcBank.getName())
                                        .build())
                                .build();
                    }
                    bankMap.put(bankKey, bank);
                }
                responseBuilder.addAllBank(bankMap.values());
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("listOTCBanks consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void listOTCShareSymbol(ListShareSymbolRequest request, StreamObserver<ListShareSymbolResponse> observer) {

        ListShareSymbolResponse.Builder builder = ListShareSymbolResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<ShareSymbol> grpcList = Lists.newArrayList();
            List<OtcSymbolShare> list = otcConfigService.listSharedSymbol(request.getBrokerId());
            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(list)) {
                //共享的币对
                List<OtcSymbolShare> shared = list.stream().filter(i -> i.getBrokerId().equals(request.getBrokerId())).collect(Collectors.toList());
                //未共享的币对
                List<OtcSymbolShare> unshare = list.stream().filter(i -> !i.getBrokerId().equals(request.getBrokerId())).collect(Collectors.toList());
                Map<String, ShareSymbol> unShareMap = Maps.newLinkedHashMap();
                Map<String, ShareSymbol> sharedMap = Maps.newLinkedHashMap();

                //构建未共享币对集合,忽略brokerId
                unshare.forEach(symbol -> {
                    String key = buildShareSymboleKey(symbol.getTokenId(), symbol.getCurrencyId());
                    boolean exist = unShareMap.containsKey(key);
                    if (!exist) {
                        ShareSymbol ss = ShareSymbol.newBuilder()
                                .setShared(ShareStatusEnum.UN_SHARE)
                                .setTokenId(symbol.getTokenId())
                                .setCurrencyId(symbol.getCurrencyId())
                                .build();
                        unShareMap.put(key, ss);
                    }

                });

                //构建已共享币对集合
                shared.forEach(symbol -> {
                    String key = buildShareSymboleKey(symbol.getTokenId(), symbol.getCurrencyId());
                    boolean exist = sharedMap.containsKey(key);
                    if (!exist) {
                        boolean shareStatus = symbol.shareStatusBool();
                        ShareSymbol ss = ShareSymbol.newBuilder()
                                .setShared(shareStatus ? ShareStatusEnum.SHARED : ShareStatusEnum.UN_SHARE)
                                .setTokenId(symbol.getTokenId())
                                .setCurrencyId(symbol.getCurrencyId())
                                .build();
                        sharedMap.put(key, ss);
                    }
                });

                grpcList.addAll(Lists.newArrayList(sharedMap.values()));
                grpcList.addAll(Lists.newArrayList(unShareMap.values()));
            }

            builder.addAllShareSymbol(grpcList);
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("listOTCShareSymbol consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private String buildShareSymboleKey(String tokenId, String currencyId) {
        return tokenId + "-" + currencyId;
    }

    @Override
    public void addOTCShareSymbol(AddOTCShareSymbolRequest request, StreamObserver<BaseResponse> observer) {

        BaseResponse.Builder builder = BaseResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcConfigService.addShareSymbol(request.getExchangeId(), request.getBrokerId(), request.getTokenId(), request.getCurrencyId());
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("addOTCShareSymbol consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void setOTCSymbolShareStatus(SetOTCSymbolShareStatusRequest request, StreamObserver<BaseResponse> observer) {
        BaseResponse.Builder builder = BaseResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            int status = -1;
            ShareStatusEnum sse = request.getShareStatus();
            if (sse == ShareStatusEnum.SHARED) {
                status = 1;
            }

            if (sse == ShareStatusEnum.UN_SHARE) {
                status = 0;
            }

            if (status == -1) {
                builder.setResult(OTCResult.PARAM_ERROR);
                observer.onNext(builder.build());
                observer.onCompleted();
                return;
            }

            otcConfigService.setSymbolShareStatus(request.getBrokerId(), request.getTokenId(),
                    request.getCurrencyId(), status);
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
            return;
        } catch (ItemNotExistException e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("setOTCSymbolShareStatus consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void updateBrokerTokenStatus(BrokerTokenStatusRequest request, StreamObserver<BaseResponse> observer) {

        BaseResponse.Builder builder = BaseResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            int tokenStatus = -1;
            if (request.getTokenStatus() == TokenStatusEnum.VALID) {
                tokenStatus = OtcBrokerToken.AVAILABLE;
            }
            boolean success = otcConfigService.updateBrokerTokenStatus(request.getExchangeId(), request.getOrgId(), request.getTokenId(), tokenStatus);
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (UnFinishedItemException e) {
            log.error(e.getMessage(), e);
            observer.onNext(BaseResponse.newBuilder().setResult(OTCResult.UN_FINISHED_ITEM).build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);

            observer.onError(e);
        } finally {
            log.info("updateBrokerTokenStatus consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void updateBrokerTokenShareStatus(BrokerTokenStatusRequest request, StreamObserver<BaseResponse> observer) {

        BaseResponse.Builder builder = BaseResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            int shareStatus = -1;
            if (request.getShareStatus() == ShareStatusEnum.SHARED) {
                shareStatus = OtcBrokerToken.SHAREABLE;
            }
            boolean success = otcConfigService.updateBrokerTokenShareStatus(request.getExchangeId(), request.getOrgId(), request.getTokenId(), shareStatus, true);
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (NotAllowShareTokenException e) {
            log.warn(e.getMessage(), e);
            builder.setResult(OTCResult.PERMISSION_DENIED);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("updateBrokerTokenShareStatus consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void saveOTCBrokerToken(SaveOTCBrokerTokenRequest request, StreamObserver<BaseResponse> observer) {

        BaseResponse.Builder builder = BaseResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCToken input = request.getOtcToken();

            String feeRateBuy = request.getOtcToken().getExt().getFeeRateBuy();
            String feeRateSell = request.getOtcToken().getExt().getFeeRateSell();

            OtcBrokerToken.FeeRate feeRate = null;
            if (StringUtils.isNoneBlank(feeRateBuy) ||
                    StringUtils.isNoneBlank(feeRateSell)) {
                feeRate = OtcBrokerToken.FeeRate.builder()
                        .buyRate(new BigDecimal(feeRateBuy))
                        .sellRate(new BigDecimal(feeRateSell))
                        .build();
            }


            OtcBrokerToken token = OtcBrokerToken.builder()
                    .orgId(request.getBaseRequest().getOrgId())
                    .tokenId(input.getTokenId().toUpperCase())
                    .minQuote(new BigDecimal(input.getMinQuote()))
                    .maxQuote(new BigDecimal(input.getMaxQuote()))
                    .sequence(input.getSequence())
                    .scale(input.getScale())
                    .upRange(new BigDecimal(input.getUpRange()))
                    .downRange(new BigDecimal(input.getDownRange()))
                    .shareStatus(0)
                    .status(input.getStatus() == 1 ? 1 : 0)
                    .createDate(new Date())
                    .updateDate(new Date())
                    .tokenName(input.getTokenName())
                    .feeRate(feeRate)
                    .build();

            boolean success = otcConfigService.saveBrokerToken(token);
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("addOTCBrokerToken consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void getOTCBrokerToken(GetOTCBrokerTokenRequest request, StreamObserver<GetOTCBrokerTokenResponse> observer) {

        GetOTCBrokerTokenResponse.Builder builder = GetOTCBrokerTokenResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Long orgId = request.getBaseRequest().getOrgId();
            String tokenId = request.getTokenId();
            OtcBrokerToken token = otcConfigService.getBrokerToken(orgId, tokenId);
            if (Objects.nonNull(token)) {

                OTCToken.TokenExt ext = OTCToken.TokenExt.newBuilder().build();
                if (Objects.nonNull(token.getFeeRate())) {
                    ext = OTCToken.TokenExt.newBuilder()
                            .setFeeRateBuy(token.getFeeRate().buyRateToString())
                            .setFeeRateSell(token.getFeeRate().sellRateToString())
                            .build();
                }

                OTCToken output = OTCToken.newBuilder()
                        .setOrgId(token.getOrgId())
                        .setScale(token.getScale())
                        .setStatus(token.getStatus())
                        .setDownRange(CommonUtil.BigDecimalToString(token.getDownRange()))
                        .setUpRange(CommonUtil.BigDecimalToString(token.getUpRange()))
                        .setMaxQuote(CommonUtil.BigDecimalToString(token.getMaxQuote()))
                        .setMinQuote(CommonUtil.BigDecimalToString(token.getMinQuote()))
                        .setSequence(token.getSequence())
                        .setTokenId(token.getTokenId())
                        .setShareStatus(token.shareStatusBool() ? ShareStatusEnum.SHARED : ShareStatusEnum.UN_SHARE)
                        .setTokenName(token.getTokenName())
                        .setExt(ext)
                        .build();
                builder.setToken(output);
            }

            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("getOTCBrokerToken consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void saveOTCBrokerCurrency(SaveOTCCurrencyRequest request, StreamObserver<BaseResponse> observer) {
        BaseResponse.Builder builder = BaseResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {

            List<OtcBrokerCurrency> list = request.getCurrencyList().stream().map(i -> {
                return OtcBrokerCurrency.builder()
                        .orgId(i.getOrgId())
                        .status(i.getStatus() == 1 ? 1 : -1)
                        .code(i.getCode().toUpperCase())
                        .language(i.getLang())
                        .name(i.getName())
                        .scale(i.getScale())
                        .amountScale(i.getAmountScale())
                        .minQuote(new BigDecimal(i.getMinQuote()))
                        .maxQuote(new BigDecimal(i.getMaxQuote()))
                        .amountScale(i.getAmountScale())
                        .build();
            }).collect(Collectors.toList());

            list.forEach(item -> otcConfigService.saveBrokerCurrency(item));
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (UnFinishedItemException e) {
            log.error(e.getMessage(), e);
            observer.onNext(BaseResponse.newBuilder().setResult(OTCResult.UN_FINISHED_ITEM).build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("addOTCBrokerCurrency consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }


    /**
     * @param request
     * @param observer
     * @see OtcConfigServiceGrpcImpl#getOTCBrokerCurrencys
     */
    @Deprecated
    @Override
    public void getOTCBrokerCurrency(GetOTCCurrencyRequest request, StreamObserver<GetOTCCurrencyResponse> observer) {

        GetOTCCurrencyResponse.Builder builder = GetOTCCurrencyResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {

            List<OtcBrokerCurrency> list = otcConfigService.getOTCBrokerCurrency(request.getBaseRequest().getOrgId(), request.getCode(), "zh_CN");
            List<OTCCurrency> output = list.stream().map(i -> {
                return OTCCurrency.newBuilder()
                        .setOrgId(i.getOrgId())
                        .setCode(i.getCode().toUpperCase())
                        .setName(i.getName())
                        .setLang(i.getLanguage())
                        .setMinQuote(CommonUtil.BigDecimalToString(i.getMinQuote()))
                        .setMaxQuote(CommonUtil.BigDecimalToString(i.getMaxQuote()))
                        .setScale(i.getScale())
                        .setAmountScale(i.getAmountScale())
                        .setStatus(i.getStatus())
                        .build();

            }).collect(Collectors.toList());

            builder.addAllCurrency(output);
            builder.setResult(OTCResult.SUCCESS);
            observer.onNext(builder.build());
            observer.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("getOTCBrokerCurrency consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }


    @Override
    public void saveBrokerExt(SaveBrokerExtRequest request, StreamObserver<SimpleResponse> responseObserver) {
        SimpleResponse resp = null;
        Stopwatch sw = Stopwatch.createStarted();
        try {

            if (request.getBrokerId() == 0 || StringUtils.isBlank(request.getBrokerName())
                    || StringUtils.isBlank(request.getPhone())) {
                resp = SimpleResponse.newBuilder().setResult(OTCResult.PARAM_ERROR).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            long now = System.currentTimeMillis();
            BrokerExt ext = BrokerExt.builder()
                    .brokerId(request.getBrokerId())
                    .phone(request.getPhone())
                    .createAt(now)
                    .updateAt(now)
                    .brokerName(request.getBrokerName())
                    .build();

            otcConfigService.saveBrokerExt(ext);
            resp = SimpleResponse.newBuilder().setResult(OTCResult.SUCCESS).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("saveBrokerExt consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }


    }

    @Override
    public void getBrokerExt(OrgIdRequest request, StreamObserver<BrokerExtResponse> responseObserver) {
        BrokerExtResponse resp = null;
        Stopwatch sw = Stopwatch.createStarted();
        try {

            long brokerId = request.getOrgId();
            if (brokerId == 0) {
                resp = BrokerExtResponse.newBuilder().setRet(OTCResult.PARAM_ERROR).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            BrokerExt ext = otcConfigService.getBrokerExt(brokerId);
            if (Objects.isNull(ext)) {
                resp = BrokerExtResponse.newBuilder().setRet(OTCResult.SUCCESS).build();
            } else {
                resp = BrokerExtResponse.newBuilder().setRet(OTCResult.SUCCESS)
                        .setBrokerId(ext.getBrokerId())
                        .setBrokerName(ext.getBrokerName())
                        .setPhone(ext.getPhone())
                        .build();
            }
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getBrokerExt consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void sortToken(SortTokenRequest request, StreamObserver<BaseResponse> responseObserver) {
        BaseResponse resp = null;
        Stopwatch sw = Stopwatch.createStarted();
        try {

            long brokerId = request.getBaseRequest().getOrgId();
            if (brokerId == 0) {
                resp = BaseResponse.newBuilder().setResult(OTCResult.PARAM_ERROR).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            otcConfigService.sortBrokerToken(brokerId, request.getTokensList());
            resp = BaseResponse.newBuilder().setResult(OTCResult.SUCCESS).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("sortToken consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void getAllBrokerExt(OrgIdRequest request, StreamObserver<AllBrokerExtResponse> responseObserver) {
        AllBrokerExtResponse resp = null;
        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<BrokerExt> ext = otcConfigService.getAllBrokerExt();
            List<io.bhex.ex.otc.BrokerExt> extList = new ArrayList<>();
            if (CollectionUtils.isEmpty(ext)) {
                resp = AllBrokerExtResponse.newBuilder().build();
            } else {
                ext.forEach(brokerExt -> {
                    OtcDepthShareBrokerWhiteList whiteList
                            = this.otcConfigService.getOtcDepthShareBrokerWhiteListFromCache(brokerExt.getBrokerId());
                    extList.add(io.bhex.ex.otc.BrokerExt.newBuilder().setBrokerId(brokerExt.getBrokerId())
                            .setBrokerName(brokerExt.getBrokerName())
                            .setPhone(brokerExt.getPhone())
                            .setCancelTime(brokerExt.getCancelTime())
                            .setAppealTime(brokerExt.getAppealTime())
                            .setIsShare(whiteList == null ? 0 : 1)
                            .build());
                });
                log.info("getAllBrokerExt list size {}", extList.size());
                resp = AllBrokerExtResponse.newBuilder().addAllBrokerExt(extList).build();
            }
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getAllBrokerExt consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }
}
