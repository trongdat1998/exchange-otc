package io.bhex.ex.otc.server;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.base.quote.OtcSide;
import io.bhex.ex.otc.CurrencyConfig;
import io.bhex.ex.otc.FindItemsByIdsReponse;
import io.bhex.ex.otc.FindItemsByIdsRequest;
import io.bhex.ex.otc.GetTradeFeeRateByTokenIdRequest;
import io.bhex.ex.otc.GetTradeFeeRateByTokenIdResponse;
import io.bhex.ex.otc.GetTradeFeeRateRequest;
import io.bhex.ex.otc.GetTradeFeeRateResponse;
import io.bhex.ex.otc.ItemBrief;
import io.bhex.ex.otc.OTCCancelItemRequest;
import io.bhex.ex.otc.OTCCancelItemResponse;
import io.bhex.ex.otc.OTCDeleteItemRequest;
import io.bhex.ex.otc.OTCDeleteItemResponse;
import io.bhex.ex.otc.OTCGetDepthRequest;
import io.bhex.ex.otc.OTCGetDepthResponse;
import io.bhex.ex.otc.OTCGetItemIdRequest;
import io.bhex.ex.otc.OTCGetItemIdResponse;
import io.bhex.ex.otc.OTCGetItemInfoRequest;
import io.bhex.ex.otc.OTCGetItemInfoResponse;
import io.bhex.ex.otc.OTCGetItemsAdminRequest;
import io.bhex.ex.otc.OTCGetItemsRequest;
import io.bhex.ex.otc.OTCGetItemsResponse;
import io.bhex.ex.otc.OTCGetLastPriceRequest;
import io.bhex.ex.otc.OTCGetLastPriceResponse;
import io.bhex.ex.otc.OTCGetOnlineItemsRequest;
import io.bhex.ex.otc.OTCItemDetail;
import io.bhex.ex.otc.OTCItemServiceGrpc;
import io.bhex.ex.otc.OTCItemStatusEnum;
import io.bhex.ex.otc.OTCNewItemRequest;
import io.bhex.ex.otc.OTCNewItemResponse;
import io.bhex.ex.otc.OTCNormalItemRequest;
import io.bhex.ex.otc.OTCNormalItemResponse;
import io.bhex.ex.otc.OTCOfflineItemRequest;
import io.bhex.ex.otc.OTCOfflineItemResponse;
import io.bhex.ex.otc.OTCOnlineItemRequest;
import io.bhex.ex.otc.OTCOnlineItemResponse;
import io.bhex.ex.otc.OTCPaymentTerm;
import io.bhex.ex.otc.OTCPriceLevel;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OTCShareWhiteList;
import io.bhex.ex.otc.OTCTradeFeeRate;
import io.bhex.ex.otc.TokenConfig;
import io.bhex.ex.otc.util.CommonUtil;
import io.bhex.ex.otc.util.DecimalUtil;
import io.bhex.ex.otc.dto.RefreshItemEvent;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcBrokerCurrency;
import io.bhex.ex.otc.entity.OtcBrokerToken;
import io.bhex.ex.otc.entity.OtcDepthShareBrokerWhiteList;
import io.bhex.ex.otc.entity.OtcItem;
import io.bhex.ex.otc.entity.OtcPaymentTerm;
import io.bhex.ex.otc.entity.OtcTradeFeeRate;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.enums.ItemStatus;
import io.bhex.ex.otc.exception.CurrencyConfigNotFoundException;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.exception.NicknameNotSetException;
import io.bhex.ex.otc.exception.PermissionDeniedException;
import io.bhex.ex.otc.exception.RiskControlInterceptionException;
import io.bhex.ex.otc.exception.UnFinishedItemException;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.mappper.OtcDepthShareBrokerWhiteListMapper;
import io.bhex.ex.otc.service.OtcItemService;
import io.bhex.ex.otc.service.OtcPaymentTermService;
import io.bhex.ex.otc.service.OtcTradeFeeService;
import io.bhex.ex.otc.service.OtcUserInfoService;
import io.bhex.ex.otc.service.config.OtcConfigService;
import io.bhex.ex.otc.service.item.OtcItemOnline;
import io.bhex.ex.otc.service.item.PriceLevel;
import io.bhex.ex.otc.util.ConvertUtil;
import io.bhex.ex.proto.OrderSideEnum;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * 商品/广告grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcItemServiceGrpcImpl extends OTCItemServiceGrpc.OTCItemServiceImplBase {

    @Autowired
    private OtcItemService otcItemService;

    @Resource
    private OtcUserInfoService otcUserInfoService;

    @Autowired
    private OtcPaymentTermService otcPaymentTermService;

    @Autowired
    private GrpcServerService grpcServerService;

    @Autowired
    private OtcTradeFeeService otcTradeFeeService;

    @Resource
    private OtcConfigService otcConfigService;

    @Resource
    private OtcDepthShareBrokerWhiteListMapper otcDepthShareBrokerWhiteListMapper;

    @Resource
    private ApplicationEventPublisher applicationEventPublisher;

    @Resource(name = "otcOrderCreateTaskExecutor")
    private TaskExecutor otcOrderCreateTaskExecutor;

    @Override
    public void addItem(OTCNewItemRequest request, StreamObserver<OTCNewItemResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        OTCNewItemResponse.Builder responseBuilder = OTCNewItemResponse.newBuilder();
        responseBuilder.setClientItemId(request.getClientItemId());
        // 处理重复请求
        Long itemId = otcItemService.getItemIdByClient(request.getOrgId(), request.getClientItemId());
        if (itemId != null && itemId > 0) {
            responseBuilder.setResult(OTCResult.SUCCESS);
            responseBuilder.setItemId(itemId);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }
        // 是否设置了付款方式
        List<OtcPaymentTerm> paymentTermList = otcPaymentTermService.getVisibleOtcPaymentTerm(request.getAccountId(), request.getOrgId());
        if (CollectionUtils.isEmpty(paymentTermList)) {
            responseBuilder.setResult(OTCResult.PAYMENT_NOT_EXIST);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        OtcItem otcItem = OtcItem.builder()
                .exchangeId(request.getBaseRequest().getExchangeId())
                .orgId(request.getBaseRequest().getOrgId())
                .accountId(request.getAccountId())
                .tokenId(request.getTokenId())
                .currencyId(request.getCurrencyId())
                .clientItemId(request.getClientItemId())
                .side(request.getSideValue())
                .priceType(request.getPriceTypeValue())
                .price(DecimalUtil.toBigDecimal(request.getPrice(), BigDecimal.ZERO))
                .premium(DecimalUtil.toBigDecimal(request.getPremium(), BigDecimal.ZERO))
                .quantity(DecimalUtil.toBigDecimal(request.getQuantity()))
                .minAmount(DecimalUtil.toBigDecimal(request.getMinAmount()))
                .maxAmount(DecimalUtil.toBigDecimal(request.getMaxAmount()))
                .remark(request.getRemark())
                .autoReply(request.getAutoReply())
                .status(ItemStatus.INIT.getStatus())
                .frozenFee(DecimalUtil.toBigDecimal(request.getFrozenFee()))
                .fee(BigDecimal.ZERO)
                .build();
        try {
            itemId = otcItemService.createItem(otcItem);
            //异步同步用户信息
            otcOrderCreateTaskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    otcUserInfoService.updateContact(request.getAccountId(), request.getMobile(), request.getEmail(), "", "");
                    //刷新缓存
                    sendRefreshItemEvent(otcItem);
                }
            });
            responseBuilder.setResult(OTCResult.SUCCESS);
            responseBuilder.setItemId(itemId);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (NicknameNotSetException e) {
            log.warn("create item failed", e);
            responseBuilder.setResult(OTCResult.NICK_NAME_NOT_SET);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (UnFinishedItemException e) {
            log.warn("create item failed", e);
            responseBuilder.setResult(OTCResult.UN_FINISHED_ITEM);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (RiskControlInterceptionException e) {
            log.warn("RiskControlInterceptionException failed", e);
            responseBuilder.setResult(OTCResult.RISK_CONTROL_INTERCEPTION_LIMIT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (CurrencyConfigNotFoundException e) {
            log.warn("CurrencyConfigNotFoundException failed", e);
            responseBuilder.setResult(OTCResult.NO_CURRENCY_CONFIGURATION_FOUND);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("addItem,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void setItemToNormal(OTCNormalItemRequest request, StreamObserver<OTCNormalItemResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            int res = otcItemService.updateItemStatusToNormal(request.getItemId());
            OtcItem item = otcItemService.getOtcItemWithoutConfigById(request.getItemId());
            sendRefreshItemEvent(item);
            OTCNormalItemResponse response = OTCNormalItemResponse.newBuilder()
                    .setResult(res == 1 ? OTCResult.SUCCESS : OTCResult.ITEM_NOT_EXIST)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("setItemToNormal,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void setItemToDelete(OTCDeleteItemRequest request, StreamObserver<OTCDeleteItemResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            int res = otcItemService.updateItemStatusToDelete(request.getItemId());
            OtcItem item = otcItemService.getOtcItemWithoutConfigById(request.getItemId());
            sendRefreshItemEvent(item);
            OTCDeleteItemResponse response = OTCDeleteItemResponse.newBuilder()
                    .setResult(res == 1 ? OTCResult.SUCCESS : OTCResult.ITEM_NOT_EXIST)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("setItemToDelete,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void cancelItemToDelete(OTCCancelItemRequest request,
                                   StreamObserver<OTCCancelItemResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        OTCCancelItemResponse.Builder responseBuilder = OTCCancelItemResponse.newBuilder();
        try {

            OtcBalanceFlow balanceFlow = otcItemService.cancelItem(request.getAccountId(), request.getItemId(),
                    request.getBaseRequest().getExchangeId());
            if (balanceFlow != null) {
                grpcServerService.sendBalanceChangeRequest(balanceFlow);
            }
            OtcItem item = otcItemService.getOtcItemWithoutConfigById(request.getItemId());
            sendRefreshItemEvent(item);

            responseBuilder.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (ItemNotExistException e) {
            log.warn("cancel item failed", e);
            responseBuilder.setResult(OTCResult.ITEM_NOT_EXIST);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (PermissionDeniedException e) {
            log.warn("cancel item failed", e);
            responseBuilder.setResult(OTCResult.PERMISSION_DENIED);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("cancelItemToDelete,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getItems(OTCGetItemsRequest request, StreamObserver<OTCGetItemsResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetItemsResponse.Builder responseBuilder = OTCGetItemsResponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            // 按照用户查询
            List<OtcItem> itemList = otcItemService.getOtcItemList(request.getAccountId(),
                    request.getStatusValueList(),
                    request.getBeginTime() > 0 ? new Date(request.getBeginTime()) : null,
                    request.getEndTime() > 0 ? new Date(request.getEndTime()) : null,
                    request.getTokenId(),
                    CollectionUtils.isEmpty(request.getSideList()) ? null : request.getSideValue(0),
                    request.getPage(),
                    request.getSize());
            if (!CollectionUtils.isEmpty(itemList)) {
                List<OTCItemDetail> detailList = itemList.stream().map(
                        otcItem -> convertOTCItemDetail(otcItem)
                                .setOrderNum(otcItem.getOrderNum())
                                .setFinishNum(otcItem.getFinishNum())
                                .build()
                ).collect(Collectors.toList());
                responseBuilder.addAllItems(detailList);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getItems,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getItemsAdmin(OTCGetItemsAdminRequest request, StreamObserver<OTCGetItemsResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetItemsResponse.Builder responseBuilder = OTCGetItemsResponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);

            List<OtcItem> itemList = otcItemService.getItemsAdmin(request);
            List<OTCItemDetail> detailList = itemList.stream().map(
                    otcItem -> convertOTCItemDetail(otcItem)
                            .setOrderNum(otcItem.getOrderNum())
                            .setFinishNum(otcItem.getFinishNum())
                            .build()
            ).collect(Collectors.toList());
            responseBuilder.addAllItems(detailList);

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getItems,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    @Override
    public void getOnlineItems(OTCGetOnlineItemsRequest request, StreamObserver<OTCGetItemsResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {

            OTCGetItemsResponse.Builder responseBuilder = OTCGetItemsResponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            OtcItemOnline otcItemOnline = null;

            long brokerId = request.getBaseRequest().getOrgId();
            long exchangeId = request.getBaseRequest().getExchangeId();
            //查询共享广告
            List<Long> exchangeIds = otcConfigService.listExchangeIdByShareSymbolV2(brokerId, exchangeId, request.getTokenId(), request.getCurrencyId());

            if (CollectionUtils.isEmpty(exchangeIds)) {
                //本交易所深度
                otcItemOnline = otcItemService.getOtcItemOnline(request.getBaseRequest().getExchangeId(),
                        request.getTokenId(), request.getCurrencyId());
            } else {
                //跨交易所广告列表
                otcItemOnline = otcItemService.getOtcItemOnlineWithMultiExchange(exchangeIds, request.getTokenId(), request.getCurrencyId());
            }

            int count = 0;
            if (otcItemOnline != null) {

                final OtcItemOnline onlineItem = otcItemOnline;

                List<OtcItem> itemList = request.getSide() == OrderSideEnum.SELL ? otcItemOnline.getAskList()
                        : otcItemOnline.getBidList();
                if (!CollectionUtils.isEmpty(itemList)) {
                    Map<Long, List<Integer>> paymentTermMap = otcItemOnline.getPaymentTermMap();
                    List<Integer> brokerPaymentConfigList = otcPaymentTermService.getBrokerPaymentConfigList(brokerId);
                    itemList = itemList.stream().filter(otcItem -> {
                        List<Integer> paymentList = paymentTermMap.getOrDefault(otcItem.getAccountId(), Lists.newArrayList());
                        paymentList.retainAll(brokerPaymentConfigList);
                        if (CollectionUtils.isEmpty(request.getPaymentList()))
                            return !CollectionUtils.isEmpty(paymentList);
                        else
                            return !CollectionUtils.isEmpty(paymentList) && paymentList.contains(request.getPaymentValue(0));
                    }).collect(Collectors.toList());
                    /*
                    if (!CollectionUtils.isEmpty(request.getPaymentList())) {
                        int payment = request.getPaymentValue(0);
                        itemList = itemList.stream().filter(otcItem -> {
                            List<Integer> paymentList = paymentTermMap.get(otcItem.getAccountId());
                            return !CollectionUtils.isEmpty(paymentList) && paymentList.contains(payment);
                        }).collect(Collectors.toList());
                    }*/
                    count = itemList.size();
                    int startIndex = (request.getPage() - 1) * request.getSize();
                    int endIndex = request.getPage() * request.getSize();
                    if (startIndex < itemList.size()) {
                        if (endIndex > itemList.size()) {
                            endIndex = itemList.size();
                        }
                        itemList = itemList.subList(startIndex, endIndex);
                    } else {
                        itemList = null;
                    }
                    if (!CollectionUtils.isEmpty(itemList)) {
                        Map<Long, OtcUserInfo> userInfoMap = otcItemOnline.getUserInfoMap();
                        List<OTCItemDetail> detailList = itemList.stream().map(
                                otcItem -> {

                                    OtcBrokerToken obt = onlineItem.findTokenConfig(otcItem.getOrgId(), otcItem.getTokenId());
                                    List<OtcBrokerCurrency> obcs = onlineItem.findCurrencyConfig(otcItem.getOrgId(), otcItem.getCurrencyId());
                                    otcItem.setTokenConfig(obt);
                                    otcItem.setCurrencyConfig(obcs);

                                    OtcUserInfo userInfo = userInfoMap.get(otcItem.getAccountId());
                                    //return convertOTCItemDetail(otcItem,obt,obcs)
                                    OTCItemDetail.Builder builder = convertOTCItemDetail(otcItem);
                                    if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(paymentTermMap.get(otcItem.getAccountId()))) {
                                        builder.addAllPaymentsValue(paymentTermMap.get(otcItem.getAccountId()));
                                    }

                                    if (Objects.nonNull(userInfo)) {
                                        builder.setNickName(userInfo.getNickName())
                                                .setRecentOrderNum(userInfo.getOrderTotalNumberDay30Safe())
                                                .setRecentExecuteNum(userInfo.getOrderFinishNumberDay30Safe());
                                    }

                                    return builder.build();


/*                                            .setRecentOrderNum(userInfo.getRecentOrderNum())
                                            .setRecentExecuteNum(userInfo.getRecentExecuteNum())*/

                                }
                        ).collect(Collectors.toList());
                        responseBuilder.addAllItems(detailList);
                    }
                }
            }
            responseBuilder.setCount(count);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("list online item,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getItem(OTCGetItemInfoRequest request, StreamObserver<OTCGetItemInfoResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OtcItem otcItem = otcItemService.getOtcItemById(request.getItemId());
            OTCGetItemInfoResponse.Builder responseBuilder = OTCGetItemInfoResponse.newBuilder();
            if (otcItem == null) {
                responseBuilder.setResult(OTCResult.ITEM_NOT_EXIST);
            } else {
                OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfo(otcItem.getAccountId());

                List<OtcPaymentTerm> paymentTermList = otcPaymentTermService.getVisibleOtcPaymentTerm(
                        otcItem.getAccountId(), userInfo.getOrgId());

                OTCItemDetail.Builder builder = convertOTCItemDetail(otcItem);
                List<OTCPaymentTerm> otcPaymentTermList = paymentTermList.stream()
                        .map(ConvertUtil::convertToOTCPaymentTerm).collect(Collectors.toList());
                builder.setNickName(userInfo.getNickName());
                builder.addAllPaymentTerms(otcPaymentTermList);
                responseBuilder.setItem(builder);
                responseBuilder.setResult(OTCResult.SUCCESS);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get item,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getDepth(OTCGetDepthRequest request, StreamObserver<OTCGetDepthResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetDepthResponse.Builder responseBuilder = OTCGetDepthResponse.newBuilder().setResult(OTCResult.SUCCESS);
            OtcItemOnline otcItemOnline = otcItemService.getOtcItemOnline(request.getBaseRequest().getExchangeId(),
                    request.getTokenId(), request.getCurrencyId());
            if (otcItemOnline != null) {
                responseBuilder.addAllAsks(convertOTCPriceLevel(otcItemOnline.getAsks()))
                        .addAllBids(convertOTCPriceLevel(otcItemOnline.getBids()));
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getDepth,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void offlineItem(OTCOfflineItemRequest request, StreamObserver<OTCOfflineItemResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        OTCOfflineItemResponse.Builder responseBuilder = OTCOfflineItemResponse.newBuilder();
        try {

            otcItemService.offlineItem(request.getAccountId(), request.getItemId(),
                    request.getBaseRequest().getExchangeId());
            OtcItem item = otcItemService.getOtcItemWithoutConfigById(request.getItemId());
            sendRefreshItemEvent(item);
            responseBuilder.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (ItemNotExistException e) {
            log.warn("offline item failed", e);

            responseBuilder.setResult(OTCResult.ITEM_NOT_EXIST);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (PermissionDeniedException e) {
            log.warn("offline item failed", e);

            responseBuilder.setResult(OTCResult.PERMISSION_DENIED);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("offlineItem,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void onlineItem(OTCOnlineItemRequest request, StreamObserver<OTCOnlineItemResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        OTCOnlineItemResponse.Builder responseBuilder = OTCOnlineItemResponse.newBuilder();
        try {
            otcItemService.onlineItem(request.getAccountId(), request.getItemId(),
                    request.getBaseRequest().getExchangeId());

            OtcItem item = otcItemService.getOtcItemWithoutConfigById(request.getItemId());
            sendRefreshItemEvent(item);

            responseBuilder.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (ItemNotExistException e) {
            log.warn("online item failed", e);

            responseBuilder.setResult(OTCResult.ITEM_NOT_EXIST);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (PermissionDeniedException e) {
            log.warn("online item failed", e);

            responseBuilder.setResult(OTCResult.PERMISSION_DENIED);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get Online Item,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

/*    @Override
    public void getLastPrice(OTCGetLastPriceRequest request, StreamObserver<OTCGetLastPriceResponse> responseObserver) {

        Stopwatch sw=Stopwatch.createStarted();
        try{

            OTCGetLastPriceResponse.Builder responseBuilder = OTCGetLastPriceResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS);
            OtcItemOnline otcItemOnline = otcItemService.getOtcItemOnline(request.getBaseRequest().getExchangeId(),
                    request.getTokenId(), request.getCurrencyId());
            if (otcItemOnline != null && otcItemOnline.getLastPrice() != null) {
                responseBuilder.setLastPrice(DecimalUtil.fromBigDecimal(otcItemOnline.getLastPrice()));
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        }catch (Exception e){
            log.error(e.getMessage(),e);
            responseObserver.onError(e);
        }finally {
            log.info("get last price,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }*/


    @Override
    public void getLastPrice(OTCGetLastPriceRequest request, StreamObserver<OTCGetLastPriceResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetLastPriceResponse.Builder responseBuilder = OTCGetLastPriceResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS);
            if ("USDT".equals(request.getTokenId()) && request.getSide().getNumber() != OrderSideEnum.UNKNOWN.getNumber() && request.getCurrencyId().equals("CNY")) {
                BigDecimal price = grpcServerService.getOTCUsdtIndex(request.getTokenId(), request.getCurrencyId(), OtcSide.forNumber(request.getSide().getNumber()));
                responseBuilder.setLastPrice(DecimalUtil.fromBigDecimal(price));
            } else {
                Map<String, BigDecimal> map = grpcServerService.getLastPrice(request.getBaseRequest().getExchangeId(),
                        request.getTokenId());
                if (MapUtils.isNotEmpty(map) && map.containsKey(request.getCurrencyId())) {
                    BigDecimal price = map.get(request.getCurrencyId());
                    responseBuilder.setLastPrice(DecimalUtil.fromBigDecimal(price));
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get last price,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getItemIdByClientId(OTCGetItemIdRequest request,
                                    StreamObserver<OTCGetItemIdResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetItemIdResponse.Builder responseBuilder = OTCGetItemIdResponse.newBuilder();
            // 处理重复请求
            Long itemId = otcItemService.getItemIdByClient(request.getOrgId(), request.getClientItemId());
            if (itemId != null && itemId > 0) {
                responseBuilder.setResult(OTCResult.SUCCESS);
                responseBuilder.setItemId(itemId);
            } else {
                responseBuilder.setResult(OTCResult.ITEM_NOT_EXIST);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get itemId by client Id,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void findItemsByIds(FindItemsByIdsRequest request,
                               StreamObserver<FindItemsByIdsReponse> observer) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            FindItemsByIdsReponse.Builder builder = FindItemsByIdsReponse.newBuilder()
                    .setResult(OTCResult.SUCCESS);
            List<OtcItem> list = otcItemService.findItemsByIds(request.getExchangeIdList(),
                    request.getItemIdList(), request.getOrgIdList());

            List<ItemBrief> briefs = list.stream().map(i -> transformBrief(i)).collect(Collectors.toList());

            builder.addAllItems(briefs);
            observer.onNext(builder.build());
            observer.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            observer.onError(e);
        } finally {
            log.info("list item by Ids,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    private ItemBrief transformBrief(OtcItem item) {

        return ItemBrief.newBuilder()
                .setAccountId(item.getAccountId())
                .setClientId(item.getClientItemId())
                .setItemId(item.getId())
                .setOrgId(item.getOrgId())
                .setItemStatus(OTCItemStatusEnum.forNumber(item.getStatus()))
                .build();
    }

    private List<OTCPriceLevel> convertOTCPriceLevel(List<PriceLevel> priceLevelList) {
        if (CollectionUtils.isEmpty(priceLevelList)) {
            return Lists.newArrayList();
        }
        if (priceLevelList.size() > 5) {
            priceLevelList = priceLevelList.subList(0, 5);
        }
        return priceLevelList.stream().map(priceLevel -> OTCPriceLevel.newBuilder()
                .setPrice(DecimalUtil.fromBigDecimal(priceLevel.getPrice()))
                .setQuantity(DecimalUtil.fromBigDecimal(priceLevel.getQuantity()))
                .setSize(priceLevel.getSize())
                .build()
        ).collect(Collectors.toList());
    }

    private OTCItemDetail.Builder convertOTCItemDetail(OtcItem otcItem) {

        OTCItemDetail.Builder builder = OTCItemDetail.newBuilder()
                .setItemId(otcItem.getId())
                .setAccountId(otcItem.getAccountId())
                .setItemStatusValue(otcItem.getStatus())
                .setOrgId(otcItem.getOrgId())
                .setTokenId(otcItem.getTokenId())
                .setCurrencyId(otcItem.getCurrencyId())
                .setSideValue(otcItem.getSide())
                .setPrice(DecimalUtil.fromBigDecimal(otcItem.getPrice()))
                .setQuantity(DecimalUtil.fromBigDecimal(otcItem.getQuantity()))
                .setLastQuantity(DecimalUtil.fromBigDecimal(otcItem.getLastQuantity()))
                .setFrozenQuantity(DecimalUtil.fromBigDecimal(otcItem.getFrozenQuantity()))
                .setExecutedQuantity(DecimalUtil.fromBigDecimal(otcItem.getExecutedQuantity()))
                .setPriceTypeValue(otcItem.getPriceType())
                .setPremium(DecimalUtil.fromBigDecimal(otcItem.getPremium()))
                .setMinAmount(DecimalUtil.fromBigDecimal(otcItem.getMinAmount()))
                .setMaxAmount(DecimalUtil.fromBigDecimal(otcItem.getMaxAmount()))
                .setRemark(StringUtils.isNotBlank(otcItem.getRemark()) ? otcItem.getRemark() : "")
                .setAutoReply(StringUtils.isNotBlank(otcItem.getAutoReply()) ? otcItem.getAutoReply() : "")
                .setFee(DecimalUtil.fromBigDecimal(otcItem.getFee() != null ? otcItem.getFee() : BigDecimal.ZERO))
                .setCreateDate(otcItem.getCreateDate().getTime())
                .setUpdateDate(otcItem.getUpdateDate().getTime())
                .setExchangeId(otcItem.getExchangeId());

        if (Objects.nonNull(otcItem.getTokenConfig())) {
            OtcBrokerToken obt = otcItem.getTokenConfig();
            builder.setTokenConfig(transform(obt));
        }

        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(otcItem.getCurrencyConfig())) {
            List<CurrencyConfig> currencyConfigs = transform(otcItem.getCurrencyConfig());
            builder.addAllCurrencyConfig(currencyConfigs);
        }

        return builder;
    }

    private OTCItemDetail.Builder convertOTCItemDetail(OtcItem otcItem, OtcBrokerToken obt, List<OtcBrokerCurrency> obcs) {
        OTCItemDetail.Builder builder = this.convertOTCItemDetail(otcItem);
        if (Objects.nonNull(obt)) {
            builder.setTokenConfig(transform(obt));
        }

        if (!CollectionUtils.isEmpty(obcs)) {
            List<CurrencyConfig> currencyConfigs = transform(obcs);
            builder.addAllCurrencyConfig(currencyConfigs);
        }

        return builder;
    }

    private TokenConfig transform(OtcBrokerToken obt) {
        return TokenConfig.newBuilder()
                .setScale(obt.getScale())
                .setSequence(obt.getSequence())
                .setDownRange(CommonUtil.BigDecimalToString(obt.getDownRange()))
                .setUpRange(CommonUtil.BigDecimalToString(obt.getUpRange()))
                .setMaxQuote(CommonUtil.BigDecimalToString(obt.getMaxQuote()))
                .setMinQuote(CommonUtil.BigDecimalToString(obt.getMinQuote()))
                .build();
    }

    private List<CurrencyConfig> transform(List<OtcBrokerCurrency> obcs) {

        return obcs.stream().map(currency -> CurrencyConfig.newBuilder()
                .setAmountScale(currency.getAmountScale())
                .setScale(currency.getScale())
                .setMaxQuote(CommonUtil.BigDecimalToString(currency.getMaxQuote()))
                .setMinQuote(CommonUtil.BigDecimalToString(currency.getMinQuote()))
                .setLanguage(currency.getLanguage())
                .setName(currency.getName())
                .build()
        ).collect(Collectors.toList());

    }

    @Override
    public void getTradeFeeRate(GetTradeFeeRateRequest request, StreamObserver<GetTradeFeeRateResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {

            GetTradeFeeRateResponse.Builder response = GetTradeFeeRateResponse.newBuilder();
            List<OtcTradeFeeRate> tradeFeeRates
                    = otcTradeFeeService.queryAllOtcTradeFee();
            if (!CollectionUtils.isEmpty(tradeFeeRates)) {
                List<OTCTradeFeeRate> detailList = tradeFeeRates.stream().map(
                        otcTradeFeeRate -> convertOTCTradeFeeRate(otcTradeFeeRate)
                                .build()
                ).collect(Collectors.toList());
                List<OtcDepthShareBrokerWhiteList> share
                        = this.otcDepthShareBrokerWhiteListMapper.queryAll();
                List<OTCShareWhiteList> otcShareWhiteList = new ArrayList<>();
                share.forEach(s -> {
                    otcShareWhiteList.add(convertOTCShareWhiteList(s));
                });
                response.addAllRate(detailList);
                response.addAllList(otcShareWhiteList);
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get trade fee rate,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getTradeFeeRateByTokenId(GetTradeFeeRateByTokenIdRequest request, StreamObserver<GetTradeFeeRateByTokenIdResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {

            GetTradeFeeRateByTokenIdResponse.Builder response = GetTradeFeeRateByTokenIdResponse.newBuilder();

            OtcTradeFeeRate otcTradeFeeRate
                    = otcTradeFeeService.queryOtcTradeFeeByTokenId(request.getOrgId(), request.getTokenId());

            if (otcTradeFeeRate != null) {
                response.setRate(convertOTCTradeFeeRate(otcTradeFeeRate));
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get trade fee rate by token ,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    private OTCTradeFeeRate.Builder convertOTCTradeFeeRate(OtcTradeFeeRate otcTradeFeeRate) {
        return OTCTradeFeeRate.newBuilder()
                .setMakerBuyFeeRate(DecimalUtil.fromBigDecimal(otcTradeFeeRate.getMakerBuyFeeRate()))
                .setMakerSellFeeRate(DecimalUtil.fromBigDecimal(otcTradeFeeRate.getMakerSellFeeRate()))
                .setOrgId(otcTradeFeeRate.getOrgId())
                .setTokenId(otcTradeFeeRate.getTokenId());
    }

    private OTCShareWhiteList convertOTCShareWhiteList(OtcDepthShareBrokerWhiteList shareBrokerWhiteList) {
        return OTCShareWhiteList.newBuilder()
                .setOrgId(shareBrokerWhiteList.getBrokerId())
                .setIsShare(1).build();
    }

    private void sendRefreshItemEvent(OtcItem item) {
        if (Objects.nonNull(item)) {
            //刷新缓存
            RefreshItemEvent event = new RefreshItemEvent();
            event.setExchangeId(item.getExchangeId());
            event.setTokenId(item.getTokenId());
            event.setCurrencyId(item.getCurrencyId());

            applicationEventPublisher.publishEvent(event);
        }
    }
}