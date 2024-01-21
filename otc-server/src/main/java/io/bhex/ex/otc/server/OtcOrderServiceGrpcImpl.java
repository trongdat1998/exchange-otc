package io.bhex.ex.otc.server;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.FindOrdersByItemIdsReponse;
import io.bhex.ex.otc.FindOrdersByItemIdsRequest;
import io.bhex.ex.otc.FindOrdersByOrderIdsReponse;
import io.bhex.ex.otc.FindOrdersByOrderIdsRequest;
import io.bhex.ex.otc.GetLastNewOrderIdResponse;
import io.bhex.ex.otc.OTCDeleteOrderRequest;
import io.bhex.ex.otc.OTCDeleteOrderResponse;
import io.bhex.ex.otc.OTCGetMessageRequest;
import io.bhex.ex.otc.OTCGetMessageResponse;
import io.bhex.ex.otc.OTCGetOrderIdRequest;
import io.bhex.ex.otc.OTCGetOrderIdResponse;
import io.bhex.ex.otc.OTCGetOrderInfoRequest;
import io.bhex.ex.otc.OTCGetOrderInfoResponse;
import io.bhex.ex.otc.OTCGetOrdersRequest;
import io.bhex.ex.otc.OTCGetOrdersResponse;
import io.bhex.ex.otc.OTCGetPendingCountRequest;
import io.bhex.ex.otc.OTCGetPendingCountResponse;
import io.bhex.ex.otc.OTCGetPendingOrdersRequest;
import io.bhex.ex.otc.OTCGetPendingOrdersResponse;
import io.bhex.ex.otc.OTCHandleOrderRequest;
import io.bhex.ex.otc.OTCHandleOrderResponse;
import io.bhex.ex.otc.OTCNewOrderRequest;
import io.bhex.ex.otc.OTCNewOrderResponse;
import io.bhex.ex.otc.OTCNormalOrderRequest;
import io.bhex.ex.otc.OTCNormalOrderResponse;
import io.bhex.ex.otc.OTCOrderContact;
import io.bhex.ex.otc.OTCOrderContactRequest;
import io.bhex.ex.otc.OTCOrderContactResponse;
import io.bhex.ex.otc.OTCOrderDetail;
import io.bhex.ex.otc.OTCOrderHandleTypeEnum;
import io.bhex.ex.otc.OTCOrderPaymentTerm;
import io.bhex.ex.otc.OTCOrderServiceGrpc;
import io.bhex.ex.otc.OTCOrderStatusEnum;
import io.bhex.ex.otc.OTCPaymentTerm;
import io.bhex.ex.otc.OTCPaymentTypeEnum;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OrderBrief;
import io.bhex.ex.otc.OrderExt;
import io.bhex.ex.otc.ShareOrderAppealInfo;
import io.bhex.ex.otc.ShareOrderAppealResponse;
import io.bhex.ex.otc.util.DecimalUtil;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcItem;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.OtcOrderDepthShareAppeal;
import io.bhex.ex.otc.entity.OtcOrderExt;
import io.bhex.ex.otc.entity.OtcOrderPayInfo;
import io.bhex.ex.otc.entity.OtcPaymentTerm;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.enums.OrderStatus;
import io.bhex.ex.otc.enums.Side;
import io.bhex.ex.otc.exception.BalanceNotEnoughExcption;
import io.bhex.ex.otc.exception.CancelOrderMaxTimesException;
import io.bhex.ex.otc.exception.CrossExchangeException;
import io.bhex.ex.otc.exception.DifferentTradeException;
import io.bhex.ex.otc.exception.ExchangeSelfException;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.exception.NicknameNotSetException;
import io.bhex.ex.otc.exception.NonPersonPaymentException;
import io.bhex.ex.otc.exception.OrderMaxException;
import io.bhex.ex.otc.exception.OrderNotExistException;
import io.bhex.ex.otc.exception.ParamErrorException;
import io.bhex.ex.otc.exception.PaymentDoesNotMatchException;
import io.bhex.ex.otc.exception.QuantityNotEnoughException;
import io.bhex.ex.otc.exception.RiskControlInterceptionException;
import io.bhex.ex.otc.exception.UserBuyTradeLimitException;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.service.OtcItemService;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.service.OtcPaymentTermService;
import io.bhex.ex.otc.service.OtcUserInfoService;
import io.bhex.ex.otc.service.order.OrderMsg;
import io.bhex.ex.otc.util.ConvertUtil;
import io.bhex.ex.proto.OrderSideEnum;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;

/**
 * 订单grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcOrderServiceGrpcImpl extends OTCOrderServiceGrpc.OTCOrderServiceImplBase {

    @Autowired
    private OtcOrderService otcOrderService;

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Autowired
    private GrpcServerService grpcServerService;

    @Autowired
    private OtcItemService otcItemService;

    @Autowired
    private OtcPaymentTermService otcPaymentTermService;

    @Resource(name = "otcOrderCreateTaskExecutor")
    private TaskExecutor otcOrderCreateTaskExecutor;


    private Map<OTCOrderHandleTypeEnum, OrderStatus> handleTypeStatusMap = Maps.newEnumMap(OTCOrderHandleTypeEnum.class);

    public static final double[] CONTROLLER_TIME_BUCKETS = new double[]{
            .1, 1, 2, 3, 5,
            10, 20, 30, 50, 75,
            100, 200, 500, 1000, 2000, 10000
    };

    private static final Histogram OTC_HANDLE_ORDER_METRICS = Histogram.build()
            .namespace("otc")
            .subsystem("handle_order")
            .name("otc_handle_order_delay_milliseconds")
            .labelNames("process_name")
            .buckets(CONTROLLER_TIME_BUCKETS)
            .help("Histogram of stream handle latency in milliseconds")
            .register();


    @PostConstruct
    public void init() {
        handleTypeStatusMap.put(OTCOrderHandleTypeEnum.APPEAL, OrderStatus.APPEAL);
        handleTypeStatusMap.put(OTCOrderHandleTypeEnum.CANCEL, OrderStatus.CANCEL);
        handleTypeStatusMap.put(OTCOrderHandleTypeEnum.FINISH, OrderStatus.FINISH);
    }

    @Override
    public void addOrder(OTCNewOrderRequest request, StreamObserver<OTCNewOrderResponse> responseObserver) {
        OTCNewOrderResponse.Builder responseBuilder = OTCNewOrderResponse.newBuilder();
        responseBuilder.setClientOrderId(request.getClientOrderId());
        Stopwatch sw = Stopwatch.createStarted();
        // 持久化订单
        try {
            OrderSideEnum side = request.getSide();
            if (side != OrderSideEnum.BUY && side != OrderSideEnum.SELL) {
                log.warn("invalid side,clientOrderId={},side={}", request.getClientOrderId(), request.getSide());
                throw new ParamErrorException("invalid side,clientId=" + request.getClientOrderId());
            }
            // 处理重复请求
            Long orderId = otcOrderService.getOrderIdByClient(request.getOrgId(), request.getClientOrderId());
            if (orderId != null && orderId > 0) {
                responseBuilder.setResult(OTCResult.SUCCESS);
                responseBuilder.setOrderId(orderId);
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

            OtcOrder otcOrder = OtcOrder.builder()
                    .exchangeId(request.getBaseRequest().getExchangeId())
                    .orgId(request.getBaseRequest().getOrgId())
                    .clientOrderId(request.getClientOrderId())
                    .accountId(request.getAccountId())
                    .itemId(request.getItemId())
                    .tokenId(request.getTokenId())
                    .quantity(DecimalUtil.toBigDecimal(request.getQuantity()))
                    .price(DecimalUtil.toBigDecimal(request.getPrice()))
                    .amount(DecimalUtil.toBigDecimal(request.getAmount()))
                    .fee(BigDecimal.ZERO)
                    // 6位数字
                    .payCode(String.valueOf(RandomUtils.nextInt(100000, 999999)))
                    .side(request.getSide().getNumber())
                    .status(OrderStatus.INIT.getStatus())
                    .language(request.getBaseRequest().getLanguage())
                    .isBusiness(request.getIsBusiness())
                    .riskBalanceType(request.getRiskBalanceTypeValue())
                    .build();

            orderId = otcOrderService.createOtcOrder(otcOrder, request.getUserFirstName(), request.getUserSecondName());
            log.info("createOrder updateContact accountId {} userFirstName {} userSecondName {}", request.getAccountId(), request.getUserFirstName(), request.getUserSecondName());
            //异步同步用户信息
            otcOrderCreateTaskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    otcUserInfoService.updateContact(request.getAccountId(), request.getMobile(), request.getEmail(), request.getUserFirstName(), request.getUserSecondName());
                }
            });
            responseBuilder.setResult(OTCResult.SUCCESS);
            responseBuilder.setOrderId(orderId);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (CancelOrderMaxTimesException e) {
            log.warn("create order failed", e);
            responseBuilder.setResult(OTCResult.CANCEL_ORDER_MAX_TIMES);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (NicknameNotSetException e) {
            log.warn("create order failed", e);
            responseBuilder.setResult(OTCResult.NICK_NAME_NOT_SET);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (ItemNotExistException e) {
            log.warn("create order failed", e);
            responseBuilder.setResult(OTCResult.ITEM_NOT_EXIST);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (ParamErrorException e) {
            log.warn("create order failed", e);
            responseBuilder.setResult(OTCResult.PARAM_ERROR);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (ExchangeSelfException e) {
            log.warn("create order failed", e);
            responseBuilder.setResult(OTCResult.CAN_NOT_EXCHANGE_SELF);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (QuantityNotEnoughException e) {
            log.warn("create order failed", e);
            responseBuilder.setResult(OTCResult.LAST_QUANTITY_NOT_ENOUGH);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (CrossExchangeException e) {
            log.warn(e.getMessage(), e);
            responseBuilder.setResult(OTCResult.OTC_CROSS_EXCHANG_FORBIDDEN);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (OrderMaxException e) {
            log.warn(e.getMessage(), e);
            responseBuilder.setResult(OTCResult.MAX_ORDER_LIMIT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (PaymentDoesNotMatchException e) {
            log.warn(e.getMessage(), e);
            responseBuilder.setResult(OTCResult.PAYMENT_DOES_NOT_MATCH);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (BalanceNotEnoughExcption e) {
            log.warn(e.getMessage(), e);
            responseBuilder.setResult(OTCResult.BALANCE_NOT_ENOUGH);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (DifferentTradeException e) {
            log.warn(e.getMessage(), e);
            responseBuilder.setResult(OTCResult.MERCHANT_ORDER_LIMIT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (UserBuyTradeLimitException e) {
            log.warn(e.getMessage(), e);
            responseBuilder.setResult(OTCResult.USER_BUY_ORDER_USDT_DAY_LIMIT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (NonPersonPaymentException e) {
            log.warn("NonPersonPaymentException failed", e);
            responseBuilder.setResult(OTCResult.NON_PERSON_PAYMENT_LIMIT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (RiskControlInterceptionException e) {
            log.warn("RiskControlInterceptionException failed", e);
            responseBuilder.setResult(OTCResult.RISK_CONTROL_INTERCEPTION_LIMIT);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("addOrder,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }


    @Override
    public void setOrderToNormal(OTCNormalOrderRequest request,
                                 StreamObserver<OTCNormalOrderResponse> responseObserver) {
        OTCNormalOrderResponse.Builder response = OTCNormalOrderResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();

        try {
            otcOrderService.updateOrderStatusToNormal(request.getOrderId());
            response.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

        } catch (OrderNotExistException e) {
            log.warn("delete order failed", e);
            response.setResult(OTCResult.ORDER_NOT_EXIST);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("setOrderToNormal,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    /**
     * 卖单冻结失败调用此接口，因此此接口不能做冻结返还
     */
    @Override
    public void setOrderToDelete(OTCDeleteOrderRequest request,
                                 StreamObserver<OTCDeleteOrderResponse> responseObserver) {
        OTCDeleteOrderResponse.Builder response = OTCDeleteOrderResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OtcBalanceFlow balanceFlow = otcOrderService.updateOrderStatusToDelete(request.getOrderId());
            if (balanceFlow != null) {
                grpcServerService.sendBalanceChangeRequest(balanceFlow);
            }
            response.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (OrderNotExistException e) {
            log.warn("delete order failed", e);
            response.setResult(OTCResult.ORDER_NOT_EXIST);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("setOrderToDelete,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    @Override
    public void handleOrder(OTCHandleOrderRequest request,
                            StreamObserver<OTCHandleOrderResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        OTCHandleOrderResponse.Builder response = OTCHandleOrderResponse.newBuilder();
        try {
            OTCResult result = execHandleOrder(request);
            response.setResult(result);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("proc order,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }


    private OTCResult execHandleOrder(OTCHandleOrderRequest request) {

        OTCResult result = null;
        OtcOrder otcOrder = otcOrderService.getOtcOrderById(request.getOrderId());
        if (otcOrder == null) {
            return OTCResult.ORDER_NOT_EXIST;
        }

        List<OtcBalanceFlow> balanceFlowList = null;
        switch (request.getType()) {
            case CANCEL:
                long cancelStartTimestamp = System.currentTimeMillis();
                // 只有付款方可以撤单，只能撤未支付的单
                if (!otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) ||
                        (otcOrder.getSide().equals(Side.BUY.getCode()) &&
                                !otcOrder.getAccountId().equals(request.getAccountId())) ||
                        (otcOrder.getSide().equals(Side.SELL.getCode()) &&
                                !otcOrder.getTargetAccountId().equals(request.getAccountId()))) {
                    result = OTCResult.PERMISSION_DENIED;
                    break;
                }
                // 撤单
                OtcBalanceFlow otcBalanceFlow = otcOrderService.cancelOrder(otcOrder);
                otcOrderService.increaseCancelNum(request.getAccountId());
                if (otcBalanceFlow != null) {
                    balanceFlowList = Lists.newArrayList(otcBalanceFlow);
                }
                result = OTCResult.SUCCESS;
                OTC_HANDLE_ORDER_METRICS.labels("cancelOrderCheck").observe(System.currentTimeMillis() - cancelStartTimestamp);
                break;
            case PAY:
                long payStartTimestamp = System.currentTimeMillis();
                // 只有付款方可以确认支付，只能支付未支付的单
                if (!otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) ||
                        (otcOrder.getSide().equals(Side.BUY.getCode()) &&
                                !otcOrder.getAccountId().equals(request.getAccountId())) ||
                        (otcOrder.getSide().equals(Side.SELL.getCode()) &&
                                !otcOrder.getTargetAccountId().equals(request.getAccountId()))) {
                    result = OTCResult.PERMISSION_DENIED;
                    break;
                }
                if (request.getPayment() == null) {
                    result = OTCResult.PARAM_ERROR;
                    break;
                }

                //兼容老版本传ID则新版处理 老版本走之前逻辑
                // 支付
                int res = 0;
                if (request.getId() > 0) {
                    res = otcOrderService.payOrderPayById(otcOrder, request.getId());
                } else {
                    res = otcOrderService.payOrder(otcOrder, request.getPaymentValue());
                }
                result = (res > 0 ? OTCResult.SUCCESS : OTCResult.PAYMENT_NOT_SUPPORT);
                OTC_HANDLE_ORDER_METRICS.labels("payOrderCheck").observe(System.currentTimeMillis() - payStartTimestamp);
                break;
            case APPEAL:
                long appealStartTimestamp = System.currentTimeMillis();
                // 双方都可以申诉，只能申诉已支付过但未经确认放币的单
                if (!otcOrder.getStatus().equals(OrderStatus.UNCONFIRM.getStatus()) ||
                        (!otcOrder.getAccountId().equals(request.getAccountId()) &&
                                !otcOrder.getTargetAccountId().equals(request.getAccountId()))) {
                    result = OTCResult.PERMISSION_DENIED;
                    break;
                }
                // 申诉
                otcOrder.setAppealType(request.getAppealTypeValue());
                otcOrder.setAppealContent(request.getAppealContent());
                otcOrderService.appealOrder(otcOrder, request.getAccountId());
                result = OTCResult.SUCCESS;
                OTC_HANDLE_ORDER_METRICS.labels("appealOrderCheck").observe(System.currentTimeMillis() - appealStartTimestamp);
                break;
            case FINISH:
                long finishStartTimestamp = System.currentTimeMillis();
                // 只有卖币方可以确认放币结束订单，只能结束已支付等待确认放币的单
                if ((!otcOrder.getStatus().equals(OrderStatus.UNCONFIRM.getStatus())
                        && !otcOrder.getStatus().equals(OrderStatus.APPEAL.getStatus())) ||
                        (otcOrder.getSide().equals(Side.SELL.getCode())
                                && !otcOrder.getAccountId().equals(request.getAccountId())) ||
                        (otcOrder.getSide().equals(Side.BUY.getCode())
                                && !otcOrder.getTargetAccountId().equals(request.getAccountId()))) {
                    result = OTCResult.PERMISSION_DENIED;
                    break;
                }
                // 放币结束订单
                balanceFlowList = otcOrderService.finishOrder(otcOrder);
                result = OTCResult.SUCCESS;
                OTC_HANDLE_ORDER_METRICS.labels("finishOrderCheck").observe(System.currentTimeMillis() - finishStartTimestamp);
                break;
            default:
                result = OTCResult.PERMISSION_DENIED;
                break;
        }
        if (!CollectionUtils.isEmpty(balanceFlowList)) {
            balanceFlowList.forEach(grpcServerService::sendBalanceChangeRequest);
        }
        return result;
    }


    /**
     * broker-api调用
     */
    @Override
    public void getOrders(OTCGetOrdersRequest request, StreamObserver<OTCGetOrdersResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        OTCGetOrdersResponse.Builder responseBuilder = OTCGetOrdersResponse.newBuilder();
        responseBuilder.setResult(OTCResult.SUCCESS);
        //检测客户端失效
        if (Context.current().isCancelled()) {
            log.warn("get orders, cancelled by client,request={}", ConvertUtil.messageToString(request));
            responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client")
                    .asRuntimeException());
            return;
        }
        try {
            List<OtcOrder> otcOrderList = otcOrderService.getOtcOrderList(request.getAccountId(),
                    request.getOrderStatusValueList(),
                    request.getBeginTime() > 0 ? new Date(request.getBeginTime()) : null,
                    request.getEndTime() > 0 ? new Date(request.getEndTime()) : null,
                    request.getTokenId(),
                    CollectionUtils.isEmpty(request.getSideList()) ? null : request.getSideValue(0),
                    request.getPage(),
                    request.getSize());
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                Set<Long> accountIds = otcOrderList.stream().flatMap(otcOrder ->
                        Stream.of(otcOrder.getAccountId(), otcOrder.getTargetAccountId())
                ).collect(Collectors.toSet());
                Map<Long, String> nameMap = otcUserInfoService.listUserName(Lists.newArrayList(accountIds));
                List<OTCOrderDetail> orderDetailList = otcOrderList.stream().map(
                        otcOrder -> convertOTCOrderDetail(otcOrder, request.getAccountId())
                                .setNickName(nameMap.get(otcOrder.getAccountId()))
                                .setTargetNickName(nameMap.get(otcOrder.getTargetAccountId()))
                                .build()
                ).collect(Collectors.toList());

                responseBuilder.addAllOrders(orderDetailList);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get orders,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getOrderByFromId(OTCGetOrdersRequest request, StreamObserver<OTCGetOrdersResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        OTCGetOrdersResponse.Builder responseBuilder = OTCGetOrdersResponse.newBuilder();
        responseBuilder.setResult(OTCResult.SUCCESS);
        //检测客户端失效
        if (Context.current().isCancelled()) {
            log.warn("get orders, cancelled by client,request={}", ConvertUtil.messageToString(request));
            responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client")
                    .asRuntimeException());
            return;
        }


        Integer status = request.getOrderStatusCount() > 0 ? request.getOrderStatus(0).getNumber() : null;
        Integer side = request.getSideCount() > 0 ? request.getSide(0).getNumber() : null;
        try {
            List<OtcOrder> otcOrderList = otcOrderService.getOtcOrderListByFromId(request.getBaseRequest().getOrgId(),
                    request.getAccountId(),
                    request.getTokenId(),
                    status,
                    side,
                    request.getBeginTime() > 0 ? new Timestamp(request.getBeginTime()) : null,
                    request.getEndTime() > 0 ? new Timestamp(request.getEndTime()) : null,
                    request.getFromId(),
                    request.getLastId(),
                    request.getLimit());
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                Set<Long> accountIds = otcOrderList.stream().flatMap(otcOrder ->
                        Stream.of(otcOrder.getAccountId(), otcOrder.getTargetAccountId())
                ).collect(Collectors.toSet());
                Map<Long, String> nameMap = otcUserInfoService.listUserName(Lists.newArrayList(accountIds));
                List<OTCOrderDetail> orderDetailList = otcOrderList.stream().map(
                        otcOrder -> convertOTCOrderDetail(otcOrder, request.getAccountId())
                                .setNickName(nameMap.get(otcOrder.getAccountId()))
                                .setTargetNickName(nameMap.get(otcOrder.getTargetAccountId()))
                                .build()
                ).collect(Collectors.toList());

                responseBuilder.addAllOrders(orderDetailList);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get orders,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }


    @Override
    public void getOrderInfo(OTCGetOrderInfoRequest request, StreamObserver<OTCGetOrderInfoResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OtcOrder otcOrder = otcOrderService.getOtcOrderWithExtById(request.getOrderId());
            OTCGetOrderInfoResponse.Builder responseBuilder = OTCGetOrderInfoResponse.newBuilder();
            if (otcOrder == null || (request.getAccountId() > 0 && !otcOrder.getAccountId().equals(request.getAccountId())
                    && !otcOrder.getTargetAccountId().equals(request.getAccountId()))) {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
            } else {
                Map<Long, OtcUserInfo> userInfoMap = otcUserInfoService.getOtcUserInfoMap(Lists.newArrayList(Arrays.asList(otcOrder.getAccountId(), otcOrder.getTargetAccountId())));

                OtcItem otcItem = otcItemService.getOtcItemById(otcOrder.getItemId());

                long buyer, buyerOrgId, seller, sellerOrgId;
                // 判断买卖双方
                if (otcOrder.getSide().equals(Side.BUY.getCode())) {
                    buyer = otcOrder.getAccountId();
                    seller = otcOrder.getTargetAccountId();
                } else {
                    buyer = otcOrder.getTargetAccountId();
                    seller = otcOrder.getAccountId();
                }
                buyerOrgId = userInfoMap.get(buyer).getOrgId();
                sellerOrgId = userInfoMap.get(seller).getOrgId();
                Set<Integer> paymentIntersectionList = otcPaymentTermService.getBrokerPaymentConfigIntersectionList(buyerOrgId, sellerOrgId);

                long accountId = request.getAccountId() > 0 ? request.getAccountId() : otcOrder.getAccountId();
                List<OtcPaymentTerm> paymentTermList = new ArrayList<>();
                // 未支付之前展示收款方可用的支付方式，支付之后展示收款方所有支付方式
                if (otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) ||
                        otcOrder.getStatus().equals(OrderStatus.CANCEL.getStatus())) {
                    paymentTermList = otcPaymentTermService.getVisibleOtcPaymentTerm(seller);
                } else {
                    paymentTermList = otcPaymentTermService.getOtcPaymentTerm(seller);
                }

                //Seller兼容老版本 每个支付种类只保留一种支付
                if (paymentTermList != null && paymentTermList.size() > 0) {
                    List<OtcPaymentTerm> newOtcPaymentTerm = new ArrayList<>();
                    Map<Integer, List<OtcPaymentTerm>> collect
                            = paymentTermList
                            .stream()
                            .collect(Collectors.groupingBy(OtcPaymentTerm::getPaymentType));
                    collect.keySet().forEach(s -> {
                        List<OtcPaymentTerm> paymentInfo = collect.get(s);
                        if (paymentInfo != null && paymentInfo.size() > 0) {
                            OtcPaymentTerm paymentTerm
                                    = paymentInfo
                                    .stream()
                                    .sorted(Comparator.comparing(OtcPaymentTerm::getId).reversed())
                                    .limit(1)
                                    .findFirst()
                                    .orElse(null);
                            if (paymentInfo != null) {
                                newOtcPaymentTerm.add(paymentTerm);
                            }
                        }
                    });
                    if (newOtcPaymentTerm.size() > 0) {
                        paymentTermList = newOtcPaymentTerm;
                    }
                }

                List<OTCPaymentTerm> otcPaymentTermList = paymentTermList.stream()
                        .filter(paymentTerm -> paymentIntersectionList.contains(paymentTerm.getPaymentType()))
                        .map(ConvertUtil::convertToOTCPaymentTerm).collect(Collectors.toList());

                OtcUserInfo userInfo = userInfoMap.get(otcOrder.getAccountId().equals(accountId) ?
                        otcOrder.getTargetAccountId() : otcOrder.getAccountId());
                OTCOrderDetail.Builder otcOrderBuilder = convertOTCOrderDetail(otcOrder, accountId);
                List<OtcPaymentTerm> payerList = otcPaymentTermService.getVisibleOtcPaymentTerm(buyer);
                payerList = payerList.stream().filter(otcPaymentTerm -> paymentIntersectionList.contains(otcPaymentTerm.getPaymentType())).collect(Collectors.toList());

                if (!CollectionUtils.isEmpty(payerList)) {
                    otcOrderBuilder.setPaymentInfo(ConvertUtil.convertToOTCPaymentTerm(payerList.get(0)));
                }

                OtcOrderPayInfo orderPayInfo
                        = this.otcPaymentTermService.getOrderPayInfoByOrderId(request.getOrderId());
                OTCOrderPaymentTerm.Builder orderPaymentTerm = OTCOrderPaymentTerm.newBuilder();
                if (orderPayInfo != null) {
                    orderPaymentTerm = OTCOrderPaymentTerm
                            .newBuilder()
                            .setPaymentType(OTCPaymentTypeEnum.valueOf(orderPayInfo.getPaymentType()))
                            .setAccountNo(StringUtils.isNotEmpty(orderPayInfo.getAccountNo()) ? orderPayInfo.getAccountNo() : "")
                            .setBankName(StringUtils.isNotEmpty(orderPayInfo.getBankName()) ? orderPayInfo.getBankName() : "")
                            .setQrcode(StringUtils.isNotEmpty(orderPayInfo.getQrcode()) ? orderPayInfo.getQrcode() : "")
                            .setBranchName(StringUtils.isNotEmpty(orderPayInfo.getBranchName()) ? orderPayInfo.getBranchName() : "")
                            .setRealName(StringUtils.isNotEmpty(orderPayInfo.getRealName()) ? orderPayInfo.getRealName() : "")
                            .setPayMessage(StringUtils.isNotEmpty(orderPayInfo.getPayMessage()) ? orderPayInfo.getPayMessage() : "")
                            .setFirstName(StringUtils.isNotEmpty(orderPayInfo.getFirstName()) ? orderPayInfo.getFirstName() : "")
                            .setLastName(StringUtils.isNotEmpty(orderPayInfo.getLastName()) ? orderPayInfo.getLastName() : "")
                            .setSecondLastName(StringUtils.isNotEmpty(orderPayInfo.getSecondLastName()) ? orderPayInfo.getSecondLastName() : "")
                            .setClabe(StringUtils.isNotEmpty(orderPayInfo.getClabe()) ? orderPayInfo.getClabe() : "")
                            .setMobile(StringUtils.isNotEmpty(orderPayInfo.getMobile()) ? orderPayInfo.getMobile() : "")
                            .setDebitCardNumber(StringUtils.isNotEmpty(orderPayInfo.getDebitCardNumber()) ? orderPayInfo.getDebitCardNumber() : "")
                            .setBusinesName(StringUtils.isNotEmpty(orderPayInfo.getBusinessName()) ? orderPayInfo.getBusinessName() : "")
                            .setConcept(StringUtils.isNotEmpty(orderPayInfo.getConcept()) ? orderPayInfo.getConcept() : "");
                }
                responseBuilder.setOrder(otcOrderBuilder.addAllPaymentTerms(otcPaymentTermList)
                        .setNickName(userInfoMap.get(otcOrder.getAccountId()).getNickName())
                        .setTargetNickName(userInfoMap.get(otcOrder.getTargetAccountId()).getNickName())
                        .setRemark(otcItem.getRemark())
                        .setRecentOrderNum(userInfo.getOrderTotalNumberDay30Safe())
                        .setRecentExecuteNum(userInfo.getOrderFinishNumberDay30Safe())
                        .setOrderPaymentInfo(orderPaymentTerm.build())
                        .build());
                responseBuilder.setResult(OTCResult.SUCCESS);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get order info,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getNewOrderInfo(OTCGetOrderInfoRequest request, StreamObserver<OTCGetOrderInfoResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OtcOrder otcOrder = otcOrderService.getOtcOrderWithExtById(request.getOrderId());
            OTCGetOrderInfoResponse.Builder responseBuilder = OTCGetOrderInfoResponse.newBuilder();
            if (otcOrder == null || (request.getAccountId() > 0 && !otcOrder.getAccountId().equals(request.getAccountId())
                    && !otcOrder.getTargetAccountId().equals(request.getAccountId()))) {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
            } else {
                Map<Long, OtcUserInfo> userInfoMap = otcUserInfoService.getOtcUserInfoMap(Lists.newArrayList(Arrays.asList(otcOrder.getAccountId(), otcOrder.getTargetAccountId())));

                OtcItem otcItem = otcItemService.getOtcItemWithoutConfigById(otcOrder.getItemId());

                long buyer, buyerOrgId, seller, sellerOrgId;
                // 判断买卖双方
                if (otcOrder.getSide().equals(Side.BUY.getCode())) {
                    buyer = otcOrder.getAccountId();
                    seller = otcOrder.getTargetAccountId();
                } else {
                    buyer = otcOrder.getTargetAccountId();
                    seller = otcOrder.getAccountId();
                }
                buyerOrgId = userInfoMap.get(buyer).getOrgId();
                sellerOrgId = userInfoMap.get(seller).getOrgId();
                Set<Integer> paymentIntersectionList = otcPaymentTermService.getBrokerPaymentConfigIntersectionList(buyerOrgId, sellerOrgId);

                long accountId = request.getAccountId() > 0 ? request.getAccountId() : otcOrder.getAccountId();
                List<OtcPaymentTerm> paymentTermList = new ArrayList<>();
                // 未支付之前展示收款方可用的支付方式，支付之后展示收款方所有支付方式
                if (otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) ||
                        otcOrder.getStatus().equals(OrderStatus.CANCEL.getStatus())) {
                    paymentTermList = otcPaymentTermService.getVisibleOtcPaymentTerm(seller);
                } else {
                    paymentTermList = otcPaymentTermService.getOtcPaymentTerm(seller);
                }
                List<OTCPaymentTerm> otcPaymentTermList = paymentTermList.stream()
                        .filter(paymentTerm -> paymentIntersectionList.contains(paymentTerm.getPaymentType()))
                        .map(ConvertUtil::convertToOTCPaymentTerm).collect(Collectors.toList());

                OtcUserInfo userInfo = userInfoMap.get(otcOrder.getAccountId().equals(accountId) ?
                        otcOrder.getTargetAccountId() : otcOrder.getAccountId());
                OTCOrderDetail.Builder otcOrderBuilder = convertOTCOrderDetail(otcOrder, accountId);
                List<OtcPaymentTerm> payerList = otcPaymentTermService.getVisibleOtcPaymentTerm(buyer);
                payerList = payerList.stream().filter(otcPaymentTerm -> paymentIntersectionList.contains(otcPaymentTerm.getPaymentType())).collect(Collectors.toList());

                if (!CollectionUtils.isEmpty(payerList)) {
                    otcOrderBuilder.setPaymentInfo(ConvertUtil.convertToOTCPaymentTerm(payerList.get(0)));
                }

                OtcOrderPayInfo orderPayInfo
                        = this.otcPaymentTermService.getOrderPayInfoByOrderId(request.getOrderId());
                OTCOrderPaymentTerm.Builder orderPaymentTerm = OTCOrderPaymentTerm.newBuilder();
                if (orderPayInfo != null) {
                    orderPaymentTerm = OTCOrderPaymentTerm
                            .newBuilder()
                            .setPaymentType(OTCPaymentTypeEnum.valueOf(orderPayInfo.getPaymentType()))
                            .setAccountNo(StringUtils.isNotEmpty(orderPayInfo.getAccountNo()) ? orderPayInfo.getAccountNo() : "")
                            .setBankName(StringUtils.isNotEmpty(orderPayInfo.getBankName()) ? orderPayInfo.getBankName() : "")
                            .setQrcode(StringUtils.isNotEmpty(orderPayInfo.getQrcode()) ? orderPayInfo.getQrcode() : "")
                            .setBranchName(StringUtils.isNotEmpty(orderPayInfo.getBranchName()) ? orderPayInfo.getBranchName() : "")
                            .setRealName(orderPayInfo.getRealNameSafe())
                            .setPayMessage(StringUtils.isNotEmpty(orderPayInfo.getPayMessage()) ? orderPayInfo.getPayMessage() : "")
                            .setFirstName(StringUtils.isNotEmpty(orderPayInfo.getFirstName()) ? orderPayInfo.getFirstName() : "")
                            .setLastName(StringUtils.isNotEmpty(orderPayInfo.getLastName()) ? orderPayInfo.getLastName() : "")
                            .setSecondLastName(StringUtils.isNotEmpty(orderPayInfo.getSecondLastName()) ? orderPayInfo.getSecondLastName() : "")
                            .setClabe(StringUtils.isNotEmpty(orderPayInfo.getClabe()) ? orderPayInfo.getClabe() : "")
                            .setMobile(StringUtils.isNotEmpty(orderPayInfo.getMobile()) ? orderPayInfo.getMobile() : "")
                            .setDebitCardNumber(StringUtils.isNotEmpty(orderPayInfo.getDebitCardNumber()) ? orderPayInfo.getDebitCardNumber() : "")
                            .setBusinesName(StringUtils.isNotEmpty(orderPayInfo.getBusinessName()) ? orderPayInfo.getBusinessName() : "")
                            .setConcept(StringUtils.isNotEmpty(orderPayInfo.getConcept()) ? orderPayInfo.getConcept() : "");
                }
                //对手方KYC信息
                OtcUserInfo otcUserInfo = userInfoMap.get(otcOrderBuilder.getTargetAccountId());
                responseBuilder.setOrder(otcOrderBuilder.addAllPaymentTerms(otcPaymentTermList)
                        .setNickName(userInfoMap.get(otcOrder.getAccountId()).getNickName())
                        .setTargetNickName(userInfoMap.get(otcOrder.getTargetAccountId()).getNickName())
                        .setRemark(StringUtils.isEmpty(otcItem.getRemark()) ? "" : otcItem.getRemark())
                        .setRecentOrderNum(userInfo.getOrderTotalNumberDay30Safe())
                        .setRecentExecuteNum(userInfo.getOrderFinishNumberDay30Safe())
                        .setOrderPaymentInfo(orderPaymentTerm.build())
                        .setTargetFirstName(otcUserInfo != null && StringUtils.isNotEmpty(otcUserInfo.getUserFirstName()) ? otcUserInfo.getUserFirstName() : "")
                        .setTargetSecondName(otcUserInfo != null && StringUtils.isNotEmpty(otcUserInfo.getUserSecondName()) ? otcUserInfo.getUserSecondName() : "")
                        .setSellerUserId(userInfoMap.get(seller).getUserId())
                        .setBuyerUserId(userInfoMap.get(buyer).getUserId())
                        .setSellerOrgId(userInfoMap.get(seller).getOrgId())
                        .setBuyerOrgId(userInfoMap.get(buyer).getOrgId())
                        .build());
                responseBuilder.setResult(OTCResult.SUCCESS);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get new order,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getPendingOrderCount(OTCGetPendingCountRequest request,
                                     StreamObserver<OTCGetPendingCountResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            responseObserver.onNext(OTCGetPendingCountResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS)
                    .setCount(otcOrderService.getPendingOrderCount(request.getAccountId()))
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("count pending order,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getPendingOrders(OTCGetPendingOrdersRequest request,
                                 StreamObserver<OTCGetPendingOrdersResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetPendingOrdersResponse.Builder responseBuilder = OTCGetPendingOrdersResponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            List<OtcOrder> otcOrderList = otcOrderService.getPendingOrderList(request.getAccountId());
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                Set<Long> accountIds = otcOrderList.stream().flatMap(otcOrder ->
                        Stream.of(otcOrder.getAccountId(), otcOrder.getTargetAccountId())
                ).collect(Collectors.toSet());
                Map<Long, OtcUserInfo> userInfoMap = otcUserInfoService.getOtcUserInfoMap(Lists.newArrayList(accountIds));
                List<OTCOrderDetail> orderDetailList = otcOrderList.stream().map(otcOrder -> {
                            long buyer, buyerOrgId, seller, sellerOrgId;
                            // 判断买卖双方
                            if (otcOrder.getSide().equals(Side.BUY.getCode())) {
                                buyer = otcOrder.getAccountId();
                                seller = otcOrder.getTargetAccountId();
                            } else {
                                buyer = otcOrder.getTargetAccountId();
                                seller = otcOrder.getAccountId();
                            }
                            buyerOrgId = userInfoMap.get(buyer).getOrgId();
                            sellerOrgId = userInfoMap.get(seller).getOrgId();
                            Set<Integer> paymentIntersectionList = otcPaymentTermService.getBrokerPaymentConfigIntersectionList(buyerOrgId, sellerOrgId);
                            OtcUserInfo targetUser = userInfoMap.get(otcOrder.getAccountId().equals(request.getAccountId()) ?
                                    otcOrder.getTargetAccountId() : otcOrder.getAccountId());
                            List<OtcPaymentTerm> paymentTermList;
                            // 未支付之前展示收款方可用的支付方式，支付之后展示收款方所有支付方式
                            if (otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) || otcOrder.getStatus().equals(OrderStatus.CANCEL.getStatus())) {
                                paymentTermList = otcPaymentTermService.getVisibleOtcPaymentTerm(seller);
                            } else {
                                paymentTermList = otcPaymentTermService.getOtcPaymentTerm(seller);
                            }

                            List<OTCPaymentTerm> otcPaymentTermList = paymentTermList.stream()
                                    .filter(paymentTerm -> paymentIntersectionList.contains(paymentTerm.getPaymentType()))
                                    .map(ConvertUtil::convertToOTCPaymentTerm).collect(Collectors.toList());
                            OTCOrderDetail.Builder otcOrderBuilder = convertOTCOrderDetail(otcOrder, request.getAccountId());
                            List<OtcPaymentTerm> payerList = otcPaymentTermService.getVisibleOtcPaymentTerm(buyer);
                            payerList = payerList.stream().filter(otcPaymentTerm -> paymentIntersectionList.contains(otcPaymentTerm.getPaymentType())).collect(Collectors.toList());

                            if (!CollectionUtils.isEmpty(payerList)) {
                                otcOrderBuilder.setPaymentInfo(ConvertUtil.convertToOTCPaymentTerm(payerList.get(0)));
                            }

                            OtcOrderPayInfo orderPayInfo = this.otcPaymentTermService.getOrderPayInfoByOrderId(otcOrder.getId());
                            OTCOrderPaymentTerm.Builder orderPaymentTerm = OTCOrderPaymentTerm.newBuilder();
                            if (orderPayInfo != null) {
                                orderPaymentTerm = OTCOrderPaymentTerm
                                        .newBuilder()
                                        .setPaymentType(OTCPaymentTypeEnum.valueOf(orderPayInfo.getPaymentType()))
                                        .setAccountNo(StringUtils.isNotEmpty(orderPayInfo.getAccountNo()) ? orderPayInfo.getAccountNo() : "")
                                        .setBankName(StringUtils.isNotEmpty(orderPayInfo.getBankName()) ? orderPayInfo.getBankName() : "")
                                        .setQrcode(StringUtils.isNotEmpty(orderPayInfo.getQrcode()) ? orderPayInfo.getQrcode() : "")
                                        .setBranchName(StringUtils.isNotEmpty(orderPayInfo.getBranchName()) ? orderPayInfo.getBranchName() : "")
                                        .setRealName(StringUtils.isNotEmpty(orderPayInfo.getRealName()) ? orderPayInfo.getRealName() : "")
                                        .setPayMessage(StringUtils.isNotEmpty(orderPayInfo.getPayMessage()) ? orderPayInfo.getPayMessage() : "")
                                        .setFirstName(StringUtils.isNotEmpty(orderPayInfo.getFirstName()) ? orderPayInfo.getFirstName() : "")
                                        .setLastName(StringUtils.isNotEmpty(orderPayInfo.getLastName()) ? orderPayInfo.getLastName() : "")
                                        .setSecondLastName(StringUtils.isNotEmpty(orderPayInfo.getSecondLastName()) ? orderPayInfo.getSecondLastName() : "")
                                        .setClabe(StringUtils.isNotEmpty(orderPayInfo.getClabe()) ? orderPayInfo.getClabe() : "")
                                        .setMobile(StringUtils.isNotEmpty(orderPayInfo.getMobile()) ? orderPayInfo.getMobile() : "")
                                        .setDebitCardNumber(StringUtils.isNotEmpty(orderPayInfo.getDebitCardNumber()) ? orderPayInfo.getDebitCardNumber() : "")
                                        .setBusinesName(StringUtils.isNotEmpty(orderPayInfo.getBusinessName()) ? orderPayInfo.getBusinessName() : "")
                                        .setConcept(StringUtils.isNotEmpty(orderPayInfo.getConcept()) ? orderPayInfo.getConcept() : "");
                            }

                            return otcOrderBuilder.setNickName(userInfoMap.get(otcOrder.getAccountId()).getNickName())
                                    .setTargetNickName(userInfoMap.get(otcOrder.getTargetAccountId()).getNickName())
                                    .addAllPaymentTerms(otcPaymentTermList)
                                    .setRecentOrderNum(targetUser.getOrderTotalNumberDay30Safe())
                                    .setRecentExecuteNum(targetUser.getOrderFinishNumberDay30Safe())
                                    .setOrderPaymentInfo(orderPaymentTerm)
                                    .setSellerUserId(userInfoMap.get(seller).getUserId())
                                    .setBuyerUserId(userInfoMap.get(buyer).getUserId())
                                    .setSellerOrgId(userInfoMap.get(seller).getOrgId())
                                    .setBuyerOrgId(userInfoMap.get(buyer).getOrgId())
                                    .build();
                        }
                ).collect(Collectors.toList());
                responseBuilder.addAllOrders(orderDetailList);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("list pending order,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getOrderIdByClientId(OTCGetOrderIdRequest request,
                                     StreamObserver<OTCGetOrderIdResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetOrderIdResponse.Builder responseBuilder = OTCGetOrderIdResponse.newBuilder();
            // 处理重复请求
            Long orderId = otcOrderService.getOrderIdByClient(request.getOrgId(), request.getClientOrderId());
            if (orderId != null && orderId > 0) {
                responseBuilder.setResult(OTCResult.SUCCESS);
                responseBuilder.setOrderId(orderId);
            } else {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("get order by client id,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getOrderList(OTCGetOrdersRequest request, StreamObserver<OTCGetOrdersResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();

        try {
            OTCGetOrdersResponse.Builder responseBuilder = OTCGetOrdersResponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            List<OtcOrder> otcOrderList = listOrder(request);
/*        if (request.getBaseRequest() != null && request.getBaseRequest().getOrgId() > 0) {
            otcOrderList = otcOrderService.getOtcOrderList(request.getOrderStatusValueList(),
                    request.getBeginTime() > 0 ? new Date(request.getBeginTime()) : null,
                    request.getEndTime() > 0 ? new Date(request.getEndTime()) : null,
                    request.getTokrnId(),
                    request.getBaseRequest().getOrgId(),
                    CollectionUtils.isEmpty(request.getSideList()) ? null : request.getSideValue(0),
                    request.getPage(),
                    request.getSize());
        }*/
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                Set<Long> accountIds = otcOrderList.stream().flatMap(otcOrder ->
                        Stream.of(otcOrder.getAccountId(), otcOrder.getTargetAccountId())
                ).collect(Collectors.toSet());
                Map<Long, OtcUserInfo> userInfoMap = otcUserInfoService.getOtcUserInfoMap(Lists.newArrayList(accountIds));
                List<OTCOrderDetail> orderDetailList = otcOrderList.stream().map(
                        otcOrder -> convertOTCOrderDetail(otcOrder, otcOrder.getAccountId())
                                .setNickName(userInfoMap.get(otcOrder.getAccountId()).getNickName())
                                .setTargetNickName(userInfoMap.get(otcOrder.getTargetAccountId()).getNickName())
                                .build()
                ).collect(Collectors.toList());
                responseBuilder.addAllOrders(orderDetailList);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("list order,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getOrderListForAdmin(OTCGetOrdersRequest request, StreamObserver<OTCGetOrdersResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCGetOrdersResponse.Builder responseBuilder = OTCGetOrdersResponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            List<OtcOrder> otcOrderList = listOrderForAdmin(request);
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                Set<Long> accountIds = otcOrderList.stream().flatMap(otcOrder ->
                        Stream.of(otcOrder.getAccountId(), otcOrder.getTargetAccountId())
                ).collect(Collectors.toSet());
                Map<Long, OtcUserInfo> userInfoMap = otcUserInfoService.getOtcUserInfoMap(Lists.newArrayList(accountIds));
                List<OTCOrderDetail> orderDetailList = otcOrderList.stream().map(
                        otcOrder -> convertOTCOrderDetail(otcOrder, otcOrder.getAccountId())
                                .setNickName(userInfoMap.get(otcOrder.getAccountId()).getNickName())
                                .setTargetNickName(userInfoMap.get(otcOrder.getTargetAccountId()).getNickName())
                                .build()
                ).collect(Collectors.toList());
                responseBuilder.addAllOrders(orderDetailList);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("list order for admin,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }


    private List<OtcOrder> listOrder(OTCGetOrdersRequest request) {
        List<OtcOrder> otcOrderList = Lists.newArrayList();
        if (request.getBaseRequest() != null && request.getBaseRequest().getOrgId() > 0) {
            otcOrderList = otcOrderService.getOtcOrderList(request.getOrderStatusValueList(),
                    request.getBeginTime() > 0 ? new Date(request.getBeginTime()) : null,
                    request.getEndTime() > 0 ? new Date(request.getEndTime()) : null,
                    request.getTokenId(),
                    request.getBaseRequest().getOrgId(),
                    CollectionUtils.isEmpty(request.getSideList()) ? null : request.getSideValue(0),
                    request.getPage(),
                    request.getSize());
        }

        return otcOrderList;
    }

    private List<OtcOrder> listOrderForAdmin(OTCGetOrdersRequest request) {
        List<OtcOrder> otcOrderList = Lists.newArrayList();
        if (request.getBaseRequest() != null && request.getBaseRequest().getOrgId() > 0) {
            Long userId = 0L;
            if (request.getUserId() > 0) {
                userId = request.getUserId();
                OtcUserInfo otcUserInfo
                        = otcUserInfoService.getOtcUserInfoByUserId(request.getBaseRequest().getOrgId(), userId);
                if (otcUserInfo == null) {
                    return new ArrayList<>();
                }
            } else if (StringUtils.isNotEmpty(request.getMobile())) {
                OtcUserInfo otcUserInfo
                        = this.otcUserInfoService.getUserByMobile(request.getBaseRequest().getOrgId(), request.getMobile());
                if (otcUserInfo != null) {
                    userId = otcUserInfo.getUserId();
                } else {
                    return new ArrayList<>();
                }
            } else if (StringUtils.isNotEmpty(request.getEmail())) {
                OtcUserInfo otcUserInfo
                        = this.otcUserInfoService.getUserByEmail(request.getBaseRequest().getOrgId(), request.getEmail());
                if (otcUserInfo != null) {
                    userId = otcUserInfo.getUserId();
                } else {
                    return new ArrayList<>();
                }
            }

            Long accountId = 0L;
            if (userId != null && userId > 0) {
                OtcUserInfo otcUserInfo = otcUserInfoService.getOtcUserInfoByUserId(request.getBaseRequest().getOrgId(), userId);
                if (otcUserInfo != null) {
                    accountId = otcUserInfo.getAccountId();
                }
            }
            otcOrderList = otcOrderService.getOtcOrderListForAdmin(request.getOrderStatusValueList(),
                    request.getBeginTime() > 0 ? new Date(request.getBeginTime()) : null,
                    request.getEndTime() > 0 ? new Date(request.getEndTime()) : null,
                    request.getTokenId(),
                    request.getBaseRequest().getOrgId(),
                    request.getPage(),
                    request.getSize(),
                    request.getId(),
                    accountId,
                    request.getSideValueList());
        }
        return otcOrderList;
    }

    @Override
    public void adminHandleOrder(OTCHandleOrderRequest request,
                                 StreamObserver<OTCHandleOrderResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {

            OTCHandleOrderResponse.Builder responseBuilder = OTCHandleOrderResponse.newBuilder();
            OtcOrder otcOrder = otcOrderService.getOtcOrderById(request.getOrderId());
            if (otcOrder == null ||
                    request.getBaseRequest() == null ||
                    //!otcOrder.getOrgId().equals(request.getBaseRequest().getOrgId()) ||
                    !otcOrder.getStatus().equals(OrderStatus.APPEAL.getStatus())) {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            List<OtcBalanceFlow> balanceFlowList = null;

            long brokerId = request.getBaseRequest().getOrgId();
            //共享深度订单申诉处理需要双方确认，且一致后才能继续处理
            if (otcOrder.getDepthShareBool()) {
                boolean bothConfirm = otcOrderService.mutualConfirm(brokerId, otcOrder,
                        handleTypeStatusMap.get(request.getType()), request.getExt(), request.getAppealContent());
                if (!bothConfirm) {
                    responseBuilder.setResult(OTCResult.SUCCESS);
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }
            }

            switch (request.getType()) {
                case CANCEL:
                    // 撤单
                    OtcBalanceFlow otcBalanceFlow = otcOrderService.adminCancelOrder(otcOrder);
                    // 买方撤单计数加一
                    otcOrderService.increaseCancelNum(otcOrder.getSide().equals(Side.BUY.getCode()) ?
                            otcOrder.getAccountId() : otcOrder.getTargetAccountId());
                    if (otcBalanceFlow != null) {
                        balanceFlowList = Lists.newArrayList(otcBalanceFlow);
                    }
                    responseBuilder.setResult(OTCResult.SUCCESS);
                    break;
                case FINISH:
                    // 放币结束订单
                    balanceFlowList = otcOrderService.finishOrder(otcOrder);
                    responseBuilder.setResult(OTCResult.SUCCESS);
                    break;
                default:
                    responseBuilder.setResult(OTCResult.PERMISSION_DENIED);
                    break;
            }
            if (!CollectionUtils.isEmpty(balanceFlowList)) {
                balanceFlowList.forEach(grpcServerService::sendBalanceChangeRequest);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("adminHandleOrder,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getMessage(OTCGetMessageRequest request, StreamObserver<OTCGetMessageResponse> responseObserver) {
        try {
            OrderMsg orderMsg = otcOrderService.popMessage(request.getBaseRequest().getOrgId());
            if (orderMsg == null) {
                responseObserver.onNext(OTCGetMessageResponse.newBuilder()
                        .setResult(OTCResult.NO_DATA)
                        .build());
                responseObserver.onCompleted();
                return;
            }
            OrderSideEnum orderSide = orderMsg.getSide() != null ? OrderSideEnum.forNumber(orderMsg.getSide()) : OrderSideEnum.UNKNOWN;
            responseObserver.onNext(OTCGetMessageResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS)
                    .setMsgCode(orderMsg.getMsgCode())
                    .setOrgId(orderMsg.getOrgId())
                    .setUserId(orderMsg.getUserId())
                    .setBuyer(orderMsg.getBuyer())
                    .setSeller(orderMsg.getSeller())
                    .setTokenId(orderMsg.getTokenId())
                    .setCurrencyId(orderMsg.getCurrencyId())
                    .setQuantity(orderMsg.getQuantity())
                    .setAmount(orderMsg.getAmount())
                    .setCancelTime(orderMsg.getCancelTime() != null ? orderMsg.getCancelTime() : 15)
                    .setAppealTime(orderMsg.getAppealTime() != null ? orderMsg.getAppealTime() : 30)
                    .setOrderId(orderMsg.getOrderId() != null && orderMsg.getOrderId() > 0 ? orderMsg.getOrderId() : 0l)
                    .setSide(orderSide)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void findOrdersByItemIds(FindOrdersByItemIdsRequest request,
                                    StreamObserver<FindOrdersByItemIdsReponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {

            FindOrdersByItemIdsReponse.Builder responseBuilder = FindOrdersByItemIdsReponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            List<OtcOrder> otcOrderList = otcOrderService.listOtcOrderByItemIds(request.getExchangeIdList(),
                    request.getOrgIdList(), request.getItemIdList());
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                List<OrderBrief> list = otcOrderList.stream().map(order -> {
                    return OrderBrief.newBuilder()
                            .setAccountId(order.getAccountId())
                            .setOrderId(order.getId())
                            .setClientOrderId(order.getClientOrderId())
                            .setItemId(order.getItemId())
                            .setOrderStatus(OTCOrderStatusEnum.forNumber(order.getStatus()))
                            .setTargetAccountId(order.getTargetAccountId())
                            .setAccountId(order.getAccountId())
                            .build();
                }).collect(Collectors.toList());
                responseBuilder.addAllOrders(list);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("findOrdersByItemIds,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    @Override
    public void findOrdersByOrderIds(FindOrdersByOrderIdsRequest request,
                                     StreamObserver<FindOrdersByOrderIdsReponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            FindOrdersByOrderIdsReponse.Builder responseBuilder = FindOrdersByOrderIdsReponse.newBuilder();
            responseBuilder.setResult(OTCResult.SUCCESS);
            List<OtcOrder> otcOrderList = otcOrderService.listOtcOrderByOrderIds(request.getOrderIdsList());
            if (!CollectionUtils.isEmpty(otcOrderList)) {
                List<OrderBrief> list = otcOrderList.stream().map(order -> {
                    return OrderBrief.newBuilder()
                            .setAccountId(order.getAccountId())
                            .setOrderId(order.getId())
                            .setClientOrderId(order.getClientOrderId())
                            .setItemId(order.getItemId())
                            .setOrderStatus(OTCOrderStatusEnum.forNumber(order.getStatus()))
                            .setTargetAccountId(order.getTargetAccountId())
                            .setAccountId(order.getAccountId())
                            .build();
                }).collect(Collectors.toList());
                responseBuilder.addAllOrders(list);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("findOrdersByOrderIds,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    private OTCOrderDetail.Builder convertOTCOrderDetail(OtcOrder otcOrder, long accountId) {
        // 订单展示做转换，当不是taker时，反转订单方向并切换买卖双方
        if (!otcOrder.getAccountId().equals(accountId)) {
            otcOrder.setSide(otcOrder.getSide().equals(Side.BUY.getCode()) ? Side.SELL.getCode() : Side.BUY.getCode());
            otcOrder.setTargetAccountId(otcOrder.getAccountId());
            otcOrder.setAccountId(accountId);
        }
        OTCOrderDetail.Builder builder = OTCOrderDetail.newBuilder()
                .setOrderId(otcOrder.getId())
                .setAccountId(otcOrder.getAccountId())
                .setOrderStatusValue(otcOrder.getStatus())
                .setItemId(otcOrder.getItemId())
                .setTargetAccountId(otcOrder.getTargetAccountId())
                .setTokenId(otcOrder.getTokenId())
                .setTokenName(otcOrder.getTokenName())
                .setCurrencyId(otcOrder.getCurrencyId())
                .setSideValue(otcOrder.getSide())
                .setPrice(DecimalUtil.fromBigDecimal(otcOrder.getPrice()))
                .setQuantity(DecimalUtil.fromBigDecimal(otcOrder.getQuantity()))
                .setAmount(DecimalUtil.fromBigDecimal(otcOrder.getAmount()))
                .setPayCode(otcOrder.getPayCode())
                .setCreateDate(otcOrder.getCreateDate().getTime())
                .setUpdateDate(otcOrder.getUpdateDate().getTime())
                .setMakerFee(DecimalUtil.fromBigDecimal(otcOrder.getMakerFee() != null ? otcOrder.getMakerFee() : BigDecimal.ZERO))
                .setTakerFee(DecimalUtil.fromBigDecimal(otcOrder.getTakerFee() != null ? otcOrder.getTakerFee() : BigDecimal.ZERO))
                .setDepthShare(otcOrder.getDepthShareBool());
        if (otcOrder.getPaymentType() != null) {
            builder.setPaymentValue(otcOrder.getPaymentType());
        }
        if (otcOrder.getTransferDate() != null) {
            builder.setTransferDate(otcOrder.getTransferDate().getTime());
        }
        if (otcOrder.getStatus().equals(OrderStatus.APPEAL.getStatus())) {
            builder.setAppealTypeValue(otcOrder.getAppealType());
            if (StringUtils.isNotBlank(otcOrder.getAppealContent())) {
                builder.setAppealContent(otcOrder.getAppealContent());
            }
        }

        if (Objects.nonNull(otcOrder.getTakerBrokerId())) {
            builder.setTakerOrgId(otcOrder.getTakerBrokerId());
        }

        if (otcOrder.getDepthShareBool()) {
            if (Objects.nonNull(otcOrder.getMakerBrokerId())) {
                builder.setMakerOrgId(otcOrder.getMakerBrokerId());
            }
        }

        if (Objects.nonNull(otcOrder.getOrderExt())) {
            OtcOrderExt ext = otcOrder.getOrderExt();
            builder.setOrderExt(OrderExt.newBuilder()
                    .setCurrencyAmountScale(ext.getCurrencyAmountScale())
                    .setCurrencyScale(ext.getCurrencyScale())
                    .setTokenScale(ext.getTokenScale())
            );
        }

        return builder;
    }

    @Override
    public void getShareOrderAppealInfo(OTCNormalOrderRequest request,
                                        StreamObserver<ShareOrderAppealResponse> responseObserver) {

        ShareOrderAppealResponse.Builder response = ShareOrderAppealResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            if (request.getOrderId() == 0) {
                throw new OrderNotExistException("orderId is 0");
            }
            List<OtcOrderDepthShareAppeal> list = otcOrderService.listShareOrderAppealInfo(request.getOrderId());
            List<ShareOrderAppealInfo> grpcEntityList = list.stream().map(i -> convertAppealInfo(i)).collect(Collectors.toList());
            response.addAllAppealInfo(grpcEntityList);
            response.setOrderId(request.getOrderId());
            response.setResult(OTCResult.SUCCESS);

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (OrderNotExistException e) {
            log.error(e.getMessage(), e);

            response.setResult(OTCResult.ORDER_NOT_EXIST);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getShareOrderAppealInfo,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    private ShareOrderAppealInfo convertAppealInfo(OtcOrderDepthShareAppeal info) {

        return ShareOrderAppealInfo.newBuilder()
                .setBrokerId(info.getBrokerId())
                .setStatus(OTCOrderStatusEnum.forNumber(info.getStatus()))
                .setComment(info.getComment())
                .build();
    }

    @Override
    public void getOrderContact(OTCOrderContactRequest request,
                                StreamObserver<OTCOrderContactResponse> responseObserver) {

        OTCOrderContactResponse.Builder response = OTCOrderContactResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            if (request.getOrderId() == 0) {
                throw new OrderNotExistException("orderId is 0");
            }
            Pair<OtcUserInfo, OtcUserInfo> result = otcOrderService.getOrderContact(request.getOrderId());
            if (Objects.nonNull(result)) {
                OtcUserInfo maker = result.getKey();
                OtcUserInfo taker = result.getValue();

                OTCOrderContact makerContact = OTCOrderContact.newBuilder()
                        .setAccountId(maker.getAccountId())
                        .setOrgId(maker.getOrgId())
                        .setUserId(maker.getUserId())
                        .setEmail(maker.getEmail())
                        .setMobile(maker.getMobile())
                        .build();

                OTCOrderContact takerContact = OTCOrderContact.newBuilder()
                        .setAccountId(taker.getAccountId())
                        .setOrgId(taker.getOrgId())
                        .setUserId(taker.getUserId())
                        .setEmail(taker.getEmail())
                        .setMobile(taker.getMobile())
                        .build();

                response.setMaker(makerContact);
                response.setTaker(takerContact);
            }

            response.setResult(OTCResult.SUCCESS);

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (OrderNotExistException e) {
            log.error(e.getMessage(), e);

            response.setResult(OTCResult.ORDER_NOT_EXIST);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getOrderContact,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }

    }

    @Override
    public void getLastNewOrderId(OTCGetPendingCountRequest request,
                                  StreamObserver<GetLastNewOrderIdResponse> responseObserver) {

        GetLastNewOrderIdResponse.Builder respBuilder = GetLastNewOrderIdResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Long orderId = otcOrderService.getLastNewOrderId(request.getAccountId());
            respBuilder.setResult(OTCResult.SUCCESS).setOrderId(orderId);

            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getLastNewOrderId,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }


    }

}