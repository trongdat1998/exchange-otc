package io.bhex.ex.otc.server;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.*;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.OtcOrderMessage;
import io.bhex.ex.otc.enums.MsgType;
import io.bhex.ex.otc.enums.OrderStatus;
import io.bhex.ex.otc.service.OtcOrderMessageService;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.util.ConvertUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * 订单消息（聊天记录）grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-17
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcOrderMessageServiceGrpcImpl extends OTCMessageServiceGrpc.OTCMessageServiceImplBase {

    @Autowired
    private OtcOrderService otcOrderService;

    @Autowired
    private OtcOrderMessageService otcOrderMessageService;

    @Override
    public void addMessage(OTCNewMessageRequest request, StreamObserver<OTCNewMessageResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OtcOrder otcOrder = checkOwner(request.getAccountId(), request.getOrderId());
            OTCNewMessageResponse.Builder responseBuilder = OTCNewMessageResponse.newBuilder();
            if (otcOrder == null) {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
            } else {
                // 未完结订单可以聊天
                if (!otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) &&
                        !otcOrder.getStatus().equals(OrderStatus.APPEAL.getStatus()) &&
                        !otcOrder.getStatus().equals(OrderStatus.UNCONFIRM.getStatus())) {
                    responseBuilder.setResult(OTCResult.PERMISSION_DENIED);
                } else {

                    int msgType = MsgType.WORD_MSG.getType();
                    if (request.getMsgType() != null) {
                        msgType = request.getMsgTypeValue();
                    }

                    OtcOrderMessage otcOrderMessage = OtcOrderMessage.builder()
                            .accountId(request.getAccountId())
                            .orderId(request.getOrderId())
                            .msgType(msgType)
                            .msgCode(0)
                            .message(request.getMessage())
                            .createDate(new Date())
                            .build();
                    otcOrderMessageService.addOtcOrderMessage(otcOrderMessage);
                    responseBuilder.setResult(OTCResult.SUCCESS);
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("addMessage,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    @Override
    public void getMessages(OTCGetMessagesRequest request, StreamObserver<OTCGetMessagesResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {

            OtcOrder otcOrder = checkOwner(request.getAccountId(), request.getOrderId());
            OTCGetMessagesResponse.Builder responseBuilder = OTCGetMessagesResponse.newBuilder();
            if (otcOrder == null) {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
            } else {
                List<OtcOrderMessage> messageList = otcOrderMessageService.getOtcOrderMessageList(request.getAccountId(),
                        request.getOrderId(), request.getStartMessageId(), request.getSize());
                if (!CollectionUtils.isEmpty(messageList)) {
                    List<OTCMessageDetail> messageDetailList = Lists.newArrayList();

                    for (OtcOrderMessage message : messageList) {
                        messageDetailList.add(this.convertMessageDetail(message));
                    }
                    responseBuilder.addAllMessages(messageDetailList);
                }
                responseBuilder.setResult(OTCResult.SUCCESS);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getMessages,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }


    @Override
    public void getAppealMessages(GetAppealMessagesRequest request, StreamObserver<GetAppealMessagesResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {

            OtcOrder otcOrder = checkOwner(request.getAccountId(), request.getOrderId());
            GetAppealMessagesResponse.Builder responseBuilder = GetAppealMessagesResponse.newBuilder();
            if (otcOrder == null) {
                responseBuilder.setResult(OTCResult.ORDER_NOT_EXIST);
            }

            List<OtcOrderMessage> messageList = otcOrderMessageService.getOtcOrderAppealMessage(request.getOrderId());
            if (!CollectionUtils.isEmpty(messageList)) {
                List<OTCMessageDetail> messageDetailList = Lists.newArrayList();
                for (OtcOrderMessage message : messageList) {
                    if (request.getAccountId() > 0 && message.getAccountId() != request.getAccountId()) {
                        continue;
                    }

                    messageDetailList.add(this.convertMessageDetail(message));
                }
                responseBuilder.addAllMessages(messageDetailList);
            }
            responseBuilder.setResult(OTCResult.SUCCESS);


            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getAppealMessages,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }

    public OTCMessageDetail convertMessageDetail(OtcOrderMessage message) {
        return OTCMessageDetail.newBuilder()
                .setId(message.getId())
                .setAccountId(message.getAccountId())
                .setMsgTypeValue(message.getMsgType())
                .setMessage(StringUtils.isNotBlank(message.getMessage()) ? message.getMessage() : "")
                .setMsgCode(message.getMsgCode())
                .setCreateDate(message.getCreateDate().getTime())
                .build();
    }


    private OtcOrder checkOwner(long accountId, long orderId) {
        OtcOrder otcOrder = otcOrderService.getOtcOrderById(orderId);
        if (otcOrder == null) {
            return null;
        }
        // 下单双方有操作权限
        if (!otcOrder.getAccountId().equals(accountId) && !otcOrder.getTargetAccountId().equals(accountId)) {
            return null;
        }
        return otcOrder;
    }
}