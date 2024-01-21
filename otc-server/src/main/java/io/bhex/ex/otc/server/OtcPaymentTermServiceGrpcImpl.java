package io.bhex.ex.otc.server;

import com.google.common.base.Stopwatch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.BatchUpdatePaymentVisibleRequest;
import io.bhex.ex.otc.BatchUpdatePaymentVisibleResponse;
import io.bhex.ex.otc.GetBrokerPaymentConfigRequest;
import io.bhex.ex.otc.GetBrokerPaymentConfigResponse;
import io.bhex.ex.otc.OTCConfigPaymentRequest;
import io.bhex.ex.otc.OTCConfigPaymentResponse;
import io.bhex.ex.otc.OTCCreateNewPaymentRequest;
import io.bhex.ex.otc.OTCCreateNewPaymentResponse;
import io.bhex.ex.otc.OTCDeletePaymentRequest;
import io.bhex.ex.otc.OTCDeletePaymentResponse;
import io.bhex.ex.otc.OTCGetPaymentRequest;
import io.bhex.ex.otc.OTCGetPaymentResponse;
import io.bhex.ex.otc.OTCNewPaymentRequest;
import io.bhex.ex.otc.OTCNewPaymentResponse;
import io.bhex.ex.otc.OTCPaymentTerm;
import io.bhex.ex.otc.OTCPaymentTermServiceGrpc;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OTCUpdateNewPaymentRequest;
import io.bhex.ex.otc.OTCUpdateNewPaymentResponse;
import io.bhex.ex.otc.OTCUpdatePaymentRequest;
import io.bhex.ex.otc.OTCUpdatePaymentResponse;
import io.bhex.ex.otc.OTCVisibleEnum;
import io.bhex.ex.otc.QueryOtcPaymentTermListRequest;
import io.bhex.ex.otc.QueryOtcPaymentTermListResponse;
import io.bhex.ex.otc.SwitchPaymentVisibleRequest;
import io.bhex.ex.otc.SwitchPaymentVisibleResponse;
import io.bhex.ex.otc.entity.OtcPaymentItems;
import io.bhex.ex.otc.entity.OtcPaymentTerm;
import io.bhex.ex.otc.exception.BusinessException;
import io.bhex.ex.otc.exception.LeastOnePaymentException;
import io.bhex.ex.otc.exception.NonPersonPaymentException;
import io.bhex.ex.otc.exception.PaymentTermNotFindException;
import io.bhex.ex.otc.exception.ThreePaymentException;
import io.bhex.ex.otc.exception.UpdatedVersionException;
import io.bhex.ex.otc.exception.WeChatLimitException;
import io.bhex.ex.otc.mappper.OtcPaymentTermMapper;
import io.bhex.ex.otc.service.OtcPaymentTermService;
import io.bhex.ex.otc.service.OtcUserInfoService;
import io.bhex.ex.otc.util.ConvertUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * 付款方式grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-19
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcPaymentTermServiceGrpcImpl extends OTCPaymentTermServiceGrpc.OTCPaymentTermServiceImplBase {

    @Autowired
    private OtcPaymentTermService otcPaymentTermService;

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Resource
    private OtcPaymentTermMapper otcPaymentTermMapper;

    private static final Integer PAY_COUNT_LIMIT = 3;

    @Override
    public void addPaymentTerm(OTCNewPaymentRequest request, StreamObserver<OTCNewPaymentResponse> responseObserver) {
//        OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfoForUpdate(request.getAccountId());
//        if (userInfo == null || StringUtils.isBlank(userInfo.getNickName())) {
//            responseObserver.onNext(OTCNewPaymentResponse.newBuilder()
//                .setResult(OTCResult.NICK_NAME_NOT_SET)
//                .build());
//            responseObserver.onCompleted();
//            return;
//        }
        Stopwatch sw = Stopwatch.createStarted();
        try {
            if (request.getPaymentTypeValue() == 2) {
                responseObserver.onNext(OTCNewPaymentResponse.newBuilder()
                        .setResult(OTCResult.NOT_SUPPORT_WECHAT_PAYMENT_LIMIT)
                        .build());
                responseObserver.onCompleted();
                return;
            }
            //如果已经有三个展示的支付方式了 那就默认隐藏 如果三个以下就展示
            int visible
                    = otcPaymentTermMapper.selectCountVisiblePay(request.getAccountId(), OTCVisibleEnum.ENABLE_VALUE);
            Date now = new Date();
            OtcPaymentTerm otcPaymentTerm = OtcPaymentTerm.builder()
                    .accountId(request.getAccountId())
                    .realName(request.getRealName())
                    .paymentType(request.getPaymentTypeValue())
                    .bankCode(request.getBankName())
                    .bankName(request.getBankName())
                    .branchName(request.getBranchName())
                    .accountNo(request.getAccountNo())
                    .qrcode(request.getQrcode())
                    .payMessage("")
                    .firstName("")
                    .lastName("")
                    .secondLastName("")
                    .clabe("")
                    .debitCardNumber("")
                    .mobile("")
                    .businessName("")
                    .concept("")
                    .status(1)
                    .visible(visible >= PAY_COUNT_LIMIT ? OTCVisibleEnum.DISABLE_VALUE : OTCVisibleEnum.ENABLE_VALUE)
                    .createDate(now)
                    .updateDate(now)
                    .build();
            otcPaymentTermService.addPaymentTerm(otcPaymentTerm);
            responseObserver.onNext(OTCNewPaymentResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        } finally {
            log.info("addPaymentTerm consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void addNewPaymentTerm(OTCCreateNewPaymentRequest request, StreamObserver<OTCCreateNewPaymentResponse> responseObserver) {
//        OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfoForUpdate(request.getAccountId());
//        if (userInfo == null || StringUtils.isBlank(userInfo.getNickName())) {
//            responseObserver.onNext(OTCNewPaymentResponse.newBuilder()
//                .setResult(OTCResult.NICK_NAME_NOT_SET)
//                .build());
//            responseObserver.onCompleted();
//            return;
//        }
        //禁止微信支付
        if (request.getPaymentTypeValue() == 2) {
            responseObserver.onNext(OTCCreateNewPaymentResponse.newBuilder()
                    .setResult(OTCResult.NOT_SUPPORT_WECHAT_PAYMENT_LIMIT)
                    .build());
            responseObserver.onCompleted();
            return;
        }
        Stopwatch sw = Stopwatch.createStarted();
        try {
            //如果已经有三个展示的支付方式了 那就默认隐藏 如果三个以下就展示
            int visible
                    = otcPaymentTermMapper.selectCountVisiblePay(request.getAccountId(), OTCVisibleEnum.ENABLE_VALUE);
            Date now = new Date();
            OtcPaymentTerm otcPaymentTerm = OtcPaymentTerm.builder()
                    .accountId(request.getAccountId())
                    .realName(request.getRealName())
                    .paymentType(request.getPaymentTypeValue())
                    .bankCode(request.getBankName())
                    .bankName(request.getBankName())
                    .branchName(request.getBranchName())
                    .accountNo(request.getAccountNo())
                    .qrcode(request.getQrcode())
                    .payMessage(request.getPayMessage())
                    .firstName(request.getFirstName())
                    .lastName(request.getLastName())
                    .secondLastName(request.getSecondLastName())
                    .clabe(request.getClabe())
                    .debitCardNumber(request.getDebitCardNumber())
                    .mobile(request.getMobile())
                    .businessName(request.getBusinesName())
                    .concept(request.getConcept())
                    .status(1)
                    .visible(visible >= PAY_COUNT_LIMIT ? OTCVisibleEnum.DISABLE_VALUE : OTCVisibleEnum.ENABLE_VALUE)
                    .createDate(now)
                    .updateDate(now)
                    .build();
            otcPaymentTermService.addPaymentTerm(otcPaymentTerm);
            responseObserver.onNext(OTCCreateNewPaymentResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS)
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("addPaymentTerm consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void getPaymentTerms(OTCGetPaymentRequest request, StreamObserver<OTCGetPaymentResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();

        try {
            OTCGetPaymentResponse.Builder responseBuilder = OTCGetPaymentResponse.newBuilder();
            List<OtcPaymentTerm> paymentTermList = otcPaymentTermService.getOtcPaymentTerm(request.getAccountId());

            List<Integer> brokerPaymentConfigList = otcPaymentTermService.getBrokerPaymentConfigList(request.getBaseRequest().getOrgId());


            if (!CollectionUtils.isEmpty(paymentTermList)) {
                List<OTCPaymentTerm> otcPaymentTermList = paymentTermList.stream()
                        .filter(otcPaymentTerm -> brokerPaymentConfigList.contains(otcPaymentTerm.getPaymentType()))
                        .map(ConvertUtil::convertToOTCPaymentTerm)
                        .collect(Collectors.toList());
                responseBuilder.addAllPaymentTerms(otcPaymentTermList);
            }

            responseBuilder.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getPaymentTerms consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void updatePaymentTerm(OTCUpdatePaymentRequest request,
                                  StreamObserver<OTCUpdatePaymentResponse> responseObserver) {
        //禁止微信支付
        if (request.getPaymentTypeValue() == 2) {
            responseObserver.onNext(OTCUpdatePaymentResponse.newBuilder()
                    .setResult(OTCResult.NOT_SUPPORT_WECHAT_PAYMENT_LIMIT)
                    .build());
            responseObserver.onCompleted();
            return;
        }
        OtcPaymentTerm otcPaymentTerm = OtcPaymentTerm.builder()
                .accountId(request.getAccountId())
                .realName(request.getRealName())
                .paymentType(request.getPaymentTypeValue())
                .bankCode(request.getBankName())
                .bankName(request.getBankName())
                .branchName(request.getBranchName())
                .accountNo(request.getAccountNo())
                .qrcode(request.getQrcode())
                .updateDate(new Date())
                .id(request.getId())
                .payMessage("")
                .firstName("")
                .lastName("")
                .secondLastName("")
                .clabe("")
                .debitCardNumber("")
                .mobile("")
                .businessName("")
                .concept("")
                .build();

        OTCUpdatePaymentResponse.Builder updatePaymentResponse = OTCUpdatePaymentResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcPaymentTermService.updateOtcPaymentTerm(otcPaymentTerm);
            updatePaymentResponse.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(updatePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (UpdatedVersionException e) {
            log.warn("delete payment term failed", e);
            updatePaymentResponse.setResult(OTCResult.UPDATED_VERSION);
            responseObserver.onNext(updatePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (BusinessException e) {
            log.warn("delete payment term failed", e);
            updatePaymentResponse.setResult(OTCResult.SYS_ERROR);
            responseObserver.onNext(updatePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("updatePaymentTerm consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void updateNewPaymentTerm(OTCUpdateNewPaymentRequest request,
                                     StreamObserver<OTCUpdateNewPaymentResponse> responseObserver) {
        //禁止微信支付
        if (request.getPaymentTypeValue() == 2) {
            responseObserver.onNext(OTCUpdateNewPaymentResponse.newBuilder()
                    .setResult(OTCResult.NOT_SUPPORT_WECHAT_PAYMENT_LIMIT)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        OtcPaymentTerm otcPaymentTerm = OtcPaymentTerm.builder()
                .accountId(request.getAccountId())
                .realName(request.getRealName())
                .paymentType(request.getPaymentTypeValue())
                .bankCode(request.getBankName())
                .bankName(request.getBankName())
                .branchName(request.getBranchName())
                .accountNo(request.getAccountNo())
                .qrcode(request.getQrcode())
                .updateDate(new Date())
                .id(request.getId())
                .payMessage(request.getPayMessage())
                .firstName(request.getFirstName())
                .lastName(request.getLastName())
                .secondLastName(request.getSecondLastName())
                .clabe(request.getClabe())
                .debitCardNumber(request.getDebitCardNumber())
                .mobile(request.getMobile())
                .businessName(request.getBusinesName())
                .concept(request.getConcept())
                .build();

        OTCUpdateNewPaymentResponse.Builder updateNewPaymentResponse = OTCUpdateNewPaymentResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcPaymentTermService.updateNewOtcPaymentTerm(otcPaymentTerm);
            updateNewPaymentResponse.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(updateNewPaymentResponse.build());
            responseObserver.onCompleted();
        } catch (UpdatedVersionException e) {
            log.warn("delete payment term failed", e);
            updateNewPaymentResponse.setResult(OTCResult.UPDATED_VERSION);
            responseObserver.onNext(updateNewPaymentResponse.build());
            responseObserver.onCompleted();
        } catch (BusinessException e) {
            log.warn("delete payment term failed", e);
            updateNewPaymentResponse.setResult(OTCResult.SYS_ERROR);
            responseObserver.onNext(updateNewPaymentResponse.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("updatePaymentTerm consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }


    @Override
    public void configPaymentTerm(OTCConfigPaymentRequest request,
                                  StreamObserver<OTCConfigPaymentResponse> responseObserver) {
        List<OtcPaymentTerm> paymentTermList = otcPaymentTermService.getVisibleOtcPaymentTerm(request.getAccountId(), request.getBaseRequest().getOrgId());
        Stopwatch sw = Stopwatch.createStarted();
        try {
            if (CollectionUtils.isEmpty(paymentTermList) || paymentTermList.size() == 1) {
                responseObserver.onNext(OTCConfigPaymentResponse.newBuilder()
                        .setResult(OTCResult.PAYMENT_NOT_EXIST)
                        .build());
            } else {
                otcPaymentTermService.configOtcPaymentTerm(request.getAccountId(), request.getPaymentTypeValue(),
                        request.getVisibleValue());
                responseObserver.onNext(OTCConfigPaymentResponse.newBuilder()
                        .setResult(OTCResult.SUCCESS)
                        .build());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("configPaymentTerm consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void deletePaymentTerm(OTCDeletePaymentRequest request, StreamObserver<OTCDeletePaymentResponse> responseObserver) {
        OTCDeletePaymentResponse.Builder otcDeletePaymentResponse = OTCDeletePaymentResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcPaymentTermService.deleteUserPaymentTerm(request.getPaymentId(), request.getAccountId());
            otcDeletePaymentResponse.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(otcDeletePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (PaymentTermNotFindException e) {
            log.warn("delete payment term failed", e);
            otcDeletePaymentResponse.setResult(OTCResult.PAYMENT_NOT_EXIST);
            responseObserver.onNext(otcDeletePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (LeastOnePaymentException e) {
            log.warn("delete payment term failed", e);
            otcDeletePaymentResponse.setResult(OTCResult.AT_LEAST_ONE_PAYMENT_METHOD);
            responseObserver.onNext(otcDeletePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (BusinessException e) {
            log.warn("delete payment term failed", e);
            otcDeletePaymentResponse.setResult(OTCResult.SYS_ERROR);
            responseObserver.onNext(otcDeletePaymentResponse.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("deletePaymentTerm consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void switchPaymentVisible(SwitchPaymentVisibleRequest request, StreamObserver<SwitchPaymentVisibleResponse> responseObserver) {
        SwitchPaymentVisibleResponse.Builder switchPaymentVisibleResponse = SwitchPaymentVisibleResponse.newBuilder();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcPaymentTermService.switchPaymentVisible(request.getPaymentId(), request.getAccountId(), request.getVisibleValue(), request.getRealName(), request.getIsBusines());
            switchPaymentVisibleResponse.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (PaymentTermNotFindException e) {
            log.warn("switch payment visible failed", e);
            switchPaymentVisibleResponse.setResult(OTCResult.PAYMENT_NOT_EXIST);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (LeastOnePaymentException e) {
            log.warn("switch payment visible failed", e);
            switchPaymentVisibleResponse.setResult(OTCResult.AT_LEAST_ONE_PAYMENT_METHOD);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (ThreePaymentException e) {
            log.warn("switch payment visible failed", e);
            switchPaymentVisibleResponse.setResult(OTCResult.OPEN_UP_TO_THREE_PAYMENT_METHOD);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (BusinessException e) {
            log.warn("switch payment visible failed", e);
            switchPaymentVisibleResponse.setResult(OTCResult.SYS_ERROR);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (NonPersonPaymentException e) {
            log.warn("NonPersonPaymentException failed", e);
            switchPaymentVisibleResponse.setResult(OTCResult.NON_PERSON_PAYMENT_LIMIT);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (WeChatLimitException e) {
            log.warn("WeChatLimitException failed", e);
            switchPaymentVisibleResponse.setResult(OTCResult.NOT_SUPPORT_WECHAT_PAYMENT_LIMIT);
            responseObserver.onNext(switchPaymentVisibleResponse.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("switchPaymentVisible consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS), ConvertUtil.messageToString(request));
        }

    }

    @Override
    public void getBrokerPaymentConfig(GetBrokerPaymentConfigRequest request, StreamObserver<GetBrokerPaymentConfigResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            GetBrokerPaymentConfigResponse.Builder responseBuilder = GetBrokerPaymentConfigResponse.newBuilder();

            List<OtcPaymentItems> brokerPaymentConfigList = otcPaymentTermService.getBrokerPaymentConfig(request.getBaseRequest().getOrgId(), request.getBaseRequest().getLanguage());
            if (!CollectionUtils.isEmpty(brokerPaymentConfigList)) {

                List<GetBrokerPaymentConfigResponse.OTCPaymentItems> otcPaymentTermList = brokerPaymentConfigList.stream()
                        .map(this::convertOTCPaymentItems)
                        .collect(Collectors.toList());
                responseBuilder.addAllPaymentItems(otcPaymentTermList);
            }
            responseBuilder.setResult(OTCResult.SUCCESS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getBrokerPaymentConfig consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    private GetBrokerPaymentConfigResponse.OTCPaymentItems convertOTCPaymentItems(OtcPaymentItems otcPaymentItems) {
        return GetBrokerPaymentConfigResponse.OTCPaymentItems.newBuilder()
                .setPaymentTypeValue(otcPaymentItems.getPaymentType())
                .setPaymentItems(otcPaymentItems.getPaymentItems())
                .build();
    }

    @Override
    public void queryOtcPaymentTermList(QueryOtcPaymentTermListRequest request, StreamObserver<QueryOtcPaymentTermListResponse> observer) {
        QueryOtcPaymentTermListResponse response;
        try {
            response = otcPaymentTermService.queryOtcPaymentTermList(request.getPage(), request.getSize(),request.getAccountId());
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("queryOtcPaymentTermList error", e);
            observer.onError(e);
        }
    }

    @Override
    public void batchUpdatePaymentVisible(BatchUpdatePaymentVisibleRequest request, StreamObserver<BatchUpdatePaymentVisibleResponse> observer) {
        try {
            int row = otcPaymentTermService.batchUpdatePaymentVisible(request.getIds());
            log.info("batchUpdatePaymentVisible row {}", row);
            observer.onNext(BatchUpdatePaymentVisibleResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            log.error("batchUpdatePaymentVisible error", e);
            observer.onError(e);
        }
    }
}