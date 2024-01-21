package io.bhex.ex.otc.server;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.GetUserByNicknameRequest;
import io.bhex.ex.otc.GetUserByNicknameResponse;
import io.bhex.ex.otc.ListOtcUserRequest;
import io.bhex.ex.otc.ListOtcUserResponse;
import io.bhex.ex.otc.OTCGetNickNameRequest;
import io.bhex.ex.otc.OTCGetNickNameResponse;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OTCSetNickNameRequest;
import io.bhex.ex.otc.OTCSetNickNameResponse;
import io.bhex.ex.otc.OTCSetWhiteUserRequest;
import io.bhex.ex.otc.OTCSetWhiteUserResponse;
import io.bhex.ex.otc.OTCUser;
import io.bhex.ex.otc.OTCUserContactResponse;
import io.bhex.ex.otc.OTCUserServiceGrpc;
import io.bhex.ex.otc.entity.OtcBrokerCurrency;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.enums.UserStatusFlag;
import io.bhex.ex.otc.mappper.OtcBrokerCurrencyMapper;
import io.bhex.ex.otc.service.OtcUserInfoService;
import io.bhex.ex.otc.util.CommonUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * 用户信息grpc服务接口
 *
 * @author lizhen
 * @date 2018-09-19
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcUserServiceGrpcImpl extends OTCUserServiceGrpc.OTCUserServiceImplBase {

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Resource
    private OtcBrokerCurrencyMapper otcBrokerCurrencyMapper;

    @Override
    public void setNickName(OTCSetNickNameRequest request, StreamObserver<OTCSetNickNameResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCSetNickNameResponse.Builder responseBuilder = OTCSetNickNameResponse.newBuilder();
            OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfo(request.getNickName());
            if (userInfo != null) {
                responseBuilder.setResult(OTCResult.NICK_NAME_EXIST);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            Date now = new Date();
            OtcUserInfo otcUserInfo = OtcUserInfo.builder()
                    .accountId(request.getAccountId())
                    .orgId(request.getBaseRequest().getOrgId())
                    .userId(request.getBaseRequest().getUserId())
                    .nickName(request.getNickName())
                    .status(0)
                    .orderNum(0)
                    .executeNum(0)
                    .recentOrderNum(0)
                    .recentExecuteNum(0)
                    .createDate(now)
                    .updateDate(now)
                    .email(Strings.EMPTY)
                    .mobile(Strings.EMPTY)
                    .build();
            otcUserInfoService.setNickName(otcUserInfo);
            responseBuilder.setResult(OTCResult.SUCCESS);
            responseBuilder.setNickName(request.getNickName());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("setNickName consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void getNickName(OTCGetNickNameRequest request, StreamObserver<OTCGetNickNameResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            //获取券商配置的法币
            OtcUserInfo userInfo = otcUserInfoService.getOtcUserInfo(request.getAccountId());
            OTCGetNickNameResponse.Builder response = OTCGetNickNameResponse.newBuilder();
            if (userInfo != null) {
                List<String> currencyList = new ArrayList<>();
                if (userInfo != null && userInfo.getOrgId() > 0) {
                    List<OtcBrokerCurrency> currencies = this.otcBrokerCurrencyMapper.queryBrokerAllCurrencyConfig(userInfo.getOrgId());
                    currencyList = currencies.stream().map(OtcBrokerCurrency::getCode).collect(Collectors.toList());
                }
                response.setResult(OTCResult.SUCCESS)
                        .setNickName(StringUtils.isNotBlank(userInfo.getNickName()) ? userInfo.getNickName() : "")
                        .setTradeFlag(userInfo.getStatus())
                        .setOrderNum(userInfo.getOrderNum())
                        .setExecuteNum(userInfo.getExecuteNum())
                        .setRecentOrderNum(userInfo.getOrderTotalNumberDay30Safe())
                        .setRecentExecuteNum(userInfo.getOrderFinishNumberDay30Safe());
                response.addAllCurrencyList(currencyList);
            } else {
                response.setResult(OTCResult.NICK_NAME_NOT_SET);
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getNickName consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void setWhiteUser(OTCSetWhiteUserRequest request, StreamObserver<OTCSetWhiteUserResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcUserInfoService.setStatus(request.getAccountId(), UserStatusFlag.OTC_WHITE, 1);
            responseObserver.onNext(OTCSetWhiteUserResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS)
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("setWhiteUser consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void getUserByNickname(GetUserByNicknameRequest request,
                                  StreamObserver<GetUserByNicknameResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            GetUserByNicknameResponse.Builder builder = GetUserByNicknameResponse.newBuilder();
            if (StringUtils.isEmpty(request.getNickname())) {
                builder.setResult(OTCResult.PARAM_ERROR);
            } else {
                OtcUserInfo user = otcUserInfoService.getUserByNickname(request.getNickname());
                if (Objects.nonNull(user)) {
                    OTCUser ou = OTCUser.newBuilder()
                            .setId(user.getId())
                            .setUserId(user.getUserId())
                            .setOrgId(user.getOrgId())
                            .setAccountId(user.getAccountId())
                            .setExecuteNum(user.getExecuteNum())
                            .setNickname(user.getNickName())
                            .setOrderNum(user.getOrderNum())
                            .setRecentExecuteNum(user.getOrderFinishNumberDay30Safe())
                            .setRecentOrderNum(user.getOrderTotalNumberDay30Safe())
                            .setStatus(user.getStatus())
                            .build();
                    builder.setUser(ou).setResult(OTCResult.SUCCESS);
                } else {
                    builder.setResult(OTCResult.NO_DATA);
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getUserByNickname consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void getUserContact(OTCGetNickNameRequest request,
                               StreamObserver<OTCUserContactResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OTCUserContactResponse.Builder builder = OTCUserContactResponse.newBuilder();
            if (request.getAccountId() < 1L) {
                builder.setResult(OTCResult.PARAM_ERROR);
            } else {
                OtcUserInfo user = otcUserInfoService.getOtcUserInfo(request.getAccountId());
                if (Objects.nonNull(user)) {
                    builder.setMobile(user.getMobileSafe())
                            .setEmail(user.getEmailSafe())
                            .setResult(OTCResult.SUCCESS);
                } else {
                    builder.setResult(OTCResult.NO_DATA);
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getUserContact consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }

    @Override
    public void listUser(ListOtcUserRequest request,
                         StreamObserver<ListOtcUserResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {
            ListOtcUserResponse.Builder builder = ListOtcUserResponse.newBuilder();
            Pair<List<OtcUserInfo>, Integer> pair = otcUserInfoService.listUserInfo(request.getPageNo(), request.getPageSize(), request.getUserIdsList());

            List<OTCUser> list = pair.getLeft().stream().map(i -> {

                OTCUser.Ext.Builder exBuilder = OTCUser.Ext.newBuilder();
                if (Objects.nonNull(i.getUserExt())) {
                    exBuilder.setUserId(i.getUserId())
                            .setUsdtValue24HoursBuy(i.getUserExt().usdtValue24HoursBuyToString());
                }

                return OTCUser.newBuilder()
                        .setAccountId(i.getAccountId())
                        .setUserId(i.getUserId())
                        .setId(i.getId())
                        .setStatus(i.getStatus())
                        .setNickname(i.getNickName())
                        .setFinishOrderNumber30Days(i.getOrderFinishNumberDay30Safe())
                        .setFinishOrderRate30Days(CommonUtil.BigDecimalToString(i.getCompleteRateDay30Safe()))
                        .setExt(exBuilder.build())
                        .build();

            }).collect(Collectors.toList());
            builder.addAllUsers(list).setTotal(pair.getRight()).setResult(OTCResult.SUCCESS);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("listUser consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }
}