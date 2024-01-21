package io.bhex.ex.otc.server;

import com.google.common.base.Stopwatch;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.InitOtcConfigRequest;
import io.bhex.ex.otc.InitOtcConfigResponse;
import io.bhex.ex.otc.OTCAdminServiceGrpc;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.service.OtcUtilService;
import io.bhex.ex.otc.util.ConvertUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;


@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcAdminServiceGrpcImpl extends OTCAdminServiceGrpc.OTCAdminServiceImplBase {

    // 暂时固定ID 直接进行copy 如需外层需要指定from参数可以改造
    private static final Long FROM_ORG_ID = 7007L;

    private static final Long FROM_EXCHANGE_ID = 602L;

    @Autowired
    private OtcUtilService otcUtilService;

    @Override
    public void initOtcConfig(InitOtcConfigRequest request, StreamObserver<InitOtcConfigResponse> responseObserver) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            otcUtilService.createOtc(request.getOrgId(), FROM_ORG_ID, request.getExchangeId(), FROM_EXCHANGE_ID, "'USDT','BTC','ETH'");
            responseObserver.onNext(InitOtcConfigResponse.newBuilder().setResult(OTCResult.SUCCESS).build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("init otc config ,consume={} mill,request={}",
                    sw.stop().elapsed(TimeUnit.MILLISECONDS),
                    ConvertUtil.messageToString(request));
        }
    }
}
