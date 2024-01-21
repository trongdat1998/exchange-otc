package io.bhex.ex.otc.server;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.base.grpc.server.interceptor.GrpcServerLogInterceptor;
import io.bhex.ex.otc.GetAllStatisticDataRequest;
import io.bhex.ex.otc.GetAllStatisticDataResponse;
import io.bhex.ex.otc.OTCResult;
import io.bhex.ex.otc.OTCStatisticData;
import io.bhex.ex.otc.OTCStatisticServiceGrpc;
import io.bhex.ex.otc.entity.OtcStatisticData;
import io.bhex.ex.otc.service.OtcStatisticDataService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

/**
 * 数据统计grpc服务接口
 *
 * @author lizhen
 * @date 2018-12-07
 */
@Slf4j
@GrpcService(interceptors = GrpcServerLogInterceptor.class)
public class OtcStatisticDataServiceGrpcImpl extends OTCStatisticServiceGrpc.OTCStatisticServiceImplBase {

    @Autowired
    private OtcStatisticDataService otcStatisticDataService;

    @Override
    public void getAllStatisticData(GetAllStatisticDataRequest request,
                                    StreamObserver<GetAllStatisticDataResponse> responseObserver) {

        Stopwatch sw = Stopwatch.createStarted();
        try {

            List<OtcStatisticData> dataList = otcStatisticDataService.getDataList(request.getOrgId(),
                    request.getType(), request.getDate());
            List<OTCStatisticData> resultList = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(dataList)) {
                resultList = dataList.stream().map(
                        otcStatisticData -> OTCStatisticData.newBuilder()
                                .setOrgId(otcStatisticData.getOrgId())
                                .setType(otcStatisticData.getType())
                                .setDate(otcStatisticData.getStatisticDate())
                                .setStatisticDetail(otcStatisticData.getStatisticDetail())
                                .setAmount(otcStatisticData.getAmount().stripTrailingZeros().toPlainString())
                                .setCreateDate(otcStatisticData.getCreateDate().getTime())
                                .build()
                ).collect(Collectors.toList());
            }
            responseObserver.onNext(GetAllStatisticDataResponse.newBuilder()
                    .setResult(OTCResult.SUCCESS)
                    .addAllData(resultList)
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            responseObserver.onError(e);
        } finally {
            log.info("getAllStatisticData consume={} mill", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }

    }
}