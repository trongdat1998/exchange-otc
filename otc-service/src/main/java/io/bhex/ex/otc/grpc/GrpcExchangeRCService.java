package io.bhex.ex.otc.grpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.bhex.base.rc.RcBalanceResponse;
import io.bhex.base.rc.UserRequest;
import io.bhex.ex.otc.config.GrpcClientFactory;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class GrpcExchangeRCService {

    @Autowired
    private GrpcClientFactory grpcClientFactory;

    public RcBalanceResponse getUserRcBalance(UserRequest request) {
        try {
            return grpcClientFactory.exchangeRCServiceBlockingStub().getUserRcBalance(request);
        } catch (StatusRuntimeException e) {
            log.error("getUserRcBalance exception  ", e);
            throw e;
        }
    }
}
