package io.bhex.ex.otc.inteceptor;

import io.bhex.ex.otc.exception.BusinessException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * grpc异常拦截处理
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@Component
@Scope("singleton")
@Aspect
public class GrpcExceptionAspect {

    @Around(value = "execution(public * io.bhex.ex.otc.server.*.*(..))")
    public Object aroundGrpcExecute(ProceedingJoinPoint point) {
        try {
            return point.proceed();
        } catch (BusinessException re) {
            log.error("inner business execute exception", re);
            handleException(point, Status.INTERNAL);
        } catch (Throwable e) {
            log.error("grpc request exception", e);
            handleException(point, Status.INVALID_ARGUMENT);
        }
        return null;
    }

    private void handleException(ProceedingJoinPoint point, Status status) {
        try {
            if (point.getArgs() != null && point.getArgs().length == 2) {
                StreamObserver streamObserver = (StreamObserver) point.getArgs()[1];
                streamObserver.onError(new StatusRuntimeException(status));
            }
        } catch (Exception ex) {
            log.error("exception handle error", ex);
        }
    }
}