package io.bhex.ex.otc.exception;


/**
 * 非本人支付方式异常
 *
 * @author yuehao
 * @date 2019-03-29
 */
public class NonPersonPaymentException extends RuntimeException {

    public NonPersonPaymentException(String message) {
        super(message);
    }
}