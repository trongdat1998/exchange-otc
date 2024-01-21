package io.bhex.ex.otc.exception;


/**
 * 至少一种支付方式
 *
 * @author yuehao
 * @date 2019-03-29
 */
public class LeastOnePaymentException extends RuntimeException {

    public LeastOnePaymentException(String message) {
        super(message);
    }
}