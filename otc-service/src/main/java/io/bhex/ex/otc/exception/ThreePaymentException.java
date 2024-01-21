package io.bhex.ex.otc.exception;


/**
 * 未找到支付方式异常
 *
 * @author yuehao
 * @date 2019-03-29
 */
public class ThreePaymentException extends RuntimeException {

    public ThreePaymentException(String message) {
        super(message);
    }
}