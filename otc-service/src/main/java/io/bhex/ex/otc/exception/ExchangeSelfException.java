package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-10-17
 */
public class ExchangeSelfException extends RuntimeException {

    public ExchangeSelfException(String message) {
        super(message);
    }
}