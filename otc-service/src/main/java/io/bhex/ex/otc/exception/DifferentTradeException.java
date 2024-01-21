package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-10-17
 */
public class DifferentTradeException extends RuntimeException {

    public DifferentTradeException(String message) {
        super(message);
    }
}