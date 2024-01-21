package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-10-17
 */
public class QuantityNotEnoughException extends RuntimeException {

    public QuantityNotEnoughException(String message) {
        super(message);
    }
}