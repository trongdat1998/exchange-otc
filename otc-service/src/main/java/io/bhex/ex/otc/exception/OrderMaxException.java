package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-11-08
 */
public class OrderMaxException extends RuntimeException {

    public OrderMaxException(String message) {
        super(message);
    }
}