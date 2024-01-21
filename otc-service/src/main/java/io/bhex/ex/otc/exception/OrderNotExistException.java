package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-09-23
 */
public class OrderNotExistException extends RuntimeException {

    public OrderNotExistException(String message) {
        super(message);
    }
}