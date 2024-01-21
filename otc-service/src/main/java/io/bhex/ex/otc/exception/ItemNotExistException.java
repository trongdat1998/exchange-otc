package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-09-23
 */
public class ItemNotExistException extends RuntimeException {

    public ItemNotExistException(String message) {
        super(message);
    }
}