package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-11-08
 */
public class CancelOrderMaxTimesException extends RuntimeException {

    public CancelOrderMaxTimesException(String message) {
        super(message);
    }
}