package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-10-17
 */
public class ParamErrorException extends RuntimeException {

    public ParamErrorException(String message) {
        super(message);
    }
}