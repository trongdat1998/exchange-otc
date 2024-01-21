package io.bhex.ex.otc.exception;

/**
 * 业务异常
 *
 * @author lizhen
 * @date 2018-09-19
 */
public class BusinessException extends RuntimeException {

    public BusinessException(String message) {
        super(message);
    }
}