package io.bhex.ex.otc.exception;

/**
 * 升级版本异常
 *
 * @author lizhen
 * @date 2019-04-09
 */
public class UpdatedVersionException extends RuntimeException {

    public UpdatedVersionException(String message) {
        super(message);
    }
}