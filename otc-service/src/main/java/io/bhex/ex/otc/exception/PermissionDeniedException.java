package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-09-23
 */
public class PermissionDeniedException extends RuntimeException {

    public PermissionDeniedException(String message) {
        super(message);
    }
}