package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-10-17
 */
public class UserBuyTradeLimitException extends RuntimeException {

    public UserBuyTradeLimitException(String message) {
        super(message);
    }
}