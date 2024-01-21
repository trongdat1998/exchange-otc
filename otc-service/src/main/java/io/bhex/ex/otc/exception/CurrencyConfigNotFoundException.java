package io.bhex.ex.otc.exception;

/**
 * @author lizhen
 * @date 2018-10-17
 */
public class CurrencyConfigNotFoundException extends RuntimeException {

    public CurrencyConfigNotFoundException(String message) {
        super(message);
    }
}