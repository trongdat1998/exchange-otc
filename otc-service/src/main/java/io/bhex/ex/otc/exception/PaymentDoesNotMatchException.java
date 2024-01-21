package io.bhex.ex.otc.exception;

public class PaymentDoesNotMatchException extends RuntimeException {

    public PaymentDoesNotMatchException(String message) {
        super(message);
    }
}
