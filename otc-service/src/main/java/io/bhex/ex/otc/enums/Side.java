package io.bhex.ex.otc.enums;

/**
 * 下单方向
 *
 * @author lizhen
 * @date 2018-09-14
 */
public enum Side {

    BUY(0),

    SELL(1);

    private int code;

    Side(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static Side valueOf(int code) {
        for (Side s : Side.values()) {
            if (code == s.code) {
                return s;
            }
        }
        return null;
    }
}