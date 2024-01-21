package io.bhex.ex.otc.enums;

/**
 * 用户状态标识位
 *
 * @author lizhen
 * @date 2018-11-11
 */
public enum UserStatusFlag {

    RECENT_TRADE_FLAG(1),

    OTC_WHITE(2);

    private int bit;

    UserStatusFlag(int bit) {
        this.bit = bit;
    }

    public int getBit() {
        return bit;
    }
}