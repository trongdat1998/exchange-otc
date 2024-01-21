package io.bhex.ex.otc.enums;

/**
 * 价格类型
 *
 * @author lizhen
 * @date 2018-10-04
 */
public enum PriceType {

    PRICE_FIXED(0),

    PRICE_FLOATING(1);

    private int type;

    PriceType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}