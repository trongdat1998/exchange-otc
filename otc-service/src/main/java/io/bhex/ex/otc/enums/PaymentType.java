package io.bhex.ex.otc.enums;

/**
 * 付款方式
 *
 * @author lizhen
 * @date 2018-09-19
 */
public enum PaymentType {

    BANK(0, "银行卡"),

    ALIPAY(1, "支付宝"),

    WECHAT(2, "微信");

    private int type;

    private String name;

    PaymentType(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}