package io.bhex.ex.otc.enums;

/**
 * @author lizhen
 * @date 2018-12-05
 */
public enum StatisticType {

    TOKEN_TRADE_AMOUNT(1, "数字货币交易额"),

    CURRENCY_TRADE_AMOUNT(2, "法币交易额"),

    FINISH_ORDER_COUNT(3, "交易笔数"),

    FINISH_ORDER_USER_COUNT(4, "交易人数"),

    CANCEL_ORDER_COUNT(5, "取消订单数量"),

    CANCEL_ORDER_USER_COUNT(6, "取消订单用户数量"),

    APPEAL_ORDER_COUNT(7, "申诉订单数量"),

    APPEAL_ORDER_USER_COUNT(8, "申诉订单用户数量"),

    CREATE_ITEM_COUNT(9, "发布广告数量"),

    FINISH_ORDER_TOTAL_USER_COUNT(10, "交易人数"),

    CANCEL_ORDER_TOTAL_USER_COUNT(11, "取消订单用户总数量"),

    APPEAL_ORDER_TOTAL_USER_COUNT(12, "申诉订单用户总数量");

    private int type;

    private String desc;

    StatisticType(int type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public int getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }
}