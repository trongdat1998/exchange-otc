package io.bhex.ex.otc.enums;

/**
 * 消息类型
 *
 * @author lizhen
 * @date 2018-09-14
 */
public enum MsgCode {

    BUY_CREATE_MSG_TO_BUYER(1010, "买单，下单成功（买方消息）"),

    BUY_CREATE_MSG_TO_SELLER(1011, "买单，下单成功（卖方消息）"),

    BUY_CREATE_MSG_TO_BUYER_TIME(1012, "买单，下单成功（买方消息）"),

    SELL_CREATE_MSG_TO_BUYER_TIME(1013, "卖单，下单成功（买方消息）"),

    PAY_MSG_TO_BUYER(1020, "支付成功（买方消息）"),

    PAY_MSG_TO_SELLER(1021, "支付成功（卖方消息）"),

    BUY_APPEAL_MSG_TO_BUYER(1030, "买方投诉卖方，申诉（买方消息）"),

    BUY_APPEAL_MSG_TO_SELLER(1031, "买方投诉卖方，申诉（卖方消息）"),

    CANCEL_MSG_TO_BUYER(1040, "撤单（买方消息）"),

    CANCEL_MSG_TO_SELLER(1041, "撤单（卖方消息）"),

    FINISH_MSG_TO_BUYER(1050, "放币完结（买方消息）"),

    FINISH_MSG_TO_SELLER(1051, "放币完结（卖方消息）"),


    SELL_CREATE_MSG_TO_BUYER(2010, "卖单，下单成功（买方消息）"),

    SELL_CREATE_MSG_TO_SELLER(2011, "卖单，下单成功（卖方消息）"),

    SELL_APPEAL_MSG_TO_BUYER(2030, "卖方投诉买方，申诉（买方消息）"),

    SELL_APPEAL_MSG_TO_SELLER(2031, "卖方投诉买方，申诉（卖方消息）"),

    ORDER_AUTO_CANCEL(3040, "超时订单自动取消"),

    ORDER_AUTO_APPEAL_TO_BUYER(3030, "确认放币超时订单自动申诉（买方消息）"),

    ORDER_AUTO_APPEAL_TO_SELLER(3031, "确认放币超时订单自动申诉（卖方消息）"),


    ITEM_AUTO_OFFLINE_SMALL_QUANTITY(5001, "广告剩余数量较小自动下架");

    private int code;

    private String name;

    MsgCode(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}