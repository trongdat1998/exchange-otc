package io.bhex.ex.otc.enums;

/**
 * 订单状态
 *
 * @author lizhen
 * @date 2018-09-14
 */
public enum OrderStatus {

    DELETE(0, "删除，无效的"),

    INIT(1, "初始化"),

    NORMAL(10, "创建成功，待支付"),

    UNCONFIRM(20, "已支付，待确认"),

    APPEAL(30, "申诉中"),

    CANCEL(40, "撤销"),

    FINISH(50, "完全成交");

    private int status;

    private String name;

    OrderStatus(int status, String name) {
        this.status = status;
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public static OrderStatus valueOf(int status) {
        for (OrderStatus o : OrderStatus.values()) {
            if (status == o.status) {
                return o;
            }
        }
        return null;
    }
}