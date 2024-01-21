package io.bhex.ex.otc.enums;

/**
 * 动账流水类型
 *
 * @author lizhen
 * @date 2018-09-18
 */
public enum FlowType {

    BACK_ITEM_FROZEN(5, "返还广告单冻结"),

    BACK_ORDER_FROZEN(6, "返还订单冻结"),

    ADD_AVAILABLE(3, "增加可用"),

    SUBTRACT_FROZEN(4, "扣除冻结");

    private int type;

    private String name;

    FlowType(int type, String name) {
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