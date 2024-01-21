package io.bhex.ex.otc.enums;

/**
 * 申诉类型
 *
 * @author lizhen
 * @date 2018-10-05
 */
public enum AppealType {

    NOT_PAY(0, "买方没有打款"),

    NOT_PASS(1, "卖方没有放币"),

    ABUSE(2, "对方言语侮辱"),

    NOT_REPLY(3, "对方没有回复信息"),

    OTHER(4, "其他");

    private int type;

    private String name;

    AppealType(int type, String name) {
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