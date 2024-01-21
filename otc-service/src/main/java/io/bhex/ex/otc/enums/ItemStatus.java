package io.bhex.ex.otc.enums;

/**
 * @author lizhen
 * @date 2018-09-14
 */
public enum ItemStatus {

    DELETE(0, "删除，无效的"),

    INIT(1, "初始化"),

    NORMAL(10, "正常、有效"),

    OFFLINE(15, "下架"),

    CANCEL(20, "撤销"),

    FINISH(30, "完全成交");

    private int status;

    private String name;

    ItemStatus(int status, String name) {
        this.status = status;
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public static ItemStatus valueOf(int status) {
        for (ItemStatus o : ItemStatus.values()) {
            if (status == o.status) {
                return o;
            }
        }
        return null;
    }
}