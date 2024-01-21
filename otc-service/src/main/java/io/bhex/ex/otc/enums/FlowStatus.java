package io.bhex.ex.otc.enums;

/**
 * 动账流水状态
 *
 * @author lizhen
 * @date 2018-09-18
 */
public enum FlowStatus {

    WAITING_PROCESS(1, "等待处理"),

    SUCCESS(2, "处理成功");

    private int status;

    private String name;

    FlowStatus(int status, String name) {
        this.status = status;
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }
}