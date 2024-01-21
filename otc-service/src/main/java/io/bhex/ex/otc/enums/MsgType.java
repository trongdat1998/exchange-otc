package io.bhex.ex.otc.enums;

/**
 * 消息类型
 *
 * @author lizhen
 * @date 2018-09-14
 */
public enum MsgType {

    SYS_MSG(0, "系统消息"),

    WORD_MSG(1, "自定义文字消息"),

    IMAGE_MSG(2, "图片消息"),


    SYS_APPEAL_WORD_MSG(101, "申诉文字消息"),
    SYS_APPEAL_IMAGE_MSG(102, "申诉图片消息");

    private int type;

    private String name;

    MsgType(int type, String name) {
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