package io.bhex.ex.otc.enums;

/**
 * MakerTaker
 *
 * @author yuehao
 * @date 2019-04-15
 */
public enum MakerTakerType {

    MAKER_TYPE(1, "MAKER"),

    TAKER_TYPE(2, "TAKER");

    private int type;

    private String name;

    MakerTakerType(int type, String name) {
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