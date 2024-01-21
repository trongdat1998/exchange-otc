package io.bhex.ex.otc.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

public final class CommonUtil {

    private CommonUtil() {
    }

    public static String BigDecimalToString(BigDecimal value) {
        if (Objects.isNull(value)) {
            return "0";
        }

        return value.stripTrailingZeros().toPlainString();
    }

    public static <T> T nullToDefault(T value, T defaultValue) {
        if (Objects.isNull(value)) {
            return defaultValue;
        }


        return value;
    }

    public static void main(String[] args) {
        BigDecimal bg = new BigDecimal("1.005").multiply(new BigDecimal("6.92"));
        System.out.println(bg);
        System.out.println(bg.setScale(2, RoundingMode.HALF_EVEN));
        System.out.println(bg.setScale(2, RoundingMode.DOWN));
        System.out.println(bg);
    }
}
