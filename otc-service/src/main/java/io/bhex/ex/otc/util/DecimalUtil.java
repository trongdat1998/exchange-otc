package io.bhex.ex.otc.util;

import java.math.BigDecimal;
import java.util.Objects;

import io.bhex.base.constants.ProtoConstants;
import io.bhex.ex.proto.Decimal;

/**
 * @author lizhen
 * @date 2018-09-19
 */
public class DecimalUtil {

    public static BigDecimal toBigDecimal(Decimal decimalValue) {
        if (null != decimalValue.getStr() && !"".equals(decimalValue.getStr().trim())) {
            return new BigDecimal(decimalValue.getStr()).setScale(ProtoConstants.PRECISION, ProtoConstants.ROUNDMODE);
        }
        return BigDecimal.valueOf(decimalValue.getUnscaledValue(),
                decimalValue.getScale()).setScale(ProtoConstants.PRECISION, ProtoConstants.ROUNDMODE);
    }

    public static BigDecimal toBigDecimal(Decimal decimalValue, BigDecimal defaultValue) {
        try {
            return toBigDecimal(decimalValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Decimal fromBigDecimal(BigDecimal bigDecimalValue) {
        return Decimal.newBuilder()
                .setStr(bigDecimalValue.toPlainString())
                .setScale(bigDecimalValue.scale())
                .build();
    }

    public static String bigDecimalToString(BigDecimal bigDecimalValue) {

        if (Objects.isNull(bigDecimalValue)) {
            return "null";
        }

        return bigDecimalValue.stripTrailingZeros().toPlainString();

    }
}