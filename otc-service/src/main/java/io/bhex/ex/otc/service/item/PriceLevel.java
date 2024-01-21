package io.bhex.ex.otc.service.item;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

import io.bhex.ex.otc.util.CommonUtil;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lizhen
 * @date 2018-10-04
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PriceLevel implements Serializable {
    /**
     * 价格
     */
    private BigDecimal price;
    /**
     * 数量
     */
    private BigDecimal quantity;
    /**
     * 挂单个数
     */
    private int size;

    public String getPriceStr() {
        return CommonUtil.BigDecimalToString(this.price);
    }


    public void addQuantity(BigDecimal number) {
        if (Objects.isNull(number)) {
            return;
        }

        this.quantity.add(number);
    }

    public void addSize(int number) {
        this.size += number;
    }


}