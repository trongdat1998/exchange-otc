package io.bhex.ex.otc.entity;

import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_broker_currency")
public class OtcBrokerCurrency implements Serializable {

    public static final int AVAILABLE = 1;

    /**
     * id
     */
    @Id
    private Integer id;
    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * 法币代码
     */
    private String code;
    /**
     * 银行名称
     */
    private String name;
    /**
     * 多语言
     */
    private String language;
    /**
     * 最小计价单位
     */
    private BigDecimal minQuote;
    /**
     * 最大限额
     */
    private BigDecimal maxQuote;
    /**
     * 精度
     */
    private Integer scale;
    /**
     * 状态  1：可用   -1：不可用
     */
    private Integer status;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

    /**
     * 法币成交额精度
     */
    private Integer amountScale;

    public Integer getScaleSafe() {
        if (Objects.isNull(this.scale)) {
            return Integer.valueOf(0);
        }

        return this.scale;
    }
}
