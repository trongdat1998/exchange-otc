package io.bhex.ex.otc.entity;

import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * 订单表
 *
 * @author lizhen
 * @date 2018-09-11
 * @see io.bhex.ex.otc.enums.OrderStatus
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_ext")
@EqualsAndHashCode(of = "orderId")
public class OtcOrderExt {
    /**
     * ID
     */
    @Id
    private Long orderId;
    /**
     * 券商id
     */
    private Long orgId;

    /**
     * token精度
     */
    private Integer tokenScale;

    /**
     * 法币精度
     */
    private Integer currencyScale;

    /**
     * 法币成交精度
     */
    private Integer currencyAmountScale;

    /**
     * 是否是商家 0不是 1是
     */
    private Integer isBusiness;

    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;
}