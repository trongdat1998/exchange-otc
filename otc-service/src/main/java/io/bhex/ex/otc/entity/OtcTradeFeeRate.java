package io.bhex.ex.otc.entity;


import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_trade_fee_rate")
public class OtcTradeFeeRate {
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
     * token币种
     */
    private String tokenId;

    /**
     * maker fee rate
     */
    private BigDecimal makerBuyFeeRate;
    /**
     * trade fee rate
     */
    private BigDecimal makerSellFeeRate;

    private Date createdAt;

    private Date updatedAt;

    /**
     * 0 未删除 1 已删除
     */
    private Integer deleted;
}
