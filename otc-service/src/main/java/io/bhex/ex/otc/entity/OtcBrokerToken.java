package io.bhex.ex.otc.entity;

import io.bhex.ex.otc.util.CommonUtil;
import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Data
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(of = {"orgId", "tokenId"})
@Table(name = "tb_otc_broker_token")
public class OtcBrokerToken implements Serializable {

    public static final int AVAILABLE = 1;

    public static final int SHAREABLE = 1;


    /**
     * id
     */
    @Id
    private Long id;
    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * token币种
     */
    private String tokenId;

    /**
     * token名字
     */
    private String tokenName;
    /**
     * 最小计价单位
     */
    private BigDecimal minQuote;
    /**
     * 最大交易单位
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
     * 浮动范围最大值
     */
    private BigDecimal upRange;

    /**
     * 浮动范围最小值
     */
    private BigDecimal downRange;

    /**
     * 排序值
     */
    private Integer sequence;

    /**
     * 共享状态
     */
    private Integer shareStatus;

    @Transient
    private FeeRate feeRate;

    public boolean shareStatusBool() {
        if (Objects.isNull(this.shareStatus)) {
            return false;
        }

        return this.shareStatus.equals(SHAREABLE);
    }


    @Data
    @lombok.Builder(builderClassName = "Builder")
    @NoArgsConstructor
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class FeeRate {
        private BigDecimal sellRate;
        private BigDecimal buyRate;

        public String buyRateToString() {
            return CommonUtil.BigDecimalToString(this.buyRate);
        }

        public String sellRateToString() {
            return CommonUtil.BigDecimalToString(this.sellRate);
        }
    }
}
