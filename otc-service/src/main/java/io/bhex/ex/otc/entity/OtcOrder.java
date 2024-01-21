package io.bhex.ex.otc.entity;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

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
@Table(name = "tb_otc_order")
@EqualsAndHashCode(of = "id")
public class OtcOrder {
    /**
     * ID
     */
    @Id
    //@GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 交易所id
     */
    private Long exchangeId;
    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 用户id
     */
    private Long userId;
    /**
     * 客户端id
     */
    private Long clientOrderId;
    /**
     * 订单类型 0-买入；1-卖出
     */
    private Integer side;
    /**
     * 商品id
     */
    private Long itemId;
    /**
     * maker id
     */
    private Long accountId;
    /**
     * taker id
     */
    private Long targetAccountId;
    /**
     * 币种
     */
    private String tokenId;

    private String tokenName;
    /**
     * 法币币种
     */
    private String currencyId;
    /**
     * 成交单价
     */
    private BigDecimal price;
    /**
     * 成交数量
     */
    private BigDecimal quantity;
    /**
     * 订单金额
     */
    private BigDecimal amount;
    /**
     * 手续费
     */
    private BigDecimal fee;
    /**
     * 付款参考号
     */
    private String payCode;
    /**
     * 付款方式
     */
    private Integer paymentType;
    /**
     * 转账日期
     */
    private Date transferDate;
    /**
     * 状态
     *
     * @see io.bhex.ex.otc.OTCOrderStatusEnum
     */
    private Integer status;
    /**
     * 申诉类型
     */
    private Long appealAccountId;
    /**
     * 申诉类型
     */
    private Integer appealType;
    /**
     * 申诉内容
     */
    private String appealContent;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

    /**
     * 风险资金释放释放 0历史数据不处理 1:共享券商未释放 2:已释放 3:非共享券商未释放
     */
    private Integer freed;

    private Integer riskBalanceType; //0-默认，由系统确认 1-强制加入风险资金，2-强制成非风险资金

    /**
     * maker fee
     */
    private BigDecimal makerFee;

    /**
     * taker fee
     */
    private BigDecimal takerFee;

    /**
     * 共享深度标记，0=不共享，1=共享
     */
    private Short depthShare;

    /**
     * 成交方券商ID
     */
    private Long matchOrgId;

    /**
     * maker券商id
     */
    @Transient
    private Long makerBrokerId;

    @Transient
    private Long takerBrokerId;

    @Transient
    private String language;

    @Transient
    private OtcOrderExt orderExt;


    @Transient
    private Long transferDateTime;

    @Transient
    private Long createDateTime;

    @Transient
    private Long updateDateTime;

    /**
     * 是否是商家 true 是 false 否
     */
    @Transient
    private Boolean isBusiness;

    public boolean getDepthShareBool() {
        if (Objects.isNull(this.depthShare)) {
            return false;
        }

        return depthShare.intValue() == 1;
    }

    public void setDepthShareBool(Boolean isShared) {
        if (Objects.isNull(isShared)) {
            return;
        }

        this.depthShare = (short) (isShared ? 1 : 0);
    }
}