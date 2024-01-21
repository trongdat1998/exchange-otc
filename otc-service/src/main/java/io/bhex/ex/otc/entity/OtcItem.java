package io.bhex.ex.otc.entity;

import io.bhex.ex.otc.OTCItemStatusEnum;
import io.bhex.ex.otc.util.CommonUtil;
import lombok.*;
import org.apache.commons.collections4.CollectionUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * 商品/广告表
 *
 * @author lizhen
 * @date 2018-09-11
 * @see OTCItemStatusEnum
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_item")
public class OtcItem implements Serializable {
    /**
     * ID
     */
    @Id
    //@GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 交易所ID
     */
    private Long exchangeId;
    /**
     * 券商ID
     */
    private Long orgId;
    /**
     * 用户ID
     */
    private Long userId;
    /**
     * 账户ID
     */
    private Long accountId;
    /**
     * 币种
     */
    private String tokenId;
    /**
     * 法币币种
     */
    private String currencyId;
    /**
     * 客户端id
     */
    private Long clientItemId;
    /**
     * 广告类型 0.买入 1.卖出
     */
    private Integer side;
    /**
     * 推荐级别 0最高
     */
    private Integer recommendLevel;
    /**
     * 定价类型 0-固定价格；1-浮动价格
     */
    private Integer priceType;
    /**
     * 单价
     */
    private BigDecimal price;
    /**
     * 溢价比例 -5 - 5
     */
    private BigDecimal premium;
    /**
     * 剩余数量
     */
    private BigDecimal lastQuantity;
    /**
     * 数量
     */
    private BigDecimal quantity;
    /**
     * 冻结数量(未成交订单中数量)
     */
    private BigDecimal frozenQuantity;
    /**
     * 已成交数量
     */
    private BigDecimal executedQuantity;
    /**
     * 付款期限
     */
    private Integer paymentPeriod;
    /**
     * 单笔最小交易额（钱）
     */
    private BigDecimal minAmount;
    /**
     * 单笔最大交易额（钱）
     */
    private BigDecimal maxAmount;
    /**
     * 保证金(买)/手续费(卖)
     */
    private BigDecimal marginAmount;
    /**
     * 交易说明
     */
    private String remark;
    /**
     * 只与高级认证交易： 0 否， 1是
     */
    private Integer onlyHighAuth;
    /**
     * 自动回复
     */
    private String autoReply;
    /**
     * 状态
     *
     * @see io.bhex.ex.otc.OTCItemStatusEnum
     */
    private Integer status;
    /**
     * 订单数量
     */
    private Integer orderNum;
    /**
     * 完成数量
     */
    private Integer finishNum;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

    /**
     * 已收手续费
     */
    private BigDecimal fee;

    /**
     * 冻结手续费
     */
    private BigDecimal frozenFee;

    /**
     * token配置
     */
    @Transient
    private OtcBrokerToken tokenConfig;

    /**
     * 货币配置
     */
    @Transient
    private List<OtcBrokerCurrency> currencyConfig;

    /**
     * 30天成交率
     */
    @Transient
    private BigDecimal competeRate;

    /**
     * 30天成单数
     */
    @Transient
    private Integer finishOrderNumber;

    public void addTokenConfig(OtcBrokerToken brokerToken) {

        if (Objects.nonNull(brokerToken)) {
            this.tokenConfig = brokerToken;
        }
    }


    public void addCurrencyConfig(List<OtcBrokerCurrency> currencies) {

        if (CollectionUtils.isEmpty(currencies)) {
            return;
        }

        this.currencyConfig = currencies;
    }

    public Integer getFinishNumSafe() {
        if (Objects.isNull(finishNum)) {
            return 0;
        }

        return finishNum;
    }

    public String getPriceStr() {
        return CommonUtil.BigDecimalToString(this.price);
    }

    public BigDecimal getCompeteRate() {
        if (Objects.isNull(this.competeRate)) {
            return BigDecimal.ZERO;
        }

        return this.competeRate;
    }

    public Integer getFinishOrderNumber() {
        if (Objects.isNull(this.finishOrderNumber)) {
            return Integer.valueOf(1);
        }

        return this.finishOrderNumber;
    }

    @Transient
    public BigDecimal getFrozenQuantitySafe() {
        if (Objects.isNull(this.frozenQuantity)) {
            return BigDecimal.ZERO;
        }

        return this.frozenQuantity;
    }


}