package io.bhex.ex.otc.entity;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 商品/广告表
 *
 * @author lizhen
 * @date 2018-09-11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_item_finish")
public class OtcItemFinish {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 用户ID
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
     * 定价类型 1-固定价格；2-浮动价格
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
}