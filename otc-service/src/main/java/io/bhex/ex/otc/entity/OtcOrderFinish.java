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
 * 订单表
 *
 * @author lizhen
 * @date 2018-09-11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_finish")
public class OtcOrderFinish {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 券商id
     */
    private Long orgId;
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
     * maker 用户昵称
     */
    private String nickName;
    /**
     * taker id
     */
    private Long targetAccountId;
    /**
     * taker 用户昵称
     */
    private String targetNickName;
    /**
     * 币种
     */
    private String tokenId;
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