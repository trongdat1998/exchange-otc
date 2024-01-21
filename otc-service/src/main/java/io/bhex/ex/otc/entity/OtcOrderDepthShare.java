package io.bhex.ex.otc.entity;

import io.bhex.ex.otc.enums.OrderStatus;
import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * 订单共享深度信息表
 *
 * @author lizhen
 * @date 2018-09-11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_depth_share")
public class OtcOrderDepthShare {

    /**
     * 订单id
     */
    @Id
    private Long orderId;
    /**
     * maker交易所ID
     */
    private Long makerExchangeId;
    /**
     * taker券商id
     */
    private Long takerOrgId;

    /**
     * maker券商id
     */
    private Long makerOrgId;
    /**
     * 商品id
     */
    private Long itemId;
    /**
     * maker accountId
     */
    private Long makerAccountId;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

    private Integer status;


}
