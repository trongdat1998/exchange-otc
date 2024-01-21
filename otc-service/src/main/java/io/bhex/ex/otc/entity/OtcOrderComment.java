package io.bhex.ex.otc.entity;

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
 * 订单评价
 *
 * @author lizhen
 * @date 2018-09-10
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_comment")
public class OtcOrderComment {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 评价者用户id
     */
    private Long userId;
    /**
     * 评价者账户id
     */
    private Long accountId;
    /**
     * 评价者昵称
     */
    private String nickName;
    /**
     * 被评价者账户id
     */
    private Long targetAccountId;
    /**
     * 订单ID
     */
    private Long orderId;
    /**
     * 评价方（1 买方，2 卖方）
     */
    private Integer side;
    /**
     * 评价类型 0-好评，1-差评
     */
    private Integer type;
    /**
     * 等级，[0,5]
     */
    private Integer star;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;
}