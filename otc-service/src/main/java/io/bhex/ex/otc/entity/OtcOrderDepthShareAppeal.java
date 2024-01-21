package io.bhex.ex.otc.entity;

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
 * @see io.bhex.ex.otc.enums.OrderStatus
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_depth_share_appeal")
public class OtcOrderDepthShareAppeal {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 订单id
     */
    private Long orderId;
    /**
     * 券商id
     */
    private Long brokerId;
    /**
     * 操作人id
     */
    private Long adminId;
    /**
     * 状态
     */
    private Short status;
    /**
     * 备注
     */
    private String comment;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;


}
