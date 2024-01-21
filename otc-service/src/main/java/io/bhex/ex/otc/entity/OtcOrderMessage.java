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
 * 订单消息（聊天记录）
 *
 * @author lizhen
 * @date 2018-09-09
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_message")
public class OtcOrderMessage {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 订单id
     */
    private Long orderId;
    /**
     * 账户id，0表示系统发送
     */
    private Long accountId;
    /**
     * 消息类型
     */
    private Integer msgType;
    /**
     * 消息编码
     */
    private Integer msgCode;
    /**
     * 消息内容
     */
    private String message;
    /**
     * 创建时间
     */
    private Date createDate;
}