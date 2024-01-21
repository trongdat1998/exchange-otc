package io.bhex.ex.otc.service.order;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 短消息
 *
 * @author lizhen
 * @date 2018-11-15
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class OrderMsg {

    private int msgCode;

    private String language;

    private long orgId;

    private long userId;

    private String buyer;

    private String seller;

    private String tokenId;

    private String currencyId;

    private String quantity;

    private String amount;

    private Integer cancelTime;

    private Integer appealTime;

    private Long orderId;

    /**
     * 订单类型 0-买入；1-卖出
     */
    private Integer side;
}