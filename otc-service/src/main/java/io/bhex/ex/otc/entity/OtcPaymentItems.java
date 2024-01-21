package io.bhex.ex.otc.entity;


import lombok.*;

import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_payment_items")
public class OtcPaymentItems {

    /**
     * 支付方式
     */
    private Integer paymentType;

    /**
     * 支付方式要素项
     */
    private String paymentItems;

    /**
     * 语言
     */
    private String language;
}
