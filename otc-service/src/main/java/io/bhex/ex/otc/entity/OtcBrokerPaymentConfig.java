package io.bhex.ex.otc.entity;


import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_broker_payment_config")
public class OtcBrokerPaymentConfig {


    /**
     * 券商ID
     */
    @Id
    private Long orgId;

    /**
     * 配置支持的支付方式
     */
    private String paymentConfig;

}
