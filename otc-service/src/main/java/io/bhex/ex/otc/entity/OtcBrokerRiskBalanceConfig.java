package io.bhex.ex.otc.entity;

import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;


/**
 * 券商对于非共享订单成交的资产是否进行风险资产锁定及锁定时间的配置
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_broker_risk_balance_config")
public class OtcBrokerRiskBalanceConfig {
    /**
     * id
     */
    @Id
    private Integer id;

    /**
     * 券商ID
     */
    private Long orgId;

    /**
     * 状态: 1 有效 2 无效
     */
    private Integer status;

    /**
     * 风险资产持续时间（小时）
     */
    private Integer hours;

    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

}
