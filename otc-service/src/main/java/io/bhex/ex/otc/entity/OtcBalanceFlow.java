package io.bhex.ex.otc.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

/**
 * 动账流水表
 *
 * @author lizhen
 * @date 2018-09-18
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_balance_flow")
public class OtcBalanceFlow {
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
     * 金额
     */
    private BigDecimal amount;
    /**
     * 流水/业务类型
     */
    private Integer flowType;
    /**
     * 业务id
     */
    private Long objectId;
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

    /**
     * 手续费
     */
    private BigDecimal fee;
}