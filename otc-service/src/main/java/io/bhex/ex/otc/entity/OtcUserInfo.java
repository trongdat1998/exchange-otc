package io.bhex.ex.otc.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.Objects;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * otc用户信息表
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_user_info")
public class OtcUserInfo implements Serializable {
    /**
     * id
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 账户id
     */
    private Long accountId;
    /**
     * 券商id
     */
    private Long orgId;
    /**
     * 用户id
     */
    private Long userId;
    /**
     * 用户昵称
     */
    private String nickName;
    /**
     * 用户状态
     */
    private Integer status;
    /**
     * 总单数
     */
    private Integer orderNum;
    /**
     * 成交单数
     */
    private Integer executeNum;
    /**
     * 最近下单数（每日统计）
     */
    private Integer recentOrderNum;
    /**
     * 最近成交单数（每日统计）
     */
    private Integer recentExecuteNum;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;

    //联系电话
    private String mobile;

    private String email;

    private BigDecimal completeRateDay30;

    private Integer orderTotalNumberDay30;

    private Integer orderFinishNumberDay30;

    //交易用户的KYC姓名
    private String userFirstName;

    private String userSecondName;

    @Transient
    private UserExt userExt;

    public String getEmailSafe() {
        return Strings.nullToEmpty(this.email);
    }

    public String getMobileSafe() {
        return Strings.nullToEmpty(this.mobile);
    }

    public Integer getOrderTotalNumberDay30Safe() {
        if (Objects.isNull(orderTotalNumberDay30)) {
            return Integer.valueOf(0);
        }

        return orderTotalNumberDay30;
    }

    public Integer getOrderFinishNumberDay30Safe() {
        if (Objects.isNull(orderFinishNumberDay30)) {
            return Integer.valueOf(0);
        }

        return orderFinishNumberDay30;
    }

    public BigDecimal getCompleteRateDay30Safe() {
        if (Objects.isNull(completeRateDay30)) {
            return BigDecimal.ZERO;
        }

        return completeRateDay30;
    }

    public BigDecimal getExecuteRate() {
        if (Objects.isNull(recentOrderNum) || Objects.isNull(recentExecuteNum) ||
                recentOrderNum.intValue() == 0 || recentExecuteNum.intValue() == 0) {
            return BigDecimal.ZERO;
        }

        //成单率计算
        return new BigDecimal(recentExecuteNum).divide(new BigDecimal(recentOrderNum), 4, RoundingMode.HALF_EVEN);
    }
}