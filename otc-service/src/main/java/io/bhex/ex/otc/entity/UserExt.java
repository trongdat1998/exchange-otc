package io.bhex.ex.otc.entity;


import io.bhex.ex.otc.util.CommonUtil;
import lombok.*;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_user_ext")
public class UserExt {

    @Id
    private Long userId;

    private Long accountId;

    @Column(name = "usdt_value_24hours_buy")
    private BigDecimal usdtValue24HoursBuy;

    private Date createdAt;

    private Date updatedAt;

    public String usdtValue24HoursBuyToString() {
        return CommonUtil.BigDecimalToString(this.usdtValue24HoursBuy);
    }

}
