package io.bhex.ex.otc.entity;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lizhen
 * @date 2018-12-05
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_statistic_data")
public class OtcStatisticData {

    private Integer id;

    private Long orgId;

    private String statisticDate;

    private Integer type;

    private String statisticDetail;

    private BigDecimal amount;

    private Date createDate;
}