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
 * otc交易对配置信息
 *
 * @author lizhen
 * @date 2018-11-08
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_symbol")
public class OtcSymbol {

    public static final int SYMBOL_STATUS_ON = 1;

    public static final int SYMBOL_STATUS_OFF = -1;

    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;

    /**
     * 交易所id
     */
    private Long exchangeId;

    /**
     * token
     */
    private String tokenId;

    /**
     * 数字货币id
     */
    private String currencyId;

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
}