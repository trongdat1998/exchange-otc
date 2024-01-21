package io.bhex.ex.otc.entity;

import lombok.*;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;
import java.util.Objects;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_symbol_share")
public class OtcSymbolShare {

    public static final int AVAILABLE = 1;

    /**
     * id
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Integer id;
    /**
     * 交易所ID
     */
    private Long exchangeId;
    /**
     * 券商ID
     */
    private Long brokerId;
    /**
     * token币种
     */
    private String tokenId;
    /**
     * 法币币种
     */
    private String currencyId;
    /**
     * 状态  1：可用   -1：不可用
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

    public boolean shareStatusBool() {
        if (Objects.isNull(this.status)) {
            return false;
        }

        return status.intValue() == 1;
    }
}
