package io.bhex.ex.otc.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_otc_order_index")
@Slf4j
@Builder(builderClassName = "Builder", toBuilder = true)
public class OrderIndex {

    @Id
    private Long id;
    private Long orderId;
    private Long accountId;
    private Short status;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * 修改时间
     */
    private Date updateDate;
}
