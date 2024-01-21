package io.bhex.ex.otc.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 *
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_otc_order_admin_canceled")
@Slf4j
public class OtcOrderAdminCanceled {

    @Id
    private Long orderId;

    private Long accountId;

    private Long targetAccountId;

    private Integer side;

    private Date createDate;
}
