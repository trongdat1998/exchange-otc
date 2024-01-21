package io.bhex.ex.otc.entity;

import lombok.*;

import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_depth_share_whitelist")
public class OtcDepthShareBrokerWhiteList {

    @Id
    private Long brokerId;
}