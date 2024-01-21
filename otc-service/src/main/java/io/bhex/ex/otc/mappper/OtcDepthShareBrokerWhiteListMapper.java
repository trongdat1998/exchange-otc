package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcDepthShareBrokerWhiteList;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@org.apache.ibatis.annotations.Mapper
public interface OtcDepthShareBrokerWhiteListMapper extends Mapper<OtcDepthShareBrokerWhiteList> {

    @Select("select *from tb_otc_depth_share_whitelist where broker_id = #{orgId} limit 0,1")
    OtcDepthShareBrokerWhiteList queryByOrgId(@Param("orgId") Long orgId);

    @Select("select *from tb_otc_depth_share_whitelist")
    List<OtcDepthShareBrokerWhiteList> queryAll();
}
