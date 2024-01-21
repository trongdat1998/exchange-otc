package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcBrokerToken;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcBrokerTokenMapper extends Mapper<OtcBrokerToken> {

    @Select("select * from tb_otc_broker_token order by sequence desc")
    List<OtcBrokerToken> queryAllTokenList();

    @Select("select * from tb_otc_broker_token where org_id=#{orgId} order by sequence desc ")
    List<OtcBrokerToken> queryTokenListByOrgId(@Param("orgId") Long orgId);

    @Insert("INSERT INTO `tb_otc_broker_token` select null, #{orgId}, `token_id`,  `token_name`,`min_quote`, `max_quote`, `scale`, `status`, `create_date`, `update_date`,up_range,down_range,sequence,share_status  from `tb_otc_broker_token` where org_id = #{fromOrgId} and token_id in(${token})")
    int initOtcBrokerToken(@Param("orgId") Long orgId, @Param("fromOrgId") Long fromOrgId, @Param("token") String token);

    @Select("select *from tb_otc_broker_token where org_id = #{orgId}")
    List<OtcBrokerToken> queryOtcBrokerTokenByOrgId(@Param("orgId") Long orgId);

    @Select("select *from tb_otc_broker_token where org_id = #{orgId} and status = 1")
    List<OtcBrokerToken> queryEffectiveOtcBrokerTokenByOrgId(@Param("orgId") Long orgId);
}
