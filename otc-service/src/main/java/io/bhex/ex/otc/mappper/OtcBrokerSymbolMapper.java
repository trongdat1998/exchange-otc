package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcBrokerSymbol;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.IdsMapper;
import tk.mybatis.mapper.common.Mapper;
import tk.mybatis.mapper.common.special.InsertListMapper;

import java.util.List;

/**
 * @author lizhen
 * @date 2018-11-04
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcBrokerSymbolMapper extends Mapper<OtcBrokerSymbol>, IdsMapper<OtcBrokerSymbol>, InsertListMapper<OtcBrokerSymbol> {

    @Insert("INSERT INTO `tb_otc_broker_symbol` select null, #{orgId}, #{exchangeId}, `token_id`, `currency_id`, `status`, `create_date`, `update_date` from `tb_otc_broker_symbol` where org_id = #{fromOrgId} and token_id in (${token})")
    int initOtcBrokerSymbol(@Param("orgId") Long orgId, @Param("fromOrgId") Long fromOrgId, @Param("exchangeId") Long exchangeId, @Param("token") String token);

    @Select("select *from tb_otc_broker_symbol where org_id = #{orgId}")
    List<OtcBrokerSymbol> queryOtcBrokerSymbolByOrgId(@Param("orgId") Long orgId);
}
