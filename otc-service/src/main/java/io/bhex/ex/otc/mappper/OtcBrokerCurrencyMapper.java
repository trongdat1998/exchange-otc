package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcBrokerCurrency;
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
public interface OtcBrokerCurrencyMapper extends Mapper<OtcBrokerCurrency> {

    @Select("select * from tb_otc_broker_currency where code=#{code} and language=#{language} and org_id=#{orgId}")
    OtcBrokerCurrency queryOtcCurrency(@Param("code") String code, @Param("language") String language, @Param("orgId") Long orgId);

    @Insert("INSERT INTO `tb_otc_broker_currency` select null, #{orgId}, `code`, `name`, `language`, `min_quote`, `max_quote`, `scale`, `status`, `create_date`, `update_date`,amount_scale from `tb_otc_broker_currency` where org_id = #{fromOrgId}")
    int initOtcBrokerCurrency(@Param("orgId") Long orgId, @Param("fromOrgId") Long fromOrgId);

    @Select("select *from tb_otc_broker_currency where org_id = #{orgId}")
    List<OtcBrokerCurrency> queryOtcBrokerCurrencyByOrgId(@Param("orgId") Long orgId);

    @Select("select code from tb_otc_broker_currency where org_id = #{orgId} and status = 1 and code != 'CNY' group by code")
    List<OtcBrokerCurrency> queryBrokerAllCurrencyConfig(@Param("orgId") Long orgId);

    @Select("select code from tb_otc_broker_currency where org_id = #{orgId} and status = 1 group by code")
    List<OtcBrokerCurrency> queryBrokerCurrencyConfig(@Param("orgId") Long orgId);
}