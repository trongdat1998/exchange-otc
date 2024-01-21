package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcSymbol;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/**
 * otc交易对配置信息mapper
 *
 * @author lizhen
 * @date 2018-11-08
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcSymbolMapper extends Mapper<OtcSymbol> {

    @Insert("insert INTO `tb_otc_symbol` select null,#{exchangeId}, `token_id`, `currency_id`, `status`, `create_date`, `update_date`  from tb_otc_symbol where exchange_id= #{fromExchangeId} and token_id in (${token})")
    int initOtcSymbol(@Param("exchangeId") Long exchangeId, @Param("fromExchangeId") Long fromExchangeId, @Param("token") String token);

    @Select("select *from tb_otc_symbol where exchange_id=#{exchangeId}")
    List<OtcSymbol> queryOtcSymbolByExchangeId(@Param("exchangeId") Long exchangeId);
}