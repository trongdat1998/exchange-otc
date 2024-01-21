package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcTradeFeeRate;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.Collection;
import java.util.List;

@org.apache.ibatis.annotations.Mapper
public interface OtcTradeFeeRateMapper extends Mapper<OtcTradeFeeRate> {

    @Select("select *from tb_otc_trade_fee_rate where org_id = #{orgId} and token_id= #{tokenId} and deleted = 0 limit 1")
    OtcTradeFeeRate queryOtcTradeFeeByTokenId(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);

    @Select("select *from tb_otc_trade_fee_rate where deleted = 0")
    List<OtcTradeFeeRate> queryAllOtcTradeFee();

    @Insert("insert into tb_otc_trade_fee_rate (org_id, token_id, maker_buy_fee_rate, maker_sell_fee_rate, created_at, updated_at, deleted) " +
            " select #{orgId}, token_id, maker_buy_fee_rate, maker_sell_fee_rate, created_at, updated_at, deleted from tb_otc_trade_fee_rate where org_id=#{fromOrgId} ")
    void initTradeFee(@Param("orgId") Long orgId, @Param("fromOrgId") Long fromOrgId);

    @Select("select *from tb_otc_trade_fee_rate where org_id = #{orgId}")
    List<OtcTradeFeeRate> queryOtcTradeFeeByOrgId(@Param("orgId") Long orgId);
}
