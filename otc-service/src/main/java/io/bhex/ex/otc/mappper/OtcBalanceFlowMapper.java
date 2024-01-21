package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcBalanceFlow;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.util.Date;

/**
 * 动账流水mapper
 *
 * @author lizhen
 * @date 2018-09-18
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcBalanceFlowMapper extends Mapper<OtcBalanceFlow> {

    @Update("update tb_otc_balance_flow set status = #{status}, update_date = #{updateDate} where id = #{id} "
            + "and status = #{oldStatus}")
    int updateOtcBalanceFlowStatus(@Param("id") Long id,
                                   @Param("status") Integer status,
                                   @Param("oldStatus") Integer oldStatus,
                                   @Param("updateDate") Date updateDate);


    @Select("select *from tb_otc_balance_flow where object_id = #{orderId} and account_id=#{accountId} limit 0,1")
    OtcBalanceFlow queryByOrderId(@Param("orderId") Long orderId, @Param("accountId") Long accountId);
}