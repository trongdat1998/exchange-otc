package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcUserInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

/**
 * otc用户信息mapper
 *
 * @author lizhen
 * @date 2018-09-16
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcUserInfoMapper extends Mapper<OtcUserInfo> {

    @Select("select * from tb_otc_user_info where account_id = #{accountId} for update")
    OtcUserInfo selectOtcUserInfoForUpdate(@Param("accountId") Long accountId);

    @Update("update tb_otc_user_info set order_num = order_num + #{num}, recent_order_num = recent_order_num + #{num} "
            + "where account_id = #{accountId}")
    int increaseOrderNum(@Param("accountId") Long accountId, @Param("num") Integer num);

    @Update("update tb_otc_user_info set execute_num = execute_num + 1, recent_execute_num = recent_execute_num + 1 "
            + "where account_id = #{accountId}")
    int increaseExecuteNum(@Param("accountId") Long accountId);

    @Update("update tb_otc_user_info set status = #{status} where account_id = #{accountId}")
    int setStatus(@Param("accountId") Long accountId, @Param("status") Integer status);
}