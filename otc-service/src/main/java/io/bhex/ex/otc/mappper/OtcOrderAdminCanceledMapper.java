package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcOrderAdminCanceled;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.Date;

@org.apache.ibatis.annotations.Mapper
public interface OtcOrderAdminCanceledMapper extends Mapper<OtcOrderAdminCanceled> {

    @Select("select sum(a.num) from (" +
            " select count(1) num  from tb_otc_order_admin_canceled  where target_account_id=#{accountId} and side=1  and create_date between #{start} and #{end} " +
            " union all " +
            " select count(1) num  from tb_otc_order_admin_canceled  where account_id=#{accountId} and side=0  and create_date between #{start} and #{end} " +
            ") as a"
    )
    int countExclusiveOrder(@Param("accountId") Long accountId, @Param("start") Date start, @Param("end") Date end);
}
