package io.bhex.ex.otc.mappper;

import java.util.List;

import io.bhex.ex.otc.entity.OtcStatisticData;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import tk.mybatis.mapper.common.Mapper;

/**
 * @author lizhen
 * @date 2018-12-05
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcStatisticDataMapper extends Mapper<OtcStatisticData> {

    @Insert("<script>insert into tb_otc_statistic_data (org_id, statistic_date, type, statistic_detail, amount, "
            + "create_date) values <foreach collection='list' item='data' index='idx' separator=','>"
            + "(#{data.orgId}, #{data.statisticDate}, #{data.type}, #{data.statisticDetail}, #{data.amount}, "
            + "#{data.createDate})</foreach></script>")
    int batchInsertStatisticData(@Param("list") List<OtcStatisticData> list);
}