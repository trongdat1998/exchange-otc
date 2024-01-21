package io.bhex.ex.otc.mappper;

import org.apache.ibatis.annotations.Select;

import java.util.List;

import io.bhex.ex.otc.entity.OtcMessageConfig;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface OtcMessageConfigMapper extends Mapper<OtcMessageConfig> {

    @Select("select *from tb_otc_message_config")
    List<OtcMessageConfig> queryAll();
}
