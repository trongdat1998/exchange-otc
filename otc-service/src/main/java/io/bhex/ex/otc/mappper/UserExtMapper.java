package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.UserExt;
import tk.mybatis.mapper.common.IdsMapper;
import tk.mybatis.mapper.common.Mapper;

@org.apache.ibatis.annotations.Mapper
public interface UserExtMapper extends Mapper<UserExt>, IdsMapper<UserExt> {
}
