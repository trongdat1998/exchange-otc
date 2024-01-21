package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OrderIndex;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

@Mapper
@Repository
public interface OrderIndexMapper extends tk.mybatis.mapper.common.Mapper<OrderIndex> {
}
