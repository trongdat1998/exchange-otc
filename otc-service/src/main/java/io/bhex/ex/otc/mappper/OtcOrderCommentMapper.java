package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcOrderComment;
import org.apache.ibatis.annotations.Insert;
import tk.mybatis.mapper.common.Mapper;

/**
 * 订单评价mapper
 *
 * @author lizhen
 * @date 2018-09-10
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderCommentMapper extends Mapper<OtcOrderComment> {
}