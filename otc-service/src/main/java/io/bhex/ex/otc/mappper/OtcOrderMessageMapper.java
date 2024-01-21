package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcOrderMessage;
import org.apache.ibatis.annotations.Insert;
import tk.mybatis.mapper.common.Mapper;

/**
 * 订单消息（聊天记录）mapper
 *
 * @author lizhen
 * @date 2018-09-11
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderMessageMapper extends Mapper<OtcOrderMessage> {
}