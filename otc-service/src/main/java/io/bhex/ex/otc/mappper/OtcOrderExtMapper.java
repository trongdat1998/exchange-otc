package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.OtcOrderExt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * 订单mapper
 *
 * @author lizhen
 * @date 2018-09-14
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderExtMapper extends Mapper<OtcOrderExt> {

}