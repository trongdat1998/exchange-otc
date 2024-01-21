package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcOrderPayInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

/**
 * 订单支付方式mapper
 *
 * @author yuehao
 * @date 2019-03-29
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderPayInfoMapper extends Mapper<OtcOrderPayInfo> {

    @Select("select *from tb_otc_order_pay_info where order_id = #{orderId} limit 1")
    OtcOrderPayInfo queryOtcOrderPayInfoByOrderId(@Param("orderId") Long orderId);
}