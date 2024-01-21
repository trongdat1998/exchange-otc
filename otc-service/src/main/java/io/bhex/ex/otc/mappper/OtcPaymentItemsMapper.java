package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcBrokerPaymentConfig;
import io.bhex.ex.otc.entity.OtcPaymentItems;
import tk.mybatis.mapper.common.Mapper;
import tk.mybatis.mapper.entity.Example;

import java.util.List;

@org.apache.ibatis.annotations.Mapper
public interface OtcPaymentItemsMapper extends Mapper<OtcPaymentItems> {
}
