/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/29
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.ex.otc.mappper;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import io.bhex.ex.otc.entity.BrokerExt;
import io.bhex.ex.otc.entity.OtcShareLimit;

@Mapper
@Repository
public interface OtcShareLimitMapper extends tk.mybatis.mapper.common.Mapper<OtcShareLimit> {

}
