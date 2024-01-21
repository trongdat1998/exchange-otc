package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcOrderDepthShare;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.jdbc.SQL;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;
import java.util.Map;

/**
 * 订单mapper
 *
 * @author lizhen
 * @date 2018-09-14
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderDepthShareMapper extends Mapper<OtcOrderDepthShare> {

    @Select(" select * from (" +
            "                  select order_id,maker_org_id,taker_org_id,create_date " +
            "                  from tb_otc_order_depth_share " +
            "                  where maker_org_id = #{orgId} " +
            "                  union " +
            "                  select order_id,maker_org_id,taker_org_id,create_date " +
            "                  from tb_otc_order_depth_share " +
            "                  where taker_org_id = #{orgId} " +
            "              ) as a order by a.create_date desc limit #{offset},#{size}")
    List<OtcOrderDepthShare> listOrders(@Param("orgId") Long orgId, @Param("offset") int offset, @Param("size") int size);

    @SelectProvider(type = OtcOrderDepthShareSqlProvider.class, method = "listShareOrder")
    List<OtcOrderDepthShare> listOrdersV2(@Param("orgId") Long orgId, @Param("offset") Integer offset,
                                          @Param("size") Integer size, @Param("status") List<Integer> statuses);

    public static class OtcOrderDepthShareSqlProvider {

        public String listShareOrder(Map<String, Object> map) {
            Long orgId = (Long) map.get("orgId");
            Integer offset = (Integer) map.get("offset");
            Integer size = (Integer) map.get("size");
            List<Integer> statuses = (List<Integer>) map.get("status");

            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (Integer status : statuses) {
                sb.append(status + ",");
            }

            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");

            String subSql1 = "select order_id,maker_org_id,taker_org_id,create_date " +
                    "          from tb_otc_order_depth_share " +
                    "          where maker_org_id =" + orgId;

            if (CollectionUtils.isNotEmpty(statuses)) {
                subSql1 += " and status in " + sb.toString();
            }

            String subSql2 = "select order_id,maker_org_id,taker_org_id,create_date " +
                    "          from tb_otc_order_depth_share " +
                    "          where taker_org_id =" + orgId;

            if (CollectionUtils.isNotEmpty(statuses)) {
                subSql2 += " and status in " + sb.toString();
            }

            return "select * from ( " +
                    subSql1 +
                    " union " +
                    subSql2 +
                    " ) as a order by a.create_date desc limit " + offset + "," + size;
        }

    }
}
