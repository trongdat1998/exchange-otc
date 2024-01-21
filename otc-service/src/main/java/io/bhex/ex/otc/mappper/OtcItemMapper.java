package io.bhex.ex.otc.mappper;

import io.bhex.ex.otc.entity.OtcItem;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import tk.mybatis.mapper.common.Mapper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 商品/广告mapper
 *
 * @author lizhen
 * @date 2018-09-13
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcItemMapper extends Mapper<OtcItem> {

    @Update("update tb_otc_item set status = #{status}, update_date = #{updateDate} where id = #{id} "
            + "and status = #{oldStatus}")
    int updateOtcItemStatus(@Param("id") Long id,
                            @Param("status") Integer status,
                            @Param("oldStatus") Integer oldStatus,
                            @Param("updateDate") Date updateDate);

    @Select("select * from tb_otc_item where id = #{id} for update")
    OtcItem selectOtcItemForUpdate(@Param("id") Long id);

    @Select("select id from tb_otc_item where org_id = #{orgId} and client_item_id = #{clientItemId} limit 1")
    Long selectOtcItemIdByClient(@Param("orgId") Long orgId, @Param("clientItemId") Long clientItemId);

    @Select("select count(*) from tb_otc_item where account_id = #{accountId} and token_id = #{tokenId} "
            + "and currency_id = #{currencyId} and side = #{side} and status in (1, 10)")
    int selectOtcItemUnFinishCount(@Param("accountId") Long accountId,
                                   @Param("tokenId") String tokenId,
                                   @Param("currencyId") String currencyId,
                                   @Param("side") Integer side);

    @Select("select count(id) item_count, token_id from tb_otc_item where status <> 0 and status <> 1 and "
            + "create_date between #{beginTime} and #{endTime} group by token_id")
    List<Map<String, Object>> selectOtcItemStatistic(@Param("beginTime") Date beginTime,
                                                     @Param("endTime") Date endTime);

    @Update("update tb_otc_item set fee = fee + #{fee}, update_date = #{updateDate} where id = #{id}")
    int updateOtcItemFee(@Param("id") Long id,
                         @Param("fee") BigDecimal fee,
                         @Param("updateDate") Date updateDate);
}