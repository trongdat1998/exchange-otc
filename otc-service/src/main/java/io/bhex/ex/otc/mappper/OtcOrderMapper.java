package io.bhex.ex.otc.mappper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import io.bhex.ex.otc.entity.OtcOrder;
import tk.mybatis.mapper.common.Mapper;

/**
 * 订单mapper
 *
 * @author lizhen
 * @date 2018-09-14
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcOrderMapper extends Mapper<OtcOrder> {

    @Update("update tb_otc_order set status = #{status}, update_date = #{updateDate} where id = #{id} "
            + "and status = #{oldStatus}")
    int updateOtcOrderStatus(@Param("id") Long id,
                             @Param("status") Integer status,
                             @Param("oldStatus") Integer oldStatus,
                             @Param("updateDate") Date updateDate);


    @Update("update tb_otc_order set status = #{status}, update_date = #{updateDate}, maker_fee = #{makerFee}, taker_fee = #{takerFee} where id = #{id} "
            + "and status = #{oldStatus}")
    int updateOtcOrderStatusAndFee(@Param("id") Long id,
                                   @Param("status") Integer status,
                                   @Param("oldStatus") Integer oldStatus,
                                   @Param("updateDate") Date updateDate,
                                   @Param("makerFee") BigDecimal makerFee,
                                   @Param("takerFee") BigDecimal takerFee);

    @Update("update tb_otc_order set status = #{status}, appeal_type = #{appealType}, "
            + "appeal_account_id = #{appealAccountId}, appeal_content = #{appealContent}, "
            + "update_date = #{updateDate} where id = #{id} and status = #{oldStatus}")
    int appealOrder(@Param("id") Long id,
                    @Param("status") Integer status,
                    @Param("appealType") Integer appealType,
                    @Param("appealAccountId") Long appealAccountId,
                    @Param("appealContent") String appealContent,
                    @Param("oldStatus") Integer oldStatus,
                    @Param("updateDate") Date updateDate);

    @Select("select id from tb_otc_order where org_id = #{orgId} and client_order_id = #{clientOrderId} limit 1")
    Long selectOtcOrderIdByClient(@Param("orgId") Long orgId, @Param("clientOrderId") Long clientOrderId);

    @Select("select * from tb_otc_order where status = 50 and freed in (1,3) and side = 0 limit #{offset},#{size}")
    List<OtcOrder> selectOtcOrderForFreed(@Param("offset") int offset, @Param("size") int size);

    @Update("update tb_otc_order set freed = 2 where id = #{orderId}")
    int updateOtcOrderFreed(@Param("orderId") Long orderId);

    @Select("select id,payment_type,account_id,target_account_id from tb_otc_order where payment_type is not null")
    List<OtcOrder> selectOrderListByPaymentNotNull();

    @Select("select * from tb_otc_order where status=10 and create_date<#{endTime} order by create_date desc limit #{offset},#{size}")
    List<OtcOrder> listUnpayTimeoutOrder(@Param("endTime") Date endTime, @Param("offset") int offset, @Param("size") int size);

    @Select("select * from tb_otc_order where status=20 and transfer_date<=#{endTime} order by create_date desc limit #{offset},#{size}")
    List<OtcOrder> listAppealTimeoutOrder(@Param("endTime") Date endTime, @Param("offset") int offset, @Param("size") int size);

    @Select("select * from tb_otc_order where (account_id =#{accountId} or target_account_id=#{accountId}) and status in (40,50) order by create_date desc limit #{offset},#{size}")
    List<OtcOrder> listClosedOrder(@Param("accountId") long accountId, @Param("offset") int offset, @Param("size") int size);

    @Select("select * from tb_otc_order where (account_id =#{accountId} or target_account_id=#{accountId}) and status in (40,50) and side = #{side} order by create_date desc limit #{offset},#{size}")
    List<OtcOrder> listClosedOrderBySide(@Param("accountId") long accountId, @Param("side") Integer side, @Param("offset") int offset, @Param("size") int size);

    @Deprecated
    @SelectProvider(type = OrderSqlProvider.class, method = "listClosedOrderV2")
    List<OtcOrder> listClosedOrderV2(@Param("takerAccountId") Long takerAccountId, @Param("makerAccountId") Long makerAccountId, @Param("offset") int offset, @Param("size") int size);

    @Select("select target_account_id from tb_otc_order where status>=10 and create_date between #{start} and #{end} group by target_account_id")
    List<Long> groupMerchantTargetAccountId(@Param("start") Date dateStart, @Param("end") Date dateEnd);

    @Select("select account_id from tb_otc_order where status>=10 and create_date between #{start} and #{end} group by account_id")
    List<Long> groupMerchantAccountId(@Param("start") Date dateStart, @Param("end") Date dateEnd);

    @Select("select count(1) from tb_otc_order where (target_account_id=#{targetAccountId} or account_id=#{targetAccountId}) and status=50 " +
            "and create_date between #{start} and #{end} ")
    Long countFinishOrderNumber(@Param("targetAccountId") Long targetAccountId, @Param("start") Date start, @Param("end") Date end);

    @Select("select count(1) from tb_otc_order where target_account_id=#{targetAccountId} and status >=10 " +
            "and create_date between #{start} and #{end} ")
    Long countTotalOrderNumber(@Param("targetAccountId") Long targetAccountId, @Param("start") Date start, @Param("end") Date end);

    @Select("select sum(a.num) from (" +
            " select count(1) num  from tb_otc_order where target_account_id=#{targetAccountId} and side=1 and status >=10 and create_date between #{start} and #{end} " +
            " union all " +
            " select count(1) num  from tb_otc_order where target_account_id=#{targetAccountId} and side=0 and status >=10 and status<> 40 and create_date between #{start} and #{end}" +
            " union all " +
            " select count(1) num  from tb_otc_order where account_id=#{targetAccountId} and side=1 and status >=10 and status<> 40 and create_date between #{start} and #{end}" +
            " union all " +
            " select count(1) num  from tb_otc_order where account_id=#{targetAccountId} and side=0 and status >=10 and create_date between #{start} and #{end} " +
            ") as a" +
            ""
    )
    Long countTotalOrderNumberV2(@Param("targetAccountId") Long targetAccountId, @Param("start") Date start, @Param("end") Date end);


    @Select("select sum(a.num) from (" +
            " select count(1) num  from tb_otc_order where target_account_id=#{targetAccountId} and side=1 and status >30 and create_date between #{start} and #{end} " +
            " union all " +
            " select count(1) num  from tb_otc_order where target_account_id=#{targetAccountId} and side=0 and status > 40 and create_date between #{start} and #{end}" +
            " union all " +
            " select count(1) num  from tb_otc_order where account_id=#{targetAccountId} and side=1 and status > 40 and create_date between #{start} and #{end}" +
            " union all " +
            " select count(1) num  from tb_otc_order where account_id=#{targetAccountId} and side=0 and status >30 and create_date between #{start} and #{end} " +
            ") as a" +
            ""
    )
    Long countTotalOrderNumberV3(@Param("targetAccountId") Long targetAccountId, @Param("start") Date start, @Param("end") Date end);

    @Select({"<script>"
            , "SELECT * "
            , "FROM tb_otc_order "
            , "WHERE org_id=#{orgId} "
            , "<if test=\"tokenId != null and tokenId != ''\">AND token_id = #{tokenId}</if> "
            , "<if test=\"status != null and status != ''\">AND status = #{status}</if> "
            , "<if test=\"side != null and side != ''\">AND side = #{side}</if> "
            , "<if test=\"accountId != null and accountId != ''\">AND account_id = #{accountId}</if> "
            , "<if test=\"startTime != null\">AND create_date &gt;= #{startTime}</if> "
            , "<if test=\"endTime != null\">AND create_date &lt;= #{endTime}</if> "
            , "<if test=\"fromId != null and fromId &gt; 0\">AND id &lt; #{fromId}</if> "
            , "<if test=\"lastId != null and lastId &gt; 0\">AND id &gt; #{lastId}</if> "
            , "ORDER BY id DESC limit #{limit} "
            , "</script>"})
    List<OtcOrder> queryOrderById(@Param("orgId") Long orgId,
                                  @Param("tokenId") String tokenId,
                                  @Param("status") Integer status,
                                  @Param("side") Integer side,
                                  @Param("accountId") Long accountId,
                                  @Param("startTime") Timestamp startTime,
                                  @Param("endTime") Timestamp endTime,
                                  @Param("fromId") Long fromId,
                                  @Param("lastId") Long lastId,
                                  @Param("limit") Integer limit);

    @Select("select count(1) from tb_otc_order where org_id = #{orgId} and account_id = #{accountId} and side = #{side} and status in(1,10,20,30)")
    int selectUnderwayCountOtcOrderByUserId(@Param("orgId") Long orgId, @Param("accountId") Long accountId, @Param("side") Integer side);

    @Select("select id from tb_otc_order where target_account_id = #{accountId} and status =#{status} order by create_date desc limit 1")
    Long getLastOrderIdByStatus(@Param("accountId") long accountId, @Param("status") int status);

    @Select("select account_id from tb_otc_order where side=0 and status=50 and create_date between #{start} and #{end} group by account_id")
    List<Long> listAccountIdsFromBuyOrderIn24Hours(@Param("start") Date dateStart, @Param("end") Date dateEnd);

    @Select("select sum(a.num) from (" +
            " select count(1) num  from tb_otc_order where account_id=#{accountId} and status in (10,20,30) " +
            " union all " +
            " select count(1) num  from tb_otc_order where target_account_id=#{accountId} and status in (10,20,30) " +
            ") as a"
    )
    int countPendingOrder(@Param("accountId") long accountId);

    @Select("select ifnull(sum(quantity),0) from tb_otc_order where org_id =#{orgId} and token_id = #{tokenId} and side = 0 and depth_share = 1 and status = 50")
    BigDecimal sumShardBuyOrderQuantity(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);

    @Select("select ifnull(sum(quantity),0) from tb_otc_order where org_id =#{orgId} and token_id = #{tokenId} and side = 1 and depth_share = 1 and status in(1,10,20,40,50)")
    BigDecimal sumShardSellOrderQuantity(@Param("orgId") Long orgId, @Param("tokenId") String tokenId);

    @Select("select ifnull(sum(quantity),0) from tb_otc_order where account_id = #{accountId} and status = 50 and side = 0 and token_id=#{tokenId} and create_date between #{start} and #{end}")
    BigDecimal sumQuantityFromBuyOrderInDayByAccountId(@Param("accountId") Long accountId, @Param("tokenId") String tokenId, @Param("start") Date dateStart, @Param("end") Date dateEnd);
}