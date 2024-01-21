/**********************************
 *@项目名称: broker-parent
 *@文件名称: io.bhex.broker.mapper
 *@Date 2018/6/25
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.ex.otc.mappper;

import org.apache.ibatis.jdbc.SQL;

import java.util.Map;

@Deprecated
public class OrderSqlProvider {

    private static final String TABLE_NAME = "tb_otc_order";

    private static final String COLUMNS =
            "id,exchange_id, org_id, user_id, client_order_id, account_id, target_account_id, token_id" +
                    ", currency_id, payment_type, transfer_date, appeal_account_id, appeal_type, appeal_content" +
                    ", create_date, update_date, maker_fee, taker_fee,depth_share";


    public String listClosedOrderV2(Map<String, ?> param) {
        Long takerAccountId = (Long) param.get("takerAccountId");
        Long makerAccountId = (Long) param.get("makerAccountId");
        Integer offset = (Integer) param.get("offset");
        Integer size = (Integer) param.get("size");

        return new SQL() {
            {
                SELECT(" * ").FROM(TABLE_NAME);
                if (takerAccountId == null) {
                    WHERE("target_account_id=" + makerAccountId, "status in (40,50)");
                } else {
                    WHERE("account_id=" + takerAccountId, "status in (40,50)");
                }
                ORDER_BY("create_date desc limit " + offset + "," + size);
            }
        }.toString();
    }

}
