package io.bhex.ex.otc.mappper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.Date;
import java.util.List;
import java.util.Map;

import io.bhex.ex.otc.entity.OtcPaymentTerm;
import tk.mybatis.mapper.common.Mapper;

/**
 * 付款方式mapper
 *
 * @author lizhen
 * @date 2018-09-11
 */
@org.apache.ibatis.annotations.Mapper
public interface OtcPaymentTermMapper extends Mapper<OtcPaymentTerm> {

    @Insert("insert into tb_otc_payment_term (account_id, real_name, payment_type, bank_code, bank_name, branch_name, "
            + "account_no, qrcode, pay_message, first_name, last_name, second_last_name, clabe, debit_card_number, mobile, business_name, concept,"
            + "status, visible, create_date, update_date) values (#{accountId}, #{realName}, "
            + "#{paymentType}, #{bankCode}, #{bankName}, #{branchName}, #{accountNo}, #{qrcode}, "
            + "#{payMessage}, #{firstName}, #{lastName}, #{secondLastName}, #{clabe}, #{debitCardNumber}, #{mobile}, #{businessName}, #{concept},"
            + "#{status}, #{visible}, #{createDate}, #{updateDate})")
    int insertPaymentTerm(OtcPaymentTerm paymentTerm);

    //@Insert("insert into tb_otc_payment_term (account_id, real_name, payment_type, bank_code, bank_name, branch_name, "
    //    + "account_no, qrcode, status, visible, create_date, update_date) values (#{accountId}, #{realName}, "
    //    + "#{paymentType}, #{bankCode}, #{bankName}, #{branchName}, #{accountNo}, #{qrcode}, #{status}, #{visible}, "
    //    + "#{createDate}, #{updateDate}) on duplicate key update real_name = #{realName}, bank_code = #{bankCode}, "
    //    + "bank_name = #{bankName}, branch_name = #{branchName}, account_no = #{accountNo}, qrcode = #{qrcode}, "
    //    + "update_date = #{updateDate}")
    //int insertPaymentTerm(OtcPaymentTerm paymentTerm);

    @Update("update tb_otc_payment_term set visible = #{visible}, update_date = #{updateDate} where "
            + "account_id = #{accountId} and payment_type = #{paymentType}")
    int updatePaymentTermVisible(@Param("accountId") Long accountId,
                                 @Param("paymentType") Integer paymentType,
                                 @Param("visible") Integer visible,
                                 @Param("updateDate") Date updateDate);

    @Update("update tb_otc_payment_term set real_name = #{realName}, bank_name = #{bankName}, branch_name = "
            + "#{branchName}, account_no = #{accountNo}, qrcode = #{qrcode} , pay_message = #{payMessage}, "
            + " first_name = #{firstName}, last_name = #{lastName}, second_last_name = #{secondLastName}, "
            + " clabe = #{clabe}, debit_card_number = #{debitCardNumber}, mobile = #{mobile},"
            + " business_name = #{businessName}, concept = #{concept}, create_date = #{updateDate}, update_date = #{updateDate} "
            + "where  account_id = #{accountId} and payment_type = #{paymentType}")
    int updatePaymentTerm(OtcPaymentTerm paymentTerm);

    @Update("update tb_otc_payment_term set real_name = #{realName}, bank_name = #{bankName}, branch_name = "
            + "#{branchName}, account_no = #{accountNo}, qrcode = #{qrcode} , pay_message = #{payMessage}, "
            + " first_name = #{firstName}, last_name = #{lastName}, second_last_name = #{secondLastName}, "
            + " clabe = #{clabe}, debit_card_number = #{debitCardNumber}, mobile = #{mobile},"
            + " business_name = #{businessName}, concept = #{concept}, create_date = #{updateDate}, update_date = #{updateDate} "
            + "where account_id = #{accountId} and payment_type = #{paymentType} and id = #{id}")
    int updatePaymentTermById(OtcPaymentTerm paymentTerm);

    @Select("<script>select account_id, group_concat(payment_type separator ',') type from tb_otc_payment_term "
            + "where account_id in "
            + "<foreach collection='accountIds' item='item' index='idx' open='(' separator=',' close=')'>#{item}</foreach>"
            + " and visible = 0 and status = 1 group by account_id</script>")
    List<Map<String, Object>> getUserPaymentType(@Param("accountIds") List<Long> accountIds);

    @Select("select count(1) from tb_otc_payment_term where account_id = #{accountId} and status = 1")
    int selectCountAvailablePay(@Param("accountId") Long accountId);

    @Select("select count(1) from tb_otc_payment_term where account_id = #{accountId} and status = 1 and visible = #{visible}")
    int selectCountVisiblePay(@Param("accountId") Long accountId, @Param("visible") Integer visible);

    @Select("select * from tb_otc_payment_term where account_id = #{accountId} and status = 1 and visible = #{visible}")
    List<OtcPaymentTerm> selectCountVisiblePayList(@Param("accountId") Long accountId, @Param("visible") Integer visible);

    @Update("update tb_otc_payment_term set visible = #{visible} where id=#{id} and account_id=#{accountId}")
    int switchPaymentVisible(@Param("id") Long id, @Param("accountId") Long accountId, @Param("visible") Integer visible);

    @Select("select id from tb_otc_payment_term where account_id = #{accountId} and status = 1" +
            " and visible = 0 and payment_type = #{paymentType} limit 1")
    Long selectPaymentTermByPaymentType(@Param("accountId") Long accountId, @Param("paymentType") Integer paymentType);

    @Select("select * from tb_otc_payment_term where account_id = #{accountId} and payment_type = #{paymentType}")
    List<OtcPaymentTerm> selectPaymentTermInfoByPaymentType(@Param("accountId") Long accountId, @Param("paymentType") Integer paymentType);

    @Select("select id,real_name,payment_type,status,visible,account_id from tb_otc_payment_term where payment_type in(0,1,2) and status = 1 and visible = 0 order by id desc limit #{offset},#{size}")
    List<OtcPaymentTerm> queryOtcPaymentTermList(@Param("offset") int offset, @Param("size") int size);

    @Select("select id,real_name,payment_type,status,visible,account_id from tb_otc_payment_term where payment_type in(0,1,2) and status = 1 and visible = 0 and account_id = #{accountId}")
    List<OtcPaymentTerm> queryOtcPaymentTermListByAccountId(@Param("accountId") Long accountId);

    @Update("update tb_otc_payment_term set visible = 1 where id in (${ids})")
    int batchUpdatePaymentVisible(@Param("ids") String ids);
}