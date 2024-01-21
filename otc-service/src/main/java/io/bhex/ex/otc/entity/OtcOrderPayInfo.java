package io.bhex.ex.otc.entity;

import lombok.*;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * 付款方式
 *
 * @author yuehao
 * @date 2019-03-29
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table(name = "tb_otc_order_pay_info")
public class OtcOrderPayInfo {
    /**
     * ID
     */
    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    /**
     * 订单id
     */
    private Long orderId;
    /**
     * 姓名
     */
    private String realName;
    /**
     * 银行类别：0-商业银行;1-支付宝;2-微信
     */
    private Integer paymentType;
    /**
     * 银行编码
     */
    private String bankCode;
    /**
     * 银行名称
     */
    private String bankName;
    /**
     * 分行名称
     */
    private String branchName;
    /**
     * 账户号
     */
    private String accountNo;
    /**
     * 二维码
     */
    private String qrcode;

    /**
     * 汇款信息 SWIFT、西联汇款 使用
     */
    private String payMessage;
    /**
     * 名  仅Financial Payment用到
     */
    private String firstName;
    /**
     * 姓  仅Financial Payment用到
     */
    private String lastName;
    /**
     * 第二姓氏  仅Financial Payment用到
     */
    private String secondLastName;
    /**
     * CLABE号  仅Financial Payment用到
     */
    private String clabe;
    /**
     * 借记卡  仅Financial Payment用到
     */
    private String debitCardNumber;
    /**
     * 手机号  仅Financial Payment用到
     */
    private String mobile;

    /**
     * 商户名称  仅Mercadopago用到
     */
    private String businessName;
    /**
     * 描述  仅Mercadopago用到
     */
    private String concept;

    /**
     * 创建日期
     */
    private Date created;

    public String getRealNameSafe() {
        if (StringUtils.isBlank(realName)) {
            return "";
        }

        return RegExUtils.replaceAll(this.realName, " ", "");
    }
}