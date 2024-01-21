package io.bhex.ex.otc.util;

import com.google.common.base.Strings;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.bhex.ex.otc.OTCPaymentTerm;
import io.bhex.ex.otc.entity.OtcPaymentTerm;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * proto转换
 *
 * @author lizhen
 * @date 2018-10-10
 */
@Slf4j
public class ConvertUtil {

    public static OTCPaymentTerm convertToOTCPaymentTerm(OtcPaymentTerm paymentTerm) {
        return OTCPaymentTerm.newBuilder()
                .setId(paymentTerm.getId())
                .setAccountId(paymentTerm.getAccountId())
                .setPaymentTypeValue(paymentTerm.getPaymentType())
                .setRealName(paymentTerm.getRealName())
                .setBankName(StringUtils.isNotBlank(paymentTerm.getBankName()) ? paymentTerm.getBankName() : "")
                .setBranchName(StringUtils.isNotBlank(paymentTerm.getBranchName()) ? paymentTerm.getBranchName() : "")
                .setAccountNo(paymentTerm.getAccountNo())
                .setQrcode(StringUtils.isNotBlank(paymentTerm.getQrcode()) ? paymentTerm.getQrcode() : "")
                .setPayMessage(StringUtils.isNotBlank(paymentTerm.getPayMessage()) ? paymentTerm.getPayMessage() : "")
                .setFirstName(StringUtils.isNotBlank(paymentTerm.getFirstName()) ? paymentTerm.getFirstName() : "")
                .setLastName(StringUtils.isNotBlank(paymentTerm.getLastName()) ? paymentTerm.getLastName() : "")
                .setSecondLastName(StringUtils.isNotBlank(paymentTerm.getSecondLastName()) ? paymentTerm.getSecondLastName() : "")
                .setClabe(StringUtils.isNotBlank(paymentTerm.getClabe()) ? paymentTerm.getClabe() : "")
                .setDebitCardNumber(StringUtils.isNotBlank(paymentTerm.getDebitCardNumber()) ? paymentTerm.getDebitCardNumber() : "")
                .setMobile(StringUtils.isNotBlank(paymentTerm.getMobile()) ? paymentTerm.getMobile() : "")
                .setBusinesName(StringUtils.isNotBlank(paymentTerm.getBusinessName()) ? paymentTerm.getBusinessName() : "")
                .setConcept(StringUtils.isNotBlank(paymentTerm.getConcept()) ? paymentTerm.getConcept() : "")
                .setVisibleValue(paymentTerm.getVisible())
                .setBankCode(Strings.nullToEmpty(paymentTerm.getBankCode()))
                .build();
    }

    public static String messageToString(Message message) {
        try {
            return JsonFormat.printer().print(message);
            /*return JsonFormat.printer().print(message)
                    .replace("\\r","")
                    .replace("\\t","")
                    .replace("\\n","")

                    ;*/
/*            if(Objects.isNull(message)){
                return "null";
            }
            return message.toString()
                    .replace("\\r","")
                    .replace("\\t","")
                    .replace("\\n","");*/
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return "";
        }
    }

    public static void main(String[] args) {
        String str = "{\n" +
                "  \"baseRequest\": {\n" +
                "    \"userId\": \"230478863992619008\",\n" +
                "    \"orgId\": \"6002\",\n" +
                "    \"language\": \"zh_CN\"\n" +
                "  },\n" +
                "  \"accountId\": \"230478864304965888\"\n" +
                "}";

        System.out.println();
    }
}