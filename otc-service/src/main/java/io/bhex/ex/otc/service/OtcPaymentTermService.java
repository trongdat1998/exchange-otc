package io.bhex.ex.otc.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import io.bhex.ex.otc.OTCPaymentInfo;
import io.bhex.ex.otc.OTCPaymentTypeEnum;
import io.bhex.ex.otc.QueryOtcPaymentTermListResponse;
import io.bhex.ex.otc.entity.OtcBrokerPaymentConfig;
import io.bhex.ex.otc.entity.OtcOrderPayInfo;
import io.bhex.ex.otc.entity.OtcPaymentItems;
import io.bhex.ex.otc.entity.OtcPaymentTerm;
import io.bhex.ex.otc.entity.OtcPaymentTermHistory;
import io.bhex.ex.otc.enums.PaymentType;
import io.bhex.ex.otc.exception.BusinessException;
import io.bhex.ex.otc.exception.LeastOnePaymentException;
import io.bhex.ex.otc.exception.NonPersonPaymentException;
import io.bhex.ex.otc.exception.PaymentTermNotFindException;
import io.bhex.ex.otc.exception.ThreePaymentException;
import io.bhex.ex.otc.exception.UpdatedVersionException;
import io.bhex.ex.otc.exception.WeChatLimitException;
import io.bhex.ex.otc.mappper.OtcBrokerPaymentConfigMapper;
import io.bhex.ex.otc.mappper.OtcOrderPayInfoMapper;
import io.bhex.ex.otc.mappper.OtcPaymentItemsMapper;
import io.bhex.ex.otc.mappper.OtcPaymentTermHistoryMapper;
import io.bhex.ex.otc.mappper.OtcPaymentTermMapper;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * 付款方式service
 *
 * @author lizhen
 * @date 2018-09-13
 */
@Slf4j
@Service
public class OtcPaymentTermService {

    @Autowired
    private OtcPaymentTermMapper otcPaymentTermMapper;

    @Autowired
    private OtcPaymentTermHistoryMapper otcPaymentTermHistoryMapper;

    @Resource
    private OtcOrderPayInfoMapper otcOrderPayInfoMapper;

    @Autowired
    private OtcBrokerPaymentConfigMapper otcBrokerPaymentConfigMapper;

    @Autowired
    private OtcPaymentItemsMapper otcPaymentItemsMapper;

    private static final Set<Integer> CHECK_TYPE_LIST = new HashSet<>(Arrays.asList(0, 1, 2));

    public void addPaymentTerm(OtcPaymentTerm otcPaymentTerm) {
        if (otcPaymentTerm.getPaymentType().equals(2)) {
            throw new WeChatLimitException("WeChat limit: accountId=" + otcPaymentTerm.getAccountId());
        }
        otcPaymentTermMapper.insertPaymentTerm(otcPaymentTerm);
    }

    public List<OtcPaymentTerm> getOtcPaymentTerm(long accountId) {
        OtcPaymentTerm example = OtcPaymentTerm.builder()
                .accountId(accountId)
                .status(1)
                .build();
        return otcPaymentTermMapper.select(example);
    }

    public List<OtcPaymentTerm> getVisibleOtcPaymentTerm(long accountId) {
        OtcPaymentTerm example = OtcPaymentTerm.builder()
                .accountId(accountId)
                .status(1)
                .visible(0)
                .build();
        return otcPaymentTermMapper.select(example);
    }

    /**
     * 获取broker支持的用户可见支付方式
     */
    public List<OtcPaymentTerm> getVisibleOtcPaymentTerm(long accountId, long orgId) {

        List<Integer> brokerPaymentConfigList = getBrokerPaymentConfigList(orgId);
        Example example = new Example(OtcPaymentTerm.class);
        example.createCriteria().andEqualTo("accountId", accountId)
                .andEqualTo("status", 1)
                .andEqualTo("visible", 0)
                .andIn("paymentType", brokerPaymentConfigList);

        return otcPaymentTermMapper.selectByExample(example);
    }

    public OtcPaymentTerm getOtcPaymentTerm(long accountId, int paymentType) {
        OtcPaymentTerm example = OtcPaymentTerm.builder()
                .accountId(accountId)
                .paymentType(paymentType)
                .status(1)
                .build();
        return otcPaymentTermMapper.selectOne(example);
    }

    public int configOtcPaymentTerm(long accountId, int paymentType, int visible) {
        return otcPaymentTermMapper.updatePaymentTermVisible(accountId, paymentType, visible, new Date());
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int updateOtcPaymentTerm(OtcPaymentTerm otcPaymentTerm) {
        OtcPaymentTerm history;
        if (otcPaymentTerm.getId() != null && otcPaymentTerm.getId() > 0L) {
            history = otcPaymentTermMapper.selectByPrimaryKey(otcPaymentTerm.getId());
        } else {
            List<OtcPaymentTerm> otcPaymentTerms = otcPaymentTermMapper
                    .selectPaymentTermInfoByPaymentType(otcPaymentTerm.getAccountId(), otcPaymentTerm.getPaymentType());
            if (otcPaymentTerms != null && otcPaymentTerms.size() > 1) {
                throw new UpdatedVersionException("Please update APP version");
            }
            history = otcPaymentTerms.get(0);
        }

        if (history == null) {
            throw new BusinessException("payment term not exist: accountId=" + otcPaymentTerm.getAccountId() +
                    ", paymentType=" + otcPaymentTerm.getPaymentType());
        }
        //支付方式写到历史记录表
        insertOtcPaymentTermHistory(history);

        if (otcPaymentTerm.getPaymentType() == PaymentType.BANK.getType()) {
            if (StringUtils.isNotBlank(otcPaymentTerm.getRealName())) {
                history.setRealName(otcPaymentTerm.getRealName());
            }

            if (StringUtils.isNotBlank(otcPaymentTerm.getBankName())) {
                history.setBankName(otcPaymentTerm.getBankName());
            }
            history.setBranchName(otcPaymentTerm.getBranchName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getAccountNo())) {
                history.setAccountNo(otcPaymentTerm.getAccountNo());
            }
            history.setQrcode("");
        } else {
            if (StringUtils.isNotBlank(otcPaymentTerm.getRealName())) {
                history.setRealName(otcPaymentTerm.getRealName());
            }
            history.setBankName("");
            history.setBranchName("");
            if (StringUtils.isNotBlank(otcPaymentTerm.getAccountNo())) {
                history.setAccountNo(otcPaymentTerm.getAccountNo());
            }
            if (StringUtils.isNotBlank(otcPaymentTerm.getQrcode())) {
                history.setQrcode(otcPaymentTerm.getQrcode());
            }
        }
        history.setUpdateDate(new Date());
        if (history.getId() != null && history.getId() > 0) {
            return otcPaymentTermMapper.updatePaymentTermById(history);
        } else {
            return otcPaymentTermMapper.updatePaymentTerm(history);
        }
    }

    public Map<Long, List<Integer>> getUserPaymentTerm(List<Long> accountIdList) {
        List<Map<String, Object>> list = otcPaymentTermMapper.getUserPaymentType(accountIdList);
        Map<Long, List<Integer>> result = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(
                    obj -> {
                        String[] types = obj.get("type").toString().split(",");
                        List<Integer> typeList = Lists.newArrayList();
                        for (String type : types) {
                            typeList.add(Integer.parseInt(type));
                        }
                        result.put(Long.parseLong(obj.get("account_id").toString()), typeList);
                    }
            );
        }
        return result;
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class, timeout = 10)
    public Boolean deleteUserPaymentTerm(Long id, Long accountId) {
        OtcPaymentTerm paymentTerm = otcPaymentTermMapper.selectByPrimaryKey(id);
        if (paymentTerm == null) {
            throw new PaymentTermNotFindException("payment term not exist: accountId=" + accountId +
                    ", id=" + id);
        }

        List<OtcPaymentTerm> otcPaymentTerms
                = otcPaymentTermMapper.selectCountVisiblePayList(accountId, 0);

        if (CollectionUtils.isEmpty(otcPaymentTerms)) {
            throw new LeastOnePaymentException("At least one mode of payment");
        }

        if (otcPaymentTerms.size() == 1) {
            if (paymentTerm.getId().equals(otcPaymentTerms.get(0).getId())) {
                throw new LeastOnePaymentException("At least one mode of payment");
            }
        }

        //支付方式写到历史表
        insertOtcPaymentTermHistory(paymentTerm);

        if (otcPaymentTermMapper.deleteByPrimaryKey(id) == 1) {
            return Boolean.TRUE;
        } else {
            throw new BusinessException("delete payment term error : accountId=" + accountId +
                    ", id=" + id);
        }
    }

    public void bindingPaymentToOrderPayInfo(Long orderId, Long paymentId) {
        OtcPaymentTerm paymentTerm = otcPaymentTermMapper.selectByPrimaryKey(paymentId);
        if (paymentTerm == null) {
            throw new BusinessException("payment term not exist: orderId=" + orderId +
                    ", id=" + paymentId);
        }

        OtcOrderPayInfo otcOrderPayInfo = OtcOrderPayInfo.builder()
                .orderId(orderId)
                .realName(paymentTerm.getRealName())
                .paymentType(paymentTerm.getPaymentType())
                .accountNo(paymentTerm.getAccountNo())
                .bankCode(paymentTerm.getBankCode())
                .bankName(paymentTerm.getBankName())
                .branchName(paymentTerm.getBranchName() != null ? paymentTerm.getBranchName() : "")
                .qrcode(paymentTerm.getQrcode())
                .payMessage(paymentTerm.getPayMessage())
                .firstName(paymentTerm.getFirstName())
                .lastName(paymentTerm.getLastName())
                .secondLastName(paymentTerm.getSecondLastName())
                .clabe(paymentTerm.getClabe())
                .debitCardNumber(paymentTerm.getDebitCardNumber())
                .mobile(paymentTerm.getMobile())
                .businessName(paymentTerm.getBusinessName())
                .concept(paymentTerm.getConcept())
                .created(new Date())
                .build();
        otcOrderPayInfoMapper.insert(otcOrderPayInfo);
    }

    private void insertOtcPaymentTermHistory(OtcPaymentTerm paymentTerm) {
        OtcPaymentTermHistory otcPaymentTermHistory = OtcPaymentTermHistory.builder()
                .accountId(paymentTerm.getAccountId())
                .realName(paymentTerm.getRealName())
                .paymentType(paymentTerm.getPaymentType())
                .accountNo(paymentTerm.getAccountNo())
                .bankCode(paymentTerm.getBankCode())
                .bankName(paymentTerm.getBankName())
                .qrcode(paymentTerm.getQrcode())
                .payMessage(paymentTerm.getPayMessage())
                .firstName(paymentTerm.getFirstName())
                .lastName(paymentTerm.getLastName())
                .secondLastName(paymentTerm.getSecondLastName())
                .clabe(paymentTerm.getClabe())
                .debitCardNumber(paymentTerm.getDebitCardNumber())
                .mobile(paymentTerm.getMobile())
                .businessName(paymentTerm.getBusinessName())
                .concept(paymentTerm.getConcept())
                .createDate(paymentTerm.getCreateDate())
                .build();
        otcPaymentTermHistoryMapper.insert(otcPaymentTermHistory);
    }

    public OtcOrderPayInfo getOrderPayInfoByOrderId(Long orderId) {
        return otcOrderPayInfoMapper.queryOtcOrderPayInfoByOrderId(orderId);
    }


    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Throwable.class, timeout = 10)
    public Boolean switchPaymentVisible(Long id, Long accountId, Integer onOff, String realName, Integer isBusiness) {
        OtcPaymentTerm paymentTerm = otcPaymentTermMapper.selectByPrimaryKey(id);
        if (paymentTerm == null) {
            throw new PaymentTermNotFindException("payment term not exist: accountId=" + accountId +
                    ", id=" + id);
        }

        //打开
        if (onOff == 0) {
            if (otcPaymentTermMapper.selectCountVisiblePay(accountId, 0) >= 3) {
                throw new ThreePaymentException("Payment method can only open three: accountId=" + accountId +
                        ", id=" + id);
            }
            //如果不等于本人的卡 不允许打开操作
            if (CHECK_TYPE_LIST.contains(paymentTerm.getPaymentType())) {
                if (paymentTerm.getPaymentType().equals(2)) {
                    throw new WeChatLimitException("WeChat limit: accountId=" + accountId +
                            ", id=" + id);
                }
                //普通用户
                if (isBusiness.equals(0)) {
                    if (StringUtils.isNoneBlank(realName)) {
                        if (!realName.trim().equalsIgnoreCase(paymentTerm.getRealName().trim())) {
                            log.info("switchPaymentVisible fail accountId {} realName {} payRealName {}", accountId, realName.trim(), paymentTerm.getRealName().trim());
                            throw new NonPersonPaymentException("Non person payment limit: accountId=" + accountId +
                                    ", id=" + id);
                        }
                    }
                }
            }
            //关闭
        } else if (onOff == 1) {
            if (otcPaymentTermMapper.selectCountVisiblePay(accountId, 0) <= 1) {
                throw new LeastOnePaymentException("Keep at least one support method: accountId=" + accountId +
                        ", id=" + id);
            }
        } else {
            throw new BusinessException("Unknown type : onOff=" + onOff +
                    ", accountId=" + accountId);
        }

        if (otcPaymentTermMapper.switchPaymentVisible(id, accountId, onOff) == 1) {
            return Boolean.TRUE;
        } else {
            throw new BusinessException("switch payment visible error : accountId=" + accountId +
                    ", id=" + id);
        }
    }

    /**
     * 获取两家券商支持的支付方式的交集，目前用于获取共享otc广告时对支付方式的过滤控制
     */
    public Set<Integer> getBrokerPaymentConfigIntersectionList(long orgId, long targetOrgId) {
        OtcBrokerPaymentConfig otcBrokerPaymentConfig = otcBrokerPaymentConfigMapper.selectByPrimaryKey(orgId);
        Set<Integer> respList = new HashSet<>();
        if (otcBrokerPaymentConfig == null) {
            //没有配置则默认取 支付宝、微信、银行卡
            respList.add(OTCPaymentTypeEnum.OTC_PAYMENT_BANK_VALUE);
            respList.add(OTCPaymentTypeEnum.OTC_PAYMENT_WECHAT_VALUE);
            respList.add(OTCPaymentTypeEnum.OTC_PAYMENT_ALIPAY_VALUE);
        } else {
            if (!StringUtils.isBlank(otcBrokerPaymentConfig.getPaymentConfig())) {
                for (String value : otcBrokerPaymentConfig.getPaymentConfig().split(",")) {
                    respList.add(Integer.valueOf(value));
                }
            }
        }

        if (orgId != targetOrgId) {
            OtcBrokerPaymentConfig targetOtcBrokerPaymentConfig = otcBrokerPaymentConfigMapper.selectByPrimaryKey(targetOrgId);
            List<Integer> list = new ArrayList<>();
            if (targetOtcBrokerPaymentConfig == null) {
                //没有配置则默认取 支付宝、微信、银行卡
                list.add(OTCPaymentTypeEnum.OTC_PAYMENT_BANK_VALUE);
                list.add(OTCPaymentTypeEnum.OTC_PAYMENT_WECHAT_VALUE);
                list.add(OTCPaymentTypeEnum.OTC_PAYMENT_ALIPAY_VALUE);
            } else {
                if (!StringUtils.isBlank(targetOtcBrokerPaymentConfig.getPaymentConfig())) {
                    for (String value : targetOtcBrokerPaymentConfig.getPaymentConfig().split(",")) {
                        list.add(Integer.valueOf(value));
                    }
                }
            }
            respList.retainAll(list);
        }
        return respList;
    }

    /**
     * 获取券商支持的支付方式List<Integer>
     */
    public List<Integer> getBrokerPaymentConfigList(long orgId) {

        OtcBrokerPaymentConfig otcBrokerPaymentConfig = otcBrokerPaymentConfigMapper.selectByPrimaryKey(orgId);

        List<Integer> respList = new ArrayList<>();

        if (otcBrokerPaymentConfig == null) {
            //没有配置则默认取 支付宝、微信、银行卡
            respList.add(OTCPaymentTypeEnum.OTC_PAYMENT_BANK_VALUE);
            respList.add(OTCPaymentTypeEnum.OTC_PAYMENT_WECHAT_VALUE);
            respList.add(OTCPaymentTypeEnum.OTC_PAYMENT_ALIPAY_VALUE);

        } else {

            if (StringUtils.isBlank(otcBrokerPaymentConfig.getPaymentConfig()))
                return new ArrayList<>();

            for (String value : otcBrokerPaymentConfig.getPaymentConfig().split(",")) {
                respList.add(Integer.valueOf(value));
            }
        }

        return respList;
    }

    public List<OtcPaymentItems> getBrokerPaymentConfig(long orgId, String language) {

        OtcBrokerPaymentConfig otcBrokerPaymentConfig = otcBrokerPaymentConfigMapper.selectByPrimaryKey(orgId);
        Example otcPaymentItemsExample = new Example(OtcPaymentItems.class);
        Example.Criteria criteria = otcPaymentItemsExample.createCriteria();
//        if ("en-us".equals(language))

        criteria.andEqualTo("language", language);

        List<OtcPaymentItems> otcPaymentItems = null;

        List<Object> paymentTypeList = null;

        if (otcBrokerPaymentConfig == null) {
            //没有配置则默认取 支付宝、微信、银行卡
            paymentTypeList = Arrays.asList(OTCPaymentTypeEnum.OTC_PAYMENT_BANK_VALUE, OTCPaymentTypeEnum.OTC_PAYMENT_WECHAT_VALUE, OTCPaymentTypeEnum.OTC_PAYMENT_ALIPAY_VALUE);
            criteria.andIn("paymentType", paymentTypeList);
            otcPaymentItems = otcPaymentItemsMapper.selectByExample(otcPaymentItemsExample);

        } else {

            if (StringUtils.isBlank(otcBrokerPaymentConfig.getPaymentConfig()))
                return new ArrayList<>();

            paymentTypeList = Arrays.asList(otcBrokerPaymentConfig.getPaymentConfig().split(","));
            criteria.andIn("paymentType", paymentTypeList);
            otcPaymentItems = otcPaymentItemsMapper.selectByExample(otcPaymentItemsExample);
        }

        if (otcPaymentItems.isEmpty() && !"en_US".equals(language)) {
            otcPaymentItemsExample.clear();
            otcPaymentItemsExample.createCriteria().andEqualTo("language", "en_US").andIn("paymentType", paymentTypeList);
            otcPaymentItems = otcPaymentItemsMapper.selectByExample(otcPaymentItemsExample);
        }

        return otcPaymentItems;

    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int updateNewOtcPaymentTerm(OtcPaymentTerm otcPaymentTerm) {
        OtcPaymentTerm history;
        if (otcPaymentTerm.getId() != null && otcPaymentTerm.getId() > 0L) {
            history = otcPaymentTermMapper.selectByPrimaryKey(otcPaymentTerm.getId());
        } else {
            List<OtcPaymentTerm> otcPaymentTerms = otcPaymentTermMapper
                    .selectPaymentTermInfoByPaymentType(otcPaymentTerm.getAccountId(), otcPaymentTerm.getPaymentType());
            if (otcPaymentTerms != null && otcPaymentTerms.size() > 1) {
                throw new UpdatedVersionException("Please update APP version");
            }
            history = otcPaymentTerms.get(0);
        }

        if (history == null) {
            throw new BusinessException("payment term not exist: accountId=" + otcPaymentTerm.getAccountId() +
                    ", paymentType=" + otcPaymentTerm.getPaymentType());
        }
        //支付方式写到历史记录表
        insertOtcPaymentTermHistory(history);

        if (otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_BANK_VALUE)) {
            if (StringUtils.isNotBlank(otcPaymentTerm.getRealName()))
                history.setRealName(otcPaymentTerm.getRealName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getBankName()))
                history.setBankName(otcPaymentTerm.getBankName());

            history.setBranchName(otcPaymentTerm.getBranchName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getAccountNo()))
                history.setAccountNo(otcPaymentTerm.getAccountNo());

            history.setQrcode("");
        } else if (otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_PAYPAL_VALUE)
                || otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_PAYTM_VALUE)
                || otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_QIWI_VALUE)
                || otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_PAYNOW_VALUE)
                || otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_YANDEX_VALUE)) {

            if (StringUtils.isNotBlank(otcPaymentTerm.getRealName()))
                history.setRealName(otcPaymentTerm.getRealName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getAccountNo()))
                history.setAccountNo(otcPaymentTerm.getAccountNo());

        } else if (otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_SWIFT_VALUE)
                || otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_WESTERN_UNION_VALUE)) {
            //swift和西联汇款 payMessage
            if (StringUtils.isNotBlank(otcPaymentTerm.getRealName()))
                history.setRealName(otcPaymentTerm.getRealName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getPayMessage()))
                history.setPayMessage(otcPaymentTerm.getPayMessage());

        } else if (otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_FINANCIAL_PAYMENT_VALUE)) {
            //Financial Payment 名、姓、第二姓氏、开户行名称、账号
            if (StringUtils.isNotBlank(otcPaymentTerm.getFirstName()))
                history.setFirstName(otcPaymentTerm.getFirstName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getLastName()))
                history.setLastName(otcPaymentTerm.getLastName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getSecondLastName()))
                history.setSecondLastName(otcPaymentTerm.getSecondLastName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getBankName()))
                history.setBankName(otcPaymentTerm.getBankName());

            if (StringUtils.isNotBlank(otcPaymentTerm.getAccountNo()))
                history.setAccountNo(otcPaymentTerm.getAccountNo());

            if (StringUtils.isNotBlank(otcPaymentTerm.getClabe()))
                history.setClabe(otcPaymentTerm.getClabe());

            if (StringUtils.isNotBlank(otcPaymentTerm.getDebitCardNumber()))
                history.setDebitCardNumber(otcPaymentTerm.getDebitCardNumber());

            if (StringUtils.isNotBlank(otcPaymentTerm.getMobile()))
                history.setMobile(otcPaymentTerm.getMobile());

        } else if (otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_MERCADOPAGO_VALUE)) {
            //Mercadopago 二维码、描述
            if (StringUtils.isNotBlank(otcPaymentTerm.getBusinessName())) {
                history.setBusinessName(otcPaymentTerm.getBusinessName());
            }

            if (StringUtils.isNotBlank(otcPaymentTerm.getConcept())) {
                history.setConcept(otcPaymentTerm.getConcept());
            }

            if (StringUtils.isNotBlank(otcPaymentTerm.getQrcode())) {
                history.setQrcode(otcPaymentTerm.getQrcode());
            } else {
                history.setQrcode("");
            }
        } else if (otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_ALIPAY_VALUE)
                || otcPaymentTerm.getPaymentType().equals(OTCPaymentTypeEnum.OTC_PAYMENT_WECHAT_VALUE)) {

            if (StringUtils.isNotBlank(otcPaymentTerm.getRealName()))
                history.setRealName(otcPaymentTerm.getRealName());

            history.setBankName("");
            history.setBranchName("");

            if (StringUtils.isNotBlank(otcPaymentTerm.getAccountNo()))
                history.setAccountNo(otcPaymentTerm.getAccountNo());

            if (StringUtils.isNotBlank(otcPaymentTerm.getQrcode()))
                history.setQrcode(otcPaymentTerm.getQrcode());
        }

        history.setUpdateDate(new Date());
        if (history.getId() != null && history.getId() > 0) {
            return otcPaymentTermMapper.updatePaymentTermById(history);
        } else {
            return otcPaymentTermMapper.updatePaymentTerm(history);
        }
    }

    public QueryOtcPaymentTermListResponse queryOtcPaymentTermList(int page, int size, Long accountId) {
        int currentPage = page < 1 ? 0 : page - 1;
        int offset = currentPage * size;
        List<OtcPaymentTerm> otcPaymentTermList;
        if (accountId != null && accountId > 0) {
            otcPaymentTermList = otcPaymentTermMapper.queryOtcPaymentTermListByAccountId(accountId);
        } else {
            otcPaymentTermList = otcPaymentTermMapper.queryOtcPaymentTermList(offset, size);
        }
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(otcPaymentTermList)) {
            return QueryOtcPaymentTermListResponse.getDefaultInstance();
        }
        List<OTCPaymentInfo> otcPaymentInfoList = new ArrayList<>();
        otcPaymentTermList.forEach(s -> {
            otcPaymentInfoList.add(OTCPaymentInfo
                    .newBuilder()
                    .setId(s.getId())
                    .setRealName(s.getRealName())
                    .setPaymentType(s.getPaymentType())
                    .setStatus(s.getStatus())
                    .setVisible(s.getVisible())
                    .setAccountId(s.getAccountId())
                    .build());
        });

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(otcPaymentInfoList)) {
            return QueryOtcPaymentTermListResponse.getDefaultInstance();
        }
        return QueryOtcPaymentTermListResponse.newBuilder().addAllPaymentInfo(otcPaymentInfoList).build();
    }

    public int batchUpdatePaymentVisible(String ids) {
        return otcPaymentTermMapper.batchUpdatePaymentVisible(ids);
    }
}