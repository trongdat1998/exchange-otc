package io.bhex.ex.otc.service;


import io.bhex.ex.otc.entity.OtcTradeFeeRate;
import io.bhex.ex.otc.mappper.OtcTradeFeeRateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class OtcTradeFeeService {

    @Resource
    private OtcTradeFeeRateMapper otcTradeFeeRateMapper;


    public OtcTradeFeeRate queryOtcTradeFeeByTokenId(Long orgId, String tokenId) {
        return otcTradeFeeRateMapper.queryOtcTradeFeeByTokenId(orgId, tokenId);
    }

    public List<OtcTradeFeeRate> queryOtcTradeFeeByOrgId(Long orgId) {
        return otcTradeFeeRateMapper.queryOtcTradeFeeByOrgId(orgId);
    }

    public List<OtcTradeFeeRate> queryAllOtcTradeFee() {
        return otcTradeFeeRateMapper.queryAllOtcTradeFee();
    }

    public boolean addOtcTradeFee(Long orgId, String tokenId,
                                  BigDecimal makerBuyFeeRate, BigDecimal makerSellFeeRate) {

        Example exp = new Example(OtcTradeFeeRate.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("tokenId", tokenId);

        int count = otcTradeFeeRateMapper.selectCountByExample(exp);
        if (count == 1) {
            return true;
        }

        return otcTradeFeeRateMapper.insert(OtcTradeFeeRate
                .builder()
                .orgId(orgId)
                .tokenId(tokenId)
                .makerBuyFeeRate(makerBuyFeeRate)
                .makerSellFeeRate(makerSellFeeRate)
                .createdAt(new Date())
                .updatedAt(new Date())
                .deleted(0)
                .build()
        ) == 1;
    }

    public boolean updateOtcTradeFee(Integer id, BigDecimal makerBuyFeeRate, BigDecimal makerSellFeeRate) {
        return otcTradeFeeRateMapper.updateByPrimaryKeySelective(
                OtcTradeFeeRate
                        .builder()
                        .id(id)
                        .makerBuyFeeRate(makerBuyFeeRate)
                        .makerSellFeeRate(makerSellFeeRate)
                        .updatedAt(new Date())
                        .build()
        ) == 1;
    }
}
