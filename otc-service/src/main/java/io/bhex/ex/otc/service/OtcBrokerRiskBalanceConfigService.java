package io.bhex.ex.otc.service;

import io.bhex.ex.otc.entity.OtcBrokerRiskBalanceConfig;
import io.bhex.ex.otc.exception.ParamErrorException;
import io.bhex.ex.otc.mappper.OtcBrokerRiskBalanceConfigMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@Service
public class OtcBrokerRiskBalanceConfigService {

    @Autowired
    OtcBrokerRiskBalanceConfigMapper otcBrokerRiskBalanceConfigMapper;

    public OtcBrokerRiskBalanceConfig getOtcBrokerRiskBalanceConfig(Long orgId) {

        Example example = Example.builder(OtcBrokerRiskBalanceConfig.class).build();

        example.createCriteria().andEqualTo("orgId", orgId);

        return otcBrokerRiskBalanceConfigMapper.selectOneByExample(example);
    }

}
