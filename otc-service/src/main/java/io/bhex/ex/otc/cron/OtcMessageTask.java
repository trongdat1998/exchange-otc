package io.bhex.ex.otc.cron;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

import javax.annotation.Resource;

import io.bhex.ex.otc.entity.OtcMessageConfig;
import io.bhex.ex.otc.mappper.OtcMessageConfigMapper;
import io.bhex.ex.otc.message.producer.OtcOrderProducer;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yuehao
 * @date 2018-11-09
 */
@Slf4j
@Component
public class OtcMessageTask {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Resource
    private OtcMessageConfigMapper otcMessageConfigMapper;

    @Resource
    private OtcOrderProducer otcOrderProducer;

    @Scheduled(cron = "10/20 * * * * ?")
    public void refreshOtcOrderProducer() {
        List<OtcMessageConfig> orgIdList = otcMessageConfigMapper.queryAll();
        if (CollectionUtils.isEmpty(orgIdList)) {
            return;
        }
        orgIdList.forEach(broker -> {
            otcOrderProducer.bootProducer(otcOrderProducer.getOtcOrderTopic(broker.getBrokerId()));
        });
    }
}