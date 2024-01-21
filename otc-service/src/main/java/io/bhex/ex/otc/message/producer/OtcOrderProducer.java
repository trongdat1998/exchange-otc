package io.bhex.ex.otc.message.producer;

import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Resource;

import io.bhex.base.mq.MessageProducer;
import io.bhex.base.mq.config.MQProperties;
import io.bhex.ex.otc.entity.OtcMessageConfig;
import io.bhex.ex.otc.mappper.OtcMessageConfigMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * otc 订单成交消息
 */
@Slf4j
@Component
public class OtcOrderProducer {

    @Resource
    MQProperties mqProperties;

    @Resource
    private OtcMessageConfigMapper otcMessageConfigMapper;


    /**
     * 'OTC_ORDER_' + orgId
     */
    private final static String OTC_ORDER_TOPIC_PREFIX = "OTC_ORDER_";

    private static final Map<String, MessageProducer> producerMap = new ConcurrentHashMap<>();

    public void bootProducer(String topic) {
        try {
            MessageProducer producer = producerMap.get(topic);
            if (producer == null) {
                producer = createProducer(topic);
                if (producer != null) {
                    producerMap.put(topic, producer);
                    log.info("build otc producer done. topic: {}", topic);
                } else {
                    log.error("build otc producer error. topic: {}", topic);
                }
            }
        } catch (Exception e) {
            log.error(String.format("build otc producer error. topic: %s", topic), e);
        }
    }

    private MessageProducer createProducer(String topic) {
        Assert.notNull(mqProperties.getNameServers(), "name server address must be defined");

        String groupName = topic;
        // build a producer proxy
        MessageProducer sender = MessageProducer
                .newBuild(mqProperties.getNameServers(), groupName)
                .setTimeout(mqProperties.getSendMsgTimeout())
                .setWithVipChannel(mqProperties.getVipChannelEnabled());

        sender.connect();

        log.info("[MQProducer] {} is ready!", sender.getClass().getSimpleName());

        return sender;
    }

    public String getOtcOrderTopic(Long brokerId) {
        return OTC_ORDER_TOPIC_PREFIX + brokerId;
    }

    public MessageProducer getProducerByTopic(String topic) {
        bootProducer(topic);
        MessageProducer producer = producerMap.get(topic);
        if (Objects.nonNull(producer)) {
            return producer;
        }
        return null;
    }

    public void writeOtcMessageToMQ(Long brokerId, String topic, Object message) {
        List<OtcMessageConfig> brokerIdList = otcMessageConfigMapper.queryAll();
        OtcMessageConfig config = brokerIdList.stream().filter(s -> s.getBrokerId().equals(brokerId)).findFirst().orElse(null);
        if (config == null) {
            return;
        }
        MessageProducer producer = this.getProducerByTopic(topic);
        if (Objects.isNull(producer)) {
            log.error("Write Otc order Message To MQ Error: producer null. topic => {}, message => {}.", topic, message);
            return;
        }
        if (StringUtils.isNotEmpty(topic) && Objects.nonNull(message)) {
            String jsonMsg = new Gson().toJson(message);
            try {
                Message msg = new Message(topic, jsonMsg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg);
            } catch (UnsupportedEncodingException e) {
                log.error("Write Otc order Message To MQ Error: topic => {}, message => {}.", topic, message, e);
            }
        }
    }
}
