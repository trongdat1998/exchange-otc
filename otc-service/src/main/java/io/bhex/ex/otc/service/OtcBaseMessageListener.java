package io.bhex.ex.otc.service;

import io.bhex.ex.otc.mappper.OtcOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public abstract class OtcBaseMessageListener implements MessageListener {


    @Resource
    protected OtcOrderMapper otcOrderMapper;

    @Resource
    protected StringRedisTemplate stringRedisTemplate;

    protected ExecutorService executor = Executors.newFixedThreadPool(30);


    @Override
    public void onMessage(Message message, byte[] pattern) {

        byte[] body = message.getBody();
        byte[] channel = message.getChannel();
        //其中key必须为stringSerializer。和redisTemplate.convertAndSend对应
        String value = (String) stringRedisTemplate.getValueSerializer().deserialize(body);
        String topic = (String) stringRedisTemplate.getStringSerializer().deserialize(channel);

        log.info("receive message,topic=[{}]", topic);

        if (topic.equals(getTopic()) && value.equals("run")) {
            executor.submit(() -> doWork(null));
        }
    }

    public abstract void doWork(Object object);

    public abstract String getTopic();

}
