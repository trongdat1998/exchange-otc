package io.bhex.ex.otc.cron;

import com.alibaba.fastjson.JSON;
import io.bhex.ex.otc.dto.SymbolDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
@Service
public class RefreshItemMessageListener implements MessageListener {


    private ExecutorService executor = Executors.newFixedThreadPool(30);

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private OnlineItemTask onlineItemTask;


    @Override
    public void onMessage(Message message, byte[] pattern) {

        byte[] body = message.getBody();
        byte[] channel = message.getChannel();
        //其中key必须为stringSerializer。和redisTemplate.convertAndSend对应
        String value = (String) stringRedisTemplate.getValueSerializer().deserialize(body);
        String topic = (String) stringRedisTemplate.getStringSerializer().deserialize(channel);

        if (topic.equals("/redis/otc-item/refreshItem")) {

            SymbolDto dto = JSON.parseObject(value, SymbolDto.class);
            executor.submit(() -> onlineItemTask.refreshItem(dto.getExchangeId(), dto.getTokenId(), dto.getCurrencyId()));
        }

    }
}
