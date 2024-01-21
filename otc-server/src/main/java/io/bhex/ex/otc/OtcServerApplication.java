package io.bhex.ex.otc;

import org.apache.commons.lang3.RandomUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.ZoneId;
import java.util.concurrent.ThreadPoolExecutor;

import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.idgen.enums.DataCenter;
import io.bhex.base.idgen.snowflake.SnowflakeGenerator;
import io.bhex.ex.otc.cron.RefreshItemMessageListener;
import io.bhex.ex.otc.service.MerchantStaticListener;
import io.bhex.ex.otc.service.UserBuyIn24HoursStatisticsListener;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@SpringBootApplication
@EnableTransactionManagement
@ComponentScan(basePackages = "io.bhex")
@EnableScheduling
public class OtcServerApplication {

    public static void main(String args[]) {
        log.info("ZoneId={}", ZoneId.systemDefault());
        SpringApplication.run(OtcServerApplication.class, args);
    }

    @Bean
    OtcServerInitializer serverInitializer() {
        return new OtcServerInitializer();
    }

    @Bean
    MessageListenerAdapter statisticsListenerAdapter(MerchantStaticListener receiver) {
        return new MessageListenerAdapter(receiver, "onMessage");
    }

    @Bean
    MessageListenerAdapter refreshItemListenerAdapter(RefreshItemMessageListener receiver) {
        return new MessageListenerAdapter(receiver, "onMessage");
    }

    //UserBuyIn24HoursStatisticsWorker
    //redis/otc/user/statistics
    @Bean
    MessageListenerAdapter userBuyStatisticsListenerAdapter(UserBuyIn24HoursStatisticsListener receiver) {
        return new MessageListenerAdapter(receiver, "onMessage");
    }

    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
                                                   MessageListenerAdapter statisticsListenerAdapter,
                                                   MessageListenerAdapter refreshItemListenerAdapter,
                                                   MessageListenerAdapter userBuyStatisticsListenerAdapter) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(statisticsListenerAdapter, new PatternTopic("/redis/otc/merchant/statistics"));
        container.addMessageListener(refreshItemListenerAdapter, new PatternTopic("/redis/otc-item/*"));
        container.addMessageListener(userBuyStatisticsListenerAdapter, new PatternTopic("/redis/otc/user/statistics"));
        return container;
    }

/*    @Bean
    public ISequenceGenerator sequenceGenerator() {
        return DefaultGenerator.newInstance();
    }*/

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(30);
        scheduler.setThreadNamePrefix("TaskScheduler-");
        scheduler.setAwaitTerminationSeconds(10);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }


    @Bean(name = "otcOrderHandleTaskExecutor")
    public TaskExecutor otcOrderHandleTaskExecutor() {
        ThreadPoolTaskExecutor otcOrderHandleTaskExecutor = new ThreadPoolTaskExecutor();
        otcOrderHandleTaskExecutor.setCorePoolSize(20);
        otcOrderHandleTaskExecutor.setMaxPoolSize(50);
        otcOrderHandleTaskExecutor.setQueueCapacity(1000);
        otcOrderHandleTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        otcOrderHandleTaskExecutor.setThreadNamePrefix("otcOrderTaskExecutor-");
        otcOrderHandleTaskExecutor.setAwaitTerminationSeconds(8);
        otcOrderHandleTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return otcOrderHandleTaskExecutor;
    }


    @Bean(name = "otcOrderCreateTaskExecutor")
    public TaskExecutor otcOrderCreateTaskExecutor() {
        ThreadPoolTaskExecutor otcOrderCreateTaskExecutor = new ThreadPoolTaskExecutor();
        otcOrderCreateTaskExecutor.setCorePoolSize(40);
        otcOrderCreateTaskExecutor.setMaxPoolSize(80);
        otcOrderCreateTaskExecutor.setQueueCapacity(1000);
        otcOrderCreateTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        otcOrderCreateTaskExecutor.setThreadNamePrefix("otcOrderCreateTaskExecutor-");
        otcOrderCreateTaskExecutor.setAwaitTerminationSeconds(8);
        otcOrderCreateTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return otcOrderCreateTaskExecutor;
    }

    @Bean
    public ISequenceGenerator sequenceGenerator(StringRedisTemplate redisTemplate) {
        long workId;
        try {
            workId = redisTemplate.opsForValue().increment("otc-idGenerator-wordId") % 512;
        } catch (Exception e) {
            workId = RandomUtils.nextLong(0, 512);
            log.error("getIdGeneratorWorkId from redis occurred exception. set a random workId:{}", workId);
        }
        log.info("use workId:{} for IdGenerator", workId);
        return SnowflakeGenerator.newInstance(DataCenter.DC1.value(), workId);
    }

}