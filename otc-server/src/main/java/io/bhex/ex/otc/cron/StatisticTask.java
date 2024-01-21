package io.bhex.ex.otc.cron;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import javax.annotation.Resource;

import io.bhex.ex.otc.service.MerchantStatisticsService;
import io.bhex.ex.otc.service.OtcStatisticDataService;
import io.bhex.ex.otc.util.LockUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lizhen
 * @date 2018-12-06
 */
@Slf4j
@Component
public class StatisticTask {

    private static final String STATISTIC_MERCHANT_LOCK = "OTC::STATISTIC::STATISTIC_MERCHANT_TASK::LOCK";

    private static final String STATISTIC_USER_LOCK = "OTC::STATISTIC::STATISTIC_USER_TASK::LOCK";

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private OtcStatisticDataService otcStatisticDataService;

    @Resource
    private MerchantStatisticsService merchantStatisticsService;

    @Deprecated
    //@Scheduled(cron = "0 0 1 * * ?")
    public void statisticData() {
        LockUtil.getLock(redisTemplate, STATISTIC_MERCHANT_LOCK, 60 * 60);
        Date now = new Date();
        Date yesterday = DateUtils.addDays(now, -1);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        otcStatisticDataService.executeStatistic(format.format(yesterday));
    }

    @Scheduled(cron = "0 0 0/1 * * ?")
    public void statisticOtcMerchant() {
        Boolean locked = LockUtil.tryLock(redisTemplate, STATISTIC_MERCHANT_LOCK, 60 * 60 * 1000);
        if (!locked) {
            return;
        }

        LocalDateTime end = LocalDateTime.now();
        LocalDateTime start = LocalDate.now().atStartOfDay().minusMonths(1);
        merchantStatisticsService.groupMerchantId(start, end);
    }

    /**
     * 用户24小时买币统计
     */
    @Scheduled(cron = "0 5/5 * * * ?")
    public void userBuyIn24Hours() {
        boolean locked = LockUtil.tryLock(redisTemplate, STATISTIC_USER_LOCK, 5 * 60 * 1000);
        if (!locked) {
            return;
        }
        try {
            LocalDateTime end = LocalDateTime.now();
            LocalDateTime start = end.minusHours(24);
            merchantStatisticsService.userBuyIn24Hours(start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            LockUtil.releaseLock(redisTemplate, STATISTIC_USER_LOCK);
        }

    }
}