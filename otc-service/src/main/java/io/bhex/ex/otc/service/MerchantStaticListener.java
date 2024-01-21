package io.bhex.ex.otc.service;

import com.google.common.base.Splitter;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.annotation.Resource;

import io.bhex.ex.otc.entity.MerchantStatisticsEvent;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.mappper.OtcOrderAdminCanceledMapper;
import io.bhex.ex.otc.mappper.OtcUserInfoMapper;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;


@Slf4j
@Service
public class MerchantStaticListener extends OtcBaseMessageListener {

    private final static String MERCHANT_STATISTICS = "OTC_MERCHANT_STATISTICS";


    @Resource
    private OtcUserInfoMapper otcUserInfoMapper;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private OtcOrderAdminCanceledMapper otcOrderAdminCanceledMapper;

    private final static String TOPIC = "/redis/otc/merchant/statistics";

    @Override
    public String getTopic() {
        return TOPIC;
    }


    /**
     * 从redis取出商家id,进行统计
     */
    @Override
    public void doWork(Object object) {

        String accountId = null;
        boolean rollback = true;
        try {
            while (true) {
                accountId = stringRedisTemplate.opsForList().rightPop(MERCHANT_STATISTICS);

                if (StringUtils.isBlank(accountId)) {
                    log.info("Hasn't any accountId");
                    break;
                }

                log.info("exec statistics,param=[{}]", accountId);
                List<String> list = Splitter.on("*").splitToList(accountId);
                if (list.size() != 3) {
                    log.error("merchantStatistics error,accountId={}", accountId);
                    continue;
                }

                Long targetAccountId = Long.parseLong(list.get(0));
                Date start = new Date(Long.parseLong(list.get(1)));
                Date end = new Date(Long.parseLong(list.get(2)));

                long startTime = System.currentTimeMillis();
                boolean success = this.statisMerchant(targetAccountId, start, end);
                long endTime = System.currentTimeMillis();
                log.info("doWork **** accountId {} use time {} ", accountId, (endTime - startTime));
                if (success) {
                    rollback = false;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (rollback == true) {
                try {
                    stringRedisTemplate.opsForList().leftPush(MERCHANT_STATISTICS, accountId);
                } catch (Exception ex) {
                    log.error("merchantStatistics rollback fail,accountId={}", accountId);
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public boolean statisMerchant(Long targetAccountId, Date start, Date end) {

        try {
            Long finishNumber = otcOrderMapper.countFinishOrderNumber(targetAccountId, start, end);
            Long totalNumber = otcOrderMapper.countTotalOrderNumberV3(targetAccountId, start, end);
            //仲裁订单不计入总单数
            int exclusiveCount = 0;
            exclusiveCount = otcOrderAdminCanceledMapper.countExclusiveOrder(targetAccountId, start, end);
            totalNumber -= exclusiveCount;
            if (totalNumber < 0) {
                totalNumber = 0L;
            }
            BigDecimal completeRate = BigDecimal.ZERO;
            if (Objects.nonNull(finishNumber) && Objects.nonNull(totalNumber) && finishNumber.longValue() > 0 && totalNumber > 0) {
                completeRate = new BigDecimal(finishNumber).divide(new BigDecimal(totalNumber), 5, RoundingMode.HALF_EVEN);
                if (finishNumber > totalNumber) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                    log.warn("statistics error,finishCount={},exclusiveCount={},totalCount={},accountId={},start={},end={}",
                            finishNumber, exclusiveCount, totalNumber, targetAccountId, sdf.format(start), sdf.format(end));
                    totalNumber = finishNumber;
                    completeRate = BigDecimal.ONE;
                }
            }

            Example exp = new Example(OtcUserInfo.class);
            exp.createCriteria().andEqualTo("accountId", targetAccountId);
            OtcUserInfo user = OtcUserInfo.builder()
                    .completeRateDay30(completeRate)
                    .orderFinishNumberDay30(finishNumber.intValue())
                    .orderTotalNumberDay30(totalNumber.intValue())
                    .updateDate(new Date())
                    .build();

            int rows = otcUserInfoMapper.updateByExampleSelective(user, exp);
            return rows == 1;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }


    @Async
    @EventListener
    public void statisticsListener(MerchantStatisticsEvent event) {

        Long accountId = event.getAccountId();
        if (Objects.isNull(accountId) || accountId.intValue() < 1) {
            log.info("statisticsListener,invalid accountId");
            return;
        }
        LocalDateTime end = LocalDateTime.now();
        LocalDateTime start = LocalDate.now().atStartOfDay().minusMonths(1);

        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdtStart = start.atZone(zoneId);
        Date dateStart = Date.from(zdtStart.toInstant());

        ZonedDateTime zdtEnd = end.atZone(zoneId);
        Date dateEnd = Date.from(zdtEnd.toInstant());
        log.info("exec statisticsListener,accountId={}", accountId);
        boolean success = statisMerchant(accountId, dateStart, dateEnd);
        log.info("statisticsListener {},accountId={},start={},end={}", success, accountId, dateStart.toString(), dateEnd.toString());
    }
}
