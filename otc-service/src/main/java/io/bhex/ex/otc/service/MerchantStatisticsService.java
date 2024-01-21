package io.bhex.ex.otc.service;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.entity.UserExt;
import io.bhex.ex.otc.mappper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MerchantStatisticsService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private OtcOrderMapper otcOrderMapper;

    @Resource
    private OtcUserInfoService otcUserInfoService;

    @Resource
    private UserExtMapper userExtMapper;

    private final static String MERCHANT_STATISTICS = "OTC_MERCHANT_STATISTICS";

    private final static String USER_STATISTICS = "OTC_USER_STATISTICS";

    /**
     * 查询30日内有交易的商家id
     *
     * @param start
     * @param end
     */
    public void groupMerchantId(LocalDateTime start, LocalDateTime end) {

        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdtStart = start.atZone(zoneId);
        Date dateStart = Date.from(zdtStart.toInstant());

        ZonedDateTime zdtEnd = end.atZone(zoneId);
        Date dateEnd = Date.from(zdtEnd.toInstant());

        List<Long> list = otcOrderMapper.groupMerchantTargetAccountId(dateStart, dateEnd);
        List<Long> accountList = otcOrderMapper.groupMerchantAccountId(dateStart, dateEnd);
        list.addAll(accountList);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        List<String> merchantAccountIds = list.stream().map(id -> {
            return Joiner.on("*").join(Lists.newArrayList(id.toString(), dateStart.getTime() + "", dateEnd.getTime() + ""));
        }).collect(Collectors.toList());

        log.info("merchantId size={}", merchantAccountIds.size());
        List<List<String>> partitions = Lists.partition(merchantAccountIds, 100);
        partitions.forEach(item -> {
            stringRedisTemplate.opsForList().leftPushAll(MERCHANT_STATISTICS, item);
        });

        stringRedisTemplate.convertAndSend("/redis/otc/merchant/statistics", "run");
    }

    public void userBuyIn24Hours(LocalDateTime start, LocalDateTime end) {

        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdtStart = start.atZone(zoneId);
        Date dateStart = Date.from(zdtStart.toInstant());

        ZonedDateTime zdtEnd = end.atZone(zoneId);
        Date dateEnd = Date.from(zdtEnd.toInstant());

        Example exp = new Example(UserExt.class);
        exp.createCriteria()
                .andGreaterThan("usdtValue24HoursBuy", BigDecimal.ZERO)
                .andLessThan("updatedAt", start);

        UserExt ext = UserExt.builder()
                .updatedAt(new Date())
                .usdtValue24HoursBuy(BigDecimal.ZERO)
                .build();
        userExtMapper.updateByExampleSelective(ext, exp);

        List<Long> list = otcOrderMapper.listAccountIdsFromBuyOrderIn24Hours(dateStart, dateEnd);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        List<String> accountIds = list.stream().map(id -> {
            return Joiner.on("*").join(Lists.newArrayList(id.toString(), dateStart.getTime() + "", dateEnd.getTime() + "", "0"));
        }).collect(Collectors.toList());

        log.info("userAccount size={}", accountIds.size());

        List<List<String>> partitions = Lists.partition(accountIds, 20);
        partitions.forEach(item -> {
            stringRedisTemplate.opsForList().leftPushAll(USER_STATISTICS, item);
        });

        stringRedisTemplate.convertAndSend("/redis/otc/user/statistics", "run");
    }
}
