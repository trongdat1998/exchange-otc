package io.bhex.ex.otc.util;


import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lizhen
 * @date 2018-11-09
 */
@Slf4j
public class LockUtil {

    @Deprecated
    public static void getLock(StringRedisTemplate redisTemplate, String lockKey, int expire) {
        try {
            //尝试获取锁
            while (!redisTemplate.opsForValue().setIfAbsent(lockKey, String.valueOf(System.currentTimeMillis()))) {
                //未拿到锁。说明可能有其他进程在处理
                TimeUnit.MILLISECONDS.sleep(200);
                //防止死锁，判断超时时间
                String lockTime = redisTemplate.opsForValue().get(lockKey);
                if (StringUtils.isNotBlank(lockTime) && System.currentTimeMillis() - Long.valueOf(lockTime)
                        > 1000 * expire) {
                    redisTemplate.delete(lockKey);
                }
            }
        } catch (Exception e) {
            log.error("get redis lock error ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * redis锁，过期则删除key
     */
    public static boolean getLockOnce(StringRedisTemplate redisTemplate, String lockKey, int expire) {
        try {
            //尝试获取锁
            boolean success = redisTemplate.opsForValue().setIfAbsent(lockKey, String.valueOf(System.currentTimeMillis()));
            if (!success) {
                //防止死锁，判断超时时间
                String lockTime = redisTemplate.opsForValue().get(lockKey);
                if (StringUtils.isNotBlank(lockTime) && System.currentTimeMillis() - Long.valueOf(lockTime)
                        > 1000 * expire) {
                    redisTemplate.delete(lockKey);
                    //重新拿锁
                    return getLockOnce(redisTemplate, lockKey, expire);
                }
            }

            return success;
        } catch (Exception e) {
            log.error("get redis lock error ", e);
            throw new RuntimeException(e);
        }
    }

    public static void releaseLock(StringRedisTemplate redisTemplate, String lockKey) {
        try {
            redisTemplate.delete(lockKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 尝试拿锁
     *
     * @param lockExpireTime 毫秒
     */
    public static boolean tryLock(RedisTemplate redisTemplate, String key, long lockExpireTime) {
        try {
            long expireAt = System.currentTimeMillis() + lockExpireTime;
            boolean lock = redisTemplate.opsForValue().setIfAbsent(key, expireAt + "", lockExpireTime, TimeUnit.MILLISECONDS);
            if (lock) {
                log.info("tryLock  success key {} time {}", key, expireAt);
                return true;
            }

            //增加过期检查
            String expectExpireStr = (String) redisTemplate.opsForValue().get(key);
            if (StringUtils.isBlank(expectExpireStr)) {
                return false;
            }
            Long expectExpire = Long.parseLong(expectExpireStr);
            if (System.currentTimeMillis() > expectExpire) {
                redisTemplate.delete(key);
                return tryLock(redisTemplate, key, lockExpireTime);
            }
        } catch (Exception e) {
            log.info("tryLock getLock exception:{}", key, e);
        }
        return false;
    }
}