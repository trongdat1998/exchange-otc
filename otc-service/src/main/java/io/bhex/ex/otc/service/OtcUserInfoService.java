package io.bhex.ex.otc.service;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.entity.UserExt;
import io.bhex.ex.otc.enums.UserStatusFlag;
import io.bhex.ex.otc.mappper.OtcUserInfoMapper;
import io.bhex.ex.otc.mappper.UserExtMapper;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * otc用户信息service
 *
 * @author lizhen
 * @date 2018-09-16
 */
@Slf4j
@Service
public class OtcUserInfoService {

    @Resource
    private OtcUserInfoMapper otcUserInfoMapper;

    @Resource
    private UserExtMapper userExtMapper;

    public void setNickName(OtcUserInfo otcUserInfo) {
        OtcUserInfo old = getOtcUserInfo(otcUserInfo.getAccountId());
        if (old != null) {
            OtcUserInfo updater = OtcUserInfo.builder()
                    .id(old.getId())
                    .nickName(otcUserInfo.getNickName())
                    .updateDate(new Date())
                    .build();
            otcUserInfoMapper.updateByPrimaryKeySelective(updater);
        } else {
            otcUserInfoMapper.insertSelective(otcUserInfo);
        }
    }

    public OtcUserInfo getOtcUserInfo(long accountId) {
        OtcUserInfo example = OtcUserInfo.builder().accountId(accountId).build();
        return otcUserInfoMapper.selectOne(example);
    }

    public OtcUserInfo getOtcUserInfo(String nickName) {
        OtcUserInfo example = OtcUserInfo.builder().nickName(nickName).build();
        return otcUserInfoMapper.selectOne(example);
    }

    public OtcUserInfo getOtcUserInfoForUpdate(long accountId) {
        return otcUserInfoMapper.selectOtcUserInfoForUpdate(accountId);
    }

    public Map<Long, OtcUserInfo> getOtcUserInfoMap(List<Long> accountId) {
        Example example = Example.builder(OtcUserInfo.class).build();
        example.createCriteria()
                .andIn("accountId", accountId);
        List<OtcUserInfo> list = otcUserInfoMapper.selectByExample(example);
        Map<Long, OtcUserInfo> result = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(list)) {
            list.forEach(otcUserInfo -> result.put(otcUserInfo.getAccountId(), otcUserInfo));
        }
        return result;
    }

    public Map<Long, String> listUserName(List<Long> accountIds) {

        Example example = Example.builder(OtcUserInfo.class).build();
        example.selectProperties("accountId", "nickName");
        example.createCriteria()
                .andIn("accountId", accountIds);
        List<OtcUserInfo> list = otcUserInfoMapper.selectByExample(example);
        return list.stream().collect(Collectors.toMap(i -> i.getAccountId(), i -> i.getNickName()));

    }

    public int increaseOrderNum(long accountId, int num) {
        return otcUserInfoMapper.increaseOrderNum(accountId, num);
    }

    public int increaseExecuteNum(long accountId) {
        return otcUserInfoMapper.increaseExecuteNum(accountId);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int setTradeFlag(long accountId) {
        return setStatus(accountId, UserStatusFlag.RECENT_TRADE_FLAG, 1);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int setStatus(long accountId, UserStatusFlag flag, int value) {
        OtcUserInfo userInfo = otcUserInfoMapper.selectOtcUserInfoForUpdate(accountId);
        int status = setBinaryIndex(userInfo.getStatus(), flag.getBit(), value);
        return otcUserInfoMapper.setStatus(accountId, status);
    }

    public boolean getStatus(int status, UserStatusFlag flag) {
        return getBinaryIndex(status, flag.getBit()) == 1;
    }

    /**
     * 设置指定状态位值
     *
     * @param value 当前值
     * @param bit   标识位
     * @param flag  状态值
     * @return 设置后的值
     */
    private static int setBinaryIndex(int value, int bit, int flag) {
        if (getBinaryIndex(value, bit) == 1) {
            if (flag == 0) {
                value = value - (1 << bit - 1);
            }
        } else {
            if (flag == 1) {
                value = value | (1 << bit - 1);
            }
        }
        return value;
    }

    /**
     * 返回第几位0/1值
     *
     * @param value 当前值
     * @param bit   标识位
     * @return 状态值
     */
    private static int getBinaryIndex(int value, int bit) {
        int remainder = 0;
        for (int i = 0; i < bit; i++) {
            int factor = value / 2;
            remainder = value % 2;
            if (factor == 0) {
                if (i < bit - 1) {
                    remainder = 0;
                }
                break;
            }
            value = factor;
        }
        return remainder;
    }

    public OtcUserInfo getUserByNickname(String nickname) {

        Example exp = new Example(OtcUserInfo.class);
        exp.createCriteria().andEqualTo("nickName", nickname);
        return otcUserInfoMapper.selectOneByExample(exp);
    }

    public OtcUserInfo getUserByMobile(Long orgId, String mobile) {
        Example exp = new Example(OtcUserInfo.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("mobile", mobile);
        return otcUserInfoMapper.selectOneByExample(exp);
    }

    public OtcUserInfo getUserByEmail(Long orgId, String email) {
        Example exp = new Example(OtcUserInfo.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("email", email);
        return otcUserInfoMapper.selectOneByExample(exp);
    }


    public void updateContact(long accountId, String mobile, String email, String userFirstName, String userSecondName) {
        log.info("updateContact accountId {} userFirstName {} userSecondName {}", accountId, userFirstName, userSecondName);
        OtcUserInfo userInfo = this.getOtcUserInfo(accountId);
        if (Objects.isNull(userInfo)) {
            log.warn("Absent user info,accountId={}", accountId);
            return;
        }
        OtcUserInfo update = new OtcUserInfo();
        update.setId(userInfo.getId());
        if (!Strings.isNullOrEmpty(mobile)) {
            update.setMobile(mobile);
        }

        if (!Strings.isNullOrEmpty(email)) {
            update.setEmail(email);
        }

        if (!Strings.isNullOrEmpty(userFirstName)) {
            update.setUserFirstName(userFirstName);
        }

        if (!Strings.isNullOrEmpty(userSecondName)) {
            update.setUserSecondName(userSecondName);
        }
        update.setUpdateDate(new Date());
        otcUserInfoMapper.updateByPrimaryKeySelective(update);
    }

    public OtcUserInfo getOtcUserInfoByUserId(long orgId, long userId) {

        Example exp = new Example(OtcUserInfo.class);
        exp.createCriteria()
                .andEqualTo("orgId", orgId)
                .andEqualTo("userId", userId)
        ;
        return otcUserInfoMapper.selectOneByExample(exp);
    }

    public List<OtcUserInfo> listUserInfo(List<Long> orgIds, List<Long> userIds) {

        Example exp = new Example(OtcUserInfo.class);
        exp.createCriteria()
                .andIn("orgId", orgIds)
                .andIn("userId", userIds);
        return otcUserInfoMapper.selectByExample(exp);
    }

    public Pair<List<OtcUserInfo>, Integer> listUserInfo(int pageNo, int pageSize, List<Long> userIdsList) {

        Example exp = null;
        if (CollectionUtils.isNotEmpty(userIdsList)) {
            exp = new Example(OtcUserInfo.class);
            exp.createCriteria().andIn("userId", userIdsList);

            List<OtcUserInfo> list = otcUserInfoMapper.selectByExample(exp);
            //增加用户扩展信息
            appendUserExt(list);
            return Pair.of(list, userIdsList.size());
        }

        if (pageNo == 0) {
            pageNo = 1;
        }

        if (pageSize == 0) {
            pageSize = 100;
        }

        PageHelper.startPage(pageNo, pageSize);
        exp = new Example(OtcUserInfo.class);
        exp.orderBy("createDate").desc();
        List<OtcUserInfo> list = otcUserInfoMapper.selectByExample(exp);
        //增加用户扩展信息
        //appendUserExt(list);
        PageInfo pi = new PageInfo(list);
        return Pair.of(list, (int) pi.getTotal());
    }

    private void appendUserExt(List<OtcUserInfo> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        String ids = Joiner.on(",").join(list.stream().map(i -> i.getUserId()).collect(Collectors.toList()));
        List<UserExt> exts = userExtMapper.selectByIds(ids);
        if (CollectionUtils.isEmpty(exts)) {
            return;
        }

        Map<Long, UserExt> map = exts.stream().collect(Collectors.toMap(i -> i.getUserId(), i -> i));
        list.forEach(i -> {
            UserExt ext = map.get(i.getUserId());
            i.setUserExt(ext);
        });

    }
}