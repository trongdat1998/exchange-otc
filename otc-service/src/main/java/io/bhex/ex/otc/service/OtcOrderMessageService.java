package io.bhex.ex.otc.service;

import java.util.List;

import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import io.bhex.ex.otc.entity.OtcOrderMessage;
import io.bhex.ex.otc.enums.MsgType;
import io.bhex.ex.otc.mappper.OtcOrderMessageMapper;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;

/**
 * 订单消息（聊天记录）service
 *
 * @author lizhen
 * @date 2018-09-13
 */
@Service
public class OtcOrderMessageService {

    public static final List<Integer> APPEARL_MST_TYPE_LIST = Lists.newArrayList(MsgType.SYS_APPEAL_WORD_MSG.getType(), MsgType.SYS_APPEAL_IMAGE_MSG.getType());


    @Resource
    private OtcOrderMessageMapper otcOrderMessageMapper;

    /**
     * 添加聊天记录
     *
     * @param otcOrderMessage
     * @return
     */
    public int addOtcOrderMessage(OtcOrderMessage otcOrderMessage) {
        return otcOrderMessageMapper.insert(otcOrderMessage);
    }

    /**
     * 查询订单的聊天记录
     *
     * @param accountId
     * @param orderId
     * @param startMessageId
     * @param size
     * @return
     */
    public List<OtcOrderMessage> getOtcOrderMessageList(long accountId, long orderId, long startMessageId, int size) {
        Example example = Example.builder(OtcOrderMessage.class)
                .orderByDesc("id")
                .build();
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orderId", orderId)
                .andLessThan("msgType", 100)
                .andCondition("( msg_type <> 0 or (msg_type = 0 and account_id = " + accountId + "))");
        if (startMessageId > 0) {
            criteria.andGreaterThan("id", startMessageId);
        }

        PageHelper.startPage(1, size);
        return otcOrderMessageMapper.selectByExample(example);
        //return otcOrderMessageMapper.selectByExampleAndRowBounds(example, new RowBounds(0, size));
    }

    public List<OtcOrderMessage> getOtcOrderAppealMessage(long orderId) {
        Example example = Example.builder(OtcOrderMessage.class)
                .orderByAsc("id")
                .build();
        Example.Criteria criteria = example.createCriteria()
                .andEqualTo("orderId", orderId)
                .andGreaterThan("msgType", 100)
                .andIn("msgType", APPEARL_MST_TYPE_LIST);


        return otcOrderMessageMapper.selectByExample(example);
    }
}