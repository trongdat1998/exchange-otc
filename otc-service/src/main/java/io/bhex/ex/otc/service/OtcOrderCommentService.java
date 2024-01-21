package io.bhex.ex.otc.service;

import java.util.List;

import io.bhex.ex.otc.entity.OtcOrderComment;
import io.bhex.ex.otc.mappper.OtcOrderCommentMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 订单评价service
 *
 * @author lizhen
 * @date 2018-09-14
 */
@Service
public class OtcOrderCommentService {

    @Autowired
    private OtcOrderCommentMapper otcOrderCommentMapper;

    public int addOtcOrderComment(OtcOrderComment otcOrderComment) {
        return otcOrderCommentMapper.insert(otcOrderComment);
    }

    public List<OtcOrderComment> getOtcOrderCommentList(long orderId) {
        OtcOrderComment example = OtcOrderComment.builder()
                .orderId(orderId)
                .build();
        return otcOrderCommentMapper.select(example);
    }
}