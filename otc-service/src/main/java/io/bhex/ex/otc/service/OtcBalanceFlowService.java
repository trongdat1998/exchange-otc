package io.bhex.ex.otc.service;

import java.util.Date;
import java.util.List;

import com.github.pagehelper.PageHelper;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.enums.FlowStatus;
import io.bhex.ex.otc.mappper.OtcBalanceFlowMapper;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.Resource;

/**
 * 动账流水service
 *
 * @author lizhen
 * @date 2018-09-23
 */
@Service
public class OtcBalanceFlowService {

    @Resource
    private OtcBalanceFlowMapper otcBalanceFlowMapper;

    public void finishOtcBalanceFlow(long flowId) {
        otcBalanceFlowMapper.updateOtcBalanceFlowStatus(flowId, FlowStatus.SUCCESS.getStatus(),
                FlowStatus.WAITING_PROCESS.getStatus(), new Date());
    }

    public List<OtcBalanceFlow> getTimeoutOtcBalanceFlow() {
        Example example = Example.builder(OtcBalanceFlow.class).build();
        example.createCriteria()
                .andEqualTo("status", FlowStatus.WAITING_PROCESS.getStatus())
                .andLessThan("createDate", new Date(System.currentTimeMillis() - 1000 * 60));

        PageHelper.startPage(1, 100);
        return otcBalanceFlowMapper.selectByExample(example);
        //return otcBalanceFlowMapper.selectByExampleAndRowBounds(example, new RowBounds(0, 100));
    }

    public OtcBalanceFlow queryByOrderId(Long orderId, Long accountId) {
        return otcBalanceFlowMapper.queryByOrderId(orderId, accountId);
    }
}