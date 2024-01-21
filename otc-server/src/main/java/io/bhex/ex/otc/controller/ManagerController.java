package io.bhex.ex.otc.controller;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.alibaba.fastjson.JSON;

import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.account.GetOtcAvailableRequest;
import io.bhex.base.account.GetOtcAvailableResponse;
import io.bhex.base.account.ReleaseRiskOtcRequest;
import io.bhex.base.account.ReleaseRiskOtcResponse;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.ex.otc.OTCOrderHandleTypeEnum;
import io.bhex.ex.otc.config.GrpcClientFactory;
import io.bhex.ex.otc.cron.OrderTask;
import io.bhex.ex.otc.cron.StatisticTask;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcBrokerCurrency;
import io.bhex.ex.otc.entity.OtcBrokerToken;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.enums.OrderStatus;
import io.bhex.ex.otc.enums.Side;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.service.OtcBalanceFlowService;
import io.bhex.ex.otc.service.OtcItemService;
import io.bhex.ex.otc.service.OtcOrderService;
import io.bhex.ex.otc.service.OtcStatisticDataService;
import io.bhex.ex.otc.service.OtcUserInfoService;
import io.bhex.ex.otc.service.OtcUtilService;
import io.bhex.ex.otc.service.config.OtcConfigService;
import io.bhex.ex.otc.service.item.OtcItemOnline;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.extern.slf4j.Slf4j;

@Controller
@Slf4j
public class ManagerController {

    @Autowired
    private OtcStatisticDataService otcStatisticDataService;

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Autowired
    private OtcOrderService otcOrderService;

    @Autowired
    private GrpcClientFactory grpcClientFactory;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Resource
    private OtcConfigService otcConfigService;

    @Resource
    private OtcItemService otcItemService;

    @Autowired
    private OrderTask orderTask;

    @Resource
    private OtcUtilService otcUtilService;

    @Autowired
    private GrpcServerService grpcServerService;

    @Resource
    private StatisticTask statisticTask;

    @Resource
    private OtcBalanceFlowService otcBalanceFlowService;

    private static final Long RISK_FUND_TIME = 24L;

    private static final String RELEASE_TIME_KEY = "RELEASE::TIME::KEY";

    public static final String OTC_RISK_OPEN_KEY = "OTC::RISK::OPEN::KEY";


    private Map<OTCOrderHandleTypeEnum, OrderStatus> handleTypeStatusMap = Maps.newEnumMap(OTCOrderHandleTypeEnum.class);

    @PostConstruct
    public void init() {
        handleTypeStatusMap.put(OTCOrderHandleTypeEnum.APPEAL, OrderStatus.APPEAL);
        handleTypeStatusMap.put(OTCOrderHandleTypeEnum.CANCEL, OrderStatus.CANCEL);
        handleTypeStatusMap.put(OTCOrderHandleTypeEnum.FINISH, OrderStatus.FINISH);
    }

    @RequestMapping(value = "internal/metrics", produces = TextFormat.CONTENT_TYPE_004)
    @ResponseBody
    public String metrics(@RequestParam(name = "name[]", required = false) String[] names) throws IOException {
        Set<String> includedNameSet = names == null ? Collections.emptySet() : Sets.newHashSet(names);
        Writer writer = new StringWriter();
        TextFormat.write004(writer, CollectorRegistry.defaultRegistry.filteredMetricFamilySamples(includedNameSet));
        return writer.toString();
    }

    @RequestMapping(value = "internal/statistic", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String statistic(@RequestParam(name = "date", required = true) String date) {
        otcStatisticDataService.executeStatistic(date);
        return "success";
    }

    @RequestMapping(value = "internal/increaseCount", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String increaseCount(@RequestParam(name = "account_id", required = true) long accountId,
                                @RequestParam(name = "num", required = false, defaultValue = "1") int num) {
        log.warn("[internal/increaseCount] account id {}, num {}", accountId, num);

        for (int i = 0; i < num; ++i) {
            otcUserInfoService.increaseOrderNum(accountId, 1);
            otcUserInfoService.increaseExecuteNum(accountId);
        }
        return "success";
    }

    @RequestMapping(value = "internal/history/order", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String historyOrderPayInfo() {
        otcOrderService.refreshHistoryOrder();
        return "success";
    }

    @RequestMapping(value = "internal/release/order", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String releaseRiskFundByOrderId(@RequestParam(name = "orderId", required = false) Long orderId) {
        OtcOrder order = this.otcOrderService.findOtcOrderById(orderId);
        if (order == null) {
            log.info("order not exist");
        }
        //是否超过24小时 如果超过24小时调用风险资金释放接口 transfer_date
        if (order.getTransferDate() != null) {
            ReleaseRiskOtcRequest releaseRiskOtcRequest = ReleaseRiskOtcRequest
                    .newBuilder()
                    .setAccountId(order.getAccountId())
                    .setOrgId(order.getOrgId())
                    .setAmount(DecimalUtil.fromBigDecimal(order.getQuantity()))
                    .setBusinessId(order.getId())
                    .build();
            try {
                ReleaseRiskOtcResponse response = grpcClientFactory.balanceChangeBlockingStub().releaseRiskOtc(releaseRiskOtcRequest);
                if (response.getCode() == ReleaseRiskOtcResponse.ResponseCode.SUCCESS) {
                    //状态修改为已经解冻
                    otcOrderService.updateOtcOrderFreed(order.getId());
                } else {
                    log.warn("OTC release risk funds failed  orderId:{}, accountId:{}, orgId {}, quantity:{}", order.getId(),
                            order.getAccountId(), order.getOrgId(), order.getQuantity());
                }
            } catch (Exception ex) {
                log.warn("OTC release risk funds failed  orderId:{}, accountId:{}, orgId {}, quantity:{}", order.getId(),
                        order.getAccountId(), order.getOrgId(), order.getQuantity());
            }
        }
        return "success";
    }

    @RequestMapping(value = "internal/redis/add")
    @ResponseBody
    public String internalRedisSAddOperation(
            @RequestParam(name = "key", required = false) String key,
            @RequestParam(name = "value", required = false) String value) {
        log.info("redisOperation: add key: {} value : {}", key, value);
        redisTemplate.opsForValue().set(key, value);
        return String.valueOf(redisTemplate.opsForValue().get(key));
    }

    @RequestMapping(value = "internal/redis/get")
    @ResponseBody
    public String internalRedisSAddOperation(
            @RequestParam(name = "key", required = false) String key) {
        return String.valueOf(redisTemplate.opsForValue().get(key));
    }

    @RequestMapping(value = "internal/otc/available", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String getOtcAvailable(@RequestParam(name = "accountId", required = false) Long accountId,
                                  @RequestParam(name = "orgId", required = false) Long orgId,
                                  @RequestParam(name = "tokenId", required = false) String tokenId) {
        GetOtcAvailableRequest availableRequest = GetOtcAvailableRequest
                .newBuilder()
                .setAccountId(accountId)
                .setOrgId(orgId)
                .setTokenId(tokenId)
                .build();
        GetOtcAvailableResponse response = grpcClientFactory.balanceChangeBlockingStub().getOtcAvailable(availableRequest);
        log.info("otc available : {}", response.getAvailable());
        return "success";
    }

    /**
     * 增加法币 curl http://localhost:7241/internal/currency/create -d "orgId=&code=&language=&name=&minQuote=&maxQuote=&scale="
     */
    @RequestMapping(value = "/internal/currency/create")
    public String addCurrency(@RequestParam(name = "orgId") Long orgId,
                              @RequestParam(name = "code") String code,
                              @RequestParam(name = "language") String language,
                              @RequestParam(name = "name") String name,
                              @RequestParam(name = "minQuote") String minQuote,
                              @RequestParam(name = "maxQuote") String maxQuote,
                              @RequestParam(name = "scale") Integer scale) {

        try {

            OtcBrokerCurrency otcCurrency = new OtcBrokerCurrency();
            otcCurrency.setOrgId(orgId);
            otcCurrency.setCode(code);
            otcCurrency.setLanguage(language);
            otcCurrency.setMaxQuote(new BigDecimal(maxQuote));
            otcCurrency.setMinQuote(new BigDecimal(minQuote));
            otcCurrency.setName(name);
            otcCurrency.setScale(scale);
            otcCurrency.setStatus(1);
            otcCurrency.setCreateDate(new Date());
            otcCurrency.setUpdateDate(new Date());
            otcCurrency.setAmountScale(scale);
            otcConfigService.saveBrokerCurrency(otcCurrency);
            return "create success";

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }

    }

    //取消共享

    /**
     * curl http://localhost:7241/internal/share/cancel -d "orgId="
     */
    @RequestMapping(value = "/internal/share/cancel")
    public String cancelShareBroker(@RequestParam(name = "orgId") Long orgId) {
        try {
            otcConfigService.cancelBrokerShare(orgId);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "failure";
        }
    }

    /**
     * 开启白名单 curl http://localhost:7241/internal/share/add -d "orgId="
     */
    @RequestMapping(value = "/internal/share/add")
    public String addShareBroker(@RequestParam(name = "orgId") Long orgId) {

        try {
            otcConfigService.addSymoblShareBroker(orgId);
            return "success";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "failure";
        }
    }

    @ResponseBody
    @PostMapping("/internal/share/addShareBroker")
    public String addShareBroker(@RequestBody Map<String, Object> param) {
        Long brokerId = MapUtils.getLong(param, "orgId");
        return addShareBroker(brokerId);
    }

    @ResponseBody
    @PostMapping("/internal/share/cancelShareBroker")
    public String cancelShareBroker(@RequestBody Map<String, Object> param) {
        Long brokerId = MapUtils.getLong(param, "orgId");
        return cancelShareBroker(brokerId);
    }

    //获取有共享关系的券商列表
    @ResponseBody
    @RequestMapping(value = "/internal/share/list")
    public String listShareBrokers() {
        List<Long> list = otcConfigService.querySymoblShareBrokers();
        if (CollectionUtils.isEmpty(list)) {
            return "";
        }
        return String.join(",", list.stream().map(b -> b.toString()).collect(Collectors.toList()));
    }


    //增加token,包括创建token，增加symbol，增加交易费率

    /**
     * curl http://localhost:7241/internal/token/create -d "orgId=&tokenId=&tokenName=&minQuote=&maxQuote=&scale=&feeRate="
     */
    @RequestMapping(value = "/internal/token/create")
    public String addToken(@RequestParam(name = "orgId") Long orgId,
                           @RequestParam(name = "tokenId") String tokenId,
                           @RequestParam(name = "tokenName") String tokenName,
                           @RequestParam(name = "minQuote") String minQuote,
                           @RequestParam(name = "maxQuote") String maxQuote,
                           @RequestParam(name = "scale") Integer scale,
                           @RequestParam(name = "sequence", required = false, defaultValue = "100") Integer sequence,
                           @RequestParam(name = "shareStatus", required = false, defaultValue = "-1") Integer shareStatus,
                           @RequestParam(name = "feeRate", required = false, defaultValue = "0.001") String feeRate) {


        try {
            OtcBrokerToken token = OtcBrokerToken.builder()
                    .tokenId(tokenId)
                    .tokenName(tokenName)
                    .orgId(orgId)
                    .minQuote(new BigDecimal(minQuote))
                    .maxQuote(new BigDecimal(maxQuote))
                    .upRange(BigDecimal.valueOf(120))
                    .downRange(BigDecimal.valueOf(80))
                    .scale(scale)
                    .sequence(sequence)
                    .shareStatus(shareStatus)
                    .createDate(new Date())
                    .updateDate(new Date())
                    .status(1)
                    .feeRate(OtcBrokerToken.FeeRate.builder()
                            .buyRate(new BigDecimal(feeRate))
                            .sellRate(new BigDecimal(feeRate))
                            .build())
                    .build();

            otcConfigService.saveBrokerToken(token);
            return "success";

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return e.getMessage();
        }
    }


    @RequestMapping(value = "/internal/item/list")
    @ResponseBody
    public String getItemList(
            @RequestParam(name = "brokerId") Long brokerId,
            @RequestParam(name = "exchangeId") Long exchangeId,
            @RequestParam(name = "tokenId") String tokenId,
            @RequestParam(name = "currencyId") String currencyId) {

        log.info("internal/getItemList");
        List<Long> exchangeIds = otcConfigService.listExchangeIdByShareSymbolV2(brokerId, exchangeId, tokenId, currencyId);

        OtcItemOnline result = null;
        if (CollectionUtils.isEmpty(exchangeIds)) {
            //本交易所深度
            result = otcItemService.getOtcItemOnline(exchangeId, tokenId, currencyId);
        } else {
            //跨交易所广告列表
            result = otcItemService.getOtcItemOnlineWithMultiExchange(exchangeIds, tokenId, currencyId);
        }

        log.info("result={}", JSON.toJSONString(result));
        return "success";

    }


    @RequestMapping(value = "/internal/item/online")
    @ResponseBody
    public String getOnlineItemList(@RequestParam(name = "exchangeId") Long exchangeId,
                                    @RequestParam(name = "tokenId") String tokenId,
                                    @RequestParam(name = "currencyId") String currencyId) {

        log.info("internal/getOnlineItem");
        //Map<String, BigDecimal> lastPriceMap = grpcServerService.getLastPrice(exchangeId, tokenId);
        //BigDecimal lastPrice = lastPriceMap.get(currencyId);
        //otcItemService.refreshOnlineList(exchangeId,tokenId,currencyId,lastPrice);
        //String onlineKey = otcItemService.getOnlineKey(exchangeId, tokenId, currencyId);
        OtcItemOnline obj = otcItemService.getOtcItemOnline(exchangeId, tokenId, currencyId);

        log.info("result={}", JSON.toJSONString(obj));
        return "success";

    }


    @RequestMapping(value = "/internal/order/payovertime")
    @ResponseBody
    public String getPayOvertimeOrder(@RequestParam(name = "status") Integer status) {

        log.info("getPayOvertimeOrder");
        orderTask.orderPayOvertimeSchedule();
        return "success";

    }


    @RequestMapping(value = "/internal/order/list")
    @ResponseBody
    public String getOrderList(@RequestParam(name = "accountId") Long accountId,
                               @RequestParam(name = "page") Integer page,
                               @RequestParam(name = "size") Integer size,
                               @RequestParam(name = "side") Integer side) {

        log.info("getOrderList");
        List<OtcOrder> list = otcOrderService.getOtcOrderList(accountId, null, null, null, null, side, page, size);
        log.info("result={}", JSON.toJSONString(list));
        return "success";

    }

    @RequestMapping(value = "/internal/init/otc")
    @ResponseBody
    public String getPayOvertimeOrder(@RequestParam(name = "orgId") Long orgId,
                                      @RequestParam(name = "fromOrgId") Long fromOrgId,
                                      @RequestParam(name = "exchangeId") Long exchangeId,
                                      @RequestParam(name = "fromExchangeId") Long fromExchangeId,
                                      @RequestParam(name = "token") String token) {
        otcUtilService.createOtc(orgId, fromOrgId, exchangeId, fromExchangeId, token);
        return "success";
    }

    @RequestMapping(value = "/internal/get/last/price")
    @ResponseBody
    public String getPayOvertimeOrder(@RequestParam(name = "exchangeId") Long exchangeId,
                                      @RequestParam(name = "tokenId") String tokenId) {
        return JSON.toJSONString(grpcServerService.getLastPrice(exchangeId, tokenId));
    }

    @RequestMapping(value = "/internal/get/merchant/statistics")
    @ResponseBody
    public String merchantStatistics() {
        statisticTask.statisticOtcMerchant();
        return "success";
    }

    /**
     * curl http://localhost:7241/internal/admin/otc/order/list -d "status=&orgId=&page=&size="
     */
    @RequestMapping(value = "/internal/admin/otc/order/list")
    @ResponseBody
    public String listOtcOrderListForAdmin(@RequestParam(name = "status") Integer status,
                                           @RequestParam(name = "orgId") Long orgId,
                                           @RequestParam(name = "page") Integer page,
                                           @RequestParam(name = "size") Integer size,
                                           @RequestParam(name = "id") Long id,
                                           @RequestParam(name = "userId") Long userId) {
        Stopwatch sw = Stopwatch.createStarted();
        List<OtcOrder> list = otcOrderService.getOtcOrderListForAdmin(Lists.newArrayList(status), null, null, null,
                orgId, page, size, id, userId, Arrays.asList(0, 1));
        log.info("getOtcOrderListForAdmin,{}", JSON.toJSONString(list));
        log.info("listOtcOrderListForAdmin, consume={} mills", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        Map<Boolean, List<OtcOrder>> map = list.stream().collect(Collectors.groupingBy(i -> i.getDepthShareBool()));
        int shareCount = map.getOrDefault(Boolean.TRUE, Lists.newArrayList()).size();
        int unshareCount = map.getOrDefault(Boolean.FALSE, Lists.newArrayList()).size();
        log.info("shareCount {},unshareCount {}", shareCount, unshareCount);

        return "success";
    }

    @RequestMapping(value = "/internal/admin/otc/order/share")
    @ResponseBody
    public String listOtcShareOrderList(@RequestParam(name = "status") Integer status,
                                        @RequestParam(name = "orgId") Long orgId,
                                        @RequestParam(name = "page") Integer page,
                                        @RequestParam(name = "size") Integer size) {
        Stopwatch sw = Stopwatch.createStarted();
        List<OtcOrder> list = otcOrderService.listShareOrderV2(Lists.newArrayList(status), orgId, page, size);
        log.info("listOtcShareOrderList, consume={} mills", sw.stop().elapsed(TimeUnit.MILLISECONDS));
        log.info("shareCount {}", list.size());

        return "success";
    }

    @RequestMapping(value = "/internal/release/risk")
    @ResponseBody
    public String releaseRiskByOrderId(@RequestParam(name = "orderId") Long orderId) {
        OtcOrder order = this.otcOrderService.findOtcOrderById(orderId);

        if (order == null) {
            return "not find order";
        }
        OtcBalanceFlow otcBalanceFlow
                = this.otcBalanceFlowService.queryByOrderId(order.getId(), order.getAccountId());
        if (otcBalanceFlow == null) {
            return "not find record";
        }
        ReleaseRiskOtcRequest releaseRiskOtcRequest = ReleaseRiskOtcRequest
                .newBuilder()
                .setAccountId(order.getAccountId())
                .setOrgId(order.getOrgId())
                .setAmount(DecimalUtil.fromBigDecimal(order.getQuantity()))
                .setBusinessId(otcBalanceFlow.getId())
                .build();
        try {
            ReleaseRiskOtcResponse response = grpcClientFactory.balanceChangeBlockingStub().releaseRiskOtc(releaseRiskOtcRequest);
            if (response.getCode() == ReleaseRiskOtcResponse.ResponseCode.SUCCESS) {
                //状态修改为已经解冻
                otcOrderService.updateOtcOrderFreed(order.getId());
            } else {
                log.warn("OTC release risk funds failed  orderId:{}, accountId:{}, orgId {}, quantity:{}", order.getId(),
                        order.getAccountId(), order.getOrgId(), order.getQuantity());
            }
        } catch (Exception ex) {
            log.warn("OTC release risk funds failed  orderId:{}, accountId:{}, orgId {}, quantity:{}", order.getId(),
                    order.getAccountId(), order.getOrgId(), order.getQuantity());
        }
        return "success";
    }

    @RequestMapping(value = "/internal/item/cancel")
    @ResponseBody
    public String cancelItem(@RequestParam(name = "accountId") Long accountId,
                             @RequestParam(name = "itemId") Long itemId,
                             @RequestParam(name = "exchangeId") Long exchangeId) {

        log.info("cancelItem");
        OtcBalanceFlow balanceFlow = otcItemService.cancelItem(accountId, itemId, exchangeId);

        if (balanceFlow != null) {
            grpcServerService.sendBalanceChangeRequest(balanceFlow);
            return "success";
        } else {
            return "fail";
        }

    }

    @RequestMapping(value = "/internal/order/new_order/last_id")
    @ResponseBody
    public String getLastNewOrderId(@RequestParam(name = "accountId") Long accountId) {
        Long id = otcOrderService.getLastNewOrderId(accountId);
        return id.toString();
    }

    /**
     * curl http://localhost:7241/internal/order/handle -d "orderId=&orgId=&handleType="
     */
    @RequestMapping(value = "/internal/order/handle")
    @ResponseBody
    public String handleAppealOrder(@RequestParam(name = "orderId") Long orderId
            , @RequestParam(name = "orgId") Long orgId
            , @RequestParam(name = "handleType") Integer handleType) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            OtcOrder otcOrder = otcOrderService.getOtcOrderById(orderId);
            if (otcOrder == null ||
                    !otcOrder.getStatus().equals(OrderStatus.APPEAL.getStatus())) {
                return "Invalid orderId or Invalid status";
            }
            List<OtcBalanceFlow> balanceFlowList = null;

            //共享深度订单申诉处理需要双方确认，且一致后才能继续处理
            if (otcOrder.getDepthShareBool()) {
                boolean bothConfirm = otcOrderService.mutualConfirm(orgId, otcOrder,
                        handleTypeStatusMap.get(handleType), "0", "internal handle");
                if (!bothConfirm) {
                    return "Needs mutual confirm";
                }
            }

            switch (handleType) {
                //取消
                case 0:
                    // 撤单
                    OtcBalanceFlow otcBalanceFlow = otcOrderService.adminCancelOrder(otcOrder);
                    // 买方撤单计数加一
                    otcOrderService.increaseCancelNum(otcOrder.getSide().equals(Side.BUY.getCode()) ?
                            otcOrder.getAccountId() : otcOrder.getTargetAccountId());
                    if (otcBalanceFlow != null) {
                        balanceFlowList = Lists.newArrayList(otcBalanceFlow);
                    }
                    break;
                //完成
                case 3:
                    // 放币结束订单
                    balanceFlowList = otcOrderService.finishOrder(otcOrder);
                default:
                    break;
            }
            if (!CollectionUtils.isEmpty(balanceFlowList)) {
                balanceFlowList.forEach(grpcServerService::sendBalanceChangeRequest);
            }
            return "success";

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "exception";
        }
    }
}
