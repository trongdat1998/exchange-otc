package io.bhex.ex.otc.service;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

import com.alibaba.fastjson.JSON;
import com.github.pagehelper.PageHelper;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import io.bhex.base.account.GetPositionRequest;
import io.bhex.base.account.PositionResponseList;
import io.bhex.base.idgen.api.ISequenceGenerator;
import io.bhex.base.rc.RcBalanceResponse;
import io.bhex.base.rc.UserRequest;
import io.bhex.ex.otc.OTCNewOrderRequest;
import io.bhex.ex.otc.OTCOrderStatusEnum;
import io.bhex.ex.otc.entity.BrokerExt;
import io.bhex.ex.otc.entity.MerchantStatisticsEvent;
import io.bhex.ex.otc.entity.OrderIndex;
import io.bhex.ex.otc.entity.OtcBalanceFlow;
import io.bhex.ex.otc.entity.OtcBrokerCurrency;
import io.bhex.ex.otc.entity.OtcBrokerRiskBalanceConfig;
import io.bhex.ex.otc.entity.OtcBrokerToken;
import io.bhex.ex.otc.entity.OtcDepthShareBrokerWhiteList;
import io.bhex.ex.otc.entity.OtcItem;
import io.bhex.ex.otc.entity.OtcOrder;
import io.bhex.ex.otc.entity.OtcOrderAdminCanceled;
import io.bhex.ex.otc.entity.OtcOrderDepthShare;
import io.bhex.ex.otc.entity.OtcOrderDepthShareAppeal;
import io.bhex.ex.otc.entity.OtcOrderExt;
import io.bhex.ex.otc.entity.OtcOrderMessage;
import io.bhex.ex.otc.entity.OtcOrderPayInfo;
import io.bhex.ex.otc.entity.OtcPaymentTerm;
import io.bhex.ex.otc.entity.OtcShareLimit;
import io.bhex.ex.otc.entity.OtcTradeFeeRate;
import io.bhex.ex.otc.entity.OtcUserInfo;
import io.bhex.ex.otc.enums.FlowStatus;
import io.bhex.ex.otc.enums.FlowType;
import io.bhex.ex.otc.enums.ItemStatus;
import io.bhex.ex.otc.enums.MsgCode;
import io.bhex.ex.otc.enums.MsgType;
import io.bhex.ex.otc.enums.OrderStatus;
import io.bhex.ex.otc.enums.PriceType;
import io.bhex.ex.otc.enums.Side;
import io.bhex.ex.otc.exception.BalanceNotEnoughExcption;
import io.bhex.ex.otc.exception.BusinessException;
import io.bhex.ex.otc.exception.CancelOrderMaxTimesException;
import io.bhex.ex.otc.exception.CrossExchangeException;
import io.bhex.ex.otc.exception.DifferentTradeException;
import io.bhex.ex.otc.exception.ExchangeSelfException;
import io.bhex.ex.otc.exception.ItemNotExistException;
import io.bhex.ex.otc.exception.NicknameNotSetException;
import io.bhex.ex.otc.exception.NonPersonPaymentException;
import io.bhex.ex.otc.exception.OrderMaxException;
import io.bhex.ex.otc.exception.OrderNotExistException;
import io.bhex.ex.otc.exception.ParamErrorException;
import io.bhex.ex.otc.exception.PaymentDoesNotMatchException;
import io.bhex.ex.otc.exception.QuantityNotEnoughException;
import io.bhex.ex.otc.exception.RiskControlInterceptionException;
import io.bhex.ex.otc.exception.UserBuyTradeLimitException;
import io.bhex.ex.otc.grpc.GrpcExchangeRCService;
import io.bhex.ex.otc.grpc.GrpcServerService;
import io.bhex.ex.otc.mappper.OrderIndexMapper;
import io.bhex.ex.otc.mappper.OtcBalanceFlowMapper;
import io.bhex.ex.otc.mappper.OtcDepthShareBrokerWhiteListMapper;
import io.bhex.ex.otc.mappper.OtcItemMapper;
import io.bhex.ex.otc.mappper.OtcOrderAdminCanceledMapper;
import io.bhex.ex.otc.mappper.OtcOrderDepthShareAppealMapper;
import io.bhex.ex.otc.mappper.OtcOrderDepthShareMapper;
import io.bhex.ex.otc.mappper.OtcOrderExtMapper;
import io.bhex.ex.otc.mappper.OtcOrderMapper;
import io.bhex.ex.otc.mappper.OtcOrderPayInfoMapper;
import io.bhex.ex.otc.mappper.OtcPaymentTermMapper;
import io.bhex.ex.otc.mappper.OtcShareLimitMapper;
import io.bhex.ex.otc.message.producer.OtcOrderProducer;
import io.bhex.ex.otc.service.config.OtcConfigService;
import io.bhex.ex.otc.service.order.OrderMsg;
import io.bhex.ex.proto.OrderSideEnum;
import io.prometheus.client.Histogram;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tk.mybatis.mapper.entity.Example;

/**
 * 订单service
 *
 * @author lizhen
 * @date 2018-09-13
 */
@Slf4j
@Service
public class OtcOrderService {

    @Resource
    private OtcOrderMapper otcOrderMapper;

    @Resource
    private OtcItemMapper otcItemMapper;

    @Resource
    private OtcBalanceFlowMapper otcBalanceFlowMapper;

    @Autowired
    private OtcUserInfoService otcUserInfoService;

    @Resource
    private OtcPaymentTermService otcPaymentTermService;

    @Autowired
    private OtcOrderMessageService otcOrderMessageService;

    @Autowired
    private ISequenceGenerator sequenceGenerator;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private OtcPaymentTermMapper otcPaymentTermMapper;

    @Resource
    private OtcOrderPayInfoMapper otcOrderPayInfoMapper;

    @Autowired
    private OtcTradeFeeService otcTradeFeeService;

    @Resource
    private OtcOrderDepthShareMapper otcOrderDepthShareMapper;

    @Resource
    private OtcOrderDepthShareAppealMapper otcOrderDepthShareAppealMapper;

    @Resource
    private OtcConfigService otcConfigService;

    @Resource
    private OtcOrderExtMapper otcOrderExtMapper;

    @Resource
    private OrderIndexMapper orderIndexMapper;

    @Resource
    private OtcOrderAdminCanceledMapper otcOrderAdminCanceledMapper;

    @Value("${app.newFeatureEnable}")
    private Boolean newFeatureEnable = false;

    @Resource
    private ApplicationEventPublisher eventPublisher;

    @Resource
    private OtcOrderProducer producer;

    @Resource
    private OtcDepthShareBrokerWhiteListMapper otcDepthShareBrokerWhiteListMapper;

    @Resource
    private OtcShareLimitMapper otcShareLimitMapper;

    @Resource
    private GrpcServerService grpcServerService;

    @Resource
    private OtcBrokerRiskBalanceConfigService otcBrokerRiskBalanceConfigService;

    @Resource
    private GrpcExchangeRCService grpcExchangeRCService;

    private ExecutorService executorService = new ThreadPoolExecutor(10, 30, 20L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(100000), new ThreadFactoryBuilder().setNameFormat("order-%d").build(),
            new ThreadPoolExecutor.DiscardPolicy());

    private static final String OTC_ORDER_CANCEL_NUM_KEY = "OTC::ORDER::CANCEL_NUM::%s";

    private static final long OTC_ORDER_CANCEL_EXPIRE_TIME = 24 * 60 * 60 * 1000;

    public static final String OTC_ORDER_ORG_MSG_KEY = "OTC::ORDER::ORG_MSG::%s";

    public static final String OTC_RISK_OPEN_KEY = "OTC::RISK::OPEN::KEY";

    public static final String OTC_MAX_ORDER_KEY = "OTC::MAX::ORDER::KEY";

    public static final String LAST_NEW_ORDER_ID = "OTC::LAST::NEW::ORDER::ID::%s";

    private static final List<String> RISK_TOKEN_LIST = Arrays.asList("USDT", "BTC", "ETH");

    private static final String CLOSED_ORDER_LIST_KEY_PATTERN = "OTC::CLOSE::ORDER::LIST::%s";

    public static final BigDecimal OTC_BUY_ORDER_QUANTITY_LIMIT = new BigDecimal("200000");//USDT单日买单最大限制

    public static final double[] CONTROLLER_TIME_BUCKETS = new double[]{
            .1, 1, 2, 3, 5,
            10, 20, 30, 50, 75,
            100, 200, 500, 1000, 2000, 10000
    };

    @Resource(name = "otcOrderHandleTaskExecutor")
    private TaskExecutor otcOrderHandleTaskExecutor;

    @Resource(name = "otcOrderCreateTaskExecutor")
    private TaskExecutor otcOrderCreateTaskExecutor;

    private static final Histogram OTC_ORDER_METRICS = Histogram.build()
            .namespace("otc")
            .subsystem("order")
            .name("otc_order_delay_milliseconds")
            .labelNames("process_name")
            .buckets(CONTROLLER_TIME_BUCKETS)
            .help("Histogram of stream handle latency in milliseconds")
            .register();

    private static final Histogram OTC_ORDER_SERVICE_METRICS = Histogram.build()
            .namespace("otc")
            .subsystem("order_service")
            .name("otc_order_service_delay_milliseconds")
            .labelNames("process_name")
            .buckets(CONTROLLER_TIME_BUCKETS)
            .help("Histogram of stream handle latency in milliseconds")
            .register();


    @PostConstruct
    public void init() {
        log.info("newFeatureEnable={}", newFeatureEnable);
        log.info("current id={}", sequenceGenerator.getLong());

        otcConfigService.setOtcOrderService(this);
    }


    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public long createOtcOrder(OtcOrder otcOrder, String userFirstName, String userSecondName) {
        long preCheckTime = System.currentTimeMillis();
        //判断当前用户当前方向进行中的订单 超过设置单数直接抛异常 REDIS key 提前初始化！
        if (!otcOrder.getIsBusiness() && otcOrder.getSide().equals(Side.BUY.getCode())) {
            String orderLimit = stringRedisTemplate.opsForValue().get(OTC_MAX_ORDER_KEY);
            if (StringUtils.isNotEmpty(orderLimit)) {
                if (otcOrderMapper.selectUnderwayCountOtcOrderByUserId(otcOrder.getOrgId(), otcOrder.getAccountId(), otcOrder.getSide()) >= Integer.parseInt(orderLimit)) {
                    throw new OrderMaxException("order max limit, accountId=" + otcOrder.getAccountId());
                }
            }
            if (otcOrder.getTokenId().equals("USDT")) {
                BigDecimal quantity = this.sumQuantityFromBuyOrderInDayByAccountId(otcOrder.getAccountId(), otcOrder.getTokenId());
                BigDecimal quantityTotal = quantity.add(otcOrder.getQuantity());
                log.info("Buy order quantity day max limit accountId {} quantityTotal {}  ", otcOrder.getAccountId(), quantityTotal);
                if (quantityTotal.add(otcOrder.getQuantity()).compareTo(OTC_BUY_ORDER_QUANTITY_LIMIT) > 0) {
                    throw new UserBuyTradeLimitException("Buy order quantity day max limit, accountId=" + otcOrder.getAccountId());
                }
            }
        }

        String cancelNum = stringRedisTemplate.opsForValue().get(String.format(OTC_ORDER_CANCEL_NUM_KEY, otcOrder.getAccountId()));
        if (StringUtils.isNotBlank(cancelNum) && Integer.parseInt(cancelNum) >= 3) {
            throw new CancelOrderMaxTimesException("cancel order max times, accountId=" + otcOrder.getAccountId());
        }
        // 检查是否设置过昵称
        OtcUserInfo taker = otcUserInfoService.getOtcUserInfoForUpdate(otcOrder.getAccountId());
        if (taker == null || StringUtils.isBlank(taker.getNickName())) {
            throw new NicknameNotSetException("create order should set nickname first");
        }

        if (otcOrder.getSide().equals(Side.SELL.getCode())) {
            try {
                RcBalanceResponse rcBalanceResponse
                        = grpcExchangeRCService.getUserRcBalance(UserRequest.newBuilder().setUserId(taker.getUserId()).build());
                log.info("rcBalanceResponse userId {} locked {}", taker.getUserId(), rcBalanceResponse.getLocked());
                if (StringUtils.isEmpty(rcBalanceResponse.getLocked())) {
                    throw new RiskControlInterceptionException("RC LIMIT, userId=" + taker.getUserId() + ", locked=" + rcBalanceResponse.getLocked());
                }
                if (new BigDecimal(rcBalanceResponse.getLocked()).compareTo(BigDecimal.ZERO) < 0) {
                    throw new RiskControlInterceptionException("RC LIMIT, userId=" + taker.getUserId() + ", locked=" + rcBalanceResponse.getLocked());
                }
            } catch (RiskControlInterceptionException ex) {
                throw ex;
            } catch (Exception ex) {
                log.error("check UserRcBalance fail orgId {} userId {} error {}", otcOrder.getOrgId(), taker.getUserId(), ex);
            }
        }

        OtcItem otcItem = otcItemMapper.selectOtcItemForUpdate(otcOrder.getItemId());
        // 广告不存在，或者状态为不能被交易
        if (otcItem == null) {
            throw new ItemNotExistException("item not exist, itemId=" + otcOrder.getItemId());
        }

        if (!otcItem.getStatus().equals(ItemStatus.NORMAL.getStatus())) {
            throw new ItemNotExistException("item status error, itemId=" + otcOrder.getItemId() +
                    ", status=" + otcItem.getStatus());
        }

        // 参数有误: 下单和广告token不一致, 下单方向和广告方向一致
        if (!otcOrder.getTokenId().equals(otcItem.getTokenId()) || otcOrder.getSide().equals(otcItem.getSide())) {
            throw new ParamErrorException("order param error");
        }
        //不能自己和自己成交
        if (otcOrder.getAccountId().equals(otcItem.getAccountId())) {
            throw new ExchangeSelfException("can not exchange with self");
        }
        // 广告剩余可交易量不足
        if (otcItem.getLastQuantity().compareTo(otcOrder.getQuantity()) < 0) {
            throw new QuantityNotEnoughException("item last quantity not enough, itemId=" + otcOrder.getItemId() +
                    "last_quantity=" + otcItem.getLastQuantity() + ", order_quantity=" + otcOrder.getQuantity());
        }

        //如果是商家身份交易 限制不同券商的商家禁止交易
        if (otcOrder.getIsBusiness()) {
            if (!otcOrder.getOrgId().equals(otcItem.getOrgId())) {
                throw new DifferentTradeException("Prohibited transactions between different types of businesses");
            }
        }

        OTC_ORDER_METRICS.labels("preOrderCheck").observe(System.currentTimeMillis() - preCheckTime);

        long feeTime = System.currentTimeMillis();
        //检查交易双方是否有相同支付方式
        //用户卖，商家买校验是否有相同支付方式
        if (otcOrder.getSide().equals(Side.SELL.getCode())) {
            List<OtcPaymentTerm> takerPaymentList = otcPaymentTermService.getVisibleOtcPaymentTerm(otcOrder.getAccountId(), otcOrder.getOrgId());
            List<OtcPaymentTerm> newTakerPaymentList = new ArrayList<>();
            //如果是商家卖单则必须要求绑定绑定本人的支付方式
            if (otcOrder.getIsBusiness()) {
                String realName = "";
                String realName_ = "";
                if (StringUtils.isNoneBlank(userFirstName)) {
                    realName = userFirstName;
                }
                if (StringUtils.isNoneBlank(userSecondName)) {
                    realName_ = (realName + "_" + userSecondName).trim();
                    realName = (realName + userSecondName).trim();
                }
                for (int i = 0; i < takerPaymentList.size(); i++) {
                    OtcPaymentTerm otcPaymentTerm = takerPaymentList.get(i);
                    if (StringUtils.deleteWhitespace(otcPaymentTerm.getRealName()).equalsIgnoreCase(StringUtils.deleteWhitespace(realName))
                            || StringUtils.deleteWhitespace(otcPaymentTerm.getRealName()).equalsIgnoreCase(StringUtils.deleteWhitespace(realName_))) {
                        newTakerPaymentList.add(otcPaymentTerm);
                    }
                }

                if (org.apache.commons.collections4.CollectionUtils.isEmpty(newTakerPaymentList)) {
                    throw new NonPersonPaymentException("taker accountId=" + otcOrder.getAccountId() + ",maker accountId=" + otcItem.getAccountId());
                }
            } else {
                newTakerPaymentList = takerPaymentList;
            }

            List<OtcPaymentTerm> makerPaymentList = otcPaymentTermService.getVisibleOtcPaymentTerm(otcItem.getAccountId(), otcItem.getOrgId());
            Set<Integer> takerPaymentTypes = newTakerPaymentList.stream().map(i -> i.getPaymentType()).collect(Collectors.toSet());
            Set<Integer> makerPaymentTypes = makerPaymentList.stream().map(i -> i.getPaymentType()).collect(Collectors.toSet());
            Set<Integer> samePaymentTypes = Sets.intersection(takerPaymentTypes, makerPaymentTypes);
            if (org.apache.commons.collections4.CollectionUtils.isEmpty(samePaymentTypes)) {
                throw new PaymentDoesNotMatchException("taker accountId=" + otcOrder.getAccountId() + ",maker accountId=" + otcItem.getAccountId());
            }
        }

        //计算手续费并预扣 订单撤销会返还
        BigDecimal makerFee = BigDecimal.ZERO;
        if (otcOrder.getSide().equals(Side.BUY.getCode()) && otcItem.getFrozenFee().compareTo(BigDecimal.ZERO) > 0) {
            makerFee = getMakerFeeByRate(otcItem.getOrgId(), otcOrder.getTokenId(), otcOrder.getQuantity(), otcOrder.getSide());
        }
        OTC_ORDER_METRICS.labels("orderFeeCheck").observe(System.currentTimeMillis() - feeTime);

        long shareTime = System.currentTimeMillis();
        //exchangId不相等，表示共享订单
        boolean isSharedItem = !otcOrder.getExchangeId().equals(otcItem.getExchangeId());
        boolean isShared = false;
        if (isSharedItem) {
            isShared = otcConfigService.isShared(otcOrder.getOrgId(), otcOrder.getExchangeId(),
                    otcOrder.getTokenId(), otcItem.getCurrencyId());
            if (!isShared) {
                //提示不能跨交易所交易
                throw new CrossExchangeException("Cross exchange trade be not allowed,itemId=" + otcOrder.getItemId() +
                        "last_quantity=" + otcItem.getLastQuantity() + ", order_quantity=" + otcOrder.getQuantity());
            }
        }

        Date now = new Date();
        OtcItem updater = OtcItem.builder()
                .id(otcItem.getId())
                .lastQuantity(otcItem.getLastQuantity().subtract(otcOrder.getQuantity()))
                .frozenQuantity(otcItem.getFrozenQuantity().add(otcOrder.getQuantity()))
                .orderNum(otcItem.getOrderNum() + 1)
                .fee(otcItem.getFee().add(makerFee))
                .updateDate(now)
                .build();
        int res = otcItemMapper.updateByPrimaryKeySelective(updater);
        if (res != 1) {
            throw new BusinessException("update OtcItem failed, itemId=" + otcItem.getId());
        }
        // 补充order信息
        otcOrder.setId(sequenceGenerator.getLong());
        // 固定价格的，以广告单价格为准
        if (otcItem.getPriceType().equals(PriceType.PRICE_FIXED.getType())) {
            otcOrder.setPrice(otcItem.getPrice());
        }

        otcOrder.setUserId(taker.getUserId());
        otcOrder.setTargetAccountId(otcItem.getAccountId());
        otcOrder.setCurrencyId(otcItem.getCurrencyId());
        otcOrder.setCreateDate(now);
        otcOrder.setUpdateDate(now);
        otcOrder.setFreed(0);

        //判断是否是共享券商
        OtcDepthShareBrokerWhiteList otcDepthShareBrokerWhiteList = this.otcDepthShareBrokerWhiteListMapper.queryByOrgId(otcOrder.getOrgId());
        //增加开关 如果关闭 则不记录风险资产
        if (otcDepthShareBrokerWhiteList != null && RISK_TOKEN_LIST.contains(otcOrder.getTokenId())) {
            String openKey = stringRedisTemplate.opsForValue().get(OTC_RISK_OPEN_KEY);
            if (StringUtils.isNotEmpty(openKey)) {
                if (openKey.equals("1")) {
                    if (otcOrder.getSide().equals(Side.BUY.getCode())) {
                        if (otcOrder.getRiskBalanceType() == OTCNewOrderRequest.RiskBalanceType.NOT_RISK_BALANCE_VALUE) {
                            //调用方认为是非风险资产
                            otcOrder.setFreed(0);
                        } else if (otcOrder.getRiskBalanceType() == OTCNewOrderRequest.RiskBalanceType.RISK_BALANCE_VALUE) {
                            //调用方认为是风险资产
                            otcOrder.setFreed(1);
                        } else {
                            otcOrder.setFreed(!otcOrder.getIsBusiness() ? 1 : 0); //如果是非OTC商家买单 则标记风险状态
                        }
                    }
                    //otcOrder.setFreed((!otcOrder.getIsBusiness() && otcOrder.getSide().equals(Side.BUY.getCode())) ? 1 : 0); //如果是非OTC商家买单 则标记风险状态
                }
            }
        }
        //不是共享券商，判断是否开启了单独的风险资产控制配置
        if (otcDepthShareBrokerWhiteList == null && RISK_TOKEN_LIST.contains(otcOrder.getTokenId())) {
            OtcBrokerRiskBalanceConfig otcBrokerRiskBalanceConfig = otcBrokerRiskBalanceConfigService.getOtcBrokerRiskBalanceConfig(otcOrder.getOrgId());
            if (otcBrokerRiskBalanceConfig != null && otcBrokerRiskBalanceConfig.getStatus() == 1) {
                otcOrder.setFreed((!otcOrder.getIsBusiness() && otcOrder.getSide().equals(Side.BUY.getCode())) ? 3 : 0); //如果是非OTC商家买单 则标记风险状态
            }
        }

        if (otcDepthShareBrokerWhiteList != null && !otcItem.getOrgId().equals(otcOrder.getOrgId()) && otcOrder.getSide().equals(Side.SELL.getCode())) {
            Example example = new Example(OtcShareLimit.class);
            Example.Criteria criteria = example.createCriteria();
            criteria.andEqualTo("orgId", otcItem.getOrgId());
            criteria.andEqualTo("assureOrgId", otcOrder.getOrgId());
            criteria.andEqualTo("tokenId", otcOrder.getTokenId());
            OtcShareLimit otcShareLimit = this.otcShareLimitMapper.selectOneByExample(example);
            if (otcShareLimit != null) {
                //账户保证金
                BigDecimal safeAmount = BigDecimal.ZERO;
                if (otcShareLimit.getAccountId() != null && otcShareLimit.getAccountId() > 0) {
                    GetPositionRequest request = GetPositionRequest.newBuilder()
                            .setBrokerId(otcShareLimit.getOrgId())
                            .setAccountId(otcShareLimit.getAccountId())
                            .setTokenId(otcOrder.getTokenId())
                            .build();
                    PositionResponseList positionResponseList = grpcServerService.getBalancePosition(request);
                    BigDecimal locked = BigDecimal.ZERO;
                    if (positionResponseList.getPositionListCount() > 0) {
                        locked = new BigDecimal(positionResponseList.getPositionList(0).getTotal());
                    }
                    safeAmount = locked;
                }
                //安全额度
                if (otcShareLimit.getSafeAmount().compareTo(BigDecimal.ZERO) > 0) {
                    safeAmount = safeAmount.add(otcShareLimit.getSafeAmount());
                }
                //用户买入汇总 用户共享广告的买单的token数量
                BigDecimal buyAmount = this.otcOrderMapper.sumShardBuyOrderQuantity(otcOrder.getOrgId(), otcOrder.getTokenId());
                //用户卖出汇总 用户共享广告的卖单的token数量
                BigDecimal sellAmount = this.otcOrderMapper.sumShardSellOrderQuantity(otcOrder.getOrgId(), otcOrder.getTokenId());
                BigDecimal safeTotalAmount = safeAmount.add(buyAmount).setScale(8, RoundingMode.DOWN);
                BigDecimal sellTotalAmount = sellAmount.add(otcOrder.getQuantity()).setScale(8, RoundingMode.DOWN);
                log.info("Safety limit info orgId {} safeAmount {} buyAmount {} sellTotalAmount {} lastAmount {} ",
                        otcOrder.getOrgId(), safeAmount, buyAmount, sellTotalAmount, (safeTotalAmount.subtract(sellTotalAmount)).setScale(2, RoundingMode.DOWN));
                BigDecimal eightyPercentAmount = safeTotalAmount.multiply(otcShareLimit.getWarnPercent()).setScale(8, RoundingMode.DOWN);
                if (sellTotalAmount.compareTo(eightyPercentAmount) > 0) {
                    log.error("Safety limit eighty percent error orgId {} safeAmount {} buyAmount {} eightyPercentAmount {} sellTotalAmount {}",
                            otcOrder.getOrgId(), safeAmount, buyAmount, eightyPercentAmount, sellTotalAmount);
                }
                if (sellTotalAmount.compareTo(safeTotalAmount) > 0) {
                    log.warn("Safety limit warn orgId {} safeAmount {} buyAmount {} sellTotalAmount {} lastAmount {} ",
                            otcOrder.getOrgId(), safeAmount, buyAmount, sellTotalAmount, (safeTotalAmount.subtract(sellTotalAmount)).setScale(2, RoundingMode.DOWN));
                    throw new BalanceNotEnoughExcption("Balance Not Enough failed");
                }
            }
        }

        OTC_ORDER_METRICS.labels("orderShareCheck").observe(System.currentTimeMillis() - shareTime);
        long insertOrderTime = System.currentTimeMillis();
        otcOrder.setMakerFee(makerFee);
        otcOrder.setDepthShareBool(isShared);
        otcOrder.setTokenName(otcOrder.getTokenId());
        otcOrder.setMatchOrgId(otcItem.getOrgId() != null ? otcItem.getOrgId() : 0L);//广告商券商ID
        otcOrderMapper.insert(otcOrder);

        if (otcOrder.getId() == null || otcOrder.getId() == 0) {
            throw new BusinessException("insert OtcOrder failed");
        }

        //共享深度订单，记录maker信息
        if (otcOrder.getDepthShareBool()) {
            boolean success = saveOrderDepthShare(otcOrder, otcItem);
            if (!success) {
                throw new BusinessException("insert OtcOrder depth share failed");
            }
        }

        //异步同步拓展信息
        otcOrderCreateTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                //保存订单扩展信息
                saveOrderExt(otcOrder.getId(), otcOrder.getLanguage(), otcOrder.getOrgId(),
                        otcItem.getTokenId(), otcItem.getCurrencyId(), otcItem.getOrgId(), otcOrder.getIsBusiness());
                // 生成自动回复消息
                if (StringUtils.isNotBlank(otcItem.getAutoReply())) {
                    OtcOrderMessage otcOrderMessage = OtcOrderMessage.builder()
                            .accountId(otcItem.getAccountId())
                            .orderId(otcOrder.getId())
                            .msgType(MsgType.WORD_MSG.getType())
                            .msgCode(0)
                            .message(otcItem.getAutoReply())
                            .createDate(new Date())
                            .build();
                    otcOrderMessageService.addOtcOrderMessage(otcOrderMessage);
                }
                if (taker.getStatus().equals(0)) {
                    otcUserInfoService.setTradeFlag(taker.getAccountId());
                }
            }
        });

        OTC_ORDER_METRICS.labels("insertOrderCheck").observe(System.currentTimeMillis() - insertOrderTime);

        OTC_ORDER_SERVICE_METRICS.labels("createOtcOrder").observe(System.currentTimeMillis() - preCheckTime);
        return otcOrder.getId();
    }

    private BigDecimal sumQuantityFromBuyOrderInDayByAccountId(Long accountId, String tokenId) {
        Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();
        Date todayStart = Date.from(LocalDateTime.of(LocalDate.now(), LocalTime.MIN).atZone(zoneId).toInstant());
        Date todayEnd = Date.from(LocalDateTime.of(LocalDate.now(), LocalTime.MAX).atZone(zoneId).toInstant());
        log.info("sumQuantityFromBuyOrderInDayByAccountId accountId {} todayStart {} todayEnd {} ", accountId, formatter.format(todayStart), formatter.format(todayEnd));
        return otcOrderMapper.sumQuantityFromBuyOrderInDayByAccountId(accountId, tokenId, todayStart, todayEnd);
    }

    public static void main(String[] args) {
        String userFirstName = "Trieu";
        String userSecondName = "HamTan";
        String realName = "";
        String realName_ = "";
        List<OtcPaymentTerm> newTakerPaymentList = new ArrayList<>();
        if (StringUtils.isNoneBlank(userFirstName)) {
            realName = userFirstName;
        }
        if (StringUtils.isNoneBlank(userSecondName)) {
            realName_ = (realName + "_" + userSecondName).trim();
            realName = (realName + userSecondName).trim();
        }
        OtcPaymentTerm otcPaymentTerm = new OtcPaymentTerm();
        otcPaymentTerm.setRealName("Trieu HamTan");
    }

    private void saveOrderExt(Long orderId, String language, Long orderOrgId,
                              String tokenId, String currencyId, Long itemOrgId, Boolean isBusiness) {
        long startTime = System.currentTimeMillis();
        if (StringUtils.isBlank(language)) {
            language = "en_US";
        }

        String tmpLang = language;

        OtcBrokerToken token = otcConfigService.getBrokerToken(itemOrgId, tokenId);
        if (Objects.isNull(token)) {

            log.error("Save order ext fail, token is null,token={},orgId={}", tokenId, itemOrgId);
            throw new BusinessException("Save order ext fail, token is null");
        }

        List<OtcBrokerCurrency> currencies = otcConfigService.getOtcCurrencyList(itemOrgId, currencyId, language);

        if (org.apache.commons.collections4.CollectionUtils.isEmpty(currencies)) {
            log.error("Save order ext fail, currency is null,lang={},orgId={}", language, itemOrgId);
            throw new BusinessException("Save order ext fail, currency is null");
        }

        OtcBrokerCurrency currency = currencies.stream()
                .filter(i -> i.getLanguage().equalsIgnoreCase(tmpLang))
                .findFirst().get();

        OtcOrderExt ext = OtcOrderExt.builder()
                .orderId(orderId)
                .orgId(orderOrgId)
                .tokenScale(token.getScale())
                .currencyScale(currency.getScale())
                .currencyAmountScale(currency.getAmountScale())
                .createDate(new Date())
                .updateDate(new Date())
                .isBusiness(isBusiness ? 1 : 0)
                .build();

        int rows = otcOrderExtMapper.insertSelective(ext);
        if (rows == 0) {
            log.warn("Save order ext fail,rows=0");
        }
        OTC_ORDER_SERVICE_METRICS.labels("saveOrderExt").observe(System.currentTimeMillis() - startTime);
    }

    private boolean saveOrderDepthShare(OtcOrder order, OtcItem otcItem) {
        long startTime = System.currentTimeMillis();
        Object obj = otcOrderDepthShareMapper.selectByPrimaryKey(order.getId());
        if (Objects.nonNull(obj)) {
            return true;
        }

        OtcOrderDepthShare depthShare = OtcOrderDepthShare.builder()
                .orderId(order.getId())
                .makerExchangeId(otcItem.getExchangeId())
                .makerOrgId(otcItem.getOrgId())
                .makerAccountId(otcItem.getAccountId())
                .takerOrgId(order.getOrgId())
                .itemId(otcItem.getId())
                .createDate(order.getCreateDate())
                .updateDate(order.getCreateDate())
                .status(order.getStatus())
                .build();

        int row = otcOrderDepthShareMapper.insertSelective(depthShare);
        OTC_ORDER_SERVICE_METRICS.labels("saveOrderDepthShare").observe(System.currentTimeMillis() - startTime);
        return row == 1;
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int updateOrderStatusToNormal(long orderId) {
        long startTime = System.currentTimeMillis();
        OtcOrder otcOrder = getOtcOrderById(orderId);
        // 订单不存在或者状态不对
        if (otcOrder == null || !otcOrder.getStatus().equals(OrderStatus.INIT.getStatus())) {
            throw new OrderNotExistException("order not exist, orderId=" + orderId);
        }
        updateOrderShareStatus(orderId, OrderStatus.NORMAL);

        // 更新订单状态
        int res = otcOrderMapper.updateOtcOrderStatus(orderId, OrderStatus.NORMAL.getStatus(),
                OrderStatus.INIT.getStatus(), new Date());
        if (res != 1) {
            throw new OrderNotExistException("update order status failed, orderId=" + orderId);
        }

        otcOrderHandleTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                // 生成系统消息
                if (otcOrder.getSide().equals(Side.BUY.getCode())) {
                    createSysMessage(otcOrder, MsgCode.BUY_CREATE_MSG_TO_BUYER_TIME, MsgCode.BUY_CREATE_MSG_TO_SELLER);
                } else {
                    createSysMessage(otcOrder, MsgCode.SELL_CREATE_MSG_TO_BUYER_TIME, MsgCode.SELL_CREATE_MSG_TO_SELLER);
                }
                // 增加用户单量计数
                otcUserInfoService.increaseOrderNum(otcOrder.getAccountId(), 1);
                otcUserInfoService.increaseOrderNum(otcOrder.getTargetAccountId(), 1);
            }
        });
        OTC_ORDER_SERVICE_METRICS.labels("updateOrderStatusToNormal").observe(System.currentTimeMillis() - startTime);
        return res;
    }

    private void updateOrderShareStatus(Long orderId, OrderStatus status) {
        OtcOrderDepthShare oods = new OtcOrderDepthShare();
        oods.setOrderId(orderId);
        oods.setStatus(status.getStatus());
        oods.setUpdateDate(new Date());
        otcOrderDepthShareMapper.updateByPrimaryKeySelective(oods);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public OtcBalanceFlow updateOrderStatusToDelete(long orderId) {
        long startTime = System.currentTimeMillis();
        OtcOrder otcOrder = getOtcOrderById(orderId);
        // 订单不存在或者状态不对
        if (otcOrder == null || !otcOrder.getStatus().equals(OrderStatus.INIT.getStatus())) {
            throw new OrderNotExistException("order not exist, orderId=" + orderId);
        }
        // 返还冻结
        //OtcBalanceFlow otcBalanceFlow = backFrozen(otcOrder);
        int res = otcOrderMapper.updateOtcOrderStatus(orderId, OrderStatus.DELETE.getStatus(),
                OrderStatus.INIT.getStatus(), new Date());
        if (res != 1) {
            throw new BusinessException("delete OtcOrder failed, orderId=" + otcOrder.getId());
        }

        updateOrderShareStatus(orderId, OrderStatus.DELETE);
        OTC_ORDER_SERVICE_METRICS.labels("updateOrderStatusToDelete").observe(System.currentTimeMillis() - startTime);
        //return otcBalanceFlow;
        return null;
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public OtcBalanceFlow cancelOrder(OtcOrder otcOrder) {
        return cancelOrder(otcOrder, false);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public OtcBalanceFlow cancelOrder(OtcOrder otcOrder, boolean auto) {
        long startTime = System.currentTimeMillis();
        // 撤单，卖方下单数需要减一
        otcUserInfoService.increaseOrderNum(otcOrder.getSide().equals(Side.BUY.getCode()) ?
                otcOrder.getTargetAccountId() : otcOrder.getAccountId(), -1);
        // 返还冻结
        OtcBalanceFlow otcBalanceFlow = backFrozen(otcOrder);
        int res = otcOrderMapper.updateOtcOrderStatus(otcOrder.getId(), OrderStatus.CANCEL.getStatus(),
                OrderStatus.NORMAL.getStatus(), new Date());
        if (res != 1) {
            throw new BusinessException("cancel OtcOrder failed, orderId=" + otcOrder.getId());
        }
        updateOrderShareStatus(otcOrder.getId(), OrderStatus.CANCEL);

        otcOrderCreateTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                // 生成系统消息
                if (auto) {
                    // 超时自动撤单时，给双方发的消息一致
                    createSysMessage(otcOrder, MsgCode.ORDER_AUTO_CANCEL, MsgCode.ORDER_AUTO_CANCEL);
                } else {
                    // 主动撤单，分别发消息
                    createSysMessage(otcOrder, MsgCode.CANCEL_MSG_TO_BUYER, MsgCode.CANCEL_MSG_TO_SELLER);
                }
                publishClearOrderListCacheEvent(otcOrder.getAccountId(), otcOrder.getTargetAccountId());
            }
        });

        OTC_ORDER_SERVICE_METRICS.labels("cancelOrder").observe(System.currentTimeMillis() - startTime);
        return otcBalanceFlow;
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public OtcBalanceFlow adminCancelOrder(OtcOrder otcOrder) {
        long startTime = System.currentTimeMillis();
        // 生成系统消息
        createSysMessage(otcOrder, MsgCode.CANCEL_MSG_TO_BUYER, MsgCode.CANCEL_MSG_TO_SELLER);
        // 撤单，卖方下单数需要减一
        otcUserInfoService.increaseOrderNum(otcOrder.getSide().equals(Side.BUY.getCode()) ?
                otcOrder.getTargetAccountId() : otcOrder.getAccountId(), -1);
        // 返还冻结
        OtcBalanceFlow otcBalanceFlow = backFrozen(otcOrder);
        int res = otcOrderMapper.updateOtcOrderStatus(otcOrder.getId(), OrderStatus.CANCEL.getStatus(),
                OrderStatus.APPEAL.getStatus(), new Date());
        if (res != 1) {
            throw new BusinessException("cancel OtcOrder failed, orderId=" + otcOrder.getId());
        }

        //记录管理员取消订单
        OtcOrderAdminCanceled ooac = new OtcOrderAdminCanceled();
        ooac.setAccountId(otcOrder.getAccountId());
        ooac.setTargetAccountId(otcOrder.getTargetAccountId());
        ooac.setOrderId(otcOrder.getId());
        ooac.setSide(otcOrder.getSide());
        ooac.setCreateDate(new Date());

        otcOrderAdminCanceledMapper.insertSelective(ooac);

        updateOrderShareStatus(otcOrder.getId(), OrderStatus.CANCEL);
        publishClearOrderListCacheEvent(otcOrder.getAccountId(), otcOrder.getTargetAccountId());
        OTC_ORDER_SERVICE_METRICS.labels("adminCancelOrder").observe(System.currentTimeMillis() - startTime);
        return otcBalanceFlow;
    }

    private void publishClearOrderListCacheEvent(Long accountId, Long targetAccountId) {
        ClearOrderListCacheEvent event = new ClearOrderListCacheEvent();
        event.setAccountId(accountId);
        event.setTargetAccountId(targetAccountId);
        eventPublisher.publishEvent(event);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int payOrder(OtcOrder otcOrder, Integer paymentType) {
        long startTime = System.currentTimeMillis();
        // 查询卖方支持的付款方式
        List<OtcPaymentTerm> paymentTermList = otcPaymentTermService.getOtcPaymentTerm(
                otcOrder.getSide().equals(Side.BUY.getCode()) ? otcOrder.getTargetAccountId() : otcOrder.getAccountId());
        if (CollectionUtils.isEmpty(paymentTermList)) {
            return 0;
        }
        boolean support = false;
        for (OtcPaymentTerm paymentTerm : paymentTermList) {
            if (paymentTerm.getPaymentType().equals(paymentType) && paymentTerm.getVisible().equals(0)) {
                support = true;
                //保存订单的支付方式
                otcPaymentTermService.bindingPaymentToOrderPayInfo(otcOrder.getId(), paymentTerm.getId());
                break;
            }
        }
        if (!support) {
            return 0;
        }
        Date now = new Date();
        OtcOrder updater = OtcOrder.builder()
                .transferDate(now)
                .paymentType(paymentType)
                .status(OrderStatus.UNCONFIRM.getStatus())
                .updateDate(now)
                .build();
        Example example = Example.builder(OtcOrder.class).build();
        example.createCriteria()
                .andEqualTo("id", otcOrder.getId())
                .andEqualTo("status", OrderStatus.NORMAL.getStatus());
        int res = otcOrderMapper.updateByExampleSelective(updater, example);
        if (res != 1) {
            throw new BusinessException("pay OtcOrder failed, orderId=" + otcOrder.getId());
        }

        updateOrderShareStatus(otcOrder.getId(), OrderStatus.UNCONFIRM);
        otcOrderCreateTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                // 生成系统消息
                createSysMessage(otcOrder, MsgCode.PAY_MSG_TO_BUYER, MsgCode.PAY_MSG_TO_SELLER);
            }
        });
        OTC_ORDER_SERVICE_METRICS.labels("payOrder").observe(System.currentTimeMillis() - startTime);
        return res;
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public int payOrderPayById(OtcOrder otcOrder, Long id) {
        long startTime = System.currentTimeMillis();
        //订单幂等检查
        if (otcOrder.getStatus().equals(OrderStatus.UNCONFIRM.getStatus())) {
            return 1;
        }

        //合法状态检查
        if (!otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus())) {
            return 0;
        }

        OtcPaymentTerm otcPaymentTerm = otcPaymentTermMapper.selectByPrimaryKey(id);
        if (otcPaymentTerm == null) {
            return 0;
        }

        //支付记录幂等检查
        OtcOrderPayInfo info = otcPaymentTermService.getOrderPayInfoByOrderId(otcOrder.getId());
        if (Objects.isNull(info)) {
            //保存订单的支付方式
            otcPaymentTermService.bindingPaymentToOrderPayInfo(otcOrder.getId(), otcPaymentTerm.getId());
        }
        Date now = new Date();
        OtcOrder updater = OtcOrder.builder()
                .transferDate(now)
                .paymentType(otcPaymentTerm.getPaymentType())
                .status(OrderStatus.UNCONFIRM.getStatus())
                .updateDate(now)
                .build();
        Example example = Example.builder(OtcOrder.class).build();
        example.createCriteria()
                .andEqualTo("id", otcOrder.getId())
                .andEqualTo("status", OrderStatus.NORMAL.getStatus());
        int res = otcOrderMapper.updateByExampleSelective(updater, example);
        if (res != 1) {
            throw new BusinessException("pay OtcOrder failed, orderId=" + otcOrder.getId());
        }

        updateOrderShareStatus(otcOrder.getId(), OrderStatus.UNCONFIRM);
        otcOrderHandleTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                // 生成系统消息
                createSysMessage(otcOrder, MsgCode.PAY_MSG_TO_BUYER, MsgCode.PAY_MSG_TO_SELLER);
            }
        });
        OTC_ORDER_SERVICE_METRICS.labels("payOrderPayById").observe(System.currentTimeMillis() - startTime);
        return res;
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public void appealOrder(OtcOrder otcOrder, long accountId) {
        appealOrder(otcOrder, accountId, false);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public void appealOrder(OtcOrder otcOrder, long accountId, boolean auto) {
        long startTime = System.currentTimeMillis();
        int res = otcOrderMapper.appealOrder(otcOrder.getId(), OrderStatus.APPEAL.getStatus(),
                otcOrder.getAppealType(), accountId, otcOrder.getAppealContent(),
                OrderStatus.UNCONFIRM.getStatus(), new Date());
        if (res != 1) {
            throw new BusinessException("appeal OtcOrder failed, orderId=" + otcOrder.getId());
        }
        updateOrderShareStatus(otcOrder.getId(), OrderStatus.APPEAL);
        otcOrderCreateTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (auto) {
                    // 超时自动撤单时，给双方发的消息一致
                    createSysMessage(otcOrder, MsgCode.ORDER_AUTO_APPEAL_TO_BUYER, MsgCode.ORDER_AUTO_APPEAL_TO_SELLER);
                } else {
                    // 判断是买方投诉还是卖方投诉，并生成系统消息
                    if (otcOrder.getSide().equals(Side.BUY.getCode()) && otcOrder.getAccountId().equals(accountId)) {
                        createSysMessage(otcOrder, MsgCode.BUY_APPEAL_MSG_TO_BUYER, MsgCode.BUY_APPEAL_MSG_TO_SELLER);
                    } else if (otcOrder.getSide().equals(Side.SELL.getCode()) &&
                            otcOrder.getTargetAccountId().equals(accountId)) {
                        createSysMessage(otcOrder, MsgCode.BUY_APPEAL_MSG_TO_BUYER, MsgCode.BUY_APPEAL_MSG_TO_SELLER);
                    } else {
                        createSysMessage(otcOrder, MsgCode.SELL_APPEAL_MSG_TO_BUYER, MsgCode.SELL_APPEAL_MSG_TO_SELLER);
                    }
                }
            }
        });
        OTC_ORDER_SERVICE_METRICS.labels("appealOrder").observe(System.currentTimeMillis() - startTime);
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class, timeout = 10)
    public List<OtcBalanceFlow> finishOrder(OtcOrder otcOrder) {
        long startTime = System.currentTimeMillis();
        OtcItem otcItem = checkAndLockItem(otcOrder.getItemId(), otcOrder.getQuantity());
        // 检查广告剩余单量
        if (otcItem.getFrozenQuantity().compareTo(otcOrder.getQuantity()) < 0) {
            throw new BusinessException("item last quantity less than order quantity, itemId=" + otcItem.getId());
        }
        Date now = new Date();
        // 扣掉冻结
        OtcItem updater = OtcItem.builder()
                .id(otcItem.getId())
                .frozenQuantity(otcItem.getFrozenQuantity().subtract(otcOrder.getQuantity()))
                .executedQuantity(otcItem.getExecutedQuantity().add(otcOrder.getQuantity()))
                .finishNum(otcItem.getFinishNum() + 1)
                .updateDate(now)
                .build();
        // 广告单已被完全成交，则将广告单状态置为已完结
        if (otcItem.getStatus().equals(ItemStatus.NORMAL.getStatus()) && updater.getExecutedQuantity().equals(otcItem.getQuantity())) {
            updater.setStatus(ItemStatus.FINISH.getStatus());
        }
        int res = otcItemMapper.updateByPrimaryKeySelective(updater);
        if (res != 1) {
            throw new BusinessException("update OtcItem failed, itemId=" + otcItem.getId());
        }
        // 生成一对动账流水
        OtcBalanceFlow otcBalanceFlowTaker = OtcBalanceFlow.builder()
                .accountId(otcOrder.getAccountId())
                .orgId(otcOrder.getOrgId())
                .userId(otcOrder.getUserId())
                .tokenId(otcOrder.getTokenId())
                .amount(otcOrder.getQuantity())
                .flowType(otcOrder.getSide().equals(Side.BUY.getCode()) ? FlowType.ADD_AVAILABLE.getType()
                        : FlowType.SUBTRACT_FROZEN.getType())
                .objectId(otcOrder.getId())
                .status(FlowStatus.WAITING_PROCESS.getStatus())
                .createDate(now)
                .updateDate(now)
                .build();
        //maker 卖 ->从item冻结里面减去手续费 买 -> 从到账金额扣手续费
        BigDecimal makerFee = BigDecimal.ZERO;
        if (otcOrder.getSide().equals(Side.BUY.getCode()) && otcItem.getFrozenFee().compareTo(BigDecimal.ZERO) > 0) {
            makerFee = getMakerFeeByRate(otcItem.getOrgId(), otcOrder.getTokenId(), otcOrder.getQuantity(), otcOrder.getSide());
        } else if (!otcOrder.getSide().equals(Side.BUY.getCode())) {
            makerFee = getMakerFeeByRate(otcItem.getOrgId(), otcOrder.getTokenId(), otcOrder.getQuantity(), otcOrder.getSide());
        }

        OtcBalanceFlow otcBalanceFlowMaker = OtcBalanceFlow.builder()
                .accountId(otcItem.getAccountId())
                .orgId(otcItem.getOrgId())
                .userId(otcItem.getUserId())
                .tokenId(otcItem.getTokenId())
                .amount(otcOrder.getQuantity())
                .flowType(otcOrder.getSide().equals(Side.BUY.getCode()) ? FlowType.SUBTRACT_FROZEN.getType()
                        : FlowType.ADD_AVAILABLE.getType())
                .objectId(otcOrder.getId())
                .status(FlowStatus.WAITING_PROCESS.getStatus())
                .createDate(now)
                .updateDate(now)
                .fee(makerFee)
                .build();
        otcBalanceFlowMapper.insert(otcBalanceFlowTaker);
        otcBalanceFlowMapper.insert(otcBalanceFlowMaker);
        if (otcBalanceFlowTaker.getId() == null || otcBalanceFlowTaker.getId() == 0 || otcBalanceFlowMaker.getId() == null || otcBalanceFlowMaker.getId() == 0) {
            throw new BusinessException("insert OtcBalanceFlow failed");
        }
        res = otcOrderMapper.updateOtcOrderStatusAndFee(otcOrder.getId(), OrderStatus.FINISH.getStatus(),
                otcOrder.getStatus(), new Date(), makerFee, BigDecimal.ZERO);

        //如果order里面的marker fee > 0 则不用再更新item fee了 如果等于0则需要加进去
        if (otcOrder.getMakerFee().compareTo(BigDecimal.ZERO) > 0) {
            makerFee = BigDecimal.ZERO;
        }
        if (res != 1 || otcItemMapper.updateOtcItemFee(otcItem.getId(), makerFee, new Date()) != 1) {
            throw new BusinessException("finish OtcOrder failed, orderId=" + otcOrder.getId());
        }


        //异步执行 统计 消息等操作
        otcOrderHandleTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                //修改共享订单的成交状态
                updateOrderShareStatus(otcOrder.getId(), OrderStatus.FINISH);
                //成交消息
                createSysMessage(otcOrder, MsgCode.FINISH_MSG_TO_BUYER, MsgCode.FINISH_MSG_TO_SELLER);
                publishClearOrderListCacheEvent(otcOrder.getAccountId(), otcOrder.getTargetAccountId());
                // 增加用户成交单量计数
                otcUserInfoService.increaseExecuteNum(otcOrder.getAccountId());
                otcUserInfoService.increaseExecuteNum(otcOrder.getTargetAccountId());
                //进行统计更新
                MerchantStatisticsEvent event = new MerchantStatisticsEvent();
                event.setAccountId(otcOrder.getTargetAccountId());
                eventPublisher.publishEvent(event);
                //推送OTC订单成交消息
                otcOrder.setTransferDateTime(otcOrder.getTransferDate().getTime());
                otcOrder.setCreateDateTime(otcOrder.getCreateDate().getTime());
                otcOrder.setUpdateDateTime(otcOrder.getUpdateDate().getTime());
                otcOrder.setStatus(OrderStatus.FINISH.getStatus());
                producerPushMessage(otcOrder);
            }
        });

        OTC_ORDER_SERVICE_METRICS.labels("finishOrder").observe(System.currentTimeMillis() - startTime);
        return Lists.newArrayList(otcBalanceFlowTaker, otcBalanceFlowMaker);
    }

    //OTC成交消息发送
    private void producerPushMessage(OtcOrder otcOrder) {
        try {
            this.producer.writeOtcMessageToMQ(otcOrder.getOrgId(), producer.getOtcOrderTopic(otcOrder.getOrgId()), otcOrder);
        } catch (Exception ex) {
            log.error("Write Otc order Message To MQ Error: topic => {}, message => {}.", producer.getOtcOrderTopic(otcOrder.getOrgId()), new Gson().toJson(otcOrder), ex);
        }
    }

    /**
     * 计算maker手续费
     *
     * @param orgId   orgId
     * @param tokenId tokenId
     * @param amount  amount
     * @param side    side
     * @return bigDecimal
     */
    private BigDecimal getMakerFeeByRate(Long orgId, String tokenId, BigDecimal amount, Integer side) {
        OtcTradeFeeRate otcTradeFeeRate
                = otcTradeFeeService.queryOtcTradeFeeByTokenId(orgId, tokenId);
        if (otcTradeFeeRate == null) {
            return BigDecimal.ZERO;
        }

        BigDecimal fee = BigDecimal.ZERO;
        if (Side.BUY.getCode() == side.intValue()) {
            fee = amount.multiply(otcTradeFeeRate.getMakerSellFeeRate());
        } else if (Side.SELL.getCode() == side.intValue()) {
            fee = amount.multiply(otcTradeFeeRate.getMakerBuyFeeRate());
        }
        if (fee.compareTo(BigDecimal.ZERO) > 0) {
            return fee;
        }
        log.info("getMakerFeeByRate amount {} side {} fee {}", amount, side, fee);
        return BigDecimal.ZERO;
    }

    public Long getOrderIdByClient(long orgId, long clientOrderId) {
        return otcOrderMapper.selectOtcOrderIdByClient(orgId, clientOrderId);
    }

    public OtcOrder getOtcOrderById(long orderId) {
        OtcOrder order = otcOrderMapper.selectByPrimaryKey(orderId);
        if (Objects.isNull(order)) {
            return null;
        }
        if (order.getDepthShareBool()) {
/*            Example exp=new Example(OtcOrderDepthShare.class);
            exp.createCriteria()
                    .andEqualTo("orderId",orderId);*/
            OtcOrderDepthShare oods = otcOrderDepthShareMapper.selectByPrimaryKey(orderId);
            order.setTakerBrokerId(oods.getTakerOrgId());
            order.setMakerBrokerId(oods.getMakerOrgId());
        } else {

            OtcUserInfo makerUserInfo = otcUserInfoService.getOtcUserInfo(order.getTargetAccountId());
            order.setTakerBrokerId(order.getOrgId());
            order.setMakerBrokerId(makerUserInfo.getOrgId());
        }
        return order;
    }

    public OtcOrder getOtcOrderWithExtById(long orderId) {
        OtcOrder order = this.getOtcOrderById(orderId);
        if (Objects.isNull(order)) {
            return null;
        }

        OtcOrderExt ext = findOrderExtById(order.getId());
        if (Objects.nonNull(ext)) {
            order.setOrderExt(ext);
        }

        return order;
    }

    public List<OtcOrder> getOtcOrderList(long accountId, List<Integer> status, Date beginTime, Date endTime,
                                          String tokenId, Integer side, int page, int size) {
        String key = String.format(CLOSED_ORDER_LIST_KEY_PATTERN, accountId);
        String subkey = page + "-" + size;
        String cache = (String) stringRedisTemplate.opsForHash().get(key, subkey);
        if (StringUtils.isNoneBlank(cache)) {
            return JSON.parseArray(cache, OtcOrder.class);
        }
        List<OtcOrder> list = this.listClosedOrder(accountId, side, page, size);
        if (Objects.isNull(list)) {
            list = Lists.newArrayList();
        }

        stringRedisTemplate.opsForHash().put(key, subkey, JSON.toJSONString(list));
        stringRedisTemplate.expire(key, 20, TimeUnit.SECONDS);
        return list;
    }

    @EventListener
    public void clearOrderListCache(ClearOrderListCacheEvent event) {
        List<String> keys = Stream.of(event.getAccountId(), event.getTargetAccountId())
                .map(id -> String.format(CLOSED_ORDER_LIST_KEY_PATTERN, id))
                .collect(Collectors.toList());
        stringRedisTemplate.delete(keys);
    }


    public List<OtcOrder> getOtcOrderListByFromId(long orgId, long accountId, String tokenId, Integer status, Integer side, Timestamp startTime, Timestamp endTime, Long fromId, Long lastId, Integer limit) {
        return this.queryOrderListByFromId(orgId, accountId, tokenId, status, side, startTime, endTime, fromId, lastId, limit);
    }

    public List<OtcOrder> getOtcOrderListOriginal(long accountId, List<Integer> status, Date beginTime, Date endTime,
                                                  String tokenId, Integer side, int page, int size) {
        Example example = Example.builder(OtcOrder.class)
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria = example.createCriteria();
        if (side == null) {
            criteria.andCondition("(account_id = " + accountId + " or target_account_id = " + accountId + ")");
        } else {
            Side reverseSide = side.equals(Side.BUY.getCode()) ? Side.SELL : Side.BUY;
            criteria.andCondition("((account_id = " + accountId + " and side = " + side +
                    ") or (target_account_id = " + accountId + " and side = " + reverseSide.getCode() + "))");
        }

        if (StringUtils.isNotBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }

        criteria.andIn("status", Lists.newArrayList(
                OrderStatus.CANCEL.getStatus(),
                OrderStatus.FINISH.getStatus()
        ));
        if (beginTime != null) {
            criteria.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria.andLessThanOrEqualTo("createDate", endTime);
        }

        PageHelper.startPage(page, size);
        List<OtcOrder> list = otcOrderMapper.selectByExample(example);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }

        appendOrderExt(list);
        return list;
    }

    public List<OtcOrder> listClosedOrder(long accountId, Integer side, int page, int size) {
        long start = System.currentTimeMillis();
        try {
            log.info("listClosedOrder,accountId={},page={},size={},side={}", accountId, page, size, side);
            int offset = page > 1 ? (page - 1) * size : 0;
            List<OtcOrder> list;
            if (side == null) {
                list = otcOrderMapper.listClosedOrder(accountId, offset, size);
            } else {
                list = otcOrderMapper.listClosedOrderBySide(accountId, side, offset, size);
            }
            if (org.apache.commons.collections4.CollectionUtils.isEmpty(list)) {
                return Lists.newArrayList();
            }
            appendOrderExt(list);
            return list;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            log.info("listClosedOrder,consume={} mills,accountId={}", System.currentTimeMillis() - start, accountId);
        }
    }

    public List<OtcOrder> queryOrderListByFromId(long orgId, long accountId, String tokenId, Integer status, Integer side, Timestamp startTime, Timestamp endTime, Long fromId, Long lastId, Integer limit) {
        long start = System.currentTimeMillis();
        try {
            List<OtcOrder> list = otcOrderMapper.queryOrderById(orgId, tokenId, status, side, accountId, startTime, endTime, fromId, lastId, limit);
            if (org.apache.commons.collections4.CollectionUtils.isEmpty(list)) {
                return Lists.newArrayList();
            }

            appendOrderExt(list);
            return list;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            log.info("listClosedOrder,consume={} mills,accountId={}", System.currentTimeMillis() - start, accountId);
        }
    }

    @Deprecated
    public List<OtcOrder> listClosedOrderV2(long accountId, int page, int size) {
        long start = System.currentTimeMillis();
        try {
            log.info("listClosedOrder,accountId={},page={},size={}", accountId, page, size);

            Example itemExp = new Example(OtcItem.class);
            itemExp.createCriteria()
                    .andEqualTo("accountId", accountId);

            int number = otcItemMapper.selectCountByExample(itemExp);
            Long makerAccountId = null;
            Long takerAccountId = null;
            if (number == 0) {
                takerAccountId = accountId;
            } else {
                makerAccountId = accountId;
            }

            int offset = page > 1 ? (page - 1) * size : 0;
            List<OtcOrder> list = otcOrderMapper.listClosedOrderV2(takerAccountId, makerAccountId, offset, size);
            if (org.apache.commons.collections4.CollectionUtils.isEmpty(list)) {
                return Lists.newArrayList();
            }

            appendOrderExt(list);
            return list;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            log.info("listClosedOrder,consume={} mills,accountId={}", System.currentTimeMillis() - start, accountId);
        }
    }

    @Deprecated
    public List<OtcOrder> getOtcOrderListFromIndex(long accountId, List<Integer> status, Date beginTime, Date endTime,
                                                   String tokenId, Integer side, int page, int size) {
        log.info("getOtcOrderListFromIndex,accountId={}", accountId);
        Example indexExp = Example.builder(OrderIndex.class)
                .select("orderId")
                .orderByDesc("createDate")
                .build();
        indexExp.createCriteria()
                .andEqualTo("accountId", accountId)
                .andIn("status", Lists.newArrayList(
                        OrderStatus.CANCEL.getStatus(),
                        OrderStatus.FINISH.getStatus()
                ));

        PageHelper.startPage(page, size);
        return listOtcOrderFromIndex(indexExp);
    }

    private void appendOrderExt(Collection<OtcOrder> orderList) {
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(orderList)) {
            return;
        }

        List<Long> ids = orderList.stream().map(i -> i.getId()).collect(Collectors.toList());
        List<OtcOrderExt> extList = listOrderExtByIds(ids);
        List<OtcOrderDepthShare> shareOrderList = listOtcOrderDepthShareByIds(ids);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(extList)) {
            return;
        }
        Map<Long, OtcOrderExt> extMap = extList.stream().collect(Collectors.toMap(i -> i.getOrderId(), i -> i));
        Map<Long, OtcOrderDepthShare> shareMap = shareOrderList.stream().collect(Collectors.toMap(i -> i.getOrderId(), i -> i));

        orderList.forEach(order -> {
            OtcOrderExt ext = extMap.get(order.getId());
            order.setOrderExt(ext);
            OtcOrderDepthShare shareOrderInfo = shareMap.get(order.getId());
            if (shareOrderInfo != null) {
                order.setMakerBrokerId(Objects.nonNull(shareOrderInfo.getMakerOrgId()) ? shareOrderInfo.getMakerOrgId() : 0L);
                order.setTakerBrokerId(Objects.nonNull(shareOrderInfo.getTakerOrgId()) ? shareOrderInfo.getTakerOrgId() : 0L);
            }
        });
    }

    private List<OtcOrderExt> listOrderExtByIds(List<Long> ids) {

        Example exp = new Example(OtcOrderExt.class);
        exp.createCriteria().andIn("orderId", ids);

        return otcOrderExtMapper.selectByExample(exp);
    }

    private List<OtcOrderDepthShare> listOtcOrderDepthShareByIds(List<Long> ids) {
        Example exp = new Example(OtcOrderDepthShare.class);
        exp.createCriteria().andIn("orderId", ids);
        return otcOrderDepthShareMapper.selectByExample(exp);
    }

    public OtcOrderExt findOrderExtById(Long id) {
        return otcOrderExtMapper.selectByPrimaryKey(id);
    }

    public int getPendingOrderCount(long accountId) {
        return otcOrderMapper.countPendingOrder(accountId);
    }

    public int getPendingOrderCountOriginal(long accountId) {
        Example example = Example.builder(OtcOrder.class).build();
        example.createCriteria()
                .andCondition("(account_id = " + accountId + " or target_account_id = " + accountId + ")")
                .andIn("status", Lists.newArrayList(
                        OrderStatus.NORMAL.getStatus(),
                        OrderStatus.UNCONFIRM.getStatus(),
                        OrderStatus.APPEAL.getStatus()
                ))
        ;
        return otcOrderMapper.selectCountByExample(example);
    }

    @Deprecated
    public int getPendingOrderCountFromIndex(long accountId) {
        log.info("getPendingOrderCountFromIndex,accountId={}", accountId);
        Example indexExp = Example.builder(OrderIndex.class).build();
        indexExp.createCriteria()
                .andEqualTo("accountId", accountId)
                .andIn("status", Lists.newArrayList(
                        OrderStatus.NORMAL.getStatus(),
                        OrderStatus.UNCONFIRM.getStatus(),
                        OrderStatus.APPEAL.getStatus()
                ));

        return orderIndexMapper.selectCountByExample(indexExp);
    }


    public List<OtcOrder> getPendingOrderList(long accountId) {
/*        if (newFeatureEnable) {
            return this.getPendingOrderListFromIndex(accountId);
        } else {
            return this.getPendingOrderListOrginal(accountId);
        }*/

        return this.getPendingOrderListOrginal(accountId);
    }

    public List<OtcOrder> getPendingOrderListOrginal(long accountId) {
        Example example = Example.builder(OtcOrder.class)
                .orderByDesc("createDate")
                .build();
        example.createCriteria()
                .andCondition("(account_id = " + accountId + " or target_account_id = " + accountId + ")")
                .andIn("status", Lists.newArrayList(
                        OrderStatus.NORMAL.getStatus(),
                        OrderStatus.UNCONFIRM.getStatus(),
                        OrderStatus.APPEAL.getStatus()
                ))
        ;
        List<OtcOrder> list = otcOrderMapper.selectByExample(example);
        appendOrderExt(list);
        return list;
    }


    @Deprecated
    public List<OtcOrder> getPendingOrderListFromIndex(long accountId) {
        log.info("getPendingOrderListFromIndex,accountId={}", accountId);
        Example indexExp = Example.builder(OrderIndex.class)
                .select("orderId")
                .orderByDesc("createDate")
                .build();
        indexExp.createCriteria()
                .andEqualTo("accountId", accountId)
                .andIn("status", Lists.newArrayList(
                        OrderStatus.NORMAL.getStatus(),
                        OrderStatus.UNCONFIRM.getStatus(),
                        OrderStatus.APPEAL.getStatus()
                ))
        ;

        return listOtcOrderFromIndex(indexExp);
    }

    @Deprecated
    private List<OtcOrder> listOtcOrderFromIndex(Example indexExp) {

        List<OrderIndex> indexList = orderIndexMapper.selectByExample(indexExp);
        List<Long> ids = indexList.stream().map(i -> i.getOrderId()).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(ids)) {
            return Lists.newArrayList();
        }

        Example orderExp = new Example(OtcOrder.class);
        orderExp.createCriteria()
                .andIn("id", ids);

        List<OtcOrder> list = otcOrderMapper.selectByExample(orderExp);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        //创建日期倒排
        list = list.stream().sorted(Comparator.comparing(OtcOrder::getCreateDate).reversed()).collect(Collectors.toList());
        appendOrderExt(list);
        return list;

    }

    //分页拿到所有待取消的订单
    public List<OtcOrder> listUnpayTimeoutOrder(Date endTime) {
        List<OtcOrder> orderList = new ArrayList<>();
        int page = 0;
        int limit = 100;
        while (true) {
            int index = page > 0 ? page * 100 : 0;
            List<OtcOrder> list = otcOrderMapper.listUnpayTimeoutOrder(endTime, index, limit);
            if (CollectionUtils.isEmpty(list)) {
                break;
            }
            orderList.addAll(list);
            page++;
        }
        log.info("listUnpayTimeoutOrder query list size {}", orderList.size());
        return orderList;
    }

    public List<OtcOrder> getOtcOrderListForTask(List<Integer> status, Date beginTime, Date endTime, String tokenId,
                                                 Long orgId, Integer side, int page, int size) {
        Example example = Example.builder(OtcOrder.class)
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria = example.createCriteria();
        if (StringUtils.isNotBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        if (!CollectionUtils.isEmpty(status)) {
            criteria.andIn("status", status);
        }
        if (orgId != null) {
            criteria.andEqualTo("orgId", orgId);
        }
        if (side != null) {
            criteria.andEqualTo("side", side);
        }
        if (beginTime != null) {
            criteria.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria.andLessThanOrEqualTo("createDate", endTime);
        }

/*        PageHelper.startPage(page,size);
        List<OtcOrder> list=otcOrderMapper.selectByExample(example);
*/

        List<OtcOrder> list = otcOrderMapper.selectByExampleAndRowBounds(example, new RowBounds((page - 1) * size, size + 1));

        log.info("getOtcOrderListForTask,list size={}", list.size());

        return list;
    }

    /**
     * 被broker-admin，及otc-server自动任务调用
     */
    public List<OtcOrder> getOtcOrderList(List<Integer> status, Date beginTime, Date endTime, String tokenId,
                                          Long orgId, Integer side, int page, int size) {
        Example example = Example.builder(OtcOrder.class)
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria = example.createCriteria();
        if (StringUtils.isNotBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }
        if (!CollectionUtils.isEmpty(status)) {
            criteria.andIn("status", status);
        }
        if (orgId != null) {
            criteria.andEqualTo("orgId", orgId);
        }
        if (side != null) {
            criteria.andEqualTo("side", side);
        }
        if (beginTime != null) {
            criteria.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria.andLessThanOrEqualTo("createDate", endTime);
        }

        PageHelper.startPage(page, size);
        List<OtcOrder> list = otcOrderMapper.selectByExample(example);
        //todo 需要优化
        //List<OtcOrder> list = otcOrderMapper.selectByExampleAndRowBounds(example, new RowBounds((page - 1) * size, size + 1));
        //去掉共享订单，共享订单由共享订单列表输出
        list = list.stream().filter(i -> !i.getDepthShareBool()).collect(Collectors.toList());
        //查询深度共享订单
        List<OtcOrder> shareOrderList = this.listShareOrder(status, beginTime, endTime, tokenId,
                orgId, side);

        list.addAll(shareOrderList);
        //按照创建时间倒排序
        return list.stream().sorted(new Comparator<OtcOrder>() {
            @Override
            public int compare(OtcOrder o1, OtcOrder o2) {
                return o2.getCreateDate().compareTo(o1.getCreateDate());
            }
        }).collect(Collectors.toList());
        //return otcOrderMapper.selectByExampleAndRowBounds(example, new RowBounds((page - 1) * size, size + 1));
    }

    public List<OtcOrder> getOtcOrderListForAdmin(List<Integer> status, Date beginTime, Date endTime, String tokenId,
                                                  Long orgId, int page, int size, Long id, Long accountId, List<Integer> sideEnums) {

        Example example = Example.builder(OtcOrder.class)
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria = example.createCriteria();
        if (orgId != null && !orgId.equals(6002L)) {
            criteria.andEqualTo("orgId", orgId);
        } else {
            //6002可以查当前券商下的所有订单和所有的共享订单
            Example.Criteria or = example.createCriteria();
            or.orEqualTo("orgId", 6002).orEqualTo("depthShare", 1);
            example.and(or);
        }

        if (id != null && id > 0) {
            criteria.andEqualTo("id", id);
        }
        if (accountId != null && accountId > 0) {
            Example.Criteria account = example.createCriteria();
            account.orEqualTo("accountId", accountId).orEqualTo("targetAccountId", accountId);
            example.and(account);
        }

        if (StringUtils.isNotBlank(tokenId)) {
            criteria.andEqualTo("tokenId", tokenId);
        }

        if (!CollectionUtils.isEmpty(status)) {
            criteria.andIn("status", status);
        }

        if (!CollectionUtils.isEmpty(sideEnums)) {
            criteria.andIn("side", sideEnums);
        }

        if (beginTime != null) {
            criteria.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria.andLessThanOrEqualTo("createDate", endTime);
        }
        PageHelper.startPage(page, size);
        List<OtcOrder> list = otcOrderMapper.selectByExampleAndRowBounds(example, new RowBounds((page - 1) * size, size + 1));
        appendOrderExt(list);
        return list;
    }

    public List<OtcOrder> listShareOrderV2(List<Integer> status, Long orgId, int page, int size) {
        long startTime = System.currentTimeMillis();
        if (Objects.isNull(orgId)) {
            return Lists.newArrayList();
        }
        Stopwatch sw = Stopwatch.createStarted();
        try {
            //查询深度共享订单id
            int offset = page > 1 ? (page - 1) * size : 0;
            List<OtcOrderDepthShare> shareList = otcOrderDepthShareMapper.listOrdersV2(orgId, offset, size, status);
            List<Long> shareIds = shareList.stream().map(i -> i.getOrderId()).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(shareIds)) {
                return Lists.newArrayList();
            }

            Map<Long, OtcOrderDepthShare> shareOrderMap = shareList.stream().collect(Collectors.toMap(i -> i.getOrderId(), i -> i));

            //查询对应订单列表
            Example example2 = Example.builder(OtcOrder.class)
                    .orderByDesc("createDate")
                    .build();
            Example.Criteria criteria2 = example2.createCriteria();
            criteria2.andIn("id", shareIds);
            if (!CollectionUtils.isEmpty(status)) {
                criteria2.andIn("status", status);
            }

            List<OtcOrder> list = otcOrderMapper.selectByExample(example2);
            list = list.stream().map(i -> {
                OtcOrderDepthShare shareOrder = shareOrderMap.get(i.getId());
                if (Objects.isNull(shareOrder)) {
                    log.warn("Share order is null,orderId={}", i.getId());
                    return i;
                }
                i.setMakerBrokerId(shareOrder.getMakerOrgId());
                i.setTakerBrokerId(shareOrder.getTakerOrgId());
                return i;
            }).collect(Collectors.toList());

            OTC_ORDER_SERVICE_METRICS.labels("listShareOrderV2").observe(System.currentTimeMillis() - startTime);
            return list;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return Lists.newArrayList();
        } finally {
            log.info("listShareOrderV2 status={},orgId={},size={},page={} consume {} mills",
                    status, orgId, size, page,
                    sw.stop().elapsed(TimeUnit.MILLISECONDS));
        }
    }


    private List<OtcOrder> listShareOrder(List<Integer> status, Date beginTime, Date endTime, String tokenId,
                                          Long orgId, Integer side) {
        long startTime = System.currentTimeMillis();
        if (Objects.isNull(orgId)) {
            return Lists.newArrayList();
        }

        //查询深度共享订单id
        Example example = Example.builder(OtcOrderDepthShare.class)
                .select("orderId", "makerOrgId", "takerOrgId")
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria = example.createCriteria();
        criteria.orEqualTo("makerOrgId", orgId)
                .orEqualTo("takerOrgId", orgId);

        if (beginTime != null) {
            criteria.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria.andLessThanOrEqualTo("createDate", endTime);
        }

        List<OtcOrderDepthShare> shareList = otcOrderDepthShareMapper.selectByExample(example);
        List<Long> shareIds = shareList.stream().map(i -> i.getOrderId()).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(shareIds)) {
            return Lists.newArrayList();
        }

        Map<Long, OtcOrderDepthShare> shareOrderMap = shareList.stream().collect(Collectors.toMap(i -> i.getOrderId(), i -> i));

        //查询对应订单列表
        Example example2 = Example.builder(OtcOrder.class)
                .orderByDesc("createDate")
                .build();
        Example.Criteria criteria2 = example2.createCriteria();
        criteria2.andIn("id", shareIds);
        if (StringUtils.isNotBlank(tokenId)) {
            criteria2.andEqualTo("tokenId", tokenId);
        }
        if (!CollectionUtils.isEmpty(status)) {
            criteria2.andIn("status", status);
        }

        if (side != null) {
            criteria2.andEqualTo("side", side);
        }
        if (beginTime != null) {
            criteria2.andGreaterThanOrEqualTo("createDate", beginTime);
        }
        if (endTime != null) {
            criteria2.andLessThanOrEqualTo("createDate", endTime);
        }

        List<OtcOrder> list = otcOrderMapper.selectByExample(example2);
        list = list.stream().map(i -> {
            OtcOrderDepthShare shareOrder = shareOrderMap.get(i.getId());
            if (Objects.isNull(shareOrder)) {
                log.warn("Share order is null,orderId={}", i.getId());
                return i;
            }
            i.setMakerBrokerId(shareOrder.getMakerOrgId());
            i.setTakerBrokerId(shareOrder.getTakerOrgId());
            return i;
        }).collect(Collectors.toList());

        OTC_ORDER_SERVICE_METRICS.labels("listShareOrder").observe(System.currentTimeMillis() - startTime);
        return list;
    }

    public List<OtcOrder> getFinishOvertimeOrderList() {
        Date finishTime = new Date(System.currentTimeMillis() - (30 * 60 * 1000));
        long startTime = System.currentTimeMillis();
        List<OtcOrder> orderList = new ArrayList<>();
        int page = 0;
        int limit = 100;
        while (true) {
            int index = page > 0 ? page * 100 : 0;
            List<OtcOrder> list = otcOrderMapper.listAppealTimeoutOrder(finishTime, index, limit);
            if (CollectionUtils.isEmpty(list)) {
                break;
            }
            orderList.addAll(list);
            page++;
        }
        OTC_ORDER_SERVICE_METRICS.labels("getFinishOvertimeOrderList").observe(System.currentTimeMillis() - startTime);
        log.info("getFinishOvertimeOrderList query list size {}", orderList.size());
        return orderList;
    }

    public void increaseCancelNum(long accountId) {
        String key = String.format(OTC_ORDER_CANCEL_NUM_KEY, accountId);
        try {
            boolean expireFlag = StringUtils.isBlank(stringRedisTemplate.opsForValue().get(key));
            long curNum = stringRedisTemplate.opsForValue().increment(key, 1);
            if (curNum >= 3) {
                expireFlag = true;
            }
            if (expireFlag) {
                stringRedisTemplate.expire(key, OTC_ORDER_CANCEL_EXPIRE_TIME, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            log.error("increase order cancel num failed", e);
        }
    }

    public OrderMsg popMessage(long orgId) {
        String msg = stringRedisTemplate.opsForList().rightPop(String.format(OTC_ORDER_ORG_MSG_KEY, orgId));
        if (!Strings.isNullOrEmpty(msg)) {
            return JSON.parseObject(msg, OrderMsg.class);
        }
        return null;
    }

    /**
     * 创建系统消息
     *
     * @param orderId   订单id
     * @param accountId 账户id
     * @param msgCode   消息编码
     */
    private void createSysOrderMessage(long orderId, long accountId, MsgCode msgCode) {
        OtcOrderMessage message = OtcOrderMessage.builder()
                .orderId(orderId)
                .accountId(accountId)
                .msgType(MsgType.SYS_MSG.getType())
                .msgCode(msgCode.getCode())
                .createDate(new Date())
                .build();
        otcOrderMessageService.addOtcOrderMessage(message);
    }

    /**
     * 创建系统消息
     *
     * @param otcOrder      订单
     * @param buyerMsgCode  买方消息
     * @param sellerMsgCode 卖方消息
     */
    private void createSysMessage(OtcOrder otcOrder, MsgCode buyerMsgCode, MsgCode sellerMsgCode) {
        long startTime = System.currentTimeMillis();
        long buyer, seller;
        // 判断买卖双方
        if (otcOrder.getSide().equals(Side.BUY.getCode())) {
            buyer = otcOrder.getAccountId();
            seller = otcOrder.getTargetAccountId();
        } else {
            buyer = otcOrder.getTargetAccountId();
            seller = otcOrder.getAccountId();
        }
        // 保存系统消息至db，聊天记录展示
        createSysOrderMessage(otcOrder.getId(), buyer, buyerMsgCode);
        createSysOrderMessage(otcOrder.getId(), seller, sellerMsgCode);

        OtcUserInfo buyerInfo = otcUserInfoService.getOtcUserInfo(buyer);
        OtcUserInfo sellerInfo = otcUserInfoService.getOtcUserInfo(seller);

        //获取当前券商的申诉时间配置(内存获取) 如果大于的话直接进入申诉状态
        BrokerExt brokerExt = this.otcConfigService.getBrokerExtFromCache(otcOrder.getOrgId());
        //当前券商是否是白名单券商
        OtcDepthShareBrokerWhiteList brokerWhite = this.otcConfigService.getOtcDepthShareBrokerWhiteListFromCache(otcOrder.getOrgId());

        int cancelTime = 15;
        int appealTime = 30;
        if (brokerWhite == null && brokerExt != null) {
            if (brokerExt.getCancelTime() != null && brokerExt.getCancelTime() > 0) {
                cancelTime = brokerExt.getCancelTime();
            }
            if (brokerExt.getAppealTime() != null && brokerExt.getAppealTime() > 0) {
                appealTime = brokerExt.getAppealTime();
            }
        }

        // 保存短消息至redis，发短信或邮件
        try {
            OrderMsg buyerMsg = OrderMsg.builder()
                    .msgCode(buyerMsgCode.getCode())
                    .orgId(buyerInfo.getOrgId())
                    .userId(buyerInfo.getUserId())
                    .buyer(buyerInfo.getNickName())
                    .seller(sellerInfo.getNickName())
                    .tokenId(otcOrder.getTokenId())
                    .currencyId(otcOrder.getCurrencyId())
                    .quantity(otcOrder.getQuantity().stripTrailingZeros().toPlainString())
                    .amount(otcOrder.getAmount().stripTrailingZeros().toPlainString())
                    .cancelTime(cancelTime)
                    .appealTime(appealTime)
                    .orderId(otcOrder.getId() != null && otcOrder.getId() > 0 ? otcOrder.getId() : 0L)
                    .side(OrderSideEnum.BUY_VALUE)
                    .build();
            stringRedisTemplate.opsForList().leftPush(String.format(OTC_ORDER_ORG_MSG_KEY, buyerInfo.getOrgId()),
                    JSON.toJSONString(buyerMsg));
            OrderMsg sellerMsg = OrderMsg.builder()
                    .msgCode(sellerMsgCode.getCode())
                    .orgId(sellerInfo.getOrgId())
                    .userId(sellerInfo.getUserId())
                    .buyer(buyerInfo.getNickName())
                    .seller(sellerInfo.getNickName())
                    .tokenId(otcOrder.getTokenId())
                    .currencyId(otcOrder.getCurrencyId())
                    .quantity(otcOrder.getQuantity().stripTrailingZeros().toPlainString())
                    .amount(otcOrder.getAmount().stripTrailingZeros().toPlainString())
                    .cancelTime(cancelTime)
                    .appealTime(appealTime)
                    .orderId(otcOrder.getId() != null && otcOrder.getId() > 0 ? otcOrder.getId() : 0L)
                    .side(OrderSideEnum.SELL_VALUE)
                    .build();
            stringRedisTemplate.opsForList().leftPush(String.format(OTC_ORDER_ORG_MSG_KEY, sellerInfo.getOrgId()),
                    JSON.toJSONString(sellerMsg));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("create order msg failed, orderId={}", otcOrder.getId());
        }
        OTC_ORDER_SERVICE_METRICS.labels("createSysMessage").observe(System.currentTimeMillis() - startTime);
    }

    /**
     * 检查广告状态并锁定
     */
    private OtcItem checkAndLockItem(long itemId, BigDecimal quantity) {
        long startTime = System.currentTimeMillis();
        OtcItem otcItem = otcItemMapper.selectOtcItemForUpdate(itemId);
        // 检查广告冻结单量
        if (otcItem.getFrozenQuantity().compareTo(quantity) < 0) {
            throw new BusinessException("item frozen quantity less than order quantity, itemId" + itemId);
        }
        // 广告单状态错误，不能进一步处理。阻塞
        if (!otcItem.getStatus().equals(ItemStatus.NORMAL.getStatus())
                && !otcItem.getStatus().equals(ItemStatus.CANCEL.getStatus())) {
            throw new BusinessException("item status error, can not update order, itemId=" + itemId);
        }
        OTC_ORDER_SERVICE_METRICS.labels("checkAndLockItem").observe(System.currentTimeMillis() - startTime);
        return otcItem;
    }

    /**
     * 处理广告单，返还冻结数量
     */
    private OtcBalanceFlow backFrozen(OtcOrder otcOrder) {
        long startTime = System.currentTimeMillis();
        OtcItem otcItem = checkAndLockItem(otcOrder.getItemId(), otcOrder.getQuantity());
        Date now = new Date();
        //更新广告单数据 需要把预扣手续费从成交预扣字段减掉
        OtcItem updater = OtcItem.builder()
                .id(otcItem.getId())
                .lastQuantity(otcItem.getLastQuantity().add(otcOrder.getQuantity()))
                .frozenQuantity(otcItem.getFrozenQuantity().subtract(otcOrder.getQuantity()))
                .fee(otcItem.getFee().subtract(otcOrder.getMakerFee()))
                .updateDate(now)
                .build();
        // 如果是无效订单，则订单数量减一，如果是正常撤单则已完结订单数量加一
        if (otcOrder.getStatus().equals(OrderStatus.INIT.getStatus())) {
            updater.setOrderNum(otcItem.getOrderNum() - 1);
        } else {
            updater.setFinishNum(otcItem.getFinishNum() + 1);
        }
        int res = otcItemMapper.updateByPrimaryKeySelective(updater);
        if (res != 1) {
            throw new BusinessException("update OtcItem failed, itemId" + otcItem.getId());
        }
        // 订单为卖单的时候，撤单需要生成返还账户冻结到可用的流水
        if (otcOrder.getSide().equals(Side.SELL.getCode()) &&
                (otcOrder.getStatus().equals(OrderStatus.NORMAL.getStatus()) ||
                        otcOrder.getStatus().equals(OrderStatus.APPEAL.getStatus()))) {
            OtcBalanceFlow otcBalanceFlow = OtcBalanceFlow.builder()
                    .accountId(otcOrder.getAccountId())
                    .orgId(otcOrder.getOrgId())
                    .userId(otcOrder.getUserId())
                    .tokenId(otcOrder.getTokenId())
                    .amount(otcOrder.getQuantity())
                    .flowType(FlowType.BACK_ORDER_FROZEN.getType())
                    .objectId(otcOrder.getId())
                    .status(FlowStatus.WAITING_PROCESS.getStatus())
                    .createDate(now)
                    .updateDate(now)
                    .build();
            otcBalanceFlowMapper.insert(otcBalanceFlow);
            if (otcBalanceFlow.getId() == null || otcBalanceFlow.getId() == 0) {
                throw new BusinessException("insert OtcBalanceFlow failed");
            }
            return otcBalanceFlow;
        }
        // 订单为买单，则检查广告的当前状态
        if (otcItem.getStatus().equals(ItemStatus.CANCEL.getStatus())) {
            // 如果是卖单广告，且广告已经撤单，还要生成返还账户冻结到可用的流水

            //TODO 加上手续费都要返回去 预扣的手续费需要减掉
            OtcBalanceFlow otcBalanceFlow = OtcBalanceFlow.builder()
                    .accountId(otcItem.getAccountId())
                    .orgId(otcItem.getOrgId())
                    .userId(otcItem.getUserId())
                    .tokenId(otcItem.getTokenId())
                    .amount(otcOrder.getQuantity().add(otcOrder.getMakerFee()))
                    .flowType(FlowType.BACK_ITEM_FROZEN.getType())
                    .objectId(otcItem.getId())
                    .status(FlowStatus.WAITING_PROCESS.getStatus())
                    .createDate(now)
                    .updateDate(now)
                    .build();
            otcBalanceFlowMapper.insert(otcBalanceFlow);
            if (otcBalanceFlow.getId() == null || otcBalanceFlow.getId() == 0) {
                throw new BusinessException("insert OtcBalanceFlow failed");
            }
            return otcBalanceFlow;
        }
        OTC_ORDER_SERVICE_METRICS.labels("backFrozen").observe(System.currentTimeMillis() - startTime);
        return null;
    }

    public List<OtcOrder> listOtcOrderByItemIds(List<Long> exchangeIds, List<Long> orgIds, List<Long> itemIds) {
        long startTime = System.currentTimeMillis();
        Example exp = new Example(OtcOrder.class);
        exp.selectProperties("id", "status", "accountId", "targetAccountId", "clientOrderId", "itemId");
        exp.createCriteria().andIn("exchangeId", exchangeIds)
                .andIn("orgId", orgIds)
                .andIn("itemId", itemIds);

        List<OtcOrder> orderList = otcOrderMapper.selectByExample(exp);

        OTC_ORDER_SERVICE_METRICS.labels("listOtcOrderByOrderIds").observe(System.currentTimeMillis() - startTime);
        return orderList;
    }

    public List<OtcOrder> listOtcOrderByOrderIds(List<Long> orderIdsList) {
        long startTime = System.currentTimeMillis();
        Example exp = new Example(OtcOrder.class);
        exp.selectProperties("id", "status", "accountId", "targetAccountId", "clientOrderId", "itemId");
        exp.createCriteria().andIn("id", orderIdsList);

        List<OtcOrder> orderList = otcOrderMapper.selectByExample(exp);
        OTC_ORDER_SERVICE_METRICS.labels("listOtcOrderByOrderIds").observe(System.currentTimeMillis() - startTime);
        return orderList;
    }


    public List<OtcOrder> selectOtcOrderForFreed(int page, int size) {
        int currentPage = page < 1 ? 0 : page - 1;
        int offset = currentPage * size;
        return otcOrderMapper.selectOtcOrderForFreed(offset, size);
    }

    public int updateOtcOrderFreed(Long orderId) {
        return otcOrderMapper.updateOtcOrderFreed(orderId);
    }

    public void refreshHistoryOrder() {
        List<OtcOrder> otcOrderList
                = otcOrderMapper.selectOrderListByPaymentNotNull();
        if (CollectionUtils.isEmpty(otcOrderList)) {
            log.info("order list is null");
        }

        otcOrderList.forEach(order -> {
            if (otcOrderPayInfoMapper.queryOtcOrderPayInfoByOrderId(order.getId()) == null) {
                Long paymentId
                        = otcPaymentTermMapper
                        .selectPaymentTermByPaymentType(order.getTargetAccountId(), order.getPaymentType());

                if (paymentId != null && paymentId > 0) {
                    log.info("orderId {} accountId {} paymentId {}", order.getId(), order.getAccountId(), paymentId);
                    otcPaymentTermService.bindingPaymentToOrderPayInfo(order.getId(), paymentId);
                }
            }
        });
    }

    public OtcOrder findOtcOrderById(Long orderId) {
        return otcOrderMapper.selectByPrimaryKey(orderId);
    }

    public boolean mutualConfirm(Long brokerId, OtcOrder otcOrder, OrderStatus status, String ext, String appealContent) {

        if (Objects.isNull(status)) {
            return false;
        }

        Long orderId = otcOrder.getId();
        Example exp1 = new Example(OtcOrderDepthShareAppeal.class);
        exp1.createCriteria().andEqualTo("orderId", orderId);

        Long adminId = Long.parseLong(ext);
        List<OtcOrderDepthShareAppeal> list = otcOrderDepthShareAppealMapper.selectByExample(exp1);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(list)) {
            this.saveAppealInfo(brokerId, otcOrder, status, adminId, appealContent);
            return false;
        }

        Map<Long, OtcOrderDepthShareAppeal> appealMap = list.stream().collect(Collectors.toMap(OtcOrderDepthShareAppeal::getBrokerId, i -> i));
        OtcOrderDepthShareAppeal appeal = appealMap.get(brokerId);
        Short existStatus = Objects.isNull(appeal) ? 0 : appeal.getStatus();

        if (Objects.isNull(appeal)) {
            if (appealMap.size() == 1) {
                this.saveAppealInfo(brokerId, otcOrder, status, adminId, appealContent);
                //比对状态
                Short tmpStatus = appealMap.get(Lists.newArrayList(appealMap.keySet()).get(0)).getStatus();
                return status.getStatus() == tmpStatus;
            }
            return false;
        } else {
            //移除已有的操作状态
            appealMap.remove(brokerId);
            //状态不相等，更新
            if (existStatus != status.getStatus()) {
                //更新状态
                OtcOrderDepthShareAppeal update = OtcOrderDepthShareAppeal.builder()
                        .id(appeal.getId())
                        .adminId(adminId)
                        .status((short) status.getStatus())
                        .comment(appealContent)
                        .updateDate(new Date())
                        .build();
                otcOrderDepthShareAppealMapper.updateByPrimaryKeySelective(update);
            }

            //比对两次操作状态
            if (appealMap.size() > 0) {
                Short tmpStatus = appealMap.get(Lists.newArrayList(appealMap.keySet()).get(0)).getStatus();
                //状态双方一致
                return status.getStatus() == tmpStatus.intValue();
            }

            return false;
        }
    }

    private boolean saveAppealInfo(Long brokerId, OtcOrder otcOrder, OrderStatus status, Long adminId, String appealContent) {
        Date now = new Date();
        OtcOrderDepthShareAppeal appeal = OtcOrderDepthShareAppeal.builder()
                .adminId(adminId)
                .orderId(otcOrder.getId())
                .brokerId(brokerId)
                .status((short) status.getStatus())
                .comment(Strings.nullToEmpty(appealContent))
                .createDate(now)
                .updateDate(now)
                .build();

        return otcOrderDepthShareAppealMapper.insert(appeal) == 1;
    }

    public boolean greaterThan(Long number, Long compareTo) {

        Objects.requireNonNull(number, "number is null");
        Objects.requireNonNull(compareTo, "compare to is null");

        return number.compareTo(compareTo) == 1;
    }

    public List<OtcOrderDepthShareAppeal> listShareOrderAppealInfo(long orderId) {

        Example exp = new Example(OtcOrderDepthShareAppeal.class);
        exp.createCriteria().andEqualTo("orderId", orderId);

        return otcOrderDepthShareAppealMapper.selectByExample(exp);
    }

    public Pair<OtcUserInfo, OtcUserInfo> getOrderContact(long orderId) {

        Pair<OtcUserInfo, OtcUserInfo> result = null;
        OtcOrder order = this.findOtcOrderById(orderId);
        if (Objects.isNull(order)) {
            return result;
        }

        OtcItem item = otcItemMapper.selectByPrimaryKey(order.getItemId());
        if (Objects.isNull(item)) {
            return result;
        }

        List<Long> orgIds = Lists.newArrayList(item.getOrgId(), order.getOrgId());
        List<Long> userIds = Lists.newArrayList(item.getUserId(), order.getUserId());

        List<OtcUserInfo> users = otcUserInfoService.listUserInfo(orgIds, userIds);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(users)) {
            return result;
        }

        Map<String, OtcUserInfo> map = users.stream().collect(Collectors.toMap(i -> i.getOrgId() + "-" + i.getUserId(), i -> i));
        OtcUserInfo maker = map.get(item.getOrgId() + "-" + item.getUserId());
        OtcUserInfo taker = map.get(order.getOrgId() + "-" + order.getUserId());

        if (Objects.isNull(maker) || Objects.isNull(taker)) {
            return null;
        }

        return Pair.of(maker, taker);
    }

    public Long getLastNewOrderId(long accountId) {

        String key = String.format(LAST_NEW_ORDER_ID, accountId);
        String idStr = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isNoneBlank(idStr)) {
            return Long.parseLong(idStr);
        }

        Long id = otcOrderMapper.getLastOrderIdByStatus(accountId, OrderStatus.NORMAL.getStatus());
        stringRedisTemplate.opsForValue().set(key, Objects.isNull(id) ? "0" : id.toString(), 5, TimeUnit.SECONDS);
        return Objects.isNull(id) ? 0L : id;
    }

    public int countUnfinishOrderByItemId(List<Long> itemIds) {

        if (CollectionUtils.isEmpty(itemIds)) {
            return 0;
        }

        Example exp = new Example(OtcOrder.class);
        exp.createCriteria().andIn("itemId", itemIds)
                .andIn("status", Lists.newArrayList(OTCOrderStatusEnum.OTC_ORDER_INIT_VALUE, OTCOrderStatusEnum.OTC_ORDER_APPEAL_VALUE,
                        OTCOrderStatusEnum.OTC_ORDER_NORMAL_VALUE, OTCOrderStatusEnum.OTC_ORDER_UNCONFIRM_VALUE));

        return otcOrderMapper.selectCountByExample(exp);
    }

    @Data
    public static class ClearOrderListCacheEvent {
        private Long accountId;
        private Long targetAccountId;
    }
}