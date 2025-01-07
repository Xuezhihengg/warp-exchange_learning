package org.warpexchange_learning.tradingengine;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.bean.OrderBookBean;
import org.warpexchange_learning.common.message.TickMessage;
import org.warpexchange_learning.common.message.event.AbstractEvent;
import org.warpexchange_learning.common.message.event.OrderCancelEvent;
import org.warpexchange_learning.common.message.event.OrderRequestEvent;
import org.warpexchange_learning.common.message.event.TransferEvent;
import org.warpexchange_learning.common.messaging.MessageConsumer;
import org.warpexchange_learning.common.messaging.MessageProducer;
import org.warpexchange_learning.common.messaging.Messaging;
import org.warpexchange_learning.common.messaging.MessagingFactory;
import org.warpexchange_learning.common.model.trade.OrderEntity;
import org.warpexchange_learning.common.redis.RedisService;
import org.warpexchange_learning.common.support.LoggerSupport;
import org.warpexchange_learning.common.util.IpUtil;
import org.warpexchange_learning.tradingengine.assets.AssetService;
import org.warpexchange_learning.tradingengine.assets.Transfer;
import org.warpexchange_learning.tradingengine.clearing.ClearingService;
import org.warpexchange_learning.tradingengine.match.MatchEngine;
import org.warpexchange_learning.tradingengine.match.MatchResult;
import org.warpexchange_learning.tradingengine.order.OrderService;
import org.warpexchange_learning.tradingengine.store.StoreService;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

@Component
public class TradingEngineService extends LoggerSupport {

    @Autowired(required = false)
    ZoneId zoneId = ZoneId.systemDefault();

    @Value("#{exchangeConfiguration.orderBookDepth}")
    int orderBookDepth = 100;

    @Value("#{exchangeConfiguration.debugMode}")
    boolean debugMode = false;

    boolean fatalError = false;

    @Autowired
    AssetService assetService;

    @Autowired
    OrderService orderService;

    @Autowired
    MatchEngine matchEngine;

    @Autowired
    ClearingService clearingService;

    @Autowired
    MessagingFactory messagingFactory;

    @Autowired
    StoreService storeService;

    @Autowired
    RedisService redisService;

    private MessageConsumer consumer;

    private MessageProducer<TickMessage> producer;

    private long lastSequenceId = 0;

    private boolean orderBookChanged = false;

    private String shaUpdateOrderBookLua;

    private Thread tickThread;
    private Thread notifyThread;
    private Thread apiResultThread;
    private Thread orderBookThread;
    private Thread dbThread;

    private OrderBookBean latestOrderBook = null;

    @PostConstruct
    public void init() {
        // 接收发给TRADE的消息,并使用processMessages处理消息
        this.consumer = this.messagingFactory.createBatchMessageListener(Messaging.Topic.TRADE, IpUtil.getHostId(), this::processMessages);
        this.producer = this.messagingFactory.createMessageProducer(Messaging.Topic.TICK, TickMessage.class);
    }

    public void processMessages(List<AbstractEvent> messages){
        this.orderBookChanged = false;
        for (AbstractEvent message : messages) {
            processEvent(message);
        }
        if (this.orderBookChanged) {
            // 如果某个事件引起了订单簿变化，则更新最新的订单簿快照
            // 获取最新的OrderBook快照:
            this.latestOrderBook = this.matchEngine.getOrderBook(this.orderBookDepth);
        }
    }

    public void processEvent(AbstractEvent event) {
        if (this.fatalError) {
            return;
        }
        // 判断是否有重复消息，有则丢弃
        if (event.sequenceId <= this.lastSequenceId) {
            logger.warn("skip duplicate event: {}", event);
            return;
        }
        // 判断是否丢失了消息，如果是则从数据库中恢复丢失的消息（依据lastSequenceId）
        if (event.previousId > this.lastSequenceId) {
            logger.warn("event lost: expected previous id {} but actual {} for event {}", this.lastSequenceId,
                    event.previousId, event);
            List<AbstractEvent> events = this.storeService.loadEventsFromDb(this.lastSequenceId);
            if (events.isEmpty()) {
                logger.error("cannot load lost event from db.");
                panic();
                return;
            }
            for (AbstractEvent e : events) {
                this.processEvent(e);
            }
        }
        // 其他异常情况
        if (event.previousId != this.lastSequenceId) {
            logger.error("bad event: expected previous id {} but actual {} for event: {}", this.lastSequenceId, event.previousId, event);
            panic();
            return;
        }
        // 正常情况
        if (logger.isDebugEnabled()) {
            logger.debug("process event {} -> {}: {}...", this.lastSequenceId, event.sequenceId, event);
        }
        try {
            if (event instanceof OrderRequestEvent) {
                createOrder((OrderRequestEvent) event);
            } else if (event instanceof OrderCancelEvent) {
                cancelOrder((OrderCancelEvent) event);
            } else if (event instanceof TransferEvent) {
                transfer((TransferEvent) event);
            } else {
                logger.error("unable to process event type: {}", event.getClass().getName());
                panic();
                return;
            }
        }catch (Exception e){
            logger.error("process event error.", e);
            panic();
            return;
        }
        // 处理完event后的收尾工作
        this.lastSequenceId = event.sequenceId;
        if (logger.isDebugEnabled()) {
            logger.debug("set last processed sequence id: {}...", this.lastSequenceId);
        }
        if (debugMode) {
            this.validate();
            this.debug();
        }
    }

    void transfer(TransferEvent event) {
        this.assetService.tryTransfer(Transfer.AVAILABLE_TO_AVAILABLE, event.fromUserId, event.toUserId, event.asset, event.amount, event.sufficient);
    }

    void createOrder(OrderRequestEvent event) {
        ZonedDateTime zdt = Instant.ofEpochMilli(event.createdAt).atZone(zoneId);
        int year = zdt.getYear();
        int month = zdt.getMonth().getValue();
        long orderId = event.sequenceId * 10000 + (year * 100 + month);
        OrderEntity order = this.orderService.createOrder(event.sequenceId, event.createdAt, orderId, event.userId, event.direction, event.price, event.quantity);
        if (order == null) {
            logger.warn("create order failed.");
//            // 推送失败结果:
//            this.apiResultQueue.add(ApiResultMessage.createOrderFailed(event.refId, event.createdAt));
            return;
        }
        // 由orderService创建订单后让matchEngine进行搓单
        MatchResult result = this.matchEngine.processOrder(event.sequenceId, order);
        // 搓单成功后由clearingService清算MatchResult
        this.clearingService.clearMatchResult(result);
//        // 推送成功结果,注意必须复制一份OrderEntity,因为将异步序列化:
//        this.apiResultQueue.add(ApiResultMessage.orderSuccess(event.refId, order.copy(), event.createdAt));
        this.orderBookChanged = true;




    }

    private void panic() {
        logger.error("application panic. exit now...");
        this.fatalError = true;
        System.exit(1);
    }

    public void debug() {
        System.out.println("========== trading engine ==========");
        this.assetService.debug();
        this.orderService.debug();
        this.matchEngine.debug();
        System.out.println("========== // trading engine ==========");
    }

    void validate() {
        logger.debug("start validate...");
        validateAssets();
        validateOrders();
        validateMatchEngine();
        logger.debug("validate ok.");
    }

    private void validateMatchEngine() {
    }

    private void validateOrders() {
    }

    private void validateAssets() {
    }


}
