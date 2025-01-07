package org.warpexchange_learning.tradingapi.web.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import org.warpexchange_learning.common.ApiError;
import org.warpexchange_learning.common.ApiErrorResponse;
import org.warpexchange_learning.common.bean.OrderBookBean;
import org.warpexchange_learning.common.bean.OrderRequestBean;
import org.warpexchange_learning.common.ctx.UserContext;
import org.warpexchange_learning.common.message.ApiResultMessage;
import org.warpexchange_learning.common.message.event.OrderRequestEvent;
import org.warpexchange_learning.common.redis.RedisCache;
import org.warpexchange_learning.common.redis.RedisService;
import org.warpexchange_learning.common.support.AbstractApiController;
import org.warpexchange_learning.common.util.IdUtil;
import org.warpexchange_learning.common.util.JsonUtil;
import org.warpexchange_learning.tradingapi.service.SendEventService;
import org.warpexchange_learning.tradingapi.service.TradingEngineApiProxyService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@RestController
@RequestMapping("/api")
public class TradingApiController extends AbstractApiController {

    @Autowired
    private RedisService redisService;

    @Autowired
    private SendEventService sendEventService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TradingEngineApiProxyService tradingEngineApiProxyService;

    private Long asyncTimeout = Long.valueOf(500);

    // 操作超时的响应（序列化后）
    private String timeoutJson = null;

    private String getTimeoutJson() throws IOException{
        if (timeoutJson == null) {
            timeoutJson = this.objectMapper.writeValueAsString(new ApiErrorResponse(ApiError.OPERATION_TIMEOUT, null, ""));
        }
        return timeoutJson;
    }


    // 对异步响应对象进行存储，当API收到Redis推送的事件后，根据refId找到之前保存的DeferredResult
    Map<String, DeferredResult<ResponseEntity<String>>> deferredResultMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        this.redisService.subscribe(RedisCache.Topic.TRADING_API_RESULT, this::onApiResultMessage);
    }

    @ResponseBody
    @GetMapping(value = "/assets", produces = "application/json")
    public String getAssets() throws IOException {
        return tradingEngineApiProxyService.get("/internal/" + UserContext.getRequiredUserId() + "/assets");
    }

    @ResponseBody
    @GetMapping(value = "/orders/{orderId}", produces = "application/json")
    public String getOpenOrder(@PathVariable("orderId") Long orderId) throws IOException {
        final Long userId = UserContext.getRequiredUserId();
        return tradingEngineApiProxyService.get("/internal/" + userId + "/orders/" + orderId);
    }

    @ResponseBody
    @GetMapping(value = "/orders", produces = "application/json")
    public String getOpenOrders() throws IOException {
        return tradingEngineApiProxyService.get("/internal/" + UserContext.getRequiredUserId() + "/orders");
    }

    @ResponseBody
    @GetMapping(value = "/orderBook", produces = "application/json")
    public String getOrderBook() {
        String data = redisService.get(RedisCache.Key.ORDER_BOOK);
        return data == null ? OrderBookBean.EMPTY : data;
    }


    /*
     * 关于DeferredResult：
     * 当一个请求到达API接口，如果该API接口的return返回值是DeferredResult，在没有超时或者DeferredResult对象设置setResult时，接口不会返回
     * 但是Servlet容器线程会结束，DeferredResult会另起线程来进行结果处理(即这种操作提升了服务短时间的吞吐能力)，并setResult，如此以来这个请求不会占用服务连接池太久
     * 使用DeferredResult的流程：
     *      1 浏览器发起请求
     *      2 请求到达服务端被挂起
     *      3 向浏览器进行响应，分为两种情况：
     *          3.1 调用DeferredResult.setResult()，请求被唤醒，返回结果
     *          3.2 超时，返回一个你设定的结果
     *      4 浏览得到响应，再次重复1，处理此次响应结果
     */

    /**
     由于createOrder仅仅通过消息系统给定序系统发了一条消息，消息系统本身并不是类似HTTP的请求-响应模式，无法直接拿到消息处理的结果
     这就需要借助Spring的异步响应模型DeferredResult以及Redis的pub/sub模型
     当API发送消息时，使用全局唯一refId跟踪消息，当交易引擎处理完订单请求后，向Redis发送pub事件，API收到Redis推送的事件后，
     根据refId找到DeferredResult，设置结果后由Spring异步返回给客户端
     这也就是为什么TradingApiController的init中需要redis订阅channel
     */
    @PostMapping(value = "/orders", produces = "application/json")
    @ResponseBody
    public DeferredResult<ResponseEntity<String>> createOrder(@RequestBody OrderRequestBean orderRequest) throws IOException {
        final Long userId = UserContext.getRequiredUserId();
        orderRequest.validate();
        final String refId = IdUtil.generateUniqueId();
        var event = new OrderRequestEvent();
        event.refId = refId;
        event.userId = userId;
        event.direction = orderRequest.direction;
        event.price = orderRequest.price;
        event.quantity = orderRequest.quantity;
        event.createdAt = System.currentTimeMillis();
        // 操作超时的响应体,即默认响应
        ResponseEntity<String> timeout = new ResponseEntity<>(getTimeoutJson(), HttpStatus.BAD_REQUEST);
        DeferredResult<ResponseEntity<String>> deferred = new DeferredResult<>(asyncTimeout, timeout);
        deferred.onTimeout(() -> {
            logger.warn("deferred order request refId = {} timeout.", event.refId);
            this.deferredResultMap.remove(event.refId);
        });
        // track deferred:
        this.deferredResultMap.put(event.refId, deferred);
        this.sendEventService.sendMessage(event);
        return deferred;
    }


    // message callback ///////////////////////////////////////////////////////

    /**
     * redis订阅的回调函数，当trade-engine中处理完订单请求后，向Redis发送pub事件，然后会触发trade-api中的这个回调函数
     */
    public void onApiResultMessage(String msg) {
        logger.info("on subscribed message: {}", msg);
        try {
            ApiResultMessage message = objectMapper.readValue(msg, ApiResultMessage.class);
            if (message.refId != null) {
                DeferredResult<ResponseEntity<String>> deferred = this.deferredResultMap.get(message.refId);
                if (deferred != null) {
                    // 判断处理结果是否异常
                    if (message.error != null) {
                        String error = objectMapper.writeValueAsString(message.error);
                        ResponseEntity<String> resp = new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
                        // 调用DeferredResult.setResult()，请求被唤醒，返回结果
                        deferred.setResult(resp);
                    } else {
                        ResponseEntity<String> resp = new ResponseEntity<>(JsonUtil.writeJson(message.result), HttpStatus.OK);
                        deferred.setResult(resp);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Invalid ApiResultMessage: {}", msg, e);
        }
    }


}
