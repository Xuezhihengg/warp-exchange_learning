package org.warpexchange_learning.quotation.quotation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mysql.cj.protocol.Message;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.enums.BarType;
import org.warpexchange_learning.common.message.AbstractMessage;
import org.warpexchange_learning.common.message.TickMessage;
import org.warpexchange_learning.common.messaging.MessageConsumer;
import org.warpexchange_learning.common.messaging.Messaging;
import org.warpexchange_learning.common.messaging.MessagingFactory;
import org.warpexchange_learning.common.model.quotation.*;
import org.warpexchange_learning.common.model.support.AbstractBarEntity;
import org.warpexchange_learning.common.redis.RedisCache;
import org.warpexchange_learning.common.redis.RedisService;
import org.warpexchange_learning.common.support.LoggerSupport;
import org.warpexchange_learning.common.util.IpUtil;
import org.warpexchange_learning.common.util.JsonUtil;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;

@Component
public class QuotationService extends LoggerSupport {

    @Autowired
    private ZoneId zoneId;

    /**
     * redisService在QuotationService中的作用缓存，
     * redis中维护一个List缓存近期的ticks，同时维护四个Zset（分别对应不同的时间尺度）缓存Bar（也就是一个个K线）
     */
    @Autowired
    private RedisService redisService;

    @Autowired
    private QuotationDbService quotationDbService;

    @Autowired
    private MessagingFactory messagingFactory;

    private MessageConsumer tickConsumer;

    private String shaUpdateRecentTicksLua = null;

    private String shaUpdateBarLua = null;

    // track last processed sequence id:
    private long sequenceId;

    @PostConstruct
    public void init() {
        // init redis:
        this.shaUpdateRecentTicksLua = this.redisService.loadScriptFromClassPath("/redis/update-recent-ticks.lua");
        this.shaUpdateBarLua = this.redisService.loadScriptFromClassPath("/redis/update-bar.lua");
        // init mq:
        String groupId = Messaging.Topic.TICK.name() + "_" + IpUtil.getHostId();
        // 接受发给TICK的消息（来自交易引擎的一批ticks）
        this.tickConsumer = this.messagingFactory.createBatchMessageListener(Messaging.Topic.TICK, groupId, this::processMessages);
    }

    @PreDestroy
    public void destroy() {
        if (this.tickConsumer != null) {
            this.tickConsumer.stop();
            this.tickConsumer = null;
        }
    }

    public void processMessages(List<AbstractMessage> messages) {
        for (AbstractMessage message : messages) {
            processMessage((TickMessage) message);
        }
    }

    /**
     * QuotationService的主要任务是处理交易引擎发来的一个个ticks
     */
    public void processMessage(TickMessage message) {
        // 忽略重复的消息:
        if (message.sequenceId < this.sequenceId) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("process ticks: sequenceId = {}, {} ticks...", message.sequenceId, message.ticks.size());
        }
        // 生成[tick, tick...]列表以及合并为一个Bar:
        this.sequenceId = message.sequenceId;
        final long createdAt = message.createdAt;
        StringJoiner ticksStrJoiner = new StringJoiner(",", "[", "]");
        StringJoiner ticksJoiner = new StringJoiner(",", "[", "]");
        BigDecimal openPrice = BigDecimal.ZERO;
        BigDecimal closePrice = BigDecimal.ZERO;
        BigDecimal highPrice = BigDecimal.ZERO;
        BigDecimal lowPrice = BigDecimal.ZERO;
        BigDecimal quantity = BigDecimal.ZERO;
        // 遍历ticks找到openPrice、closePrice、highPrice、lowPrice、quantity
        for (TickEntity tick : message.ticks) {
            String json = tick.toJson();
            // ticksStrJoiner构造的结果用于update-recent-ticks.lua脚本中发布redis Pub
            ticksStrJoiner.add("\"" + json + "\"");
            // ticksJoiner构造的结果用于update-recent-ticks.lua脚本中更新ticks List
            ticksJoiner.add(json);
            if (openPrice.signum() == 0) {
                // 按ticks中的第一个tick初始化openPrice、closePrice等变量
                openPrice = tick.price;
                closePrice = tick.price;
                highPrice = tick.price;
                lowPrice = tick.price;
            } else {
                // openPrice已被设置，即第一个tick的price,随循环不断更新其他变量
                closePrice = tick.price;
                highPrice = highPrice.max(tick.price);
                lowPrice = lowPrice.min(tick.price);
            }
            // 期间的交易量不断累计
            quantity = quantity.add(tick.quantity);
        }
        long sec = createdAt / 1000;
        long min = sec / 60;
        long hour = min / 60;
        long secStartTime = sec * 1000; // 秒K的开始时间
        long minStartTime = min * 60 * 1000; // 分钟K的开始时间
        long hourStartTime = hour * 3600 * 1000; // 小时K的开始时间
        long dayStartTime = Instant.ofEpochMilli(hourStartTime).atZone(zoneId).withHour(0).toEpochSecond() * 1000; // 日K的开始时间，与TimeZone相关

        // 更新Redis最近的Ticks缓存:
        String ticksData = ticksStrJoiner.toString();
        if (logger.isDebugEnabled()) {
            logger.debug("generated ticks data: {}", ticksData);
        }
        Boolean tickOk = redisService.executeScriptReturnBoolean(
                this.shaUpdateRecentTicksLua,
                new String[]{RedisCache.Key.RECENT_TICKS},
                new String[]{String.valueOf(this.sequenceId), ticksData, ticksStrJoiner.toString()}
        );
        if (!tickOk.booleanValue()) {
            logger.warn("ticks are ignored by Redis.");
            return;
        }
        // 保存Tick至数据库:
        this.quotationDbService.saveTicks(message.ticks);

        // 更新各种类型的K线: 返回的strCreatedBars是序列化了的需要持久化的Bars，
        String strCreatedBars = this.redisService.executeScriptReturnString(
                this.shaUpdateBarLua,
                new String[]{ // KEY
                        RedisCache.Key.DAY_BARS,
                        RedisCache.Key.HOUR_BARS,
                        RedisCache.Key.MIN_BARS,
                        RedisCache.Key.SEC_BARS,
                },
                new String[]{ // ARGV
                        String.valueOf(this.sequenceId), // sequence id
                        String.valueOf(secStartTime), // sec-start-time
                        String.valueOf(minStartTime), // min-start-time
                        String.valueOf(hourStartTime), // hour-start-time
                        String.valueOf(dayStartTime), // day-start-time
                        String.valueOf(openPrice), // open
                        String.valueOf(highPrice), // high
                        String.valueOf(lowPrice), // low
                        String.valueOf(closePrice), // close
                        String.valueOf(quantity) // quantity
                }
        );
        logger.info("returned created bars: " + strCreatedBars);
        // 将Redis返回的K线保存至数据库:
        Map<BarType, BigDecimal[]> barMap = JsonUtil.readJson(strCreatedBars, TYPE_BARS);
        if (!barMap.isEmpty()) {
            SecBarEntity secBar = createBar(SecBarEntity::new, barMap.get(BarType.SEC));
            MinBarEntity minBar = createBar(MinBarEntity::new, barMap.get(BarType.MIN));
            HourBarEntity hourBar = createBar(HourBarEntity::new, barMap.get(BarType.HOUR));
            DayBarEntity dayBar = createBar(DayBarEntity::new, barMap.get(BarType.DAY));
            this.quotationDbService.saveBars(secBar, minBar, hourBar, dayBar);
        }
    }

    static <T extends AbstractBarEntity> T createBar(Supplier<T> fn, BigDecimal[] data) {
        if (data == null) {
            return null;
        }
        T t = fn.get();
        t.startTime = data[0].longValue();
        t.openPrice = data[1];
        t.highPrice = data[2];
        t.lowPrice = data[3];
        t.closePrice = data[4];
        t.quantity = data[5];
        return t;
    }

    private static final TypeReference<Map<BarType, BigDecimal[]>> TYPE_BARS = new TypeReference<>() {
    };
}
