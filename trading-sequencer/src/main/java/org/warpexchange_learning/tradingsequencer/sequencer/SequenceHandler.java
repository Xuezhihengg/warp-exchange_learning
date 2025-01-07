package org.warpexchange_learning.tradingsequencer.sequencer;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.warpexchange_learning.common.message.event.AbstractEvent;
import org.warpexchange_learning.common.messaging.MessageTypes;
import org.warpexchange_learning.common.model.trade.EventEntity;
import org.warpexchange_learning.common.model.trade.UniqueEventEntity;
import org.warpexchange_learning.common.support.AbstractDbService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process events as batch.
 */
@Component
@Transactional(rollbackFor = Throwable.class)
public class SequenceHandler extends AbstractDbService {

    private long lastTimestamp = 0;

    /**
     * Set sequence for each message, persist into database as batch.
     *
     * @return Sequenced messages.
     */
    public List<AbstractEvent> sequenceMessages(final MessageTypes messageTypes, final AtomicLong sequence, final List<AbstractEvent> messages) throws Exception {
        // 检测系统是否出现了时间倒退
        final long t = System.currentTimeMillis();
        if (t < this.lastTimestamp) {
            logger.warn("[Sequence] current time {} is turned back from {}!", t, this.lastTimestamp);
        }
        // 利用UniqueEventEntity去重:
        List<UniqueEventEntity> uniques = null;
        Set<String> uniqueKeys = null;
        List<AbstractEvent> sequencedMessages = new ArrayList<>(messages.size());  // 用于保存定序好的message
        List<EventEntity> events = new ArrayList<>(messages.size());  // 用于数据库批量写入
        for (AbstractEvent message : messages) {
            // 对于每一条消息，先根据uniqueId进行去重，然后为其设置sequenceId和previousId
            UniqueEventEntity unique = null;
            final String uniqueId = message.uniqueId;
            if (uniqueId != null) {
                // uniqueKeys在uniqueKeys中或在数据库中,则该消息为重复消息，跳过处理
                if ((uniqueKeys != null && uniqueKeys.contains(uniqueId)) || db.fetch(UniqueEventEntity.class, uniqueId) != null) {
                    logger.warn("ignore processed unique message: {}", message);
                    continue;
                }
                unique = new UniqueEventEntity();
                unique.uniqueId = uniqueId;
                unique.createdAt = message.createdAt;
                if (uniques == null) {
                    uniques = new ArrayList<>();
                }
                uniques.add(unique);
                if (uniqueKeys == null) {
                    uniqueKeys = new HashSet<>();
                }
                uniqueKeys.add(uniqueId);
                logger.info("unique event {} sequenced.", uniqueId);
            }
            final long previousId = sequence.get();
            final long currentId = sequence.getAndIncrement();

            // 为message设置sequenceId和previousId
            message.sequenceId = currentId;
            message.previousId = previousId;
            message.createdAt = this.lastTimestamp;

            // 如果此消息关联了UniqueEvent，给UniqueEvent加上相同的sequenceId：
            if (unique != null) {
                unique.sequenceId = message.sequenceId;
            }
            // 准备写入数据库的Event:
            EventEntity event = new EventEntity();
            event.sequenceId = currentId;
            event.previousId = previousId;
            event.data = messageTypes.serialize(message);
            event.createdAt = this.lastTimestamp; // same as message.createdAt

            events.add(event);
            sequencedMessages.add(message);
        }
        if (uniques != null) {
            db.insert(uniques);  // 这是UniqueEventEntity入库，用于前面去重处理
        }
        db.insert(events);  // 这是EventEntity入库，用于丢失事件追溯
        return sequencedMessages;
    }

    /**
     * 用于定序器重启后正确初始化下一个序列号
     */
    public long getMaxSequenceId() {
        EventEntity last = db.from(EventEntity.class).orderBy("sequenceId").desc().first();
        if (last == null) {
            logger.info("no max sequenceId found. set max sequenceId = 0.");
            return 0;
        }
        this.lastTimestamp = last.createdAt;
        logger.info("find max sequenceId = {}, last timestamp = {}", last.sequenceId, this.lastTimestamp);
        return last.sequenceId;
    }
}
