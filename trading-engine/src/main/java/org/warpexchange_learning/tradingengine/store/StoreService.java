package org.warpexchange_learning.tradingengine.store;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.warpexchange_learning.common.db.DbTemplate;
import org.warpexchange_learning.common.message.event.AbstractEvent;
import org.warpexchange_learning.common.messaging.MessageTypes;
import org.warpexchange_learning.common.model.support.EntitySupport;
import org.warpexchange_learning.common.model.trade.EventEntity;
import org.warpexchange_learning.common.support.LoggerSupport;


@Component
@Transactional
public class StoreService extends LoggerSupport {

    @Autowired
    MessageTypes messageTypes;

    @Autowired
    DbTemplate dbTemplate;

    // 从数据库中恢复丢失的消息
    public List<AbstractEvent> loadEventsFromDb(long lastEventId) {
        List<EventEntity> events = this.dbTemplate.from(EventEntity.class).where("sequenceId > ?", lastEventId).orderBy("sequenceId").limit(10000).list();
        return events.stream().map(event -> (AbstractEvent) messageTypes.deserialize(event.data)).collect(Collectors.toList());
    }

    public void insertIgnore(List<? extends EntitySupport> list) {
        dbTemplate.insertIgnore(list);
    }
}
