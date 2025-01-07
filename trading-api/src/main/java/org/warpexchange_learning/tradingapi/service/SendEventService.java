package org.warpexchange_learning.tradingapi.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.message.event.AbstractEvent;
import org.warpexchange_learning.common.messaging.MessageProducer;
import org.warpexchange_learning.common.messaging.Messaging;
import org.warpexchange_learning.common.messaging.MessagingFactory;

@Component
public class SendEventService {

    @Autowired
    private MessagingFactory messagingFactory;

    private MessageProducer<AbstractEvent> messageProducer;

    @PostConstruct
    public void init() {
        // 向定序逻辑发送消息的生产者
        this.messageProducer = messagingFactory.createMessageProducer(Messaging.Topic.SEQUENCE, AbstractEvent.class);
    }

    public void sendMessage(AbstractEvent message) {
        this.messageProducer.sendMessage(message);
    }
}
