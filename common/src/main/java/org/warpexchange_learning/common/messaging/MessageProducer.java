package org.warpexchange_learning.common.messaging;

import org.warpexchange_learning.common.message.AbstractMessage;

import java.util.List;

@FunctionalInterface
public interface MessageProducer<T extends AbstractMessage> {

    void sendMessage(T message);

    default void sendMessage(List<T> messages) {
        for (T message : messages) {
            sendMessage(message);
        }

    }

}
