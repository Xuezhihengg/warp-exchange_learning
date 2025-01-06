package org.warpexchange_learning.common.messaging;


import org.warpexchange_learning.common.message.AbstractMessage;

import java.util.List;

@FunctionalInterface
public interface BatchMessageHandler<T extends AbstractMessage> {

    void processMessages(List<T> messages);

}
