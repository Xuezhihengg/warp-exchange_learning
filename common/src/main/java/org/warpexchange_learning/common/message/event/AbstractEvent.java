package org.warpexchange_learning.common.message.event;

import org.springframework.lang.Nullable;
import org.warpexchange_learning.common.message.AbstractMessage;

/**
 * 基本事件消息
 */
public class AbstractEvent extends AbstractMessage {

    /**
     * Message id, set after sequenced.
     */
    public long sequenceId;

    /**
     * Previous message sequence id.
     */
    public long previousId;

    /**
     * Unique ID or null if not set.
     */
    @Nullable
    public String uniqueId;
}
