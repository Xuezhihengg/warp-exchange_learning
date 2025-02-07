package org.warpexchange_learning.common.message;


import java.io.Serializable;

/**
 * Base message object for extends.
 */
public class AbstractMessage implements Serializable {

    /**
     * Reference id, or null if not set.
     * 全局唯一，用于追踪消息
     */
    public String refId = null;

    /**
     * Message created at.
     */
    public long createdAt;
}
