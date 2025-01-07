package org.warpexchange_learning.common.redis;

import io.lettuce.core.api.sync.RedisCommands;

/**
 * 这个单方法接口是用于同步执行redis命令的callback
 */
@FunctionalInterface
public interface SyncCommandCallback<T> {

    T doInConnection(RedisCommands<String, String> commands);
}
