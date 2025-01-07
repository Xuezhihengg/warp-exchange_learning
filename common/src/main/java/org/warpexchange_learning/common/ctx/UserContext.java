package org.warpexchange_learning.common.ctx;

import org.warpexchange_learning.common.ApiError;
import org.warpexchange_learning.common.ApiException;

/**
 * Holds user context in thread-local.
 * 为每个线程提供一个隔离的用户上下文存储容器，
 * 用于在服务端处理请求时临时保存和访问与当前线程相关的用户信息（通常是用户 ID）
 */
public class UserContext implements AutoCloseable{

    static final ThreadLocal<Long> THREAD_LOCAL_CTX = new ThreadLocal<>();

    /**
     * Get current user id, or throw exception if no user.
     */
    public static Long getRequiredUserId() {
        Long userId = getUserId();
        if (userId == null) {
            throw new ApiException(ApiError.AUTH_SIGNIN_REQUIRED, null, "Need signin first.");
        }
        return userId;
    }

    /**
     * Get current user id, or null if no user.
     */
    public static Long getUserId() {
        return THREAD_LOCAL_CTX.get();
    }

    public UserContext(Long userId) {
        THREAD_LOCAL_CTX.set(userId);
    }

    @Override
    public void close(){
        THREAD_LOCAL_CTX.remove();
    }
}
