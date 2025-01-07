package org.warpexchange_learning.common;

/**
 * ApiError constants.
 */
public enum ApiError {

    PARAMETER_INVALID,  // 参数无效

    AUTH_SIGNIN_REQUIRED,  // 需要身份验证登录

    AUTH_SIGNIN_FAILED, // 身份验证失败

    USER_CANNOT_SIGNIN, // 用户无法登陆

    NO_ENOUGH_ASSET,  // 资产不足

    ORDER_NOT_FOUND,  // 订单未发现

    OPERATION_TIMEOUT,  // 操作超时

    INTERNAL_SERVER_ERROR;  // 服务器错误
}
