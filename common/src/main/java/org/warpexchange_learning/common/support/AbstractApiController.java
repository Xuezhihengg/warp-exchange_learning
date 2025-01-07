package org.warpexchange_learning.common.support;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.warpexchange_learning.common.ApiError;
import org.warpexchange_learning.common.ApiErrorResponse;
import org.warpexchange_learning.common.ApiException;

/**
 * 供了一个通用的异常处理方法 handleException，
 * 该方法会捕获 ApiException 类型的异常，并返回一个结构化的错误响应（ApiErrorResponse）
 */
public abstract class AbstractApiController extends LoggerSupport {

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(ApiException.class)
    @ResponseBody
    public ApiErrorResponse handleException(HttpServletResponse response, Exception ex) throws Exception{
        response.setContentType("application/json;charset=utf-8");
        ApiException apiEx = null;
        if (ex instanceof ApiException) {
            apiEx = (ApiException) ex;
        } else {
            apiEx = new ApiException(ApiError.INTERNAL_SERVER_ERROR, null, ex.getMessage());
        }
        return apiEx.error;
    }
}
