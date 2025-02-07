package org.warpexchange_learning.tradingapi.service;

import okhttp3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.ApiError;
import org.warpexchange_learning.common.ApiException;
import org.warpexchange_learning.common.support.LoggerSupport;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Proxy to access trading engine.
 * trading-api与trading-engine之间的通信一部分是通过消息系统，另一部分
 * 通过TradingEngineApiProxyService使用内部http通信
 */
@Component
public class TradingEngineApiProxyService extends LoggerSupport {

    @Value("#{exchangeConfiguration.apiEndpoints.tradingEngineApi}")
    private String tradingEngineInternalApiEndpoint;

    private OkHttpClient okHttpClient = new OkHttpClient.Builder()
            // set connect timeout:
            .connectTimeout(1,TimeUnit.SECONDS)
            // set read timeout:
            .readTimeout(1, TimeUnit.SECONDS)
            // set connection pool:
            .connectionPool(new ConnectionPool(20, 60, TimeUnit.SECONDS))
            // do not retry:
            .retryOnConnectionFailure(false).build();

    public String get(String url) throws IOException {
        Request request = new Request.Builder().url(tradingEngineInternalApiEndpoint + url).header("Accept", "*/*").build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                logger.error("Internal api failed with code {}: {}", Integer.valueOf(response.code()), url);
                throw new ApiException(ApiError.OPERATION_TIMEOUT, null, "operation timeout.");
            }
            try (ResponseBody body = response.body()) {
                String json = body.string();
                if (json == null || json.isEmpty()) {
                    logger.error("Internal api failed with code 200 but empty response: {}", json);
                    throw new ApiException(ApiError.INTERNAL_SERVER_ERROR, null, "response is empty.");
                }
                return json;
            }
        }
    }
}
