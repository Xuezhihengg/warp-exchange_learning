package org.warpexchange_learning.ui.ui.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.ApiError;
import org.warpexchange_learning.common.ApiException;
import org.warpexchange_learning.common.bean.AuthToken;
import org.warpexchange_learning.common.client.RestClient;
import org.warpexchange_learning.common.ctx.UserContext;
import org.warpexchange_learning.common.support.AbstractFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * ProxyFilter: forward /api/* to backend api.
 * 把ProxyFilter挂载到/api/*，通过UI转发请求的目的是简化页面JavaScript调用API，一是不再需要跨域，
 * 二是UI已经经过了登录认证，转发过程中自动生成一次性Token来调用API，这样JavaScript不再关心如何生成Authorization头。
 */
@Component
public class ProxyFilterRegistrationBean extends FilterRegistrationBean<Filter> {

    @Autowired
    RestClient tradingApiClient;

    @Autowired
    ObjectMapper objectMapper;

    @Value("#{exchangeConfiguration.hmacKey}")
    String hmacKey;

    @PostConstruct
    public void init() {
        ProxyFilter filter = new ProxyFilter();
        setFilter(filter);
        addUrlPatterns("/api/*");
        setName(filter.getClass().getSimpleName());
        setOrder(200);
    }

    class ProxyFilter extends AbstractFilter {

        @Override
        public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
            HttpServletRequest request = (HttpServletRequest) req;
            HttpServletResponse response = (HttpServletResponse) resp;
            String path = request.getRequestURI();
            logger.info("process {} {}...", request.getMethod(), path);
            Long userId = UserContext.getUserId();
            logger.info("process with userId={}...", userId);
            // request转发给API模块
            proxyForward(userId, request, response);
        }

        private void proxyForward(Long userId, HttpServletRequest request, HttpServletResponse response) throws IOException {
            String authToken = null;
            // 根据UI模块的登陆信息为API模块构建Authorization头用于API模块校验登陆信息
            if (userId != null) {
                AuthToken token = new AuthToken(userId, System.currentTimeMillis() + 60_000);
                authToken = "Bearer " + token.toSecureString(hmacKey);
            }
            String responseJson = null;
            try {
                if ("GET".equals(request.getMethod())) {
                    Map<String, String[]> params = request.getParameterMap();
                    Map<String, String> query = params.isEmpty() ? null : convertParams(params);
                    responseJson = tradingApiClient.get(String.class, request.getRequestURI(), authToken, query);
                } else if ("POST".equals(request.getMethod())) {
                    responseJson = tradingApiClient.post(String.class, request.getRequestURI(), authToken,
                            readBody(request));
                }
                response.setContentType("application/json;charset=utf-8");
                PrintWriter pw = response.getWriter();
                pw.write(responseJson);
                pw.flush();
            } catch (ApiException e) {
                logger.warn(e.getMessage(), e);
                writeApiException(request, response, e);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                writeApiException(request, response,
                        new ApiException(ApiError.INTERNAL_SERVER_ERROR, null, e.getMessage()));
            }
        }

        private String readBody(HttpServletRequest request) throws IOException {
            StringBuilder sb = new StringBuilder(2048);
            char[] buffer = new char[256];
            BufferedReader reader = request.getReader();
            for (;;) {
                int n = reader.read(buffer);
                if (n == (-1)) {
                    break;
                }
                sb.append(buffer, 0, n);
            }
            return sb.toString();
        }

        private Map<String, String> convertParams(Map<String, String[]> params) {
            Map<String, String> map = new HashMap<>();
            params.forEach((param, values) -> {
                map.put(param, values[0]);
            });
            return map;
        }

        private void writeApiException(HttpServletRequest request, HttpServletResponse response, ApiException e)
                throws IOException {
            response.setStatus(400);
            response.setContentType("application/json;charset=utf-8");
            PrintWriter pw = response.getWriter();
            pw.write(objectMapper.writeValueAsString(e.error));
            pw.flush();
        }
    }
}
