package org.warpexchange_learning.ui.ui.web;


import jakarta.annotation.PostConstruct;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.bean.AuthToken;
import org.warpexchange_learning.common.ctx.UserContext;
import org.warpexchange_learning.common.support.AbstractFilter;
import org.warpexchange_learning.common.user.UserService;

import java.io.IOException;

/**
 * UIFilter: try parse user from cookie.
 */
@Component
public class UIFilterRegistrationBean extends FilterRegistrationBean<Filter> {

    @Autowired
    UserService userService;

    @Autowired
    CookieService cookieService;

    @PostConstruct
    public void init() {
        UIFilter filter = new UIFilter();
        setFilter(filter);
        addUrlPatterns("/*");
        setName(filter.getClass().getSimpleName());
        setOrder(100);
    }

    class UIFilter extends AbstractFilter {

        @Override
        public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
                throws IOException, ServletException {
            HttpServletRequest request = (HttpServletRequest) req;
            HttpServletResponse response = (HttpServletResponse) resp;
            String path = request.getRequestURI();
            if (logger.isDebugEnabled()) {
                logger.debug("process {} {}...", request.getMethod(), path);
            }
            // set default encoding:
            request.setCharacterEncoding("UTF-8");
            response.setCharacterEncoding("UTF-8");
            response.setContentType("text/html;charset=UTF-8");
            // try parse user:
            AuthToken auth = cookieService.findSessionCookie(request);
            if (auth != null && auth.isAboutToExpire()) {
                logger.info("refresh session cookie...");
                cookieService.setSessionCookie(request, response, auth.refresh());
            }
            Long userId = auth == null ? null : auth.userId();
            if (logger.isDebugEnabled()) {
                logger.debug("parsed user {} from session cookie.", userId);
            }
            // UserContext中的登陆信息是线程隔离的，这里设置的userId只用于UI模块，是从浏览器Cookie读出来的
            // API模块也要求身份验证，是通过Authorization Bearer头验证的，
            // UI模块做了request转发给API模块，API模块的身份验证将不需要浏览器JavaScript实现，而是UI模块ProxyFilter实现
            try (UserContext ctx = new UserContext(userId)) {
                chain.doFilter(request, response);
            }
        }
    }
}
