package org.warpexchange_learning.common.support;

import jakarta.servlet.*;

import java.io.IOException;

public abstract class AbstractFilter extends LoggerSupport implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("init filter: {}...", getClass().getName());
    }

    @Override
    public void destroy() {
        logger.info("destroy filter: {}...", getClass().getName());
    }
}
