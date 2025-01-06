package org.warpexchange_learning.common.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.warpexchange_learning.common.util.JsonUtil;

import java.math.BigDecimal;
import java.util.List;

/**
 * 用于TradingEngineService保持最新订单薄（tradingEngineService.latestOrderBook），
 * 最新订单薄中有买、卖盘（matchEngine.buyBook,matchEngine.sellBook）的最新快照，分别为OrderBookBean.buy,OrderBookBean.sell，
 * buy与sell均为OrderBookItemBean的列表，OrderBookItemBean对应OrderBook.book中的元素，即订单OrderEntity。
 */
public class OrderBookBean {

    public static final String EMPTY = JsonUtil.writeJson(new OrderBookBean(0, BigDecimal.ZERO, List.of(), List.of()));

    @JsonIgnore
    public long sequenceId;

    public BigDecimal price;

    public List<OrderBookItemBean> buy;

    public List<OrderBookItemBean> sell;

    public OrderBookBean(long sequenceId, BigDecimal price, List<OrderBookItemBean> buy, List<OrderBookItemBean> sell) {
        this.sequenceId = sequenceId;
        this.price = price;
        this.buy = buy;
        this.sell = sell;
    }
}
