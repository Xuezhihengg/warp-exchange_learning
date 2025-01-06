package org.warpexchange_learning.common.bean;

import java.math.BigDecimal;


/**
 * 对应OrderBook.book中的元素，即OrderEntity，但只取其中的price和quantity。
 * 这个quantity实际上会存储OrderEntity的unfilledQuantity。
 */
public class OrderBookItemBean {

    public BigDecimal price;
    public BigDecimal quantity;

    public OrderBookItemBean(BigDecimal price, BigDecimal quantity) {
        this.price = price;
        this.quantity = quantity;
    }

    public void addQuantity(BigDecimal quantity) {
        this.quantity = this.quantity.add(quantity);
    }
}
