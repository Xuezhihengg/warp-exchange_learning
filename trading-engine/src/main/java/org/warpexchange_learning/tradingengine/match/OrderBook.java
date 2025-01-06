package org.warpexchange_learning.tradingengine.match;

import jakarta.persistence.criteria.Order;
import org.warpexchange_learning.common.bean.OrderBookItemBean;
import org.warpexchange_learning.common.enums.Direction;
import org.warpexchange_learning.common.model.trade.OrderEntity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

public class OrderBook {
    public final Direction direction;
    public final TreeMap<OrderKey, OrderEntity> book;

    public OrderBook(Direction direction) {
        this.direction = direction;
        this.book = new TreeMap<>(direction == Direction.BUY ? SORT_BUY : SORT_SELL);
    }

    public OrderEntity getFirst() {
        return this.book.isEmpty() ? null : this.book.firstEntry().getValue();
    }

    public boolean remove(OrderEntity order) {
        return this.book.remove(new OrderKey(order.sequenceId, order.price)) != null;
    }

    public boolean add(OrderEntity order) {
        return this.book.put(new OrderKey(order.sequenceId, order.price), order) == null;
    }

    public boolean exist(OrderEntity order) {
        return this.book.containsKey(new OrderKey(order.sequenceId, order.price));
    }

    public int size() {return this.book.size();}

    public List<OrderBookItemBean> getOrderBook(int maxDepth){
        List<OrderBookItemBean> items = new ArrayList<>(maxDepth);
        OrderBookItemBean prevItem = null;
        for (OrderKey key : this.book.keySet()) {
            OrderEntity order = this.book.get(key);
            if (prevItem == null) {
                // 处理第一个订单
                prevItem = new OrderBookItemBean(order.price, order.unfilledQuantity);
                items.add(prevItem);
            } else {
                if (order.price.compareTo(prevItem.price) == 0) {
                    // 合并相同价格的订单
                    prevItem.addQuantity(order.unfilledQuantity);
                } else {
                    // 处理不同价格的订单
                    if (items.size() >= maxDepth) break;
                    prevItem = new OrderBookItemBean(order.price, order.unfilledQuantity);
                    items.add(prevItem);
                }
            }
        }
        return items;
    }


    private static final Comparator<OrderKey> SORT_SELL = new Comparator<>() {
        @Override
        public int compare(OrderKey o1, OrderKey o2) {
            // 价格低在前:
            int cmp = o1.price().compareTo(o2.price());
            // 时间早在前:
            return cmp == 0 ? Long.compare(o1.sequenceId(), o2.sequenceId()) : cmp;
        }
    };

    private static final Comparator<OrderKey> SORT_BUY = new Comparator<>() {
        @Override
        public int compare(OrderKey o1, OrderKey o2) {
            // 价格高在前:
            int cmp = o2.price().compareTo(o1.price());
            // 时间早在前:
            return cmp == 0 ? Long.compare(o1.sequenceId(), o2.sequenceId()) : cmp;
        }
    };
}
