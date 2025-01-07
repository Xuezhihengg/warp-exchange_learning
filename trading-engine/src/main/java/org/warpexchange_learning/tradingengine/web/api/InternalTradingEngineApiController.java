package org.warpexchange_learning.tradingengine.web.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.warpexchange_learning.common.enums.AssetEnum;
import org.warpexchange_learning.common.model.trade.OrderEntity;
import org.warpexchange_learning.common.support.LoggerSupport;
import org.warpexchange_learning.tradingengine.assets.Asset;
import org.warpexchange_learning.tradingengine.assets.AssetService;
import org.warpexchange_learning.tradingengine.order.OrderService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@RestController
@RequestMapping("/internal")
public class InternalTradingEngineApiController extends LoggerSupport {

    @Autowired
    OrderService orderService;

    @Autowired
    AssetService assetService;

    @GetMapping("/{userId}/assets")
    public Map<AssetEnum, Asset> getAssets(@PathVariable("userId") Long userId) {
        return assetService.getAssets(userId);
    }

    @GetMapping("/{userId}/orders")
    public List<OrderEntity> getOrders(@PathVariable("userId") Long userId) {
        ConcurrentMap<Long, OrderEntity> orders = orderService.getUserOrders(userId);
        if (orders == null || orders.isEmpty()) {
            return List.of();
        }
        List<OrderEntity> list = new ArrayList<>(orders.size());
        for (OrderEntity order : orders.values()) {
            OrderEntity copy = null;
            // 基于版本号的乐观锁机制
            while (copy == null) {
                copy = order.copy();
            }
            list.add(copy);
        }
        return list;
    }

    @GetMapping("/{userId}/orders/{orderId}")
    public OrderEntity getOrder(@PathVariable("userId") Long userId, @PathVariable("orderId") Long orderId) {
        OrderEntity order = orderService.getOrder(orderId);
        if (order == null || order.userId.longValue() != userId.longValue()) {
            return null;
        }
        return order;
    }
}
