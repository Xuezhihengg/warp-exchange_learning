package org.warpexchange_learning.tradingengine.match;

import java.math.BigDecimal;

public record OrderKey(long sequenceId, BigDecimal price) {
}
