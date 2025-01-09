package org.warpexchange_learning.common.bean;


import org.warpexchange_learning.common.enums.MatchType;

import java.math.BigDecimal;

public record SimpleMatchDetailRecord(BigDecimal price, BigDecimal quantity, MatchType type) {
}
