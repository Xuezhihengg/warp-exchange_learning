package org.warpexchange_learning.common.model.trade;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.warpexchange_learning.common.enums.AssetEnum;
import org.warpexchange_learning.common.model.support.EntitySupport;

import java.math.BigDecimal;

/**
 * 用户转入转出的日志表
 */
@Entity
@Table(name = "transfer_logs")
public class TransferLogEntity implements EntitySupport {

    @Id
    @Column(nullable = false, updatable = false, length = VAR_ENUM)
    public String transferId;

    @Column(nullable = false, updatable = false, length = VAR_ENUM)
    public AssetEnum asset;

    @Column(nullable = false, updatable = false, precision = PRECISION, scale = SCALE)
    public BigDecimal amount;

    @Column(nullable = false, updatable = false)
    public Long userId;

    @Column(nullable = false, updatable = false)
    public long createdAt;

    @Column(nullable = false, length = VAR_ENUM)
    public String type;

    @Column(nullable = false, length = VAR_ENUM)
    public String status;
}
