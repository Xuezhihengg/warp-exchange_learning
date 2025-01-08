package org.warpexchange_learning.common.model.quotation;

import org.warpexchange_learning.common.model.support.AbstractBarEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * Store bars of hour.
 */
@Entity
@Table(name = "hour_bars")
public class HourBarEntity extends AbstractBarEntity {

}
