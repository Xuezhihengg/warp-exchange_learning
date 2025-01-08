package org.warpexchange_learning.common.model.quotation;


import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import org.warpexchange_learning.common.model.support.AbstractBarEntity;

/**
 * Store bars of day.
 */
@Entity
@Table(name = "day_bars")
public class DayBarEntity extends AbstractBarEntity {

}
