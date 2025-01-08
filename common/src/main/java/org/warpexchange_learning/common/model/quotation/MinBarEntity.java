package org.warpexchange_learning.common.model.quotation;

import org.warpexchange_learning.common.model.support.AbstractBarEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * Store bars of minute.
 */
@Entity
@Table(name = "min_bars")
public class MinBarEntity extends AbstractBarEntity {

}
