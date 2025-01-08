package org.warpexchange_learning.quotation.quotation;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.warpexchange_learning.common.model.quotation.*;
import org.warpexchange_learning.common.support.AbstractDbService;

import java.util.List;

@Component
@Transactional
public class QuotationDbService extends AbstractDbService {

    public void saveBars(SecBarEntity sec, MinBarEntity min, HourBarEntity hour, DayBarEntity day) {
        if (sec != null) {
            this.db.insertIgnore(sec);
        }
        if (min != null) {
            this.db.insertIgnore(min);
        }
        if (hour != null) {
            this.db.insertIgnore(hour);
        }
        if (day != null) {
            this.db.insertIgnore(day);
        }
    }

    public void saveTicks(List<TickEntity> ticks) {
        db.insertIgnore(ticks);
    }


}
