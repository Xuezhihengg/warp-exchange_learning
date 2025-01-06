package org.warpexchange_learning.common.message;

import org.warpexchange_learning.common.model.quotation.TickEntity;

import java.util.List;



public class TickMessage extends AbstractMessage {

    public long sequenceId;

    public List<TickEntity> ticks;
}
