package org.warpexchange_learning.common.support;


import org.springframework.beans.factory.annotation.Autowired;
import org.warpexchange_learning.common.db.DbTemplate;

/**
 * Service with db support.
 */
public abstract class AbstractDbService extends LoggerSupport {

    @Autowired
    protected DbTemplate db;
}
