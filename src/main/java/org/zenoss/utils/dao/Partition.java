/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao;

import java.sql.Timestamp;

/**
 * Represents the configuration data for a table partition
 * necessary for creating external indexes.
 */
public interface Partition {

    public String getTableName();
    public String getPartitionColumn();
    public String getPartitionName();
    public Timestamp getRangeLessThan();
    public Timestamp getRangeMinimum();
}
