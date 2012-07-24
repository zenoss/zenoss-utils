/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.utils.dao;

import java.sql.Timestamp;

/**
 * Represents the configuration data for a table partition
 * necessary for creating external indexes.
 */
public interface Partition {
    public String getPartitionName();
    public Timestamp getRangeLessThan();
    public Timestamp getRangeMinimum();
}
