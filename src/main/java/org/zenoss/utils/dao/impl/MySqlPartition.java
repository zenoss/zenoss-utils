/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao.impl;

import org.zenoss.utils.dao.Partition;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Represents the configuration of a MySQL table partition (loaded from the
 * INFORMATION_SCHEMA.PARTITIONS table).
 */
public class MySqlPartition implements Partition {
    private String partitionName;
    private Timestamp rangeLessThan = null;
    private Timestamp rangeMinimum = null;
    private String partitionMethod;
    private String partitionDescription;

    private MySqlPartition() {
    }

    @Override
    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public Timestamp getRangeLessThan() {
        return rangeLessThan;
    }

    @Override
    public Timestamp getRangeMinimum() {
        return rangeMinimum;
    }

    void setRangeMinimum(Timestamp minimum) {
        this.rangeMinimum = minimum;
    }

    @Override
    public String toString() {
        return "MySqlPartition{" +
                "partitionName='" + partitionName + '\'' +
                ", rangeLessThan=" + rangeLessThan +
                ", rangeMinimum=" + rangeMinimum +
                ", partitionMethod='" + partitionMethod + '\'' +
                ", partitionDescription='" + partitionDescription + '\'' +
                '}';
    }

    public static MySqlPartition fromMySqlResultSetFields(Map<String, Object> fields) {
        MySqlPartition partition = new MySqlPartition();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            final String field = entry.getKey();
            final Object val = entry.getValue();
            if (val == null) {
                continue;
            }
            if ("PARTITION_NAME".equals(field)) {
                partition.partitionName = (String) val;
            } else if ("PARTITION_METHOD".equals(field)) {
                partition.partitionMethod = (String) val;
            } else if ("PARTITION_DESCRIPTION".equals(field)) {
                partition.partitionDescription = (String) val;
            }
        }
        if ("RANGE".equals(partition.partitionMethod)) {
            if ("MAXVALUE".equalsIgnoreCase(partition.partitionDescription)) {
                partition.rangeLessThan = new Timestamp(Long.MAX_VALUE);
            } else {
                partition.rangeLessThan = new Timestamp(Long.parseLong(partition.partitionDescription));
            }
        }
        return partition;
    }
}
