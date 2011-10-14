/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao.impl;

import java.sql.Timestamp;
import java.util.Map;

import org.zenoss.utils.dao.Partition;

/**
 * Represents the configuration of a PostgreSQL "partition" (really an 
 * inheriting table, loaded from the pg_class, pg_inherits, pg_constraints 
 * tables).
 */
public class PostgreSqlPartition implements Partition {

    private String tableName;
    private String columnName;
    private String partitionName;
    private Timestamp rangeLessThan;
    private Timestamp rangeMinimum;

    /* package */ PostgreSqlPartition() {
    }

    /* package */ PostgreSqlPartition(String tableName,
            String columnName,
            String partitionName,
            Timestamp rangeLessThan,
            Timestamp rangeMinimum) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.partitionName = partitionName;
        this.rangeLessThan = rangeLessThan;
        this.rangeMinimum = rangeMinimum;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getPartitionColumn() {
        return columnName;
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

    @Override
    public String toString() {
        return String.format(
                  "PostgreSqlPartition [tableName=%s, columnName=%s, "
                + "partitionName=%s, rangeLessThan=%s, rangeMinimum=%s]",
                tableName, columnName,
                partitionName, rangeLessThan, rangeMinimum);
    }

    public static PostgreSqlPartition fromPostgreSqlResultSetFields(
            String tableName, String columnName, Map<String, Object> fields) {
        PostgreSqlPartition partition = new PostgreSqlPartition();
        partition.tableName = tableName;
        partition.columnName = columnName;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            final String field = entry.getKey();
            final Object val = entry.getValue();
            if ("partition_name".equals(field)) {
                partition.partitionName = (String) val;
            } else if ("before_check".equals(field)) {
                String checkConstraint = (String) val;
                String beforeTs = checkConstraint.split("'")[1];
                partition.rangeLessThan = Timestamp.valueOf(beforeTs);
            } else if ("on_or_after_check".equals(field)) {
                String checkConstraint = (String) val;
                if (val == null) {
                    continue;
                }
                String onOrAfterTs = checkConstraint.split("'")[1];
                partition.rangeMinimum = Timestamp.valueOf(onOrAfterTs);
            }
        }
        return partition;
    }
}
