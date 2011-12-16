/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao.impl;

import org.zenoss.utils.dao.Partition;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the configuration of a PostgreSQL "partition" (really an 
 * inheriting table, loaded from the pg_class, pg_inherits, pg_constraints 
 * tables).
 */
public class PostgreSqlPartition implements Partition {

    private String partitionName;
    private Timestamp rangeLessThan;
    private Timestamp rangeMinimum;

    /* package */ PostgreSqlPartition() {
    }

    /* package */ PostgreSqlPartition(String partitionName,
            Timestamp rangeLessThan,
            Timestamp rangeMinimum) {
        this.partitionName = partitionName;
        this.rangeLessThan = rangeLessThan;
        this.rangeMinimum = rangeMinimum;
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
                  "PostgreSqlPartition [partitionName=%s, rangeLessThan=%s, rangeMinimum=%s]",
                partitionName, rangeLessThan, rangeMinimum);
    }

    private static final Pattern TIMESTAMP_REGEX =
            Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})([+-]\\d{2})?$");

    private static Timestamp parseTimestamp(String timestamp) {
        Matcher matcher = TIMESTAMP_REGEX.matcher(timestamp);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Timestamp: " + timestamp + " doesn't match pattern");
        }
        Calendar calendar = Calendar.getInstance();
        if (matcher.groupCount() == 7) {
            String tzId = "GMT" + matcher.group(7);
            TimeZone tz = TimeZone.getTimeZone(tzId);
            calendar.setTimeZone(tz);
        }
        calendar.set(Calendar.YEAR, Integer.parseInt(matcher.group(1)));
        calendar.set(Calendar.MONTH, Integer.parseInt(matcher.group(2)) - 1);
        calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(matcher.group(3)));
        calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(matcher.group(4)));
        calendar.set(Calendar.MINUTE, Integer.parseInt(matcher.group(5)));
        calendar.set(Calendar.SECOND, Integer.parseInt(matcher.group(6)));
        calendar.set(Calendar.MILLISECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }

    public static PostgreSqlPartition fromPostgreSqlResultSetFields(Map<String, Object> fields) {
        PostgreSqlPartition partition = new PostgreSqlPartition();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            final String field = entry.getKey();
            final Object val = entry.getValue();
            if ("partition_name".equals(field)) {
                partition.partitionName = (String) val;
            } else if ("before_check".equals(field)) {
                String checkConstraint = (String) val;
                String beforeTs = checkConstraint.split("'")[1];
                partition.rangeLessThan = parseTimestamp(beforeTs);
            } else if ("on_or_after_check".equals(field)) {
                String checkConstraint = (String) val;
                if (val == null) {
                    continue;
                }
                String onOrAfterTs = checkConstraint.split("'")[1];
                partition.rangeMinimum = parseTimestamp(onOrAfterTs);
            }
        }
        return partition;
    }
}
