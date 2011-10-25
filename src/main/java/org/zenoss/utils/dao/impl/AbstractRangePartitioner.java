/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.utils.dao.Partition;
import org.zenoss.utils.dao.RangePartitioner;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Class used to simplify creation of range partitions on integer columns.
 * Implementation class for RangePartitioner. This class does not currently 
 * support ranges on date columns (with custom expressions), sub-partitioning,
 * or reorganizing partitions (merging / splitting).
 */
public abstract class AbstractRangePartitioner implements RangePartitioner {

    private static final Logger logger = LoggerFactory
            .getLogger(AbstractRangePartitioner.class);

    protected static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyyMMdd_HHmmss");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    protected final SimpleJdbcTemplate template;
    protected final String databaseName;
    protected final String tableName;
    protected final String columnName;
    protected final int durationInMillis;

    /**
     * Creates a range partitioner helper class which creates partitions of the
     * specified range on the table.
     * 
     * @param ds
     *            DataSource to create template
     * @param databaseName
     *            Database name.
     * @param tableName
     *            Table name.
     * @param columnName
     *            Column name where range partitioning should be performed.
     * @param duration
     *            Duration of each range partition.
     * @param unit
     *            Unit of duration.
     */
    public AbstractRangePartitioner(DataSource ds, String databaseName,
            String tableName, String columnName, long duration, TimeUnit unit) {
        if (ds == null || databaseName == null || tableName == null
                || unit == null) {
            throw new NullPointerException();
        }
        if (duration <= 0) {
            throw new IllegalArgumentException("Duration <= 0");
        }
        this.template = new SimpleJdbcTemplate(ds);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        long millis = unit.toMillis(duration);
        if (millis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Overflow");
        }
        this.durationInMillis = (int) millis;
    }

    /**
     * Creates the specified number of past and future partitions for the table.
     * No new partitions are created within existing ranges (no splitting of
     * existing partitions).
     * 
     * @param pastPartitions
     *            The number of past partitions to create in the table.
     * @param futurePartitions
     *            The number of future partitions to create in the table.
     * @return The number of created partitions.
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public int createPartitions(int pastPartitions, int futurePartitions) {
        final List<Partition> currentPartitions = listPartitions();
        Timestamp lastPartitionTimestamp = new Timestamp(0L);
        if (!currentPartitions.isEmpty()) {
            lastPartitionTimestamp = currentPartitions
                    .get(currentPartitions.size() - 1).getRangeLessThan();
        }
        final List<Timestamp> partitionTimestamps =
                calculatePartitionTimestamps(pastPartitions,
                        futurePartitions, lastPartitionTimestamp);
        createPartitions(currentPartitions, partitionTimestamps);
        return partitionTimestamps.size();
    }

    protected abstract void createPartitions(List<Partition> currentPartitions,
            List<Timestamp> partitionTimestamps);

    /**
     * Calculate which partition timestamps should be created assuming the
     * specified number of past partitions and future partitions. This method
     * ensures there are no overlapping partitions by not returning any
     * timestamps which are less than the current maximum partition.
     * 
     * @param pastPartitions
     *            The number of partitions in the past to create.
     * @param futurePartitions
     *            The number of future partitions to create.
     * @param currentMaxPartition
     *            The current maximum partition in the table (used to ensure no
     *            partitions spanning existing ranges are created).
     * @return A list of all partition timestamps which should be created in the
     *         table.
     */
    protected List<Timestamp> calculatePartitionTimestamps(int pastPartitions,
            int futurePartitions, Timestamp currentMaxPartition) {
        if (pastPartitions < 0 || futurePartitions < 0) {
            throw new IllegalArgumentException(
                    "Past or future partitions cannot be negative.");
        }
        final int totalPartitions = pastPartitions + futurePartitions;
        final List<Timestamp> timestamps = new ArrayList<Timestamp>(totalPartitions);
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        /* Create on boundary of interval */
        long initialTime = cal.getTimeInMillis() + this.durationInMillis;
        initialTime -= initialTime % this.durationInMillis;
        cal.setTimeInMillis(initialTime);
        for (int i = 0; i < pastPartitions; i++) {
            cal.add(Calendar.MILLISECOND, -this.durationInMillis);
        }
        for (int i = 0; i < totalPartitions; i++) {
            final Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            if (timestamp.after(currentMaxPartition)) {
                timestamps.add(timestamp);
            }
            cal.add(Calendar.MILLISECOND, this.durationInMillis);
        }
        return timestamps;
    }
}
