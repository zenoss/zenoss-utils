/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.utils.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.utils.dao.Partition;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * MySQL compatible implementation of RangePartitioner
 */
public class MySqlRangePartitioner extends AbstractRangePartitioner {

    private static final Logger logger = LoggerFactory
            .getLogger(MySqlRangePartitioner.class);

    public MySqlRangePartitioner(DataSource ds,
            String tableName, String columnName, long duration, TimeUnit unit) {
        super(ds, tableName, columnName, duration, unit);
    }

    protected void createPartitions(List<Partition> currentPartitions,
            List<Timestamp> partitionTimestamps) {
        if (currentPartitions.isEmpty()) {
            final StringBuilder sb = new StringBuilder(String.format(
                    "ALTER TABLE %s PARTITION BY RANGE(%s) (",
                    this.tableName, this.columnName));
            for (Iterator<Timestamp> it = partitionTimestamps.iterator();
                    it.hasNext();) {
                final Timestamp partitionTimestamp = it.next();
                final String partitionName = namePartition(partitionTimestamp);
                logger.info("Adding partition {} to table {}", partitionName,
                        this.tableName);
                sb.append(String.format("PARTITION %s VALUES LESS THAN(%s)",
                        partitionName, partitionTimestamp.getTime()));
                if (it.hasNext()) {
                    sb.append(',');
                }
            }
            sb.append(')');
            this.template.update(sb.toString());
        } else {
            for (Timestamp partitionTimestamp : partitionTimestamps) {
                final String partitionName = namePartition(partitionTimestamp);
                logger.info("Adding partition {} to table {}",
                        partitionName, this.tableName);
                this.template.update(String.format(
                          " ALTER TABLE %s ADD PARTITION"
                        + " (PARTITION %s VALUES LESS THAN(%s))",
                        this.tableName,
                        partitionName, partitionTimestamp.getTime()));
            }
        }
    }

    /**
     * Prunes all partitions which are older than the specified amount of time
     * and then creates the specified number of past and future partitions for
     * the table.
     *
     * @param duration
     *            The duration of time.
     * @param unit
     *            The unit of time.
     * @param pastPartitions
     *            The number of past partitions to create in the table.
     * @param futurePartitions
     *            The number of future partitions to create in the table.
     * @return The number of created partitions.
     */
    @Override
    public int pruneAndCreatePartitions(int duration,
            TimeUnit unit,
            int pastPartitions,
            int futurePartitions) {
        List<Partition> partitions = listPartitions();
        Timestamp rangeMinimum = partitions.isEmpty() ? new Timestamp(0L)
                : partitions.get(partitions.size()-1).getRangeLessThan();
        dropPartitionsOlderThan(duration, unit, partitions);
        List<Timestamp> timestamps = calculatePartitionTimestamps(
                pastPartitions, futurePartitions, rangeMinimum);
        createPartitions(partitions, timestamps);
        return timestamps.size();
    }

    private void dropPartitionsOlderThan(int duration,
            TimeUnit unit,
            List<Partition> partitions) {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration must be >= 0");
        }
        final long millis = unit.toMillis(duration);
        final Timestamp pruneTimestamp = new Timestamp(System.currentTimeMillis()
                - millis);
        StringBuilder partitionsToRemove = new StringBuilder();
        for (Partition partition : partitions) {
            if (partition.getRangeLessThan().after(pruneTimestamp)) {
                break;
            }
            if (partitionsToRemove.length() > 0) {
                partitionsToRemove.append(',');
            }
            partitionsToRemove.append(partition.getPartitionName());
            logger.info("Pruning table {} partition {}: prune timestamp {}",
                    new Object[] { this.tableName,
                            partition.getPartitionName(), pruneTimestamp });
        }
        if (partitionsToRemove.length() > 0) {
            this.template.update(String.format(
                    "ALTER TABLE %s DROP PARTITION %s",
                    this.tableName, partitionsToRemove.toString()));
        }
    }

    @Override
    public void removeAllPartitions() {
        this.template.update(String.format("ALTER TABLE %s REMOVE PARTITIONING",
                this.tableName));
    }

    /**
     * Returns a list of all partitions found on the table. If there are no
     * partitions defined, this returns an empty list. All partitions are
     * returned in sorted order with the first partition having the lowest range
     * value.
     * 
     * @return A list of all partitions found on the table.
     */
    @Override
    @Transactional(readOnly = true)
    public List<Partition> listPartitions() {
        final List<Map<String, Object>> fields = this.template.queryForList(
                  " SELECT PARTITION_NAME,PARTITION_METHOD,PARTITION_DESCRIPTION "
                + " FROM information_schema.partitions "
                + " WHERE TABLE_SCHEMA=DATABASE() "
                + " AND TABLE_NAME=? "
                + " AND PARTITION_METHOD = 'RANGE' "
                + " AND (PARTITION_NAME IS NOT NULL "
                + "      OR SUBPARTITION_NAME IS NOT NULL) "
                + " ORDER BY PARTITION_DESCRIPTION",
                this.tableName);
        final List<Partition> partitions =
                new ArrayList<Partition>(fields.size());
        if (fields.isEmpty()) {
            return partitions;
        }
        Partition previousPartition = 
                MySqlPartition.fromMySqlResultSetFields(fields.get(0));
        partitions.add(previousPartition);
        for (Map<String, Object> map : fields.subList(1, fields.size())) {
            MySqlPartition partition = 
                    MySqlPartition.fromMySqlResultSetFields(map);
            partition.setRangeMinimum(previousPartition.getRangeLessThan());
            partitions.add(partition);
            previousPartition = partition;
        }
        return partitions;
    }

    private String namePartition(Timestamp partitionTimestamp) {
        return "p" + DATE_FORMAT.format(partitionTimestamp);
    }
}
