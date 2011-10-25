/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Interface used to simplify creation of range partitions on integer columns.
 * Does not currently support ranges on date columns (with custom
 * expressions), sub-partitioning, or reorganizing partitions (merging /
 * splitting).
 */
public interface RangePartitioner {

    /**
     * Returns a list of all partitions found on the table. If there are no
     * partitions defined, this returns an empty list. All partitions are
     * returned in sorted order with the first partition having the lowest range
     * value.
     * 
     * @return A list of all partitions found on the table.
     */
    public List<Partition> listPartitions();

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
    public int createPartitions(int pastPartitions, int futurePartitions);

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
    public int pruneAndCreatePartitions(int duration,
            TimeUnit unit,
            int pastPartitions,
            int futurePartitions);

    /**
     * Removes all partitions on the specified table.
     */
    public void removeAllPartitions();
}
