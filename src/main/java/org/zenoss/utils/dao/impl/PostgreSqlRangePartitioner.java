/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import org.zenoss.utils.dao.Partition;

/**
 * PostgreSQL compatible implementation of RangePartitioner
 */
public class PostgreSqlRangePartitioner extends AbstractRangePartitioner {

    private static final Logger logger = LoggerFactory
            .getLogger(PostgreSqlRangePartitioner.class);

    public PostgreSqlRangePartitioner(DataSource ds, String databaseName,
            String tableName, String columnName, long duration, TimeUnit unit) {
        super(ds, databaseName, tableName, columnName, duration, unit);
    }

    protected void createPartitions(List<Partition> currentPartitions,
            List<Timestamp> partitionTimestamps) {
        if (currentPartitions.isEmpty()) {
            if (partitionTimestamps.size() < 2) {
                    throw new IllegalArgumentException(
                            "Must create multiple partitions.");
            }
        } else if (partitionTimestamps.isEmpty()) {
            return; //nothing needed to change
        }
        doAnonymousBlock(buildPartitionsDdl(currentPartitions,
                partitionTimestamps));
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
        if (duration < 0) {
            throw new IllegalArgumentException("Duration must be >= 0");
        }
        List<Partition> currentPartitions = listPartitions();
        List<Partition> partitionsToPrune = new ArrayList<Partition>();
        List<Partition> partitionsToKeep = new ArrayList<Partition>();
        final long millis = unit.toMillis(duration);
        final Timestamp pruneTimestamp =
                new Timestamp(System.currentTimeMillis() - millis);
        for (Partition partition : currentPartitions) {
            if (partition.getRangeLessThan().after(pruneTimestamp)) {
                partitionsToKeep.add(partition);
            } else {
                partitionsToPrune.add(partition);
                logger.info("Pruning table {} partition {}: prune timestamp {}",
                        new Object[] { this.tableName,
                                partition.getPartitionName(), pruneTimestamp });
            }
        }
        Timestamp rangeMinimum = currentPartitions
                .get(currentPartitions.size()-1).getRangeLessThan();
        List<Timestamp> partitionTimestamps = calculatePartitionTimestamps(
                pastPartitions, futurePartitions, rangeMinimum);
        if (partitionsToPrune.isEmpty() && partitionTimestamps.isEmpty()) {
            logger.info("There are no partitions to prune or create on table "
                    + this.tableName);
            return 0;
        }
        repartition(partitionsToKeep, partitionTimestamps, partitionsToPrune);
        return partitionTimestamps.size();
    }

    private void repartition(List<Partition> partitionsToKeep,
            List<Timestamp> partitionTimestamps,
            List<Partition> partitionsToPrune) {
        StringBuilder repartitionBody = new StringBuilder();
        repartitionBody.append(buildPartitionsDdl(partitionsToKeep,
                partitionTimestamps));
        if (!partitionsToKeep.isEmpty()) {
            Partition oldest = partitionsToKeep.get(0);
            logger.debug("oldest partition rangeMinimum is "
                    + oldest.getRangeMinimum());
            if (oldest.getRangeMinimum() != null) {
                repartitionBody.append(" ALTER TABLE ")
                        .append(oldest.getPartitionName())
                        .append(" DROP CONSTRAINT on_or_after_check;");
            }
        }
        for (Partition toPrune : partitionsToPrune) {
            repartitionBody.append(" DROP TABLE ")
                    .append(toPrune.getPartitionName())
                    .append(';');
        }
        doAnonymousBlock(repartitionBody.toString());
    }

    private String buildPartitionsDdl(List<Partition> partitions,
            List<Timestamp> partitionTimestamps) {
        String trigger = "";
        Timestamp rangeMinimum = null;
        if (partitions.isEmpty()) {
            trigger = String.format(
                      " DROP TRIGGER IF EXISTS %1$s ON %2$s;"
                    + " CREATE TRIGGER %1$s BEFORE INSERT ON %2$s"
                    + "   FOR EACH ROW EXECUTE PROCEDURE %3$s();",
                    nameTrigger(), this.tableName, nameTriggerFunction());
        } else {
            rangeMinimum = partitions.get(partitions.size()-1)
                    .getRangeLessThan();
        }
        List<String> formats = getIndexFormats();
        List<Partition> allPartitions = new ArrayList<Partition>(
                partitions.size() + partitionTimestamps.size());
        allPartitions.addAll(partitions);
        StringBuilder createBody = new StringBuilder();
        for (Timestamp rangeLessThan : partitionTimestamps) {
            final String partitionName = namePartition(rangeLessThan);
            allPartitions.add(new PostgreSqlPartition(this.tableName,
                    this.columnName, partitionName, rangeLessThan,
                    rangeMinimum));
            logger.info("adding partition " + partitionName + " to table "
                    + this.tableName);
            createBody.append(buildPartition(partitionName,
                    rangeLessThan, rangeMinimum, formats));
            rangeMinimum = rangeLessThan;
        }
        createBody.append(buildTriggerFunction(allPartitions));
        createBody.append(trigger);
        return createBody.toString();
    }

    private String buildPartition(String partitionName,
            Timestamp rangeLessThan,
            Timestamp rangeMinimum,
            List<String> formats) {
        StringBuilder partitionDdl = new StringBuilder(" CREATE TABLE ");
        partitionDdl.append(partitionName)
                .append(" (");
        if (rangeMinimum != null) {
            partitionDdl.append("   CONSTRAINT on_or_after_check CHECK (")
                    .append(this.columnName)
                    .append(" >= '")
                    .append(rangeMinimum)
                    .append("'::timestamp without time zone),");
        }
        partitionDdl.append("   CONSTRAINT before_check CHECK (")
                .append(this.columnName)
                .append(" < '")
                .append(rangeLessThan)
                .append("'::timestamp without time zone) ) INHERITS (")
                .append(this.tableName)
                .append(");");
        for (String indexFormat : formats) {
            partitionDdl.append(String.format(indexFormat,
                    partitionName, partitionName));
        }
        return partitionDdl.toString();
    }

    private String buildTriggerFunction(List<Partition> partitions) {
        StringBuilder elsifs = new StringBuilder();
        for (Partition partition : partitions.subList(1, partitions.size()-1)) {
            elsifs.insert(0, String.format(
                  "   ELSIF ( NEW.%1$s >= '%2$s'::timestamp without time zone AND"
                + "           NEW.%1$s < '%3$s'::timestamp without time zone ) THEN"
                + "     INSERT INTO %4$s VALUES (NEW.*);",
                this.columnName,
                partition.getRangeMinimum(),
                partition.getRangeLessThan(),
                partition.getPartitionName()));
        }
        Partition newestPartition = partitions.get(partitions.size()-1);
        Partition oldestPartition = partitions.get(0);
        return String.format(
                  " CREATE OR REPLACE FUNCTION %1$s()"
                + " RETURNS TRIGGER AS $$"
                + " BEGIN"
                + "   IF ( NEW.%2$s >= '%3$s'::timestamp without time zone AND"
                + "        NEW.%2$s < '%4$s'::timestamp without time zone ) THEN"
                + "     INSERT INTO %5$s VALUES (NEW.*);"
                + " %6$s"
                + "   ELSIF ( NEW.%2$s < '%7$s'::timestamp without time zone ) THEN"
                + "     INSERT INTO %8$s VALUES (NEW.*);"
                + "   ELSE"
                + "     RAISE EXCEPTION 'Date out of range';"
                + "   END IF;"
                + "   RETURN NULL;"
                + " END;"
                + " $$"
                + " LANGUAGE plpgsql;",
                nameTriggerFunction(),
                this.columnName,
                newestPartition.getRangeMinimum(),
                newestPartition.getRangeLessThan(),
                newestPartition.getPartitionName(),
                elsifs.toString(),
                oldestPartition.getRangeLessThan(),
                oldestPartition.getPartitionName());
    }

    @Override
    public void removeAllPartitions() {
        StringBuilder removeBody = new StringBuilder();
        removeBody.append(String.format(
                  " DROP TRIGGER %s ON %s;"
                + " DROP FUNCTION %s();",
                nameTrigger(), this.tableName, nameTriggerFunction()));
        List<Partition> partitions = listPartitions();
        for (Partition partition : partitions) {
            removeBody.append(String.format(
                    " ALTER TABLE %s NO INHERIT %s;",
                    partition.getPartitionName(), this.tableName));
        }
        for (Partition partition : partitions) {
            removeBody.append(String.format(
                    " INSERT INTO %s SELECT * FROM %s;",
                    this.tableName, partition.getPartitionName()));
        }
        for (Partition partition : partitions) {
            removeBody.append(" DROP TABLE ")
                    .append(partition.getPartitionName())
                    .append(';');
        }
        doAnonymousBlock(removeBody.toString());
    }

    private void doAnonymousBlock(String code) {
        String block = "DO $DO$BEGIN " + code + " END $DO$;";
        logger.debug(block);
        this.template.update(block);
    }

    protected List<String> getIndexFormats() {
        // CREATE UNIQUE INDEX event_archive_pkey
        //   ON event_archive USING btree (uuid, last_seen)
        final List<Map<String, Object>> fields = this.template.queryForList(
                " SELECT indexdef FROM pg_indexes WHERE tablename = ? ",
                this.tableName);
        List<String> indexFormats = new ArrayList<String>(fields.size());
        for (Map<String, Object> map : fields) {
            String indexDef = (String) map.get("indexdef");
            indexFormats.add(" "
                    + indexDef.replaceAll(this.tableName, "%s")
                    + "; ");
        }
        return indexFormats;
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
                  " SELECT childClass.relname AS partition_name,"
                + "   beforeCheck.consrc AS before_check,"
                + "   onOrAfterCheck.consrc AS on_or_after_check"
                + " FROM pg_constraint beforeCheck"
                + " INNER JOIN pg_class childClass"
                + "   ON beforeCheck.conrelid = childClass.oid"
                + " LEFT OUTER JOIN (SELECT * FROM pg_constraint"
                + "   WHERE conname = 'on_or_after_check') onOrAfterCheck"
                + "   ON childClass.oid = onOrAfterCheck.conrelid"
                + " INNER JOIN pg_inherits inheritanceRelations"
                + "   ON inheritanceRelations.inhrelid = childClass.oid"
                + " INNER JOIN pg_class parentClass"
                + "   ON inheritanceRelations.inhparent = parentClass.oid"
                + " WHERE parentClass.relname = ?"
                + "   AND beforeCheck.conname = 'before_check'"
                + " ORDER BY childClass.relname",
                this.tableName);
        final List<Partition> partitions = new ArrayList<Partition>();
        for (Map<String, Object> map : fields) {
            partitions.add(PostgreSqlPartition.fromPostgreSqlResultSetFields(
                    this.tableName, this.columnName, map));
        }
        return partitions;
    }

    private String namePartition(Timestamp partitionTimestamp) {
        return this.tableName + "_p" + DATE_FORMAT.format(partitionTimestamp);
    }

    private String nameTrigger() {
        return "ins_" + this.tableName + "_trg";
    }

    private String nameTriggerFunction() {
        return this.tableName + "_ins_trg_fn";
    }
}
