/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils.dao.impl;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.zenoss.utils.dao.Partition;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * PostgreSQL compatible implementation of RangePartitioner
 */
public class PostgreSqlRangePartitioner extends AbstractRangePartitioner {

    private static final Logger logger = LoggerFactory
            .getLogger(PostgreSqlRangePartitioner.class);

    private static final SimpleDateFormat PARTITION_TS_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        PARTITION_TS_FMT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static final String PUBLIC_SCHEMA = "public";
    private final String triggerName;
    private final String triggerFunction;

    public PostgreSqlRangePartitioner(DataSource ds, String databaseName,
            String tableName, String columnName, long duration, TimeUnit unit) {
        super(ds, databaseName, tableName, columnName, duration, unit);
        this.triggerName = "ins_" + this.tableName + "_trg";
        this.triggerFunction = this.tableName + "_ins_trg_fn";
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
        buildPartitionsDdl(currentPartitions,
                partitionTimestamps);
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
        buildPartitionsDdl(partitionsToKeep,
                partitionTimestamps);
        if (!partitionsToKeep.isEmpty()) {
            Partition oldest = partitionsToKeep.get(0);
            logger.debug("oldest partition rangeMinimum is "
                    + oldest.getRangeMinimum());
            if (oldest.getRangeMinimum() != null) {
                this.template.update(" ALTER TABLE "
                        + oldest.getPartitionName()
                        + " DROP CONSTRAINT on_or_after_check;");
            }
        }
        for (Partition toPrune : partitionsToPrune) {
            this.template.update(" DROP TABLE "
                    + toPrune.getPartitionName());
        }
    }

    private void buildPartitionsDdl(List<Partition> partitions,
            List<Timestamp> partitionTimestamps) {
        Timestamp rangeMinimum = null;
        if (!partitions.isEmpty()) {
            rangeMinimum = partitions.get(partitions.size()-1)
                    .getRangeLessThan();
        }
        List<Trigger> triggers = getTriggers();
        List<String> formats = getIndexFormats();
        List<Partition> allPartitions = new ArrayList<Partition>(
                partitions.size() + partitionTimestamps.size());
        allPartitions.addAll(partitions);
        for (Timestamp rangeLessThan : partitionTimestamps) {
            final String partitionName = namePartition(rangeLessThan);
            allPartitions.add(new PostgreSqlPartition(this.tableName,
                    this.columnName, partitionName, rangeLessThan,
                    rangeMinimum));
            logger.info("adding partition " + partitionName + " to table "
                    + this.tableName);
            createPartition(partitionName, rangeLessThan, rangeMinimum, formats, triggers);
            rangeMinimum = rangeLessThan;
        }
        this.template.update(buildTriggerFunction(allPartitions));
        if (partitions.isEmpty()) {
            this.template.update(String.format(
                      " DROP TRIGGER IF EXISTS %1$s ON %2$s;"
                    + " CREATE TRIGGER %1$s BEFORE INSERT ON %2$s"
                    + "   FOR EACH ROW EXECUTE PROCEDURE %3$s();",
                    this.triggerName, this.tableName, this.triggerFunction));
        }
    }

    private void createPartition(String partitionName,
                                 Timestamp rangeLessThan,
                                 Timestamp rangeMinimum,
                                 List<String> formats,
                                 List<Trigger> triggers) {
        StringBuilder partitionDdl = new StringBuilder(" CREATE TABLE ");
        partitionDdl.append(partitionName)
                .append(" (");
        if (rangeMinimum != null) {
            partitionDdl.append("   CONSTRAINT on_or_after_check CHECK (")
                    .append(this.columnName)
                    .append(" >= '")
                    .append(PARTITION_TS_FMT.format(rangeMinimum))
                    .append("'::timestamp without time zone),");
        }
        partitionDdl.append("   CONSTRAINT before_check CHECK (")
                .append(this.columnName)
                .append(" < '")
                .append(PARTITION_TS_FMT.format(rangeLessThan))
                .append("'::timestamp without time zone) ) INHERITS (")
                .append(this.tableName)
                .append(");");
        this.template.update(partitionDdl.toString());

        // Create indexes from parent table
        for (String indexFormat : formats) {
            this.template.update(String.format(indexFormat, partitionName, partitionName));
        }

        // Create triggers from parent table
        for (Trigger trigger : triggers) {
            Trigger partitionTrigger = new Trigger(trigger);
            partitionTrigger.tableName = partitionName;
            String triggerSql = partitionTrigger.toSql();
            logger.debug("Creating trigger: {}", triggerSql);
            this.template.update(triggerSql);
        }
    }

    private String buildTriggerFunction(List<Partition> partitions) {
        StringBuilder elsifs = new StringBuilder();
        for (Partition partition : partitions.subList(1, partitions.size()-1)) {
            elsifs.insert(0, String.format(
                  "   ELSIF ( NEW.%1$s >= '%2$s'::timestamp without time zone AND"
                + "           NEW.%1$s < '%3$s'::timestamp without time zone ) THEN"
                + "     INSERT INTO %4$s VALUES (NEW.*);",
                this.columnName,
                PARTITION_TS_FMT.format(partition.getRangeMinimum()),
                PARTITION_TS_FMT.format(partition.getRangeLessThan()),
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
                this.triggerFunction,
                this.columnName,
                PARTITION_TS_FMT.format(newestPartition.getRangeMinimum()),
                PARTITION_TS_FMT.format(newestPartition.getRangeLessThan()),
                newestPartition.getPartitionName(),
                elsifs.toString(),
                PARTITION_TS_FMT.format(oldestPartition.getRangeLessThan()),
                oldestPartition.getPartitionName());
    }

    @Override
    public void removeAllPartitions() {
        this.template.update(" DROP TRIGGER " + this.triggerName
                + " ON " + this.tableName);
        this.template.update(" DROP FUNCTION " + this.triggerFunction + "()");
        List<Partition> partitions = listPartitions();
        for (Partition partition : partitions) {
            this.template.update("ALTER TABLE " + partition.getPartitionName()
                    + " NO INHERIT " + this.tableName);
        }
        for (Partition partition : partitions) {
            this.template.update("INSERT INTO " + this.tableName
                    + " SELECT * FROM " + partition.getPartitionName());
        }
        for (Partition partition : partitions) {
            this.template.update("DROP TABLE " + partition.getPartitionName());
        }
    }

    protected List<String> getIndexFormats() {
        // CREATE UNIQUE INDEX event_archive_pkey
        //   ON event_archive USING btree (uuid, last_seen)
        final List<String> indexDefs = this.template.getJdbcOperations().queryForList(
                "SELECT indexdef FROM pg_indexes WHERE tablename = ?", String.class,
                this.tableName);
        List<String> indexFormats = new ArrayList<String>(indexDefs.size());
        for (String indexDef : indexDefs) {
            // TODO: This may be too aggressive if column names contain portion of table name
            indexFormats.add(indexDef.replaceAll(this.tableName, "%s"));
        }
        return indexFormats;
    }

    private static class Trigger {
        String name;
        String tableSchema;
        String tableName;
        String timing;
        String orientation;
        Set<String> events = new HashSet<String>();
        Set<String> updateColumns = new HashSet<String>();
        String condition;
        String statement;

        public Trigger() {
        }
        
        public Trigger(Trigger trigger) {
            this.name = trigger.name;
            this.tableSchema = trigger.tableSchema;
            this.tableName = trigger.tableName;
            this.timing = trigger.timing;
            this.orientation = trigger.orientation;
            this.events = new HashSet<String>(trigger.events);
            this.updateColumns = new HashSet<String>(trigger.updateColumns);
            this.condition = trigger.condition;
            this.statement = trigger.statement;
        }

        public String toSql() {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TRIGGER ").append(name).append(' ').append(timing);
            for (Iterator<String> it = events.iterator(); it.hasNext(); ) {
                String event = it.next();
                sb.append(" ").append(event);
                if (!updateColumns.isEmpty()) {
                    sb.append(" OF ").append(StringUtils.collectionToCommaDelimitedString(updateColumns));
                }
                if (it.hasNext()) {
                    sb.append(" OR");
                }
            }
            sb.append(" ON ").append(tableSchema).append('.').append(tableName);
            // TODO: FROM referenced_table_name
            // TODO: { NOT DEFERRABLE | [ DEFERRABLE ] { INITIALLY IMMEDIATE | INITIALLY DEFERRED } }
            sb.append(" FOR EACH ").append(orientation);
            if (condition != null) {
                sb.append(" WHEN ").append(condition);
            }
            sb.append(" ").append(statement);
            return sb.toString();
        }

        @Override
        public String toString() {
            return toSql();
        }
    }

    protected List<Trigger> getTriggers() {
        // TODO: Support custom schemas here instead of assuming public schema
        final String sql = "SELECT tg.*,uc.event_object_column FROM INFORMATION_SCHEMA.TRIGGERS tg" +
                " LEFT JOIN INFORMATION_SCHEMA.TRIGGERED_UPDATE_COLUMNS uc" +
                " USING(trigger_catalog,trigger_schema,trigger_name,event_object_catalog,event_object_schema,event_object_table)" +
                " WHERE event_object_schema=? AND event_object_table=? AND trigger_name != ?";
        return this.template.getJdbcOperations().query(sql, new ResultSetExtractor<List<Trigger>>() {
            @Override
            public List<Trigger> extractData(ResultSet rs) throws SQLException, DataAccessException {
                Map<String,Trigger> triggersByName = new HashMap<String, Trigger>();
                while (rs.next()) {
                    String name = rs.getString("trigger_name");
                    String event = rs.getString("event_manipulation");
                    String updateColumn = Strings.emptyToNull(rs.getString("event_object_column"));
                    Trigger existing = triggersByName.get(name);
                    if (existing == null) {
                        existing = new Trigger();
                        existing.name = name;
                        existing.tableSchema = rs.getString("event_object_schema");
                        existing.tableName = rs.getString("event_object_table");
                        existing.statement = rs.getString("action_statement");
                        existing.condition = Strings.emptyToNull(rs.getString("action_condition"));
                        existing.orientation = rs.getString("action_orientation");
                        existing.timing = rs.getString("condition_timing");
                        triggersByName.put(name, existing);
                    }
                    existing.events.add(event);
                    if (updateColumn != null) {
                        existing.updateColumns.add(updateColumn);
                    }
                }
                return new ArrayList<Trigger>(triggersByName.values());
            }
        }, PUBLIC_SCHEMA, this.tableName, this.triggerName);
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
}
