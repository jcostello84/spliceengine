/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;
import com.splicemachine.derby.impl.load.HdfsImport;
import com.splicemachine.derby.impl.storage.TableSplit;
import java.sql.Types;
import java.util.*;

import com.splicemachine.backup.BackupSystemProcedures;
import com.splicemachine.derby.utils.*;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.catalog.DefaultSystemProcedureGenerator;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.procedures.external.ExternalTableSystemProcedures;

/**
 * System procedure generator implementation class that extends
 * base Derby implemention in order to add support for procedures
 * specific to Splice Machine and do not belong in Derby Layer,
 * such as those that assume the presence of HBase, and functions
 * explicitly added to the SYSFUN schema.
 *
 * @author Scott Fines
 */
public class SpliceSystemProcedures extends DefaultSystemProcedureGenerator {

    private static final String IMPORT_DATA_NAME = "SYSCS_IMPORT_DATA";
    public static final String RESTORE_DATABASE_NAME = "SYSCS_RESTORE_DATABASE";
    public static final String BACKUP_DATABASE_NAME = "SYSCS_BACKUP_DATABASE";


    /** Flag to ensure the list of procedures is only initialized once. */
    private static volatile boolean initialized = false;

    public SpliceSystemProcedures(DataDictionary dictionary) {
        super(dictionary);
    }

    @Override
    protected Map/*<UUID,List<Procedure>*/ getProcedures(DataDictionary dictionary, TransactionController tc) throws StandardException {
        @SuppressWarnings("rawtypes")
        Map sysProcedures = super.getProcedures(dictionary, tc);

        // Only initialize the list of system procedures once.
        if (initialized) {
            return sysProcedures;
        }

        // Only one initialization at a time.
        // Synchronize on the parent class since the lists of system procedures are all static members of it.
        synchronized (DefaultSystemProcedureGenerator.class) {

            // Check if the list was initialized while we were waiting.
            if (initialized) {
                return sysProcedures;
            }

            UUID sysUUID = dictionary.getSystemUtilSchemaDescriptor().getUUID();

        	/*
        	 * Our import process is different than Vanilla Derby's, so find that Procedure in
        	 * the map and replace it with our own Procedure
        	 */
            for(Object entry : sysProcedures.entrySet()){
                Object key = ((Map.Entry)entry).getKey();
                @SuppressWarnings("unchecked") List<Procedure> procedures = (List<Procedure>)((Map.Entry)entry).getValue();
                // Iterate through existing procedures to perform modifications
                for(int i=0;i<procedures.size();i++){
                    Procedure sysProc = procedures.get(i);
                    if(IMPORT_DATA_NAME.equalsIgnoreCase(sysProc.getName())){
                    } else if(RESTORE_DATABASE_NAME.equals(sysProc.getName())) {
                        Procedure restore = Procedure.newBuilder().name(RESTORE_DATABASE_NAME)
                                .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                                .varchar("directory", 32672)
                                .bigint("backupId")
                                .build();
                        procedures.set(i, restore);
                    } else if(BACKUP_DATABASE_NAME.equals(sysProc.getName())) {
                        Procedure backup = Procedure.newBuilder().name(BACKUP_DATABASE_NAME)
                                .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                                .varchar("directory", 32672)
                                .varchar("type", 32672)
                                .build();
                        procedures.set(i, backup);
                    }
                } // End iteration through existing procedures


                //
                // SYS_UTIL schema
                //
                // Create splice utility procedures
                //

                if (key.equals(sysUUID)) {
                    Procedure refreshExternalTable = Procedure.newBuilder().name("SYSCS_REFRESH_EXTERNAL_TABLE")
                            .numOutputParams(0).numResultSets(1).ownerClass(ExternalTableSystemProcedures.class.getCanonicalName())
                            .varchar("schema", 32672)
                            .varchar("table", 32672)
                            .build();
                    procedures.add(refreshExternalTable);

        			/*
        			 * Add a system procedure to enable splitting tables once data is loaded.
        			 *
        			 * We do this here because this way we know we're in the SYSCS procedure set
        			 */
                    Procedure splitProc = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .build();
                    procedures.add(splitProc);

                    Procedure splitProc1 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_AT_POINTS")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("splitPoints", 32672)
                            .build();
                    procedures.add(splitProc1);

                    Procedure splitProc2 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_EVENLY")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .integer("numSplits")
                            .build();
                    procedures.add(splitProc2);

                    procedures.add(Procedure.newBuilder().name("SYSCS_SPLIT_REGION_AT_POINTS")
                                           .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                                           .catalog("regionName")
                                           .varchar("splitPoints", 32672)
                                           .build());

         			/*
        			 * Procedure get all active services
        			 */
                    Procedure getActiveServers = Procedure.newBuilder().name("SYSCS_GET_ACTIVE_SERVERS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getActiveServers);

        			/*
        			 * Procedure get all active requests
        			 */
                    Procedure getRequests = Procedure.newBuilder().name("SYSCS_GET_REQUESTS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getRequests);


        			/*
        			 * Procedure get stats info for region servers
        			 */
                    Procedure getRegionServerStatsInfo = Procedure.newBuilder().name("SYSCS_GET_REGION_SERVER_STATS_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .build();
                    procedures.add(getRegionServerStatsInfo);

        			/*
        			 * Procedure fetch logical configuration from region servers
        			 */
                    Procedure getRegionServerConfig = Procedure.newBuilder().name("SYSCS_GET_REGION_SERVER_CONFIG_INFO")
                            .varchar("configRoot", 128) // fetch only config props with prefix, or null for fetch all
                            .integer("mode")            // 0 = fetch all, 1 = fetch only props where value not same on all servers
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getRegionServerConfig);

        			/*
        			 * Procedure get Splice Machine manifest info
        			 */
                    Procedure getVersionInfo = Procedure.newBuilder().name("SYSCS_GET_VERSION_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getVersionInfo);

        			/*
        			 * Procedure get write pipeline intake info
        			 */
                    Procedure getWriteIntakeInfo = Procedure.newBuilder().name("SYSCS_GET_WRITE_INTAKE_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getWriteIntakeInfo);

        			/*
        			 * Procedures to kill stale transactions
        			 */
                    Procedure killTransaction = Procedure.newBuilder().name("SYSCS_KILL_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .bigint("transactionId")
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killTransaction);

                    Procedure killStaleTransactions = Procedure.newBuilder().name("SYSCS_KILL_STALE_TRANSACTIONS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .bigint("maximumTransactionId")
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killStaleTransactions);

//                    Procedure dumpTransactions = Procedure.newBuilder().name("SYSCS_DUMP_TRANSACTIONS")
//                            .numOutputParams(0)
//                            .numResultSets(1)
//                            .ownerClass(TransactionAdmin.class.getCanonicalName())
//                            .build();
//                    procedures.add(dumpTransactions);

                    Procedure currTxn = Procedure.newBuilder().name("SYSCS_GET_CURRENT_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(currTxn);

                    Procedure activeTxn = Procedure.newBuilder().name("SYSCS_GET_ACTIVE_TRANSACTION_IDS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(activeTxn);

                    /*
                     * Statistics procedures
                     */
                    Procedure collectStatsForTable = Procedure.newBuilder().name("COLLECT_TABLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectStatsForTable);


                    Procedure importWithBadRecords = Procedure.newBuilder().name("IMPORT_DATA")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(importWithBadRecords);

                    Procedure upport = Procedure.newBuilder().name("UPSERT_DATA_FROM_FILE")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(upport);

                    Procedure getAutoIncLocs = Procedure.newBuilder().name("SYSCS_GET_AUTO_INCREMENT_ROW_LOCATIONS")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .build();
                    procedures.add(getAutoIncLocs);


                    Procedure dropStatsForSchema = Procedure.newBuilder().name("DROP_SCHEMA_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("schema",128)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(dropStatsForSchema);

                    Procedure dropStatsForTable = Procedure.newBuilder().name("DROP_TABLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(dropStatsForTable);

                    Procedure collectStatsForSchema = Procedure.newBuilder().name("COLLECT_SCHEMA_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectStatsForSchema);

                    Procedure enableStatsForColumn = Procedure.newBuilder().name("ENABLE_COLUMN_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .varchar("schema",1024)
                            .varchar("table",1024)
                            .varchar("column",1024)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(enableStatsForColumn);

                    Procedure disableStatsForColumn = Procedure.newBuilder().name("DISABLE_COLUMN_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .varchar("schema",1024)
                            .varchar("table",1024)
                            .varchar("column",1024)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(disableStatsForColumn);



                    /*
                     * Procedure to elevate a transaction
                     */
                    Procedure elevProc = Procedure.newBuilder().name("SYSCS_ELEVATE_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("tableName", 128) // input
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(elevProc);

                    /*
                     * Procedure to start a child transaction
                     */
                    Procedure childTxnProc = Procedure.newBuilder().name("SYSCS_START_CHILD_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .bigint("parentTransactionId") // input
                            .varchar("tableName", 128) // input
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(childTxnProc);
                    
                    /*
                     * Procedure to commit a child transaction
                     */
                    Procedure commitTxnProc = Procedure.newBuilder().name("SYSCS_COMMIT_CHILD_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .bigint("childTransactionId") // input
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(commitTxnProc);

        			/*
        			 * Procedure to get the log level for the given logger
        			 */
                    Procedure getLoggerLevel = Procedure.newBuilder().name("SYSCS_GET_LOGGER_LEVEL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("loggerName", 128)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getLoggerLevel);

        			/*
        			 * Procedure to set the log level for the given logger
        			 */
                    Procedure setLoggerLevel = Procedure.newBuilder().name("SYSCS_SET_LOGGER_LEVEL")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("loggerName", 128)
                            .varchar("loggerLevel", 128)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(setLoggerLevel);

        			/*
        			 * Procedure to get all the splice logger names in the system
        			 */
                    Procedure getLoggers = Procedure.newBuilder().name("SYSCS_GET_LOGGERS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getLoggers);

        			/*
        			 * Procedure get table info in all schema
        			 */
                    Procedure getSchemaInfo = Procedure.newBuilder().name("SYSCS_GET_SCHEMA_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getSchemaInfo);

        			/*
        			 * Procedure to perform major compaction on all tables in a schema
        			 */
                    Procedure majorComactionOnSchema = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA")
                            .varchar("schemaName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(majorComactionOnSchema);

        			/*
        			 * Procedure to perform major compaction on a table in a schema
        			 */
                    Procedure majorComactionOnTable = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE")
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(majorComactionOnTable);

        			/*
        			 * Procedure to perform major flush on a table in a schema
        			 */
                    Procedure flushTable = Procedure.newBuilder().name("SYSCS_FLUSH_TABLE")
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(flushTable);

        			/*
        			 * Procedure to get all the information related to the execution plans of the stored prepared statements (metadata queries).
        			 */
                    Procedure getStoredStatementPlanInfo = Procedure.newBuilder().name("SYSCS_GET_STORED_STATEMENT_PLAN_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getStoredStatementPlanInfo);

        			/*
        			 * Procedure to print all of the properties (JVM, Service, Database, App).
        			 */
                    Procedure getAllSystemProperties = Procedure.newBuilder().name("SYSCS_GET_ALL_PROPERTIES")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getAllSystemProperties);

                    Procedure vacuum = Procedure.newBuilder().name("VACUUM")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(vacuum);

                    /*
                     * Procedure to return timestamp generator info
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_TIMESTAMP_GENERATOR_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TimestampAdmin.class.getCanonicalName())
                            .build());

                    /*
                     * Procedure to return timestamp request info
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_TIMESTAMP_REQUEST_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TimestampAdmin.class.getCanonicalName())
                            .build());

                    /*
                     * Procedure to schedule a daily backup job
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_SCHEDULE_DAILY_BACKUP")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .varchar("directory", 32672)
                            .varchar("type", 32672)
                            .integer("hour")
                            .build());

                    /*
                     * Procedure to cancel a daily backup job
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_CANCEL_DAILY_BACKUP")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .bigint("jobId")
                            .build());

                    /*
                     * Procedure to delete a backup
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_DELETE_BACKUP")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .bigint("backupId")
                            .build());

                    /*
                     * Procedure to delete backups in a time window
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_DELETE_OLD_BACKUPS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .integer("backupWindow")
                            .build());

                    /*
                     * Procedure to get a database property on all region servers in the cluster.
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_GLOBAL_DATABASE_PROPERTY")
                    		.numOutputParams(0)
                    		.numResultSets(1)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    		.catalog("KEY")
                    		.build());

                    /*
                     * Procedure to set a database property on all region servers in the cluster.
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_SET_GLOBAL_DATABASE_PROPERTY")
                    		.numOutputParams(0)
                    		.numResultSets(0)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    		.catalog("KEY")
                    		.varchar("VALUE", Limits.DB2_VARCHAR_MAXWIDTH)
                    		.build());

                    /*
                     * Procedure to enable splice enterprise version
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_ENABLE_ENTERPRISE")
                    		.numOutputParams(0)
                    		.numResultSets(0)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    		.varchar("VALUE", Limits.DB2_VARCHAR_MAXWIDTH)
                    		.build());

                    /*
                     * Procedure to empty the statement caches on all region servers in the cluster.
                     * Essentially a wrapper around the Derby version of SYSCS_EMPTY_STATEMENT_CACHE
                     * which only operates on a single node.
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE")
                    		.numOutputParams(0)
                    		.numResultSets(0)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.NO_SQL).returnType(null).isDeterministic(false)
                    		.build());

                    procedures.add(Procedure.newBuilder().name("GET_ACTIVATION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .returnType(null).isDeterministic(false)
                            .varchar("statement", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build());

                    // Stored procedure that updates the owner (authorization id) of an existing schema.
                    procedures.add(Procedure.newBuilder().name("SYSCS_UPDATE_SCHEMA_OWNER")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA)
                        .returnType(null)
                        .isDeterministic(false)
                        .varchar("schemaName", 128)
                        .varchar("ownerName", 128)
                        .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_CANCEL_BACKUP")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_DATABASE_OWNER")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .sqlControl(RoutineAliasInfo.NO_SQL).returnType(null).isDeterministic(false)
                            .build());
                }  // End key == sysUUID

            } // End iteration through map keys (schema UUIDs)

            // Initialization was successful.  Mark the class as initialized.
            initialized = true;
        } // end of synchronized block

        return sysProcedures;
    }

    @SuppressWarnings("unchecked")
    protected void augmentProcedureMap(DataDictionary dict, Map procedures) throws StandardException {
        //
        // SYSFUN schema
        //
        // Add SYSFUN schema functions, for functions that are Splice standard
        // but not necessarily ANSI standard. More convenient than adding them
        // to the SQL grammar. Do this in Splice, not Derby, because in Derby,
        // SYSFUN is not intended to actually contain anything.
        //
        UUID sysFunUUID = dict.getSysFunSchemaDescriptor().getUUID();
        procedures.put(sysFunUUID, SYSFUN_PROCEDURES);
    }

    private static final List<Procedure> SYSFUN_PROCEDURES;

    static{
        try {
            SYSFUN_PROCEDURES = new ArrayList<>();
            SYSFUN_PROCEDURES.addAll(Arrays.asList(
                    Procedure.newBuilder().name("INSTR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.INTEGER))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("SUBSTR", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("INITCAP")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR, Limits.DB2_VARCHAR_MAXWIDTH))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("CONCAT")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR, Limits.DB2_VARCHAR_MAXWIDTH))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("ARG1", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("ARG2", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("REGEXP_LIKE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BOOLEAN))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("S", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("REGEXP", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build()
            		)
            );
            SYSFUN_PROCEDURES.addAll(Arrays.asList(
                    //
                    // Date functions
                    //
                    Procedure.newBuilder().name("ADD_MONTHS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .integer("NUMOFMONTHS")
                            .build(),
                    Procedure.newBuilder().name("LAST_DAY")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .build(),
                    Procedure.newBuilder().name("TO_DATE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TO_TIMESTAMP")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TO_TIME")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.TIME))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("NEXT_DAY")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .varchar("WEEKDAY", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("MONTH_BETWEEN")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE1", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .arg("SOURCE2", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .build(),
                    Procedure.newBuilder().name("TO_CHAR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TIMESTAMP_TO_CHAR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("STAMP", DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .varchar("OUTPUT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TRUNC_DATE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .varchar("FIELD", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),

                    //numeric functions
                    Procedure.newBuilder().name("SCALAR_POW")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(true).ownerClass(NumericFunctions.class.getCanonicalName())
                            .arg("BASE",DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .arg("SCALE",DataTypeDescriptor.getCatalogType(Types.INTEGER))
                            .build(),
                    Procedure.newBuilder().name("POW")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .isDeterministic(true).ownerClass(NumericFunctions.class.getCanonicalName())
                            .arg("BASE",DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .arg("SCALE",DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .build()

            ));

            SYSFUN_PROCEDURES.addAll(Arrays.asList(
            /*
             * Statistics functions
             */
                    Procedure.newBuilder().name("STATS_CARDINALITY")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_NULL_COUNT")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_NULL_FRACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.REAL))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_QUANTILES")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_FREQUENCIES")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_THETA")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),

                    Procedure.newBuilder().name("STATS_MIN")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_MAX")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_COL_WIDTH")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.INTEGER))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("PARTITION_EXISTS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BOOLEAN))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("CONGLOMERATE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT).getCatalogType())
                            .arg("PARTITION_ID", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR).getCatalogType())
                            .build()
            ));

            SYSFUN_PROCEDURES.addAll(Arrays.asList(
                    //
                    // General functions
                    //
                    Procedure.newBuilder().name("LONG_UUID")
                            .numOutputParams(1)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(false).ownerClass(GenericSpliceFunctions.class.getCanonicalName())
                            .build()
            ));
        }catch(StandardException se){
            throw new RuntimeException(se);
        }
    }

}
