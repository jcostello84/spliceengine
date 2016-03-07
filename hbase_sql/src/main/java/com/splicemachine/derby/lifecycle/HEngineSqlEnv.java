package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.PropertyManagerService;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.sql.HSqlExceptionFactory;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.control.CostChoosingDataSetProcessorFactory;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.JmxDatabaseAdminstrator;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HEngineSqlEnv extends EngineSqlEnvironment{

    private PropertyManager propertyManager;
    private PartitionLoadWatcher loadWatcher;
    private BackupManager backupManager;
    private DataSetProcessorFactory processorFactory;
    private SqlExceptionFactory exceptionFactory;
    private DatabaseAdministrator dbAdmin;

    @Override
    public void initialize(SConfiguration config,
                           Snowflake snowflake,
                           Connection internalConnection,
                           DatabaseVersion spliceVersion){
        super.initialize(config,snowflake,internalConnection,spliceVersion);
        this.propertyManager =PropertyManagerService.loadPropertyManager();
        this.loadWatcher = HBaseRegionLoads.INSTANCE;
        SIDriver driver =SIDriver.driver();
        ControlDataSetProcessor cdsp = new ControlDataSetProcessor(driver.getTxnSupplier(),
                driver.getTransactor(), driver.getOperationFactory());
        this.processorFactory = new CostChoosingDataSetProcessorFactory(new SparkDataSetProcessor(), cdsp);
        this.exceptionFactory = new HSqlExceptionFactory(SIDriver.driver().getExceptionFactory());
        this.dbAdmin = new JmxDatabaseAdminstrator();
        backupManager = new HBaseBackupManager();
    }

    @Override
    public DatabaseAdministrator databaseAdministrator(){
        return dbAdmin;
    }

    @Override
    public SqlExceptionFactory exceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public BackupManager getBackupManager(){
        return backupManager;
    }

    @Override
    public PartitionLoadWatcher getLoadWatcher(){
        return loadWatcher;
    }

    @Override
    public DataSetProcessorFactory getProcessorFactory(){
        return processorFactory;
    }

    @Override
    public PropertyManager getPropertyManager(){
        return propertyManager;
    }
}