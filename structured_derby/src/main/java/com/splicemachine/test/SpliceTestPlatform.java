package com.splicemachine.test;

import java.io.File;

import com.splicemachine.si2.coprocessors.SIObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.slf4j.LoggerFactory;
import com.splicemachine.derby.hbase.SpliceDerbyCoprocessor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexManagementEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.impl.load.SpliceImportCoprocessor;
import com.splicemachine.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver;
import com.splicemachine.hbase.txn.coprocessor.region.TransactionalRegionObserver;

public class SpliceTestPlatform extends TestConstants {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SpliceTestPlatform.class);
	protected MiniZooKeeperCluster miniZooKeeperCluster;
	protected MiniHBaseCluster miniHBaseCluster;
	protected String zookeeperTargetDirectory;
	protected String hbaseTargetDirectory;
	
	public SpliceTestPlatform() {
		super();
	}
	
	public SpliceTestPlatform(String targetDirectory) {
		this(targetDirectory + "zookeeper",targetDirectory + "hbase");
	}

	public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseTargetDirectory) {
		this.zookeeperTargetDirectory = zookeeperTargetDirectory;
		this.hbaseTargetDirectory = hbaseTargetDirectory;
	}

	public static void main(String[] args) throws Exception {
		SpliceTestPlatform spliceTestPlatform;
		if (args.length == 1) {
			spliceTestPlatform = new SpliceTestPlatform(args[0]);
		}
		if (args.length == 2) {
			spliceTestPlatform = new SpliceTestPlatform(args[0],args[1]);
			spliceTestPlatform.start();
		}
		if (args.length == 0 || args.length > 2) {
			System.out.println("Splice Test Platform supports one argument providing the target directory" +
					" or two arguments dictating the zookeeper and hbase directory.");
			System.exit(1);
		}
	}
	
	public void start() throws Exception {
		Configuration config = HBaseConfiguration.create();
		setBaselineConfigurationParameters(config);
		miniZooKeeperCluster = new MiniZooKeeperCluster();
		miniZooKeeperCluster.startup(new File(zookeeperTargetDirectory),3);
		miniHBaseCluster = new MiniHBaseCluster(config,1,1);
	}
	public void end() throws Exception {

	}

	public void setBaselineConfigurationParameters(Configuration configuration) {
		configuration.set("hbase.rootdir", "file://" + hbaseTargetDirectory);
		configuration.set("hbase.rpc.timeout", "6000");
		configuration.set("hbase.cluster.distributed", "true");
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		configuration.set("hbase.regionserver.handler.count", "40");
		coprocessorBaseline(configuration);
		configuration.reloadConfiguration();
	}
	
	public void coprocessorBaseline(Configuration configuration) {
		configuration.set("hbase.coprocessor.region.classes", 
				SpliceOperationRegionObserver.class.getCanonicalName() + "," +
				SpliceOperationCoprocessor.class.getCanonicalName() + "," + 
				SpliceImportCoprocessor.class.getCanonicalName() + "," + 
				SpliceIndexObserver.class.getCanonicalName() + "," + 
				SpliceDerbyCoprocessor.class.getCanonicalName() + "," + 
				SpliceIndexManagementEndpoint.class.getCanonicalName() + "," + 
				SpliceIndexEndpoint.class.getCanonicalName() + "," + 
				//TransactionalManagerRegionObserver.class.getCanonicalName() + "," +
				//TransactionalRegionObserver.class.getCanonicalName()
				//comment out SIObserver for now, and when SI is ready, 
				//uncomment it out and comment out the above old transaction observers
				//+ "," +
				SIObserver.class.getCanonicalName()
				);
	
	
	}

}
