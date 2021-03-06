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

package com.splicemachine.test;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceZoo implements Runnable {
	private static final Logger LOG = Logger.getLogger(SpliceZoo.class);
	protected QuorumPeerConfig config;
	protected QuorumPeer peer;
	public SpliceZoo(QuorumPeerConfig config, int number) throws IOException {
		this.config = config;
		this.peer = new QuorumPeer();
		ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
		cnxnFactory.configure(config.getClientPortAddress(),config.getMaxClientCnxns());
		peer.setClientPortAddress(config.getClientPortAddress());
		peer.setTxnFactory(new FileTxnSnapLog(new File(config.getDataLogDir()),
                     new File(config.getDataDir())));
		peer.setQuorumPeers(config.getServers());
		peer.setElectionType(config.getElectionAlg());
		peer.setMyid(config.getServerId());
		peer.setTickTime(config.getTickTime());
		peer.setMinSessionTimeout(config.getMinSessionTimeout());
		peer.setMaxSessionTimeout(config.getMaxSessionTimeout());
		peer.setInitLimit(config.getInitLimit());
		peer.setSyncLimit(config.getSyncLimit());
		peer.setQuorumVerifier(config.getQuorumVerifier());
		peer.setCnxnFactory(cnxnFactory);
		peer.setZKDatabase(new ZKDatabase(peer.getTxnFactory()));
		peer.setLearnerType(config.getPeerType());
		peer.setMyid(number);
	}

	@Override
	public void run() {
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();
        SpliceLogUtils.trace(LOG, "Client Address: %s",config.getClientPortAddress());
		try {
			 peer.start();
		     SpliceLogUtils.trace(LOG, "Attempting to Join: %s",config.getClientPortAddress());
			 peer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
