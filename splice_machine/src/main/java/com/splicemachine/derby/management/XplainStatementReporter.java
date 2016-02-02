package com.splicemachine.derby.management;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.db.iapi.error.StandardException;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Scott Fines
 * Date: 1/22/14
 */
public class XplainStatementReporter extends TransactionalSysTableWriter<StatementInfo> {

		public XplainStatementReporter() throws StandardException {
				super("SYSSTATEMENTHISTORY");
		}

		@Override
		protected DataHash<StatementInfo> getKeyHash() {
				return new KeyWriteableHash<StatementInfo>() {
						@Override
						protected int getNumFields() {
								return 3;
						}

						@Override
						protected void doEncode(MultiFieldEncoder encoder, StatementInfo statementInfo) {
								String localhost = null;
								try {
										localhost = InetAddress.getLocalHost().getHostName();
								} catch (UnknownHostException e) {
										throw new RuntimeException(e);
								}
								encoder.encodeNext(statementInfo.getStatementUuid())
												.encodeNext(localhost)
												.encodeNext(statementInfo.getUser());
						}
				};
		}

		@Override
		protected DataHash<StatementInfo> getDataHash() {
				return new EntryWriteableHash<StatementInfo>() {
						@Override
						protected void doEncode(MultiFieldEncoder encoder, StatementInfo statementInfo) {
								try {
										encoder.encodeNext(statementInfo.getStatementUuid()) //0 - long
														.encodeNext(InetAddress.getLocalHost().getHostName().trim()) //1 - varchar
														.encodeNext(statementInfo.getUser().trim()) //2 - varchar
														.encodeNext(statementInfo.getTxnId()) //3 - long
														.encodeNext(statementInfo.status()) //4 - varchar
														.encodeNext(statementInfo.getSql()) //5 - varchar
														.encodeNext(statementInfo.getNumJobs()) //6 - int
														.encodeNext(statementInfo.numSuccessfulJobs())
														.encodeNext(statementInfo.numFailedJobs()) //7 - int
														.encodeNext(statementInfo.numCancelledJobs()) //8 - int
														.encodeNext(statementInfo.getStartTimeMs()) //9 - long
														.encodeNext(statementInfo.getStopTimeMs()); //10 -long
								} catch (UnknownHostException e) {
										throw new RuntimeException(e);
								}
						}

						@Override
						protected EntryEncoder buildEncoder() {
								BitSet fields = new BitSet(12);
								fields.set(0,12);
								BitSet scalarFields = new BitSet(12);
								scalarFields.set(0);
								scalarFields.set(3);
								scalarFields.set(6,12);
								BitSet floatFields = new BitSet(0);
								BitSet doubleFields = new BitSet(0);
								return EntryEncoder.create(SpliceKryoRegistry.getInstance(),12,
										fields,scalarFields,floatFields,doubleFields);
						}
				};
		}

}