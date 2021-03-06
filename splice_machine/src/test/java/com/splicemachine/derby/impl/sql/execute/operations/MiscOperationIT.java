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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;
import java.sql.Statement;

public class MiscOperationIT extends SpliceUnitTest { 
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(MiscOperationIT.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(MiscOperationIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",MiscOperationIT.class.getSimpleName(),"(num int, addr varchar(50), zip char(5))");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				Statement s = spliceClassWatcher.getStatement();	
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A values(100, '100: 101 Califronia St', '94114')"); 
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A values(200, '200: 908 Glade Ct.', '94509')"); 
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A values(300, '300: my addr', '34166')"); 
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A values(400, '400: 182 Second St.', '94114')"); 
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A (num) values(500)");
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A values(600, 'new addr', '34166')"); 
				s.execute("insert into "+ MiscOperationIT.class.getSimpleName()+ ".A (num) values(700)");
				spliceClassWatcher.commit();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testAlterTableAddColumn() throws Exception {
		Statement s = methodWatcher.getStatement();
		s.execute("Alter table"+this.getPaddedTableReference("A")+"add column salary float default 0.0");	
		s.execute("update"+this.getPaddedTableReference("A")+"set salary=1000.0 where zip='94114'");
		s.execute("update"+this.getPaddedTableReference("A")+"set salary=5000.85 where zip='94509'");
		ResultSet rs = s.executeQuery("select zip, salary from"+this.getPaddedTableReference("A"));
		while (rs.next()) {
			if (rs.getString(1)!= null && rs.getString(1).equals("94114"))
				Assert.assertEquals("94114 update not correct", new Double(1000.0), new Double(rs.getDouble(2)));
			if (rs.getString(1)!= null && rs.getString(1).equals("94509"))
				Assert.assertEquals("94509 update not correct", new Double(5000.85), new Double(rs.getDouble(2)));
		}	
	}
	
	@Test 
	public void testTimestampFromSysDummy() throws Exception {
		Statement s = methodWatcher.getStatement();
		ResultSet rs = s.executeQuery("values CURRENT_TIMESTAMP");
		Assert.assertTrue("No timestamp returned",rs.next());
	}
	
}
