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

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;
import java.util.Map;

/**
 * Test for Bug 510 - Incorrect results for queries involving not null filters
 *
 * @author Jeff Cunningham
 *         Created on: 5/31/13
 */
public class NotNullFilterIT { 

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(NotNullFilterIT.class.getSimpleName());
    protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("t1",schemaWatcher.schemaName,
            "(i int, s smallint, c varchar(30), vc varchar(30), b bigint)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(t1Watcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        //  load t1
                        spliceClassWatcher.getStatement().executeUpdate("insert into "+t1Watcher.toString()+" values (0, 0, '0', '0', 0)");
                        spliceClassWatcher.getStatement().executeUpdate("insert into "+t1Watcher.toString()+" values (null, null, null, null, null)");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testVarcharFiltering() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s where c is null", t1Watcher.toString()));
        System.out.println("testVarcharFiltering");
        for (Map<Object,Object> row : TestUtils.resultSetToMaps(rs)) {
            for (Map.Entry entry : row.entrySet()) {
                System.out.println(entry);
                Assert.assertNull("Should have got NULL", entry.getValue());
            }
        }
    }

    @Test
    public void testVarcharNotNullFiltering() throws Exception {
        TestUtils.tableLookupByNumber(spliceClassWatcher);
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s where c is not null", t1Watcher.toString()));
        System.out.println("testVarcharNotNullFiltering");
        for (Map<Object,Object> row : TestUtils.resultSetToMaps(rs)) {
            for (Map.Entry entry : row.entrySet()) {
                System.out.println(entry);
                Assert.assertNotNull("Shouldn't have got NULL", entry.getValue());
            }
        }
    }

    @Test
    public void testIntFiltering() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s where i is null", t1Watcher.toString()));
        System.out.println("testIntFiltering");
        for (Map<Object,Object> row : TestUtils.resultSetToMaps(rs)) {
            for (Map.Entry entry : row.entrySet()) {
                System.out.println(entry);
                Assert.assertNull("Should have got NULL", entry.getValue());
            }
        }
    }

    @Test
    public void testIntNotNullFiltering() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s where i is not null", t1Watcher.toString()));

        System.out.println("testIntNotNullFiltering");
        for (Map<Object,Object> row : TestUtils.resultSetToMaps(rs)) {
            for (Map.Entry entry : row.entrySet()) {
                System.out.println(entry);
                    Assert.assertNotNull("Shouldn't have got NULL", entry.getValue());
            }
        }
    }
}
