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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
/**
 * Created by jyuan on 7/30/15.
 */
public class AnalyzeTableIT {
    private static final String SCHEMA = AnalyzeTableIT.class.getSimpleName();
    private static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void setup() throws Exception {
        classWatcher.executeUpdate("create table T1 (I INT)");
        classWatcher.executeUpdate("create table T2 (I INT)");

        classWatcher.executeUpdate("insert into T1 values 1, 2, 3");
        classWatcher.executeUpdate("insert into T2 values 1, 2, 3, 4, 5, 6, 7, 8, 9");

        classWatcher.execute("call syscs_util.syscs_create_user('analyzeuser', 'passwd')");
        classWatcher.execute("grant execute on procedure syscs_util.collect_table_statistics to analyzeuser");
        classWatcher.execute("grant execute on procedure syscs_util.collect_schema_statistics to analyzeuser");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        classWatcher.execute("call syscs_util.syscs_drop_user('analyzeuser')");
    }

    @Test
    public void testAnalyzeTable() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyze table AnalyzeTableIT.T1");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testAnalyzeSchema() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyze schema AnalyzeTableIT");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testDefaultSchema() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        Statement statement = connection.createStatement();
        statement.execute("set schema " + SCHEMA);
        ResultSet rs = statement.executeQuery("Analyze table t1");
        int count = 0;
        while (rs.next()) {
            ++count;
            String schema = rs.getString(1);
            Assert.assertTrue(schema.compareToIgnoreCase(SCHEMA) == 0);
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testAnalyzeTablePrivilege() throws Exception {
        String message = null;
        String expected = null;
        try {
            Connection connection = methodWatcher.createConnection("analyzeuser", "passwd");
            Statement statement = connection.createStatement();
            statement.execute("analyze table AnalyzeTableIT.T2");
        }
        catch (SQLSyntaxErrorException e) {
            expected = "4251M";
            message = e.getSQLState();
        }
        Assert.assertNotNull(message);
        Assert.assertNotNull(expected);
        Assert.assertTrue(message.compareTo(expected) == 0);
    }

    @Test
    public void testAnalyzeSchemaPrivilege() throws Exception {

        String message = null;
        String expected = null;
        try {
            Connection connection = methodWatcher.createConnection("analyzeuser", "passwd");
            Statement statement = connection.createStatement();
            statement.execute("analyze schema AnalyzeTableIT");
        }
        catch (SQLSyntaxErrorException e) {
            expected = "4251M";
            message = e.getSQLState();
        }

        Assert.assertNotNull(message);
        Assert.assertNotNull(expected);
        Assert.assertTrue(message.compareTo(expected) == 0);
    }
}
