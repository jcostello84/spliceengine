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

import com.splicemachine.customer.Price;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.util.Properties;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 * Created by jyuan on 11/3/15.
 */
@Category(SerialTest.class) //serial because it loads a jar
public class SpliceUDTIT extends SpliceUnitTest {
    public static final String CLASS_NAME = SpliceUDTIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
    private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
    private static final String STORED_PROCS_JAR_FILE = getResourceDirectory() + "UDT.jar";
    private static final String JAR_FILE_SQL_NAME = CLASS_NAME + ".UDT_JAR";

    private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')";


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void setup() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        methodWatcher.execute(String.format(CALL_REMOVE_JAR_FORMAT_STRING, JAR_FILE_SQL_NAME));
        methodWatcher.execute("DROP DERBY AGGREGATE Median RESTRICT");
        methodWatcher.execute("DROP table orders");
        methodWatcher.execute("DROP FUNCTION makePrice");
        methodWatcher.execute("DROP TYPE price restrict");
        methodWatcher.execute("drop function testConnection");
        methodWatcher.execute("drop table test");

    }

    private static void createData(Connection conn) throws Exception {
        try(Statement s = conn.createStatement()){
            s.execute(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
        }catch(SQLException se){
            if(!"SE014".equals(se.getSQLState())){
                //write-conflict. That means someone ELSE is loading this same jar. That's cool, just keep going
                throw se;
            }
        }
        try(Statement s = conn.createStatement()){
            s.execute(String.format(CALL_SET_CLASSPATH_FORMAT_STRING,JAR_FILE_SQL_NAME));
            s.execute("create derby aggregate median for int external name 'com.splicemachine.customer.Median'");

            new TableCreator(conn)
                    .withCreate("create table t(i int)")
                    .withInsert("insert into t values(?)")
                    .withRows(rows(
                            row(1),
                            row(2),
                            row(3),
                            row(4),
                            row(5)))
                    .create();

            s.execute("CREATE TYPE price EXTERNAL NAME 'com.splicemachine.customer.Price' language Java");
            s.execute("CREATE FUNCTION makePrice(varchar(30), double)\n"+
                    "RETURNS Price\n"+
                    "LANGUAGE JAVA\n"+
                    "PARAMETER STYLE JAVA\n"+
                    "NO SQL EXTERNAL NAME 'com.splicemachine.customer.CreatePrice.createPriceObject'");

            s.execute("create table orders(orderID INT,customerID INT,totalPrice price)");
            s.execute("insert into orders values (12345, 12, makePrice('USD', 12))");

            s.execute("CREATE FUNCTION testConnection()\n" +
                    "RETURNS VARCHAR(100)\n" +
                    "LANGUAGE JAVA\n" +
                    "PARAMETER STYLE JAVA \n" +
                    "EXTERNAL NAME 'com.splicemachine.customer.NielsenTesting.testInternalConnection'");
            s.execute("create table test(id integer, name varchar(30))");
            s.execute("insert into test values(1,'erin')");
        }

    }

    @Test
    public void testUDA() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select median(i) from t");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
    }

    @Test
    public void testUDT() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select orderID, customerID, totalPrice from orders");
        Assert.assertTrue(rs.next());
        int oid = rs.getInt(1);
        int cid = rs.getInt(2);
        Price price = (Price)rs.getObject(3);
        Assert.assertEquals(12345, oid);
        Assert.assertEquals(12, cid);
        Assert.assertEquals(12, price.amount, 0.01);
        Assert.assertTrue(price.currencyCode.compareTo("USD") == 0);
    }

    @Test
    public void TestSelectStatistics() throws Exception {
        methodWatcher.execute("analyze schema " + CLASS_NAME);
        ResultSet rs = methodWatcher.executeQuery("select count(*) from sys.syscolumnstatistics");
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getInt(1)>0);
    }

    @Test
    public void testConnection() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;useSpark=true";
        Connection connection = DriverManager.getConnection(url, new Properties());
        connection.setSchema(CLASS_NAME.toUpperCase());
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("select testConnection() from test");
        String result = rs.next() ? rs.getString(1) : null;
        Assert.assertNotNull(result);
        Assert.assertTrue(result, result.compareTo("Got an internal connection")==0);
    }
}
