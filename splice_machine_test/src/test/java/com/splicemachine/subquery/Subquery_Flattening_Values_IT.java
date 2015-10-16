package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.splicemachine.subquery.SubqueryITUtil.assertUnorderedResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test coverage for flattening of values subqueries.
 */
public class Subquery_Flattening_Values_IT {


    private static final String SCHEMA = Subquery_Flattening_Values_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    private static final int ALL_FLATTENED = 0;


    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int, a3 int)");
        classWatcher.executeUpdate("insert into A values(0,0,0),(1,10,10),(2,20,20),(3,30,30)");
    }

    @Test
    public void values() throws Exception {
        assertUnorderedResult(conn(), "select * from A where a1 = (values 1)", ALL_FLATTENED, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 1 |10 |10 |");
        assertUnorderedResult(conn(), "select * from A where a1 > (values 1)", ALL_FLATTENED, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |");
        assertUnorderedResult(conn(), "select * from A where a1 < (values 1)", ALL_FLATTENED, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |");
        assertUnorderedResult(conn(), "select * from A where a1 != (values 1)", ALL_FLATTENED, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |");
    }

    @Test
    public void valuesThrows() throws Exception {
        try {
            assertUnorderedResult(conn(), "select * from A where a1 = (values 1,2)", ALL_FLATTENED, "");
            fail("expected exception");
        } catch (SQLException e) {
            assertEquals("Scalar subquery is only allowed to return a single row.", e.getMessage());
        }
    }


    private Connection conn() {
        return methodWatcher.getOrCreateConnection();
    }


}