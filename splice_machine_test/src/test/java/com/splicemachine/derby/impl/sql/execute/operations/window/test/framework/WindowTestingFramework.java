package com.splicemachine.derby.impl.sql.execute.operations.window.test.framework;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.services.reflect.SpliceReflectClasses;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.SpliceExecutionFactory;
import com.splicemachine.derby.impl.sql.execute.operations.window.BaseFrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameDefinition;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregatorImpl;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import com.splicemachine.derby.utils.PartitionAwareIterator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;

/**
 * Utilities to help with testing window functions and various parts of the Splice window function infrastructure.
 * <p/>
 * Flow:<br/>
 * <ol>
 *     <li>Create a test class for a window function</li>
 *     <li>Create a nested class that will operate as an expected results generator - see {@link ExpectedResultsFunction}</li>
 *     <li>Create a test and test configuration:
 *      <ul>
 *          <li>Create and setup the window function instance to test</li>
 *          <li>Define the number of partitions and partition size</li>
 *          <li>Define the (1-based) partition columns, if any</li>
 *          <li>Define the (1-based) order by columns, if any</li>
 *          <li>Create the row definition (the number and types of each column in an input row)</li>
 *          <li>Create a window frame definition</li>
 *          <li>Create an instance of the expected results function (the nested class created above)</li>
 *      </ul>
 *      <li>Use this framework to run your test and verify results -
 *      see {@link #helpTestWindowFunction(int, int, int[], int[], int[], java.util.List, FrameDefinition, WindowTestingFramework.ExpectedResultsFunction, com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction, boolean)}</li>
 *     </li>
 * </ol>
 *
 * @author Jeff Cunningham
 *         Date: 8/18/14
 */
public class WindowTestingFramework {

    public static final boolean PRINT_RESULTS = true;
    public static final boolean DONT_PRINT_RESULTS = false;

    public static final FrameDefinition DEFAULT_FRAME_DEF = new FrameDefinition(FrameDefinition.FrameMode.RANGE.ordinal(),
                                                                                       FrameDefinition.Frame.UNBOUNDED_PRECEDING.ordinal(), -1,
                                                                                       FrameDefinition.Frame.CURRENT_ROW.ordinal(), -1);

    public static final FrameDefinition ONE_PREC_CUR_ROW_FRAME_DEF = new FrameDefinition(FrameDefinition.FrameMode.ROWS.ordinal(),
                                                                                       FrameDefinition.Frame.PRECEDING.ordinal(), 1,
                                                                                       FrameDefinition.Frame.CURRENT_ROW.ordinal(), -1);

    public static final FrameDefinition ALL_ROWS = new FrameDefinition(FrameDefinition.FrameMode.RANGE.ordinal(),
                                                                       FrameDefinition.Frame.UNBOUNDED_PRECEDING.ordinal(), -1,
                                                                       FrameDefinition.Frame.UNBOUNDED_FOLLOWING.ordinal(), -1);

    protected static LazyDataValueFactory dvf = new LazyDataValueFactory();
    protected static SpliceReflectClasses cf = new SpliceReflectClasses();
    protected static SpliceExecutionFactory ef = new SpliceExecutionFactory();

    @BeforeClass
    public static void startup() throws StandardException {
        Properties bootProps = new Properties();
        bootProps.setProperty("derby.service.jdbc", "com.splicemachine.db.jdbc.InternalDriver");
        bootProps.setProperty("derby.service.authentication", AuthenticationService.MODULE);

        Monitor.startMonitor(bootProps,System.out);
        cf.boot(false, new Properties());
        dvf.boot(false, new Properties());
        ef.boot(false, new Properties());
    }

    /**
     * Demonstrates generation of test data creates/inserts to copy into sqlfiddle or sqlshell
     */
    @Test
    @Ignore("Just for demo purposes. Can be run manually.")
    public void testGenInserts() throws Exception {
        List<TestColumnDefinition> rowDefinition = new ArrayList<>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition().setColumnName("intCol"),
                new DoubleColumnDefinition().setVariant(4).setColumnConstraint("not null"),
                new DecimalColumnDefinition().setVariant(3).setColumnName("dadecimal").setColumnConstraint("(6,2)")}));
        printInputSet(rowDefinition, 5, new int[]{1}, "temp1", System.out);
    }

    /**
     * Convenience method to run the test on the window function provided by this configuration and perform verification.<br/>
     * Your test can run some or all of the segments executed here.
     *
     * @param nPartitions the number of partitions over which to apply the function
     * @param partitionSize the size of each partition
     * @param partitionColIDs the 1-based column identifiers to include in the partition clause.
     *                        Can be empty, in which case all columns will be in the partition.
     * @param orderByColIDs the 1-based column identifiers to include in the order by clause.
     *                      Cannot be empty for ranking functions, since these are what are ranked.
     * @param inputColumnIDs the 1-based column indexes which are to provide window function input. Must have at least one
     *                       value.
     * @param inputRowDefinition the width and type for the input columns.
     * @param frameDefinition the window frame clause. Pass <code>null</code> to use the {@link #DEFAULT_FRAME_DEF}
     * @param expectedResultsFunction a function provided by the caller that implements
     * {@link WindowTestingFramework.ExpectedResultsFunction}
     *                          that is used to generate expected results to verify window function results.
     * @param function the window function to test
     * @param printResults passing <code>true</code> will print out window function output plus expected results.
     *                     Useful for initial testing but should be set to <code>false</code> before committing
     *                     the new test.
     * @throws StandardException for test setup failure or window function application failure.
     */
    public void helpTestWindowFunction(int nPartitions, int partitionSize, int[] partitionColIDs, int[] orderByColIDs,
                                       int[] inputColumnIDs, List<TestColumnDefinition> inputRowDefinition,
                                       FrameDefinition frameDefinition, ExpectedResultsFunction expectedResultsFunction,
                                       SpliceGenericWindowFunction function, boolean printResults)
        throws StandardException, IOException {
        // defaults
        FrameDefinition theFrame = frameDefinition;
        if (theFrame == null) {
            theFrame = DEFAULT_FRAME_DEF;
        }
        if (partitionColIDs == null) {
            partitionColIDs = new int[0];
        }
        if (orderByColIDs == null) {
            orderByColIDs = new int[0];
        }
        if (inputColumnIDs == null || inputColumnIDs.length == 0) {
            throw new RuntimeException("Must have at least one input column ID.");
        }

        // foreach partition, create a frameSource
        List<PartitionAwarePushBackIterator<ExecRow>> frameSources = new ArrayList<>(nPartitions);
        List<ExecRow> expectedRows = new ArrayList<>(nPartitions*partitionSize);
        List<ExecRow> allInputRows = new ArrayList<>(nPartitions * partitionSize);
        for (int i=1; i<=nPartitions; i++) {

            // create a list of rows within partition
            List<ExecRow> inputRows = createValueRows(i, partitionSize, join(partitionColIDs,orderByColIDs), inputRowDefinition);

            if (partitionColIDs.length > 0) {
                // init the frame source with rows of the partition
                PartitionAwarePushBackIterator<ExecRow> frameSource =
                    new PartitionAwarePushBackIterator<>(new TestPartitionAwareIterator(inputRows, partitionColIDs));
                frameSources.add(frameSource);
            } else {
                allInputRows.addAll(inputRows);
            }

            // calc expected results for partition
            expectedRows.addAll(calculateExpectedResults(inputRows, expectedResultsFunction));
        }

        if (partitionColIDs.length == 0) {
            // init the frame source with rows of the partition
            PartitionAwarePushBackIterator<ExecRow> frameSource =
                new PartitionAwarePushBackIterator<>(new TestPartitionAwareIterator(allInputRows, partitionColIDs));
            frameSources.add(frameSource);
        }

        // create agg function array
        int aggregatorColID = inputRowDefinition.size()+3;
        int resultColID = inputRowDefinition.size()+1;

        // create template row given
        ExecRow templateRow = createTemplateRow(inputRowDefinition, function, resultColID,
                                                inputColumnIDs, aggregatorColID, expectedResultsFunction.getNullReturnValue());
        WindowAggregator[] functions = new WindowAggregator[] {
            new WindowAggregatorImpl(function,aggregatorColID, inputColumnIDs,resultColID, frameDefinition)};

        // iterate thru the partitions creating a frame buffer for each
        List<ExecRow> results = new ArrayList<>();
        for (PartitionAwarePushBackIterator<ExecRow> frameSource : frameSources) {
            WindowFrameBuffer frameBuffer = BaseFrameBuffer.createFrameBuffer(null,
                                                                              functions,
                                                                              frameSource,
                                                                              theFrame,
                                                                              convertToZeroBased(inputColumnIDs),
                                                                              templateRow);

            // process each frame buffer
            ExecRow row = frameBuffer.next(null);
            while (row != null) {
                results.add(row);
                row = frameBuffer.next(null);
            }
        }
        // assert successful function application over rows
        assertExecRowsEqual(expectedRows, results, inputColumnIDs);
        if (printResults) {
            System.out.println(printResults(expectedRows, results, -1, new StringBuilder()));
        }
    }

    /**
     * The hardest problem in programmatic testing is in generating test data in a way that's random enough
     * to produce realistic data over a range of datatypes but deterministic enough to allow generation of
     * expected results.  I'm sure this can be made better.
     *
     * @param partitionValue the <i>value</i> to use in generating partition values.
     * @param nRows the number of rows to generate
     * @param sortColIDs the 1-based set of column IDs over which to sort the resulting exec row.
     *                   This is used to mimic the way window function rows are sorted by row key.
     * @param columnDefinitions the column types.
     * @return the list of generated rows.  The rows are sorted to simulate the way rows from TEMP are
     * sorted on partition and order by columns.
     * @throws StandardException if the data value factory has trouble creating a data value for a
     */
    public static List<ExecRow> createValueRows(int partitionValue, int nRows, int[] sortColIDs,
                                                List<TestColumnDefinition> columnDefinitions) throws StandardException {
        List<ExecRow> rows = new ArrayList<>(nRows);
        for (int j=0; j<nRows; j++) {
            ValueRow valueRow = new ValueRow(columnDefinitions.size());
            int i=1; // valueRow.setColumn() is 1-based
            for (TestColumnDefinition columnDefinition : columnDefinitions) {
                valueRow.setColumn(i++, columnDefinition.getDVD(partitionValue));
            }
            rows.add(valueRow);
        }
        Collections.sort(rows, new ExecRowComparator(sortColIDs));
        return rows;
    }

    /**
     * Given a list of input <code>ExecRows</code>, generate a list of output <code>ExecRows</code> with
     * the result column of a window function filed in with an expected value.
     * <p/>
     * <b>Note:</b> it's not really a good idea to generate expected results using the same function we're testing.
     * However, I couldn't find a better way to generate potentially large result sets of randomized input data.
     * The reason we want to produce large result sets to test with is that this test framework is as much about
     * testing the window function infrastructure as it is about testing the window functions themselves (or more so).
     *
     * @param inputRows the input data on which we will test the real function and on which <code>testGenFunction</code>
     *                  will be applied to generate the expected result column.
     * @param expectedResultsFunction a function that will be used to generate expected result column. This should not be a
     *                        copy of the window function that will be tested.
     * @return the expected output rows of a test run over a window function and the infrastructure that supports it.
     * These rows are one column wider than the input rows. The last row contains the result of the function.
     * @throws StandardException if
     */
    public List<ExecRow> calculateExpectedResults(List<ExecRow> inputRows, ExpectedResultsFunction expectedResultsFunction)
        throws StandardException {
        List<ExecRow> expectedRows = new ArrayList<>(inputRows.size());
        for (ExecRow inputRow : inputRows) {
            DataValueDescriptor[] cloneRow = inputRow.getRowArrayClone();
            ValueRow templateRow = new ValueRow(cloneRow.length + 1);
            int i=1;
            while (i <= cloneRow.length) {
                templateRow.setColumn(i, cloneRow[i-1]);
                ++i;
            }
            templateRow.setColumn(i, expectedResultsFunction.apply(cloneRow));
            expectedRows.add(templateRow);
        }
        return expectedRows;
    }


    /**
     * Create a template <code>ExecRow</code> made up of null values of the supplied column types,
     * in their order.<br/>
     * There will be two more columns added to the row; the <i>rowlen</i>+1 column will be used by the framework to hold
     * intermediate function results, the last column (<i>rowlen</i>+2) be used by the framework to will hold the
     * function instance itself, where <code><i>rowlen</i> == rowDescription.length<code/>.
     *
     * @param rowDescriptions the column types and order.
     * @param function the window function that will be used to process the rows
     * @return the template <code>ExecRow</code> made up of null values of the supplied column types.
     */
    public static ExecRow createTemplateRow(List<TestColumnDefinition> rowDescriptions,
                                            SpliceGenericWindowFunction function,
                                            int resultColID,
                                            int[] inputColIDs,
                                            int functionColID,
                                            DataValueDescriptor nullReturnValue) throws StandardException {
        ValueRow valueRow = new ValueRow(rowDescriptions.size()+3);
        int i=1;    // ValueRow#setColumn() is 1-based
        for (TestColumnDefinition rowDesc : rowDescriptions) {
            valueRow.setColumn(i++, rowDesc.getNullDVD());
        }
        // need, in order, result col type, input col type(s), function class
        valueRow.setColumn(resultColID, nullReturnValue);                           // result column
        // TODO: allow more than one input column?
        ++i;
        for (int inputColID : inputColIDs) {
            valueRow.setColumn(i++, valueRow.cloneColumn(inputColID));   // input column
        }
        valueRow.setColumn(functionColID, dvf.getDataValue(function, null));        // function column

        return valueRow;
    }

    /**
     * Used by {@link #printDVDAssertionDetails(java.util.List, java.util.List,
     * com.splicemachine.db.iapi.types.DataValueDescriptor, com.splicemachine.db.iapi.types.DataValueDescriptor, int, int, int[])}
     * to print detailed error message about the diff between expected and actual results of function application over
     * all rows.
     * <p/>
     * To call from a test;
     * <pre>System.out.println(printResults(expectedRows, results, -1, new StringBuilder()));</pre>
     * to produce output such as:
     * <pre>
     *     ...
     *      0007 { 1, 1.0, SMNFWFL, 1 }      { 1, 1.0, SMNFWFL, 1.0 }
     *      0008 { 1, 1.0, GMBSXAN, 1 }      { 1, 1.0, GMBSXAN, 1.0 }
     *      0009 { 1, 10.0, MGYEDFH, 9 }      { 1, 10.0, MGYEDFH, 9.0 }
     *      0010 { 1, 10.0, MGZOAWC, 9 }      { 1, 10.0, MGZOAWC, 9.0 }
     *      0011 { 1, 11.0, CAJLJSV, 11 }      { 1, 11.0, CAJLJSV, 11.0 }
     *     ...
     * </pre>
     * <b>Warning:</b> this method buffers all rows as strings from both <code>expectedRows</code> and <code>actualRows</code>.
     *
     * @param expectedRows the rows we expect after function application to all input rows.
     * @param actualRows the actual result rows we received after applying the function to all input rows.
     * @param firstDiffRow the row in which the first diff between <code>expectedRows</code> and <code>actualRows</code>.
     *                     To call from test code, pass in <code>-1</code>.
     * @param buf the buffer for the row strings.
     * @return the line-numbered, Sting contents of all expected and actual rows.
     */
    public static String printResults(List<ExecRow> expectedRows, List<ExecRow> actualRows, int firstDiffRow, StringBuilder buf) {
        buf.append("     Expected   /   Actual").append('\n');
        for (int i=0; i<expectedRows.size(); i++) {
            String expStr = (i<expectedRows.size() && expectedRows.get(i) != null ? expectedRows.get(i).toString() : "NULL");
            String actStr = (i<actualRows.size() && actualRows.get(i) != null ? actualRows.get(i).toString() : "NULL");
            buf.append(String.format("%04d %s      %s %s", i + 1, expStr, actStr, (firstDiffRow != -1 && i ==
                firstDiffRow ? " <<<" : ""))).append('\n');
        }
        return buf.toString();

    }

    /**
     * Ordered comparison all column values of all rows of <code>expectedRows</code> to <code>actualRows</code>.
     * <p/>
     * If comparison fails, a detailed error message is printed to STDERR.
     * @param expectedRows result we expect.
     * @param actualRows actual result.
     * @param inputColumnIDs the set of one or more, 1-based column IDs that were used for function input.
     * @throws StandardException if an error happens during DVD comparison.
     */
    public static void assertExecRowsEqual(List<ExecRow> expectedRows, List<ExecRow> actualRows, int[] inputColumnIDs) throws
        StandardException {
        Assert.assertEquals("Result sizes are different.", expectedRows.size(), actualRows.size());
        for (int i=0; i<expectedRows.size(); i++) {
            DataValueDescriptor[] expectedRow = expectedRows.get(i).getRowArray();
            DataValueDescriptor[] actualRow = actualRows.get(i).getRowArray();
            Assert.assertEquals("Row sizes differ at row "+i+1, expectedRow.length, actualRow.length);
            for (int j=0; j<expectedRow.length; j++) {
                DataValueDescriptor expectedDVD = expectedRow[j];
                DataValueDescriptor actualDVD = actualRow[j];
                int comp = -1;
                if (expectedDVD != null && actualDVD != null) {
                    comp = expectedDVD.compare(actualDVD);
                }
                Assert.assertTrue(printDVDAssertionDetails(expectedRows, actualRows, expectedDVD, actualDVD,
                                                           i, j, inputColumnIDs),comp == 0);
            }
        }
    }

    private static String printDVDAssertionDetails(List<ExecRow> expectedRows, List<ExecRow> actualRows,
                                                   DataValueDescriptor expectedDVD, DataValueDescriptor actualDVD,
                                                   int firstDiffRow, int diffCol, int[] inputColumnIDs) {
        StringBuilder buf = new StringBuilder("\nColumn contents differ at row ");
        buf.append(firstDiffRow+1).append(", col ").append(diffCol+1).append('\n').append(expectedRows.get(firstDiffRow))
           .append(" expected: ").append(expectedDVD).append('\n').append(actualRows.get(firstDiffRow)).append(" got: ")
           .append(actualDVD).append('\n').append("(Comparing column").append((inputColumnIDs.length > 1 ? "s: " : ": "))
           .append(Arrays.toString(inputColumnIDs)).append(")\n");
        return printResults(expectedRows, actualRows, firstDiffRow, buf);
    }

    /**
     * A simplified <code>PartitionAwareIterator</code> used for testing the window function framework.<br/>
     * Removes the requirement to deal with decoders, rowkeys, scanners, etc., by being the source of
     * the "scan" rows.
     */
    public static class TestPartitionAwareIterator implements PartitionAwareIterator<ExecRow> {
        private final List<ExecRow> rows;
        private final int[] partitionColumns;
        private byte[] partition;

        int index = 0;

        public TestPartitionAwareIterator(List<ExecRow> rows, int[] partitionColumns) {
            this.rows = rows;
            this.partitionColumns = partitionColumns;
        }

        @Override
        public byte[] getPartition() {
            return partition;
        }

        @Override
        public void open() throws StandardException, IOException {
            index = 0;
        }

        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws
            StandardException, IOException {
            ExecRow row = null;
            if (index < rows.size()) {
                row = rows.get(index++);
                partition = createPartitionValue(row, partitionColumns);
            }
            return row;
        }

        private byte[] createPartitionValue(ExecRow row, int[] partitionColumns) throws StandardException {
            StringBuilder buf = new StringBuilder();
            for (int partitionColumn : partitionColumns) {
                buf.append(row.getColumn(partitionColumn).toString());
            }
            return buf.toString().getBytes();
        }

        @Override
        public void close() throws StandardException, IOException {
            // do nothing
        }
    }

    /**
     * Print a set of SQL statements that can be used in tools (sqlfiddle or sqlshell) to create a table and
     * insert generated row values.
     * @param rowDefinition the column definitions that describe the table.
     * @param nRows the number of rows to generate
     * @param sortColIDs the 1-based set of column IDs over which to sort the resulting exec row.
     *                   This is used to mimic the way window function rows are sorted by row key.
     * @param tableName the name of the new table.
     * @param out output target.
     * @throws StandardException if an error occurs generating DVD values.
     */
    public static void printInputSet(List<TestColumnDefinition> rowDefinition, int nRows, int[] sortColIDs,
                                     String tableName, PrintStream out) throws StandardException {
        List<ExecRow> rows = new ArrayList<>();
        for (int i=1; i<=2; i++) {
            rows.addAll(createValueRows(i, nRows, sortColIDs, rowDefinition));
        }
        out.println(generateCreateTable(tableName.toUpperCase(), rowDefinition));
        StringBuilder buf = new StringBuilder();
        for (ExecRow row : rows) {
            buf.setLength(0);
            buf.append("INSERT INTO ").append(tableName.toUpperCase()).append(" VALUES (");
            for (DataValueDescriptor dvd : row.getRowArray()) {
                if (dvd instanceof NumberDataValue) {
                    buf.append(dvd.toString()).append(",");
                } else {
                    buf.append("'").append(dvd.toString()).append("'").append(",");
                }
            }
            if (buf.length() > 0 && buf.charAt(buf.length()-1) == ',') buf.setLength(buf.length()-1);
            buf.append(");");
            out.println(buf.toString());
        }
    }

    private static String generateCreateTable(String tableName, List<TestColumnDefinition> rowDefinition) {
        StringBuilder buf = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        int i=0;
        for (TestColumnDefinition columnDefinition : rowDefinition) {
            ++i;
            String columnName = columnDefinition.getColumnName();
            if (columnName == null || columnName.isEmpty()) {
                columnName = "col_"+(i);
            }
            buf.append(columnName.toUpperCase()).append(' ').append(columnDefinition.getSQLType())
               .append(' ').append(columnDefinition.getColumnConstraint().toUpperCase()).append(", ");
        }
        if (buf.length() > 2) {
            buf.setLength(buf.length()-2);
        }
        buf.append(");");
        return buf.toString();
    }

    //===========================================================================================================
    // Test Column Definition API
    // Data-drive input row generation from a template row definition
    //===========================================================================================================

    public interface TestColumnDefinition {
        DataValueDescriptor getNullDVD();
        DataValueDescriptor getDVD(int rowIndex) throws StandardException;
        TestColumnDefinition setVariant(int repeatPeriod);
        TestColumnDefinition setColumnName(String columName);
        String getColumnName();
        TestColumnDefinition setColumnConstraint(String columnConstraint);
        String getColumnConstraint();
        String getDefaultColumnConstraint();
        String getSQLType();
    }

    public abstract static class TestColumnDefinitionBase implements TestColumnDefinition {
        private static final int DUPLICATE_SET_SIZE_MULTIPLIER = 13;
        protected List<DataValueDescriptor> duplicateSet;
        protected int repeatPeriod;
        protected int repeatCounter = -1;
        protected int maxDuplicates;

        protected static final int DEFAULT_RANDOM_MAX = 12;
        protected static final int DEFAULT_RANDOM_MIN = 1;

        protected boolean isVariant;
        protected String columName;
        protected String columnConstraint;

        private static Random random = new Random();

        public TestColumnDefinition setVariant(int repeatPeriod) {
            this.isVariant = true;
            this.repeatCounter = 0;
            this.repeatPeriod = repeatPeriod;
            this.maxDuplicates = DUPLICATE_SET_SIZE_MULTIPLIER *this.repeatPeriod;
            this.duplicateSet = new ArrayList<>(maxDuplicates);
            return this;
        }

        @Override
        public TestColumnDefinition setColumnName(String columName) {
            this.columName = columName;
            return this;
        }

        @Override
        public String getColumnName() {
            if (this.columName == null) return "";
            return this.columName;
        }

        @Override
        public TestColumnDefinition setColumnConstraint(String columnConstraint) {
            this.columnConstraint = columnConstraint;
            return this;
        }

        @Override
        public String getColumnConstraint() {
            if (this.columnConstraint == null) return getDefaultColumnConstraint();
            return this.columnConstraint;
        }

        @Override
        public String getDefaultColumnConstraint() {
            // default is empty. override to provide meaningful default.
            return "";
        }

        public DataValueDescriptor getNextDVD(DataValueDescriptor newDVD) {
            DataValueDescriptor returned = newDVD.cloneValue(false);
            if (isVariant && repeatCounter >= -1) {
                if (repeatPeriod == repeatCounter) {
                    repeatCounter = 0;
                    returned = duplicateSet.get((int) nextRand(duplicateSet.size()-1, 0));
                } else if (duplicateSet.size() >= maxDuplicates) {
                    DataValueDescriptor dvd =duplicateSet.remove((int) nextRand(duplicateSet.size()-1, 0));
                }
                ++repeatCounter;
                duplicateSet.add(returned);
            }
            return returned;
        }

        public static long nextRand(long max, long min) {
            random = new Random();
            return random.nextInt((int) Math.abs(((max - min) + 1))) + min;
        }

        @Override
        public String getSQLType() {
            return getNullDVD().getTypeName();
        }
    }

    public static class TimestampColumnDefinition extends TestColumnDefinitionBase implements TestColumnDefinition {
        long offset = Timestamp.valueOf("2010-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2014-09-28 00:00:00").getTime();
        long diff = end - offset + 1;

        @Override
        public DataValueDescriptor getNullDVD() {
            DataValueDescriptor retVal = null;
            try {
                retVal = LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_TIMESTAMP_ID);
            } catch (StandardException e) {
                // never happen
            }
            return retVal;
        }

        @Override
        public DataValueDescriptor getDVD(int nonVariantColumnValue) throws StandardException {
            long now = System.currentTimeMillis();
            if (! isVariant) {
                return dvf.getDataValue(new Timestamp(now), (DateTimeDataValue)null);
            }
            return getNextDVD(dvf.getDataValue(new Timestamp(offset + (long)(Math.random() * diff)),
                                               (DateTimeDataValue) null));
        }
    }

    public static class DateColumnDefinition extends TimestampColumnDefinition implements TestColumnDefinition {
        @Override
        public DataValueDescriptor getNullDVD() {
            DataValueDescriptor retVal = null;
            try {
                retVal = LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_DATE_ID);
            } catch (StandardException e) {
                // never happen
            }
            return retVal;
        }

        @Override
        public DataValueDescriptor getDVD(int nonVariantColumnValue) throws StandardException {
            return dvf.getDate(super.getDVD(nonVariantColumnValue));
        }
    }

    public static class IntegerColumnDefinition extends TestColumnDefinitionBase implements TestColumnDefinition {
        @Override
        public DataValueDescriptor getDVD(int nonVariantColumnValue) throws StandardException {
            if (! isVariant) {
                return dvf.getDataValue(nonVariantColumnValue, null);
            }
            return getNextDVD(dvf.getDataValue(nextRand(DEFAULT_RANDOM_MAX, DEFAULT_RANDOM_MIN), null));
        }

        public DataValueDescriptor getNullDVD() {
            DataValueDescriptor retVal = null;
            try {
                retVal = LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_INTEGER_ID);
            } catch (StandardException e) {
                // never happen
            }
            return retVal;
        }
    }

    public static class DoubleColumnDefinition extends TestColumnDefinitionBase implements TestColumnDefinition {

        @Override
        public DataValueDescriptor getDVD(int nonVariantColumnValue) throws StandardException {
            if (! isVariant) {
                return dvf.getDataValue((double) nonVariantColumnValue, null);
            }
            return getNextDVD(dvf.getDataValue((double) nextRand(DEFAULT_RANDOM_MAX, DEFAULT_RANDOM_MIN), null));
        }


        public DataValueDescriptor getNullDVD() {
            DataValueDescriptor retVal = null;
            try {
                retVal = LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_DOUBLE_ID);
            } catch (StandardException e) {
                // never happen
            }
            return retVal;
        }

    }

    public static class DecimalColumnDefinition extends DoubleColumnDefinition implements TestColumnDefinition {

        public DataValueDescriptor getNullDVD() {
            DataValueDescriptor retVal = null;
            try {
                retVal = LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_DECIMAL_ID);
            } catch (StandardException e) {
                // never happen
            }
            return retVal;
        }

    }

    public static class VarcharColumnDefinition extends TestColumnDefinitionBase implements TestColumnDefinition {
        // ascii upper-case char range
        private static final int MAX_ASCII_CHAR = 90;
        private static final int MIN_ASCII_CHAR = 65;
        private final int length;

        public VarcharColumnDefinition(int length) {
            this.length = length;
        }

        @Override
        public DataValueDescriptor getDVD(int rowIndex) throws StandardException {
            if (! isVariant) {
                return dvf.getDataValue("joe_" +rowIndex, null);
            }
            byte[] asciiBytes = new byte[this.length];
            for (int i=0; i<length; i++) {
                asciiBytes[i] = (byte)nextRand(MAX_ASCII_CHAR,MIN_ASCII_CHAR);
            }
            return getNextDVD(dvf.getDataValue(new String(asciiBytes), null));
        }

        public DataValueDescriptor getNullDVD() {
            DataValueDescriptor retVal = null;
            try {
                retVal = LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_VARCHAR_ID);
            } catch (StandardException e) {
                // never happen
            }
            return retVal;
        }

        @Override
        public String getDefaultColumnConstraint() {
            // arbitrary default.
            return "(25)";
        }
    }

    //===========================================================================================================
    // Test ranking function API
    // Test functions are just for helping to remove the error-prone tedium of creating expected results.
    //===========================================================================================================

    /**
     * Interface for all test window functions.<br/>
     * This function is generalized to accept an array of columns (<code>DataValueDescriptor[]</code>) as
     * input and return one column value as output.<br/>
     * This array of columns can be a whole <code>ExecRow</code> or only contain one value. It is assumed the
     * function can be initialed with, and maintain,
     */
    public interface ExpectedResultsFunction {
        DataValueDescriptor apply(DataValueDescriptor[] input) throws StandardException;
        DataValueDescriptor getNullReturnValue() throws StandardException;
        void reset();
    }

    public abstract static class RankingFunct implements ExpectedResultsFunction {
        protected final int[] partitionColIDs;
        protected final int[] orderByColIDs;

        protected DataValueDescriptor[] lastRow;
        protected int rowNum;

        /**
         * Init with the identifiers of the partition and orderby columns. Ranking functions always use orderby
         * columns on which to apply the function to determine a change in rank. If the current row's orderby
         * columns are different from the last row's orderby columns, we have a change in rank.  If not, we don't.
         * <p/>
         * We always assume these column identifiers are 1-based at this point because this interaction
         * usually occurs at the column level rather than the array level. Thus, we check that subtracting
         * 1 from values does not produce a negative value for any column index. If it does, a runtime
         * exception is thrown. This is not, of course, a foolproof method of checking, nor does there exist
         * one. The hope is that this doc will be read.
         *
         * @param partitionColIDs the 1-based partition column identifiers. Can be empty.
         * @param orderByColIDs the 1-based orderby column identifiers. Can be empty.
         * @throws RuntimeException if subtracting 1 from any col value produces a negative value.
         */
        protected RankingFunct(int[] partitionColIDs, int[] orderByColIDs) {
            this.partitionColIDs = convertToZeroBased(partitionColIDs);
            this.orderByColIDs = convertToZeroBased(orderByColIDs);
        }

        public DataValueDescriptor getNullReturnValue() throws StandardException {
            return LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_LONGINT_ID);
        }

        /**
         * Reset function state if partition or frame changes.
         */
        public abstract void reset();

        /**
         * Check if the DVDs in each array located at the given <code>colIDs</code> are different.<br/>
         * This algorithm is 0-based since in this context we're working at the array level, rather than
         * at the column level.
         * <p/>
         * Returns <code>false</code> on the first differing comparison.
         *
         * @param expected the array of expected values to use to compare. It's an error if the array does
         *                 not contain a DVD at all <code>colID</code> values.
         * @param actual   the array of expected values to use to compare. It's an error if the array does
         *                 not contain a DVD at all <code>colID</code> values.
         * @param colIDs   the array of 0-based column indexes to compare.
         * @return <code>true</code> <i>all</i> column comparisons between <code>expected</code> and <code>actual</code>
         * compare equal.
         * @throws StandardException if there's an error in the DVD compare.
         * @throws RuntimeException if either array does not contain a DVD value at one of the indexes in
         * <code>colIDs</code> or the index is out of range.
         */
        protected static boolean dvdArraysDiffer(DataValueDescriptor[] expected, DataValueDescriptor[] actual,
                                                 int[] colIDs) throws StandardException {
            if (expected == null || actual == null) {
                return true;
            }
            for (int colID : colIDs) {
                // colIDs are 1-based
                if (colID < 0 || colID >= expected.length) {
                    throw new RuntimeException("Expected DVD size " + expected.length + " attempting to access col " + colID);
                }
                if (colID < 0 || colID >= actual.length) {
                    throw new RuntimeException("Actual DVD size " + actual.length + " attempting to access col " + colID);
                }
                if (!(actual[colID].compare(expected[colID]) == 0)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Take a slice of the given <code>row</code> using the given column indexes.
     *
     * @param row the array of DVDs, i.e., ExecRow, from which to take the slice.
     * @param columnIDs the indexes, in order, of the wanted columns.
     * @return a DVD array of the requested columns in the order in which they
     * were requested. If <code>columnIDs</code> is empty, a clone of the array
     * is returned.
     * @throws java.lang.RuntimeException if any of the requested array indices
     * are out of bounds.
     */
    public static DataValueDescriptor[] slice(DataValueDescriptor[] row, int[] columnIDs) {
        if (columnIDs.length == 0) {
            return row.clone();
        }
        ValueRow templateRow = new ValueRow(columnIDs.length);
        for (int colID : columnIDs) {
            if (colID < 0 || colID >= row.length) {
                throw new RuntimeException("A column ID, "+colID+", is outside the range of the row: "+row.length);
            }
            templateRow.setColumn(colID, row[colID]);
        }
        return templateRow.getRowArray();
    }

    /**
     * Join two int arrays preserving order, identity and size.
     *
     * @param left first array
     * @param right second array
     * @return the joined arrays
     */
    public static int[] join(int[] left, int[] right) {
        int[] joined = new int[left.length+right.length];
        int i=0;
        for (int val : left) {
            joined[i++] = val;
        }
        for (int val : right) {
            joined[i++] = val;
        }
        return joined;
    }

    /**
     * Comparator used to sort <code>ExecRow</code>s when creating test input rows.<br/>
     * We sort the rows of input data to replicate the way <code>WindowOperation</code> gets
     * rows from TEMP - sorted by partition and order by columns.
     * <p/>
     * The way we sort is is simple; we concatenate the <code>toString()</code> of each column
     * index in <code>sortCols</code> and compare the resulting strings.
     */
    public static class  ExecRowComparator implements Comparator<ExecRow> {
        private final int[] sortCols;

        public ExecRowComparator(int[] sortCols) {
            this.sortCols = sortCols;
        }

        @Override
        public int compare(ExecRow lr, ExecRow rr) {
            StringBuilder lBuf = new StringBuilder();
            StringBuilder rBuf = new StringBuilder();
            for (int sortCol : sortCols) {
                try {
                    lBuf.append(lr.getColumn(sortCol).toString());
                    rBuf.append(rr.getColumn(sortCol).toString());
                } catch (StandardException e) {
                    throw new RuntimeException("Error fetching col from execRow");
                }
            }
            return lBuf.toString().compareTo(rBuf.toString());
        }
    }


    public static int[] convertToZeroBased(int[] oneBased) {
        int[] zeroBased = new int[oneBased.length];
        for (int i=0; i<oneBased.length; i++) {
            int newIndex = oneBased[i] - 1;
            if (newIndex < 0) throw new RuntimeException("Column IDs should be one based, not zero based.");
            zeroBased[i] = newIndex;
        }
        return zeroBased;
    }

}