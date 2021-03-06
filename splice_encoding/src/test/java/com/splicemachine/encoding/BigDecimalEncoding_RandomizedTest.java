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

package com.splicemachine.encoding;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.testutil.RandomDerbyDecimalBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.assertTrue;

/*
 * Test BigDecimalEncoding with random values.
 */
@RunWith(Parameterized.class)
public class BigDecimalEncoding_RandomizedTest {

    private static final int NUM_TESTS = 50;
    private static final int NUM_VALUES_PER_TEST = 1000;
    private static final RandomDerbyDecimalBuilder DERBY_DECIMAL_BUILDER = new RandomDerbyDecimalBuilder().withNegatives(true);

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(NUM_TESTS);
        for (int i = 0; i < NUM_TESTS; i++) {
            data.add(new Object[]{DERBY_DECIMAL_BUILDER.buildArray(NUM_VALUES_PER_TEST)});
        }
        return data;
    }

    private final BigDecimal[] data;

    public BigDecimalEncoding_RandomizedTest(BigDecimal[] data) {
        this.data = data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(BigDecimal decimal:data){
            EncodingTestUtil.assertEncodeDecode(decimal);
            EncodingTestUtil.assertEncodeDecode(decimal.negate());
        }
    }

    @Test
    public void testSortsBytesCorrectly() throws Exception {
        List<byte[]> serializedDecimals = Lists.newArrayList();
        for (BigDecimal aData : data) {
            serializedDecimals.add(BigDecimalEncoding.toBytes(aData, false));
        }

        Collections.sort(serializedDecimals, Bytes.BASE_COMPARATOR);

        //deserialize
        BigDecimal last = null;
        BigDecimal current;
        for (byte[] serializedBytes : serializedDecimals) {
            current = BigDecimalEncoding.toBigDecimal(serializedBytes, false);
            assertTrue(String.format("last='%s', current='%s'", last, current), last == null || current.compareTo(last) >= 0);
            last = current;
        }
    }

    @Test
    public void testCanDecodeWhenManuallyConverted() throws Exception {
        /*
         * Some bits of code will manually convert from ascending to descending and back, this
         * makes sure that BigDecimals work correctly in that situation
         */

        for(BigDecimal testNum:data){
            byte[] bigDecBytes = Encoding.encode(testNum, false);

            BigDecimal result = Encoding.decodeBigDecimal(convertToDescending(bigDecBytes), true);

            assertTrue(result.compareTo(testNum) == 0);

            //check the negation as well
            BigDecimal t = testNum.negate();
            bigDecBytes = Encoding.encode(t, false);

            result = Encoding.decodeBigDecimal(convertToDescending(bigDecBytes), true);

            assertTrue(result.compareTo(t) == 0);
        }

    }

    private byte[] convertToDescending(byte[] bytes){
        byte[] retBytes = new byte[bytes.length];
        System.arraycopy(bytes,0,retBytes,0,bytes.length);
        for(int i=0;i<retBytes.length;i++){
            retBytes[i] ^=0xff;
        }

        return retBytes;
    }
}
