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

package com.splicemachine.primitives;

import org.junit.Test;
import java.util.Random;
import static org.junit.Assert.*;

/**
 * Created by jleach on 11/11/15.
 */
public class BytesTest {

    @Test
    public void toHex_fromHex() {
        byte[] bytesIn = new byte[1024];
        new Random().nextBytes(bytesIn);

        String hex = Bytes.toHex(bytesIn);
        byte[] bytesOut = Bytes.fromHex(hex);

        assertArrayEquals(bytesIn, bytesOut);
    }

    @Test
    public void bytesToLong() {
        long longIn = new Random().nextLong();
        byte[] bytes = Bytes.longToBytes(longIn);
        long longOut = Bytes.bytesToLong(bytes, 0);
        assertEquals(longIn, longOut);
    }
}
