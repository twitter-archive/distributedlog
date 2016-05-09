/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestDLSN {

    @Test(timeout = 60000)
    public void testDLSN() {
        DLSN dlsn = new DLSN(99L, 88L, 77L);
        String dlsnv0 = dlsn.serialize(DLSN.VERSION0);
        String dlsnv1 = dlsn.serialize(DLSN.VERSION1);
        String badDLSN = "baddlsn";

        assertEquals(dlsn, DLSN.deserialize(dlsnv0));
        assertEquals(dlsn, DLSN.deserialize(dlsnv1));
        try {
            DLSN.deserialize(badDLSN);
            fail("Should fail on deserializing bad dlsn");
        } catch (IllegalArgumentException iae) {
        }

        assertEquals(dlsn, DLSN.deserialize0(dlsnv0));
        try {
            DLSN.deserialize0(dlsnv1);
            fail("Should fail on deserializing version one dlsn");
        } catch (IllegalArgumentException iae) {
        }
        try {
            DLSN.deserialize0(badDLSN);
            fail("Should fail on deserializing bad dlsn");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test(timeout = 60000)
    public void testSerializeDeserializeBytes() {
        DLSN dlsn = new DLSN(99L, 88L, 77L);
        byte[] data = dlsn.serializeBytes();
        assertEquals(dlsn, DLSN.deserializeBytes(data));
    }
}
