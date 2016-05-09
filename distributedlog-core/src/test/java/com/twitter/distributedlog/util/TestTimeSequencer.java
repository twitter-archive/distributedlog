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
package com.twitter.distributedlog.util;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestTimeSequencer {

    @Test(timeout = 60000)
    public void testNonDecreasingId() {
        TimeSequencer sequencer = new TimeSequencer();
        long lastId = System.currentTimeMillis() + 3600000;
        sequencer.setLastId(lastId);
        for (int i = 0; i < 10; i++) {
            assertEquals(lastId, sequencer.nextId());
        }
        sequencer.setLastId(15);
        long prevId = 15;
        for (int i = 0; i < 10; i++) {
            long newId = sequencer.nextId();
            assertTrue("id should not decrease",
                    newId >= prevId);
            prevId = newId;
        }
    }
}
