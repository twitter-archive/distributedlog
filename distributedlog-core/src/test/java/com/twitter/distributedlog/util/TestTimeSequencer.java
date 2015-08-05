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
