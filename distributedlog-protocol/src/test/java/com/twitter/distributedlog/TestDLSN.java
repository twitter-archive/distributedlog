package com.twitter.distributedlog;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestDLSN {

    @Test
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
}
