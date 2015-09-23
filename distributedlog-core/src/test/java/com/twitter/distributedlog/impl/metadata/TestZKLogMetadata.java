package com.twitter.distributedlog.impl.metadata;

import com.twitter.distributedlog.DLMTestUtil;
import org.junit.Test;

import java.net.URI;

import static com.twitter.distributedlog.impl.metadata.ZKLogMetadata.*;
import static org.junit.Assert.*;

public class TestZKLogMetadata {

    @Test(timeout = 60000)
    public void testGetPaths() throws Exception {
        String rootPath = "/test-get-paths";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        String logRootPath = uri.getPath() + "/" + logName + "/" + logIdentifier;
        String logSegmentName = "test-segment";

        ZKLogMetadata logMetadata = new ZKLogMetadata(uri, logName, logIdentifier);
        assertEquals("wrong log name", logName, logMetadata.getLogName());
        assertEquals("wrong root path", logRootPath, logMetadata.getLogRootPath());
        assertEquals("wrong log segments path",
                logRootPath + LOGSEGMENTS_PATH,
                logMetadata.getLogSegmentsPath());
        assertEquals("wrong log segment path",
                logRootPath + LOGSEGMENTS_PATH + "/" + logSegmentName,
                logMetadata.getLogSegmentPath(logSegmentName));
        assertEquals("wrong lock path",
                logRootPath + LOCK_PATH, logMetadata.getLockPath());
        assertEquals("wrong max tx id path",
                logRootPath + MAX_TXID_PATH, logMetadata.getMaxTxIdPath());
        assertEquals("wrong allocation path",
                logRootPath + ALLOCATION_PATH, logMetadata.getAllocationPath());
        assertEquals("wrong qualified name",
                logName + ":" + logIdentifier, logMetadata.getFullyQualifiedName());
    }

}
