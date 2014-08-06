package com.twitter.distributedlog.tools;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.ReflectionUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.ZooDefs;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.metadata.DryrunZkMetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.distributedlog.tools.DistributedLogTool.*;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;

import static org.junit.Assert.*;

public class TestDistributedLogTool extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogTool.class);

    static final String defaultLedgerPath = LocalDLMEmulator.getBkLedgerPath();
    static final String defaultPath = "/test/namespace";
    static final String defaultHost = "127.0.0.1";
    static final String defaultPrivilegedZkAclId = "NathanielP";
    static URI defaultUri = null;

    static final String ADMIN_TOOL = com.twitter.distributedlog.admin.DistributedLogAdmin.class.getName();

    @BeforeClass
    public static void setupDefaults() throws Exception {
        defaultUri = DLMTestUtil.createDLMURI(defaultPath);
        DistributedLogManager dlm = DLMTestUtil.createNewDLM("DefaultStream", conf, defaultUri);
        bindStream(defaultUri, defaultLedgerPath, defaultHost);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 3, 8192);
        dlm.close();
    }

    private static int runTool(String[] args) throws Exception {
        Tool tool = ReflectionUtils.newInstance(args[0], Tool.class);
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        int rc = tool.run(newArgs);
        assertTrue(0 == rc);
        return rc;
    }

    static void bindStream(URI uri, String ledgerPath, String zkHosts) throws Exception {
        String[] args = new String[] { ADMIN_TOOL, 
            "bind", 
            "-c", 
            "-l", ledgerPath, 
            "-s", zkHosts, 
            "-f", uri.toString() };
        runTool(args);
    }

    static void createStream(URI uri, String prefix, String expression, String zkAclId) throws Exception {
        CreateCommand cmd = new CreateCommand();
        cmd.setUri(defaultUri);
        cmd.setPrefix(prefix);
        cmd.setExpression(expression);
        cmd.setForce(true);
        cmd.setZkAclId(zkAclId);
        assertEquals(0, cmd.runCmd());
    }

    void deleteStream(URI uri, String stream) throws Exception {
        DeleteCommand cmd = new DeleteCommand();
        cmd.setUri(defaultUri);
        cmd.setStreamName(stream);
        assertEquals(0, cmd.runCmd());
    }

    void list(URI uri) throws Exception {
        ListCommand cmd = new ListCommand();
        cmd.setUri(defaultUri);
        assertEquals(0, cmd.runCmd());
    }

    @Test(timeout = 60000)
    public void testToolCreate() throws Exception {
        createStream(defaultUri, "0", "TestPrefix", null);
    }

    @Test(timeout = 60000)
    public void testToolCreateZkAclId() throws Exception {
        createStream(defaultUri, "0", "CreateAclStream", defaultPrivilegedZkAclId);
        try {
            DistributedLogManager dlm = DLMTestUtil.createNewDLM("0CreateAclStream", conf, defaultUri);
            DLMTestUtil.generateCompletedLogSegments(dlm, conf, 3, 1000);
            dlm.close();
        } catch (com.twitter.distributedlog.LockingException ex) {
            assertEquals(NoAuthException.class, ex.getCause().getClass());
        }
    }

    @Test(timeout = 60000)
    public void testToolDelete() throws Exception {
        createStream(defaultUri, "1", "TestPrefix", null);
        deleteStream(defaultUri, "1TestPrefix");
    }

    @Test(timeout = 60000)
    public void testToolDeleteAllocPool() throws Exception {
        try {
            DeleteAllocatorPoolCommand cmd = new DeleteAllocatorPoolCommand();
            cmd.setUri(defaultUri);
            assertEquals(0, cmd.runCmd());
            fail("should have failed");
        } catch (org.apache.zookeeper.KeeperException.NoNodeException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testToolList() throws Exception {
        list(defaultUri);
    }

    @Test(timeout = 60000)
    public void testToolDump() throws Exception {
        DumpCommand cmd = new DumpCommand();
        cmd.setUri(defaultUri);
        cmd.setStreamName("DefaultStream");
        cmd.setFromTxnId(Long.valueOf(0));
        assertEquals(0, cmd.runCmd());
    }

    @Test(timeout = 60000)
    public void testToolShow() throws Exception {
        ShowCommand cmd = new ShowCommand();
        cmd.setUri(defaultUri);
        cmd.setStreamName("DefaultStream");
        assertEquals(0, cmd.runCmd());
    }

    @Test(timeout = 60000)
    public void testToolTruncate() throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM("TruncateStream", conf, defaultUri);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 3, 1000);
        dlm.close();

        TruncateCommand cmd = new TruncateCommand();
        cmd.setUri(defaultUri);
        cmd.setFilter("TruncateStream");
        cmd.setForce(true);
        assertEquals(0, cmd.runCmd());
    }    

    @Test(timeout = 60000)
    public void testToolInspect() throws Exception {
        InspectCommand cmd = new InspectCommand();
        cmd.setUri(defaultUri);
        cmd.setForce(true);
        assertEquals(0, cmd.runCmd());
    }

    @Test(timeout = 60000)
    public void testToolReadLastConfirmed() throws Exception {
        ReadLastConfirmedCommand cmd = new ReadLastConfirmedCommand();
        cmd.setUri(defaultUri);
        cmd.setLedgerId(99999999);

        // Too hard to predict ledger entry id. Settle for basicaly 
        // correct functionality.
        try {
            cmd.runCmd();
        } catch (BKNoSuchLedgerExistsException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testToolReadEntriesCommand() throws Exception {
        ReadEntriesCommand cmd = new ReadEntriesCommand();
        cmd.setUri(defaultUri);
        cmd.setLedgerId(99999999);
        try {
            cmd.runCmd();
        } catch (BKNoSuchLedgerExistsException ex) {
        }
    }
}
