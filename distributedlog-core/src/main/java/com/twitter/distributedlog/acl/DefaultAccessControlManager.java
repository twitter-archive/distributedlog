package com.twitter.distributedlog.acl;

public class DefaultAccessControlManager implements AccessControlManager {

    public static final DefaultAccessControlManager INSTANCE = new DefaultAccessControlManager();

    private DefaultAccessControlManager() {
    }

    @Override
    public boolean allowWrite(String stream) {
        return true;
    }

    @Override
    public boolean allowTruncate(String stream) {
        return true;
    }

    @Override
    public boolean allowDelete(String stream) {
        return true;
    }

    @Override
    public boolean allowAcquire(String stream) {
        return true;
    }

    @Override
    public boolean allowRelease(String stream) {
        return true;
    }

    @Override
    public void close() {
    }
}
