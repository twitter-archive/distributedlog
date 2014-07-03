package com.twitter.distributedlog.service.announcer;

import java.io.IOException;

public class NOPAnnouncer implements Announcer {
    @Override
    public void announce() throws IOException {
        // nop
    }

    @Override
    public void unannounce() throws IOException {
        // nop
    }

    @Override
    public void close() {
        // nop
    }
}
