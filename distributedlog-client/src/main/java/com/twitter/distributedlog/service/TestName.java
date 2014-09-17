package com.twitter.distributedlog.service;

import java.net.SocketAddress;
import java.util.List;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.Addr;
import com.twitter.finagle.Name;

public class TestName implements Name {
    static final Logger LOG = LoggerFactory.getLogger(TestName.class);
    private AbstractFunction1<Addr, BoxedUnit> callback = null;

    public void changes(AbstractFunction1<Addr, BoxedUnit> callback) {
        this.callback = callback;
    }

    public void changeAddrs(List<SocketAddress> addresses) {
        if (null != callback) {
            LOG.info("Sending a callback {}", addresses);
            callback.apply(Addr.Bound$.MODULE$.apply(addresses));
        }
    }
}
