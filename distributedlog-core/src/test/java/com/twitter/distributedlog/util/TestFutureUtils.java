package com.twitter.distributedlog.util;

import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * Test Case for {@link FutureUtils}
 */
public class TestFutureUtils {

    static class TestException extends IOException {
    }

    @Test(timeout = 60000)
    public void testWithin() throws Exception {
        OrderedScheduler scheduler = OrderedScheduler.newBuilder()
                .corePoolSize(1)
                .name("test-within")
                .build();
        final Promise<Void> promiseToTimeout = new Promise<Void>();
        final Promise<Void> finalPromise = new Promise<Void>();
        FutureUtils.within(
                promiseToTimeout,
                10,
                TimeUnit.MILLISECONDS,
                new TestException(),
                scheduler,
                "test-within"
        ).addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.setException(finalPromise, cause);
            }

            @Override
            public void onSuccess(Void value) {
                FutureUtils.setValue(finalPromise, value);
            }
        });
        try {
            FutureUtils.result(finalPromise);
            fail("Should fail with TestException");
        } catch (TestException te) {
            // expected
        }
    }

}
