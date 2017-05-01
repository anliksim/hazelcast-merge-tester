package com.anlikers.hazelcast.merge.listener;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.anlikers.hazelcast.merge.HazelcastMergeUtil.assertOpenEventually;

/**
 * @author Simon Anliker
 */
public class MergedStateListener implements LifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(MergedStateListener.class);
    private static final long USER_CONTINUE_TIMEOUT = 30;

    private final CountDownLatch mergeFinishedLatch;
    private final CountDownLatch mergeBlockingLatch;

    public MergedStateListener(int countdown) {
        this.mergeFinishedLatch = new CountDownLatch(countdown);
        this.mergeBlockingLatch = new CountDownLatch(1);
    }

    public void assertMergedEventually() {
        assertOpenEventually(mergeFinishedLatch);
    }

    public void allowMergeProcessToContinue() {
        mergeBlockingLatch.countDown();
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        if (event.getState() == LifecycleEvent.LifecycleState.MERGING) {
            if (!awaitUserContinue()) {
                handleUserContinueTimeout();
            }
        } else if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
            notifyOnMerged();
        }
    }

    protected void handleUserContinueTimeout() {
        // no operation by default
    }

    private void notifyOnMerged() {
        mergeFinishedLatch.countDown();
    }

    private boolean awaitUserContinue() {
        try {
            return mergeBlockingLatch.await(USER_CONTINUE_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Waiting for user continue was interrupted!", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
