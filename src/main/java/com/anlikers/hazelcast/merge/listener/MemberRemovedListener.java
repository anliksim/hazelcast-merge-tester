package com.anlikers.hazelcast.merge.listener;

import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.concurrent.CountDownLatch;

import static com.anlikers.hazelcast.merge.HazelcastMergeUtil.assertOpenEventually;

/**
 * @author Simon Anliker
 */
public class MemberRemovedListener implements MembershipListener {

    private final CountDownLatch memberRemovedLatch;

    public MemberRemovedListener(int countdown) {
        memberRemovedLatch = new CountDownLatch(countdown);
    }

    public void assertRemovedEventually() {
        assertOpenEventually(memberRemovedLatch);
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        memberRemovedLatch.countDown();
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        // only interested in remove events
    }

    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        // only interested in remove events
    }
}
