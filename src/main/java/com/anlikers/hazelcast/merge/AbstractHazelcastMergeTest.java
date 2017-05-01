package com.anlikers.hazelcast.merge;

import com.anlikers.hazelcast.merge.listener.MemberRemovedListener;
import com.anlikers.hazelcast.merge.listener.MergedStateListener;
import com.hazelcast.core.HazelcastInstance;

import static com.anlikers.hazelcast.merge.HazelcastMergeUtil.assertClusterSizeEventually;

/**
 * @author Simon Anliker
 */
public abstract class AbstractHazelcastMergeTest {

    protected HazelcastInstance h1;
    protected HazelcastInstance h2;

    protected void setUp(HazelcastInstance h1, HazelcastInstance h2) {
        this.h1 = h1;
        this.h2 = h2;
        HazelcastMergeUtil.warmUpPartitions(h1, h2);
    }

    protected void tearDown() {
        HazelcastMergeUtil.killAllHazelcastInstances();
    }

    public void testMergePolicy() {

        MemberRemovedListener memberRemovedListener = new MemberRemovedListener(1);
        h2.getCluster().addMembershipListener(memberRemovedListener);

        MergedStateListener mergedStateListener = new MergedStateListener(1);
        h2.getLifecycleService().addLifecycleListener(mergedStateListener);

        HazelcastMergeUtil.closeConnectionBetween(h1, h2);

        memberRemovedListener.assertRemovedEventually();
        HazelcastMergeUtil.assertClusterSizeEventually(1, h1);
        HazelcastMergeUtil.assertClusterSizeEventually(1, h2);

        performAction();

        mergedStateListener.allowMergeProcessToContinue();

        mergedStateListener.assertMergedEventually();
        HazelcastMergeUtil.assertClusterSizeEventually(2, h1);
        HazelcastMergeUtil.assertClusterSizeEventually(2, h2);

        evaluate();
    }

    protected abstract void performAction();

    protected abstract void evaluate();
}
