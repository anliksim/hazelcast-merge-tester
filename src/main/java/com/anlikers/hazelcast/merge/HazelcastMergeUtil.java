package com.anlikers.hazelcast.merge;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.concurrent.CountDownLatch;

/**
 * @author Simon Anliker
 * @see HazelcastTestSupport
 */
public class HazelcastMergeUtil {

    private HazelcastMergeUtil() {
        // utility constructor
    }

    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    public static String randomMapName() {
        return HazelcastTestSupport.randomMapName();
    }

    public static Config newConfig(String mergePolicy, String mapName) {
        Config config = new Config()
                .setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                .setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3")
                .setProperty(GroupProperty.LOGGING_TYPE.getName(), "slf4j");

        config.getGroupConfig()
                .setName(HazelcastTestSupport.generateRandomString(10));

        config.getMapConfig(mapName)
                .setMergePolicy(mergePolicy);

        return config;
    }

    public static void sleepAtLeastMillis(long sleepFor) {
        HazelcastTestSupport.sleepAtLeastMillis(sleepFor);
    }

    public static void warmUpPartitions(HazelcastInstance... instances) {
        HazelcastTestSupport.warmUpPartitions(instances);
    }

    public static void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        HazelcastTestSupport.closeConnectionBetween(h1, h2);
    }

    public static void assertOpenEventually(CountDownLatch latch) {
        HazelcastTestSupport.assertOpenEventually(latch);
    }

    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance instance) {
        HazelcastTestSupport.assertClusterSizeEventually(expectedSize, instance);
    }

    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            HazelcastTestSupport.assertClusterSizeEventually(expectedSize, instance);
        }
    }
}
