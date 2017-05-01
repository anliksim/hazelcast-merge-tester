package com.anlikers.hazelcast.merge;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.map.MergePolicyTest;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.anlikers.hazelcast.merge.HazelcastMergeUtil.newConfig;
import static com.anlikers.hazelcast.merge.HazelcastMergeUtil.randomMapName;
import static com.anlikers.hazelcast.merge.HazelcastMergeUtil.sleepAtLeastMillis;
import static org.junit.Assert.assertEquals;

/**
 * Verifying {@link LatestUpdateMapMergePolicy} as seen in hazelcast's {@link MergePolicyTest}
 *
 * @author Simon Anliker
 * @see MergePolicyTest#testLatestUpdateMapMergePolicy()
 */
public class LastUpdateHazelcastMergeTest extends AbstractHazelcastMergeTest {

    private static final String MAP_NAME = randomMapName();

    @Before
    public void setUp() {
        Config config = newConfig(LatestUpdateMapMergePolicy.class.getName(), MAP_NAME);
        super.setUp(Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config));
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    @Override
    public void testMergePolicy() {
        super.testMergePolicy();
    }

    @Override
    protected void performAction() {
        IMap<Object, Object> map1 = h1.getMap(MAP_NAME);
        IMap<Object, Object> map2 = h2.getMap(MAP_NAME);
        map1.put("key1", "value");
        // prevent updating at the same time
        sleepAtLeastMillis(1);
        map2.put("key1", "LatestUpdatedValue");
        map2.put("key2", "value2");
        // prevent updating at the same time
        sleepAtLeastMillis(1);
        map1.put("key2", "LatestUpdatedValue2");
    }

    @Override
    protected void evaluate() {
        IMap<Object, Object> mapTest = h1.getMap(MAP_NAME);
        assertEquals("LatestUpdatedValue", mapTest.get("key1"));
        assertEquals("LatestUpdatedValue2", mapTest.get("key2"));
    }
}