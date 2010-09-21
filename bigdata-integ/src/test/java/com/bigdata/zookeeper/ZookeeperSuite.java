
package com.bigdata.zookeeper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author blevine
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestHierarchicalZNodeWatcher.class,
    TestUnknownChildrenWatcher.class,
    TestZLockImpl.class,
    TestZNodeCreatedWatcher.class,
    TestZNodeDeletedWatcher.class,
    TestZooBarrier.class,
    TestZooElection.class,
    TestZookeeperAccessor.class,
    TestZooQueue.class
})

public class ZookeeperSuite
{
}
