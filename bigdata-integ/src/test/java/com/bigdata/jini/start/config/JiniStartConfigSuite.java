
package com.bigdata.jini.start.config;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author blevine
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestServiceConfiguration.class,
    TestServiceConfigurationRemote.class,
    TestZookeeperServerEntry.class
})

public class JiniStartConfigSuite
{
}
