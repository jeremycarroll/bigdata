
package com.bigdata.jini.start;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author blevine
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestJiniCoreServicesProcessHelper.class,
    TestServiceConfigurationMonitoring.class,
    TestServiceConfigurationMonitoringRemote.class,
    TestServiceConfigurationZNodeEnum.class,
    TestServiceStarter.class,
    TestServiceStarterRemote.class
})
public class JiniStartSuite
{
}
