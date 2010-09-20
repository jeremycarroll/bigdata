/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jan 6, 2009
 */

package com.bigdata.jini.start.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.bigdata.DataFinder;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;

import com.bigdata.jini.start.config.BigdataServiceConfiguration;
import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.TransactionServerConfiguration;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.util.config.ConfigDeployUtil;


/**
 * Some unit tests for {@link ServiceConfiguration} and friends focused on
 * verifying correct extraction of properties and the correct generation of
 * command lines and configuration files.
 * <p>
 * Note: all of this can be tested directly since we can parse the generated
 * configuration files.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo not testing correct generation of command lines
 * 
 * @todo not testing correct generation of configuration files.
 */
public class TestServiceConfiguration {

    protected boolean serviceImplRemote;

    /**
     * 
     */
    public TestServiceConfiguration() {
        this.serviceImplRemote = false;
    }

    protected TestServiceConfiguration(boolean serviceImplRemote) {
        this.serviceImplRemote = serviceImplRemote;
    }

    /**
     * A configuration file used by some of the unit tests in this package.
     */
    private final String configFile = DataFinder.bestURI("testing/data/com/bigdata/jini/start/config/testfed.config").toASCIIString();

    /**
     * 
     * 
     * @throws FileNotFoundException
     * @throws ConfigurationException
     */
    @Test
    public void test01() throws FileNotFoundException, ConfigurationException {

        // Note: reads from a URI.
        final Configuration config = ConfigurationProvider.getInstance(new String[] { configFile });

        System.err.println(Arrays.toString((String[])config.getEntry(
                ServiceConfiguration.class.getName(), "classpath",
                String[].class)));
        
        File serviceDirFromConfig = 
            (File)config.getEntry(ServiceConfiguration.class.getName(), "serviceDir",
                                  File.class, new File("serviceDir-NOT-SET"));

        BigdataServiceConfiguration serviceConfig = null;
        if(serviceImplRemote) {
            serviceConfig = new TransactionServerConfiguration
                                    (TransactionServer.class, config);
            Assert.assertEquals(TransactionServer.class.getName(), serviceConfig.className);
            Assert.assertArrayEquals(new String[] {"-Xmx1G", "-server"}, serviceConfig.args);
            Assert.assertArrayEquals(
                new String[] { "com.bigdata.service.jini.TransactionServer.Options.SNAPSHOT_INTERVAL=60000" },
                serviceConfig.options);
        } else {
            serviceConfig = 
                new TransactionServerConfiguration
                        (com.bigdata.transaction.ServiceImpl.class, config);

            Assert.assertEquals(com.bigdata.transaction.ServiceImpl.class.getName(), serviceConfig.className);

            String appHome = System.getProperty("app.home");
            String fSep = System.getProperty("file.separator");
            String logDir = 
                appHome+fSep+"dist"+fSep+"bigdata"+fSep+"var"+fSep+"log";
            String logFile =
                appHome+fSep+"dist"+fSep+"bigdata"+fSep+"var"+fSep+"config"
                       +fSep+"logging"+fSep+"transaction-logging.properties";
            String configFile =
                appHome+fSep+"dist"+fSep+"bigdata"+fSep+"var"+fSep+"config"
                       +fSep+"jini"+fSep+"transaction.config";

            String memVal = "-Xmx1G";
            String securityMgr = "-Djava.security.manager=";
            String log4jProp = "-Dlog4j.configuration="+logFile;
            String log4jPrimProp = "-Dlog4j.primary.configuration="+logFile;
            String logDirProp = "-Dbigdata.logDir="+logDir;
            String javaUtilProp = "-Djava.util.logging.config.file="+logFile;
            String appHomeProp = "-Dapp.home="+appHome;
            String configProp = "-Dconfig="+configFile;
            String usingProp = "-DusingServiceConfiguration=true";

            String[] expectedArgsArray = 
                new String[] { memVal, securityMgr, log4jProp, log4jPrimProp,
                               javaUtilProp, logDirProp, appHomeProp,
                               configProp, usingProp };

            Assert.assertArrayEquals(expectedArgsArray, serviceConfig.args);

            String quote = "\"";
            String comma = ",";
            String openBracket = "{";
            String closeBracket = "}";
            String snapshotOpt = "com.bigdata.transaction.EmbeddedTransactionService.Options.SNAPSHOT_INTERVAL=60000";
            String groupsOpt = "com.bigdata.transaction.groupsToJoin="
                               +"new String[]"
                               +openBracket
                               +quote+ /*System.getProperty("federation.name","testFed")*/ ConfigDeployUtil.getFederationName() +quote
                               +comma
                               +quote+System.getProperty("bigdata.zrootname","testZroot")+quote
                               +closeBracket;
            String locatorsOpt = "com.bigdata.transaction.locatorsToJoin="
                                 +"new LookupLocator[]"
                                 +openBracket
                                 +closeBracket;

            String[] expectedOptsArray = new String[] { snapshotOpt, groupsOpt, locatorsOpt };

            Assert.assertArrayEquals( expectedOptsArray, serviceConfig.options);
        }

        Assert.assertEquals(serviceDirFromConfig, serviceConfig.serviceDir);
        Assert.assertEquals(1, serviceConfig.serviceCount);
        Assert.assertEquals(1, serviceConfig.replicationCount);
        Assert.assertArrayEquals(new IServiceConstraint[0], serviceConfig.constraints);        
    }
}
