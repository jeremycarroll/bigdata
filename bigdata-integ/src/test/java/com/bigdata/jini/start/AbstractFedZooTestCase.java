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
 * Created on Jan 7, 2009
 */

package com.bigdata.jini.start;

import java.io.File;
import java.util.List;
import java.util.UUID;

import com.bigdata.DataFinder;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import org.junit.Assert;


import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.resources.ResourceFileFilter;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;


/**
 * Abstract base class for unit tests requiring a running zookeeper and a
 * running federation as configured from a test resource.
 * <p>
 * You MUST specify a security policy, e.g.:
 * 
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * for these tests to run.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractFedZooTestCase {
    
    // A configuration file used by some of the unit tests in this package.
   protected final String configFile = DataFinder.bestURI("testing/data/com/bigdata/jini/start/testfed.config").toASCIIString();

   // ACL used for the unit tests.
   protected final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
   
   protected final MockListener listener = new MockListener();

   Configuration config;
   JiniFederation fed;

   String zrootname = null;


    public AbstractFedZooTestCase() {
    }


    
    public void setUp(String name) throws Exception {

        zrootname = name + "_" + UUID.randomUUID();

        if (new File(zrootname).exists()) {
            // clean out old files.
            recursiveDelete(new File(zrootname));
        }
        
        // a unique zroot in the /test namespace.
        final String zroot = "/"+zrootname;//"/test/" + zrootname;

        System.err.println(name + ": setting up zrootname=" + zrootname);

        final String[] args = new String[] { configFile,
                // Note: overrides the zroot to be unique.
                ZookeeperClientConfig.Options.NAMESPACE + "."
                        + ZookeeperClientConfig.Options.ZROOT + "=" + "\""
                        + zroot + "\"" ,
//                // Override the federation name.
//                "bigdata.fedname=\""+fedname+"\""
                };
        
        // apply the federation name to the configuration file.
        System.setProperty("bigdata.zrootname", zrootname);

        config = ConfigurationProvider.getInstance(args);

        // if necessary, start zookeeper (a server instance).
        ZookeeperProcessHelper.startZookeeper(config, listener);

        /*
         * FIXME We need to start a jini lookup service for groups = {fedname}
         * for this test to succeed.
         */
        
        fed = JiniClient.newInstance(args).connect();

        /*
         * Create the federation zroot and config znodes.
         */
        final ZooKeeper zookeeper = fed.getZookeeper();

        // make sure that we have the zroot that we overrode above.
        Assert.assertEquals(zroot, fed.getZooConfig().zroot);

        fed.createKeyZNodes(zookeeper);
    }

    public void tearDown(String name) throws Exception {

        System.err.println(name + ": tearing down zrootname=" + zrootname);

        // destroy any processes started by this test suite.
        for (ProcessHelper t : listener.running) {           
            t.kill(true/*immediateShutdown*/);           
        }

        if (fed != null) {

            /*
             * @todo if we do this to kill zk then we must ensure that a private
             * instance was started on the desired port. That means an override
             * for the configuration file and an unused port assigned for the
             * client and peers on the zk instance started for this unit test.
             */
//            ZooHelper.kill(clientPort);
            
            fed.shutdownNow();           
        }

        if (zrootname != null && new File(zrootname).exists()) {

            /*
             * Wait a bit and then try and delete the federation directory
             * structure.
             */
            
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            recursiveDelete(new File(zrootname));
        }        
    }

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * <p>
     * Note: Files that are not recognized will be logged by the
     * {@link ResourceFileFilter}.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {
            final File[] children = f.listFiles();

            if (children == null) {
                // The directory does not exist.
                return;                
            }
            
            for (int i = 0; i < children.length; i++) {
                recursiveDelete(children[i]);
            }
        }
    }
}
