/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

package com.bigdata.quorum;

// NOTE: remove commented out references to org.junit and annotations
//       when/if the junit infrastructure is upgraded to a version that
//       supports those constructs.

import static junit.framework.Assert.*;

//import static org.junit.Assert.*;
//import org.junit.After;
//import org.junit.BeforeClass;
//import org.junit.Test;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.jini.quorum.QuorumPeerManager;
import com.bigdata.service.QuorumPeerService;
import com.bigdata.util.Util;
import com.bigdata.util.config.NicUtil;
import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.ConfigurationUtil;
import com.bigdata.util.config.LogUtil;

import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.NonActivatableServiceDescriptor.Created;
import net.jini.admin.Administrable;
import net.jini.config.AbstractConfiguration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RMISecurityManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/*
 * Tests the QuorumPeerService smart proxy implementation that wraps the
 * ZooKeeper QuorumPeerMain server. Tests include starting and stopping
 * multiple instances of the service, discovering the service as a Jini
 * service, and verification of various client interactions with the
 * service.
 *
 * NOTE: The intent of this test class is to start a single ensemble
 *       of N QuorumPeerService instances once, when this class is
 *       instantiated, allowing all test methods of this class to interact
 *       with only that one ensemble, and then shutdown the ensemble
 *       only after all tests have been run (or this class exits on
 *       failure); rather than start and stop a new ensemble with each
 *       test method executed. With more recent versions of junit, this
 *       is possible using annotations such as @BeforeClass and @After.
 *       Unfortunately, the current version of junit that is being used
 *       does not support such annotations. Additionally, the current
 *       design of the test infrastructure expects that this test class
 *       sub-classes org.junit.TestCase, which generally requires that
 *       constructors be provided with this class.
 *
 *       In order to achieve the desired intent described above, while
 *       adhering to the requirements imposed by the current test
 *       framework, this class provide a method (beforeClassSetup) that
 *       is invoked once (from within the constructor), prior to the
 *       execution of any of the test methods; and a method that is
 *       invoked only after all the test methods of this class have been
 *       invoked (afterClassTeardown).
 *
 *       When/if the junit infrastructure used by this test framework is
 *       ever upgraded to a version that supports annotations, and this test
 *       class is changed so that it no longer has to extend TestCase, then
 *       the appropriate changes should be made to this class to exploit
 *       the features of that new version; as indicated in the documentation
 *       below.
 */
public class QuorumPeerServiceTest extends TestCase {

    private static String pSep = System.getProperty("path.separator");
    private static String fSep = System.getProperty("file.separator");
    private static String tmpDir = System.getProperty("java.io.tmpdir");
    private static String userDir = System.getProperty("user.dir");
    private static String policy =
                            System.getProperty("java.security.policy");
    private static String log4jJar = System.getProperty("log4j.jar");
    private static String jskPlatformJar =
                     System.getProperty("jsk-platform.jar");
    private static String jskLibJar = System.getProperty("jsk-lib.jar");
    private static String zookeeperJar = System.getProperty("zookeeper.jar");
    private static String federationName =
                            System.getProperty("federation.name");
    private static String bigdataRoot = System.getProperty("app.home");
    private static String stateBase = tmpDir+fSep+"state";
    private static String codebasePortStr = System.getProperty
                                                ("codebase.port","23333");
    private static int codebasePort = Integer.parseInt(codebasePortStr);

    private static Logger logger;

    // for starting smart proxy implementation of ZooKeeper quorum server
    private static String thisHost;
    private static String jskCodebase;
    private static String groups;//for overriding the configured groups

    private static int[]  quorumClientPort = {2180, 2181, 2182};
    private static int[]  quorumPeerPort = {2887, 2888, 2889};
    private static int[]  quorumElectionPort = {3887, 3888, 3889};
    private static String quorumServerCodebase;
    private static String quorumCodebase;
    private static String quorumClasspath = jskPlatformJar+pSep
                                            +jskLibJar+pSep
                                            +zookeeperJar+pSep
                                            +log4jJar;
    private static String quorumImplName = "com.bigdata.quorum.ServiceImpl";
    private static String quorumConfig = bigdataRoot+fSep
                                         +"dist"+fSep
                                         +"bigdata"+fSep
                                         +"var"+fSep
                                         +"config"+fSep
                                         +"jini"+fSep
                                         +"quorum.config";
    private static String[] quorumStateDir = 
        { 
            stateBase+fSep+"quorumState"+"."+quorumClientPort[0]
                                        +"."+quorumPeerPort[0]
                                        +"."+quorumElectionPort[0],
            stateBase+fSep+"quorumState"+"."+quorumClientPort[1]
                                        +"."+quorumPeerPort[1]
                                        +"."+quorumElectionPort[1],
            stateBase+fSep+"quorumState"+"."+quorumClientPort[2]
                                        +"."+quorumPeerPort[2]
                                        +"."+quorumElectionPort[2]
        };
    private static String[] quorumPersistenceOverride =
        {
            "com.bigdata.quorum.persistenceDirectory=new String("
                                             +"\""+quorumStateDir[0]+"\")",
            "com.bigdata.quorum.persistenceDirectory=new String("
                                             +"\""+quorumStateDir[1]+"\")",
            "com.bigdata.quorum.persistenceDirectory=new String("
                                             +"\""+quorumStateDir[2]+"\")"
        };
    private static int nQuorumServicesExpected =
                           quorumPersistenceOverride.length;
    private static boolean quorumsAlreadyStarted = false;
    private static HashSet<QuorumPeerService> quorumSet =
                                              new HashSet<QuorumPeerService>();

    private static String[] groupsToDiscover = new String[] {"qaQuorumGroup"};
    private static LookupLocator[] locsToDiscover  = new LookupLocator[0];
    private static DiscoveryManagement ldm;
    private static ServiceDiscoveryManager sdm;
    private static CacheListener cacheListener;
    private static LookupCache quorumCache;

    private static ExecutorService serviceStarterTaskExecutor =
                       Executors.newFixedThreadPool(quorumStateDir.length);

    private static boolean setupOnceAlready = false;
    private static boolean exceptionInSetup = false;
    private static boolean lastTest = false;

    // When using ServiceStarter to start the desired service instances,
    // need to hold references to each service instance that is created
    // to prevent distributed garbage collection on each service ref. The
    // map below is used to hold those references; where the map's key
    // is the persistence directory path for the corresponding service 
    // reference.
    private static Map<String, Created> refMap =
                               new ConcurrentHashMap<String, Created>();

    private String testName;
    private boolean testPassed;

    // NOTE: remove constructors and when/if the junit infrastructure
    //       is upgraded to a version that supports annotations and this test
    //       is changed so that it no longer has to extend TestCase.
    public QuorumPeerServiceTest() throws Exception {
        beforeClassSetup();
    }

    public QuorumPeerServiceTest(String name) throws Exception {
        super(name);
        beforeClassSetup();
    }

    // Test framework methods ------------------------------------------------

    // Intended to be run before any test methods are executed. This method
    // starts all services and creates any resources whose life cycles 
    // are intended to span all tests; rather than being set up and torn down
    // from test to test.
    //
    // NOTE: use the @BeforeClass annotation when/if the junit framework is
    //       upgraded to a version that supports annotations.
//    @BeforeClass public static void beforeClassSetup() {
    public static synchronized void beforeClassSetup() throws Exception {
        if (setupOnceAlready) return;
        setupOnceAlready = true;
        try {
            String logConfigFile = userDir+fSep+"bigdata"+fSep+"src"+fSep
                                   +"resources"+fSep+"logging"+fSep
                                   +"log4j.properties";
            System.setProperty("log4j.configuration", logConfigFile);
            logger = LogUtil.getLog4jLogger
                            ( (QuorumPeerServiceTest.class).getName() );
            logger.debug("\n\n-- beforeClassSetup ENTER ----------\n");

            // Setup both lookup & service discovery, plus groups, codebase ...
            setupDiscovery();

            // Start the services making up the ensemble

            String ensembleSize = "com.bigdata.quorum.zookeeperEnsembleSize="
                                  +"new Integer("+quorumStateDir.length+")";
            long discoveryPeriod = 10L*1000L;
            String discoveryPeriodStr =
                   "com.bigdata.quorum.peerDiscoveryPeriod="+discoveryPeriod;
            String jmxLog4j =
                "com.bigdata.quorum.zookeeperJmxLog4j=new Boolean("+false+")";
            for (int i=0; i<quorumStateDir.length; i++) {
                String clientPort = 
                       "com.bigdata.quorum.zookeeperClientPort="
                            +"new Integer("+quorumClientPort[i]+")";
                String peerPort = 
                       "com.bigdata.quorum.zookeeperPeerPort="
                            +"new Integer("+quorumPeerPort[i]+")";
                String electionPort = 
                            "com.bigdata.quorum.zookeeperElectionPort="
                            +"new Integer("+quorumElectionPort[i]+")";
                String joinGroups = 
                       "com.bigdata.quorum.groupsToJoin=new String[] "
                       +groups;

                serviceStarterTaskExecutor.execute
                    ( new ServiceStarterTask(quorumStateDir[i],
                                             ensembleSize,
                                             clientPort,
                                             peerPort,
                                             electionPort,
                                             quorumPersistenceOverride[i],
                                             joinGroups,
                                             discoveryPeriodStr,
                                             jmxLog4j) );
            }

            // Give the services time to start & rendezvous with each other
            int nWait = 10;
            for (int i=0; i<nWait; i++) {
                if (refMap.size() == quorumStateDir.length) break;
                Util.delayMS(1L*1000L);
            }
            if (refMap.size() != quorumStateDir.length) {
                throw new Exception("did not start all expected services "
                                    +"[expected="+quorumStateDir.length
                                    +", actual="+refMap.size()+"]");
            }
        } catch(Exception e) {
            exceptionInSetup = true;
            throw e;
        }
        logger.debug("\n\n-- beforeClassSetup EXIT ----------\n");
    }

    // Intended to be run after all test methods have completed executing.
    // This method terminates all services and cleans up resources whose
    // life cycles span all tests; rather than being set up and torn down
    // from test to test.
    //
    // NOTE: use the @AfterClass annotation when/if the junit framework is
    //       upgraded to a version that supports annotations.
//    @AfterClass public void afterClassTearDown() {
    public void afterClassTearDown() {
        logger.debug("\n\n-- afterClassTearDown ENTER ----------\n");
        if(sdm != null)  {
            try {
                sdm.terminate();
                logger.log(Level.INFO, "terminated sdm");
            } catch(Throwable t) { }
        }

        if(ldm != null)  {
            try {
                ldm.terminate();
                logger.log(Level.INFO, "terminated ldm");
            } catch(Throwable t) { }
        }

        stopAllQuorums();

        // Clean up persistence directories. Note that a delay is injected
        // before attempting to delete the persistence directories. This
        // is done to allow ZooKeeper to fully complete whatever clean up
        // processing it performs. Through trial and error it has been
        // found that without such a delay, although this test class
        // seems to complete its processing successfully, the next test
        // class that is run often times exits before executing any
        // test methods; without any indication of what caused the 
        // premature exit. Thus, until it can be determined what is 
        // actually causing this issue, and how to address it, the delay
        // performed below will be used to allow ZooKeeper to clean
        // up appropriately.

        Util.delayMS(1L*1000L);//delay 1 second
        for (int i=0; i<quorumStateDir.length; i++) {
            String persistenceDirectory = quorumStateDir[i];
            File dirFd = new File(persistenceDirectory);
            try {
                if ( !removeDir(dirFd) ) {
                    logger.log(Level.WARN, "failed to remove persistence "
                               +"directory ["+persistenceDirectory+"]");
                }
            } catch(IOException e) { } //swallow and continue
        }
        try {
            if ( !removeDir( new File(stateBase) ) ) {
                logger.log(Level.WARN, "failed to remove base persistence "
                           +"directory ["+stateBase+"]");
            }
        } catch(IOException e) { } //swallow
        logger.debug("\n\n-- afterClassTearDown EXIT ----------\n");
    }

    // Run at the end of each test method. This method terminates any
    // services or resources that are intended to exist only while the
    // given test is executing.
    public void tearDown() throws Exception {
        logger.debug("\n\n-- tearDown ENTER ----------\n");
        if (lastTest || exceptionInSetup) {
            afterClassTearDown();
        }

        if(testName != null) {
            String prefix = (testPassed ? "PASSED: " : "FAILED: ");
            logger.log(Level.INFO, prefix+testName);
            logger.log(Level.INFO,
                       "--------------------------------------------------");
        } else {
            logger.log(Level.INFO, " ");
        }
        logger.debug("\n\n-- tearDown EXIT ----------\n");
    }

    // Test methods ---------------------------------------------------------

    // Verify that the myid files associated with each service in the
    // ensemble were created and populated correctly
//    @Test(timeout=5000)
    public void testMyIdFiles() throws Exception {
        testName = "testMyIdFiles";
        testPassed = false;
        logger.info("\n\n-- "+testName+" ENTER ----------\n");

        for (int i=0; i<quorumStateDir.length; i++) {
            File myIdFile = new File(quorumStateDir[i]+fSep+"data", "myid");
            assertTrue( "myid file does not exist "
                        +"["+myIdFile.toString()+"]", myIdFile.exists() );
            BufferedReader br =
                    new BufferedReader( new FileReader(myIdFile) );
            String myIdString;
            try {
                myIdString = br.readLine();
            } finally {
                br.close();
            }
            long myId = Long.parseLong(myIdString);
            assertTrue("myId not greater than 0 [myId="+myId+", "
                       +"dir="+quorumStateDir[i]+"]", (myId > 0) );
            logger.log(Level.INFO,
                       "myId="+myId+" [dir="+quorumStateDir[i]+"]");
        }
        testPassed = true;
        logger.debug("\n\n-- "+testName+" EXIT ----------\n");
    }

//    @Test(timeout=5000)
    public void testRuok() throws Exception {
        testName = "testRuok";
        testPassed = false;
        logger.info("\n\n-- "+testName+" ENTER ----------\n");

        String charSet = "ASCII";
        byte[] ruokBytes = "ruok".getBytes(charSet);
        byte[] expectedBytes = "imok".getBytes(charSet);
        String expectedReply = new String(expectedBytes, charSet);

        java.net.InetAddress addr = java.net.InetAddress.getByName(thisHost);
        int timeout = 250;//socket read timeout in milliseconds;

        for (int i=0; i<quorumClientPort.length; i++) {
            int clientPort = quorumClientPort[i];
            java.net.Socket socket = new java.net.Socket(addr, clientPort);
            try {
                socket.setSoTimeout(timeout);
                java.io.OutputStream os = socket.getOutputStream();
                os.write(ruokBytes);
                os.flush();

                // retrieve reply
                java.io.DataInputStream is =
                    new java.io.DataInputStream(socket.getInputStream());
                byte[] replyBytes = new byte[4];
                is.readFully(replyBytes);//timesout if no response

                String reply = new String(replyBytes, charSet);
                logger.info("ruok reply [port="+clientPort+"] >>> "+reply);
                assertTrue("unexpected ruok reply [expected="+expectedReply
                           +", received="+reply+", port="+clientPort+"]",
                           expectedReply.equals(reply));
            } catch (IOException e) {
                fail("no ruok reply [timeout="+timeout
                     +" ms, exception="+e+"]");
            } finally {
                if (socket != null) socket.close();
            }
        }
        testPassed = true;
        logger.debug("\n\n"+testName+" EXIT\n");
    }

    // Verifies that the ZooKeeper client can be used to connect to the
    // ensemble within a given amount of time
//    @Test(timeout=20000)
    public void testZooKeeperConnect() throws Exception {
        testName = "testZooKeeperConnect";
        testPassed = false;
        logger.info("\n\n-- "+testName+" ENTER ----------\n");

        //build connectString
        StringBuffer strBuf = 
                         new StringBuffer(thisHost+":"+quorumClientPort[0]);
        for (int i=1; i<quorumClientPort.length; i++) {
            strBuf.append(","+thisHost+":"+quorumClientPort[i]);
        }
        String connectString = strBuf.toString();
        logger.info("connectString = "+connectString);

        int sessionTimeout = 40*1000;//max when tickTime is 2000
        ZooKeeper zkClient = new ZooKeeper(connectString,
                                           sessionTimeout,
                                           new ZookeeperEventListener());
        ZooKeeper.States state = zkClient.getState();
        logger.info("state[try #0] = "+state);

        if ( !state.equals(ZooKeeper.States.CONNECTED) ) {
            int nWait = 10;
            for (int i=0; i<nWait; i++) {
                Util.delayMS(1L*1000L);
                state = zkClient.getState();
                logger.info("state[try #"+(i+1)+"] = "+zkClient.getState());
                if ( state.equals(ZooKeeper.States.CONNECTED) ) break;
            }
        }
        if ( state.equals(ZooKeeper.States.CONNECTED) ) {
            testPassed = true;
        }
        zkClient.close();
        logger.debug("\n\n"+testName+" EXIT\n");
    }

    // Verifies that the QuorumPeerManager class that wraps the ZooKeeper
    // client can be used to discover and connect to the ensemble started
    // by this test class.
//    @Test(timeout=20000)
    public void testQuorumPeerManagerConnect() throws Exception {
        testName = "testQuorumPeerManagerConnect";
        testPassed = false;
        logger.info("\n\n-- "+testName+" ENTER ----------\n");

        int sessionTimeout = 40*1000;//max when tickTime is 2000
        QuorumPeerManager peerMgr =
                          new QuorumPeerManager(sdm, sessionTimeout, logger);
        assertTrue("failed on QuorumPeerManager instantiation "
                   +"[null returned]", (peerMgr != null) );

        ZooKeeper.States state = null;
        try {
            state = peerMgr.getState();
        } catch(IOException e) {
            logger.warn("failed on QuorumPeerManager instantiation", e);
            return;
        }
        assertTrue("getState failed [null]", (state != null) );
        logger.info("state = "+state);

        assertTrue("getState failed [not connected]",
                   state.equals(ZooKeeper.States.CONNECTED) );

        testPassed = true;
        peerMgr.close();
        logger.debug("\n\n"+testName+" EXIT\n");
    }

    // Special test that is always the last test; to clearly distinguish the
    // logged output produced by the previous tests from the logged output
    // produced by the tearDown process.
    //
    // REMOVE this test when/if this test class is changed to use the
    // @BeforeClass annotation.
    public void testLast() throws Exception {
        logger.info("\n\n-- BEGIN TEARDOWN ----------\n");
        lastTest = true;
        testName = "tearDown";
        testPassed = true;
    }

    // Private utility methods ----------------------------------------------

    private static void setupDiscovery() throws Exception {

        logger.log(Level.INFO, "setupDiscovery");

        if(System.getSecurityManager() == null) {
            System.setSecurityManager(new RMISecurityManager());
        }

        // Create the base directories for persisting each service's state
        if ( !mkStateDir(stateBase) ) {
            throw new Exception
                    ("could NOT create state base directory ["+stateBase+"]");
        }

        Class[] quorumType = new Class[] {QuorumPeerService.class};
        ServiceTemplate quorumTmpl = new ServiceTemplate
                                             (null, quorumType, null);
        cacheListener = new CacheListener();

        ldm = new LookupDiscoveryManager( new String[] {federationName},
                                          new LookupLocator[0], null );
        String[] groupsToDiscover = 
                     ((DiscoveryGroupManagement)ldm).getGroups();
        LookupLocator[] locsToDiscover =
                            ((DiscoveryLocatorManagement)ldm).getLocators();
        logger.log(Level.INFO,
                   "discover >>> groups="
                   +Util.writeGroupArrayToString(groupsToDiscover)
                   +", locators="
                   +Util.writeArrayElementsToString(locsToDiscover));

        // For passing to ServiceStarter
        StringBuffer strBuf = null;
        if(groupsToDiscover[0].compareTo("") == 0) {
            strBuf = new StringBuffer("{"+"\"\"");
        } else {
            strBuf = new StringBuffer("{"+"\""+groupsToDiscover[0]+"\"");
        }//endif
        for(int i=1;i<groupsToDiscover.length;i++) {
            if(groupsToDiscover[i].compareTo("") == 0) {
                strBuf.append(", "+"\"\"");
            } else {
                strBuf.append(", ").append("\""+groupsToDiscover[i]+"\"");
            }//endif
        }//end loop
        strBuf.append("}");
        groups = strBuf.toString();

        // For codebase construction
        thisHost = NicUtil.getIpAddress
                       ( "default.nic",
                         ConfigDeployUtil.getString("node.serviceNetwork"),
                         false);

        jskCodebase = ConfigurationUtil.computeCodebase
                                       (thisHost,"jsk-dl.jar",codebasePort);
        quorumCodebase = jskCodebase;

        // Turn on Lookup discovery in ldm
        ldm.addDiscoveryListener(new LookupDiscoveryListener());

        // Setup service discovery
        sdm = new ServiceDiscoveryManager(ldm, null);
        quorumCache = sdm.createLookupCache(quorumTmpl, null, cacheListener);
    }

    private static boolean mkStateDir(String statePath) throws IOException {
        File stateDirFd = new File(statePath);
        if( !(stateDirFd.exists() ? stateDirFd.isDirectory() 
                                  : stateDirFd.mkdir()) ) 
        {
            return false;
        }
        return true;
    }

    private void stopAllQuorums() {
        HashSet<QuorumPeerService> quorumSetClone;
        synchronized(quorumSet) {
            quorumSetClone = (HashSet<QuorumPeerService>)(quorumSet.clone());
        }

        for (Iterator<QuorumPeerService> itr = quorumSetClone.iterator();
                 itr.hasNext(); )
        {
            stopQuorum( itr.next() );
        }
    }

    private void stopQuorum(QuorumPeerService serviceRef) {
        if(serviceRef == null) return;
        if(serviceRef instanceof Administrable) {
            try {
                Object serviceAdmin  = ((Administrable)serviceRef).getAdmin();
                if(serviceAdmin instanceof DestroyAdmin) {
                    try {
                        ((DestroyAdmin)serviceAdmin).destroy();
                    } catch(Throwable t) { }
                } else {
                    logger.log(Level.WARN,
                               "admin not instance of DestroyAdmin");
                }
            } catch(Throwable t) { 
                logger.log(Level.WARN, "exception on getAdmin", t);
            }
        } else {
            logger.log(Level.WARN, "not instance of Administrable");
        }
    }

    private boolean removeDir(File fd) throws IOException {
        boolean removed = false;
        if( (fd == null) || (!fd.exists()) ) return true;

        // FILE: remove it and return
        if( fd.isFile() ) {
            removed = fd.delete();
            if(removed) {
                logger.log(Level.INFO, "removed "+fd.getPath());
            } else {
                logger.log(Level.WARN, "could NOT remove "+fd.getPath());
            }
            return removed;
        }

        // DIRECTORY: get its contents
        File[] fdArray = fd.listFiles();

        // EMPTY: remove directory and return
        if( fdArray == null ) {
            removed = fd.delete();
            if(removed) {
                logger.log(Level.INFO, "removed "+fd.getPath());
            } else {
                logger.log(Level.WARN, "could NOT remove "+fd.getPath());
            }
            return removed;
        }

        // NON-EMPTY: remove all files in the directory
        for(int i=0; i<fdArray.length; i++) {
            removeDir(fdArray[i]);
        }

        // EMPTY: remove directory and return
        removed = fd.delete();
        if(removed) {
            logger.log(Level.INFO, "removed "+fd.getPath());
        } else {
            logger.log(Level.WARN, "could NOT remove "+fd.getPath());
        }

        return removed;
    }

    // Nested classes

    private static class LookupDiscoveryListener implements DiscoveryListener {
        public void discovered(DiscoveryEvent evnt) {
            logger.log(Level.INFO, "discovery event ...");
            ServiceRegistrar[] regs = evnt.getRegistrars();
            for(int i=0;i<regs.length;i++) {
                try {
                    LookupLocator loc = regs[i].getLocator();
                    String[] groups = regs[i].getGroups();
                    logger.log(Level.INFO, "discovered locator   = "+loc);
                    for(int j=0;j<groups.length;j++) {
                        logger.log(Level.INFO,
                                   "discovered groups["+j+"] = "+groups[j]);
                    }
                } catch(Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        public void discarded(DiscoveryEvent evnt) {
            logger.log(Level.INFO, "discarded event ...");
        }
    }

    private static class CacheListener implements ServiceDiscoveryListener {
	public void serviceAdded(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPostEventServiceItem();
            Class serviceType = (item.service).getClass();
            logger.log(Level.INFO, 
                       "serviceAdded >>> service = "
                       +serviceType+", ID = "+item.serviceID);
            if( (QuorumPeerService.class).isAssignableFrom(serviceType) ) {
                synchronized(quorumSet) {
                    quorumSet.add((QuorumPeerService)item.service);
                }
           }

	}

	public void serviceRemoved(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPreEventServiceItem();
            Class serviceType = (item.service).getClass();
            logger.log(Level.INFO,
                       "serviceRemoved >>> service = "
                       +serviceType+", ID = "+item.serviceID);
            if((QuorumPeerService.class).isAssignableFrom(serviceType)) {
                synchronized(quorumSet) {
                    quorumSet.remove((QuorumPeerService)item.service);
                }
            }
        }

	public void serviceChanged(ServiceDiscoveryEvent event) {
            ServiceItem preItem  = event.getPreEventServiceItem();
            ServiceItem postItem = event.getPostEventServiceItem();
            Class serviceType = (postItem.service).getClass();
            logger.log(Level.INFO,
                       "serviceChanged >>> service = "
                       +serviceType+", ID = "+postItem.serviceID);
        }
    }

    private static class ServiceStarterConfig extends AbstractConfiguration {

        private String sdComponent = "com.sun.jini.start";
        private String sdEntryName = "servicePreparer";
        private Class  sdType      = ProxyPreparer.class;
        private ProxyPreparer proxyPreparer;

        ServiceStarterConfig(ProxyPreparer proxyPreparer) {
            this.proxyPreparer = proxyPreparer;
        }

        protected Object getEntryInternal(String component,
                                          String name,
                                          Class  type,
                                          Object data)
                                                 throws ConfigurationException
        {
            if( (component == null) || (name == null) || (type == null) ) {
                throw new NullPointerException("component, name and type "
                                               +"cannot be null");
            }
            if(    (sdComponent.equals(component))
                && (sdEntryName.equals(name))
                && (sdType.isAssignableFrom(type)) )
            {
                return proxyPreparer;
            } else {
                throw new ConfigurationException("entry not found for "
                                                 +"component "+component
                                                 +", name " + name);
            }

        }
    }

    private static class ZookeeperEventListener implements Watcher {
	public void process(WatchedEvent event) {
            KeeperState eventState = event.getState();
            switch (eventState) {
                case Unknown:
                    logger.warn
                        ("zookeeper event [state="+eventState
                         +", event="+event+"]");
                    break;
                case Disconnected:
                    logger.info
                        ("zookeeper event [state="+eventState+"]");;
                    break;
                case SyncConnected:
                    logger.info
                        ("zookeeper event [state="+eventState+"]");;
                    break;
                case Expired:
                    logger.warn
                        ("zookeeper event [state="+eventState+"]");
                    break;
            }

	}
    }

    private static class ServiceStarterTask implements Runnable {

        private String serviceStateDir;
        private String ensembleSizeOverride;
        private String clientPortOverride;
        private String peerPortOverride;
        private String electionPortOverride;
        private String persistenceOverride;
        private String joinGroupsOverride;
        private String discoveryPeriodOverride;
        private String jmxLog4jOverride;

        public ServiceStarterTask(String serviceStateDir,
                                  String ensembleSizeOverride,
                                  String clientPortOverride,
                                  String peerPortOverride,
                                  String electionPortOverride,
                                  String persistenceOverride,
                                  String joinGroupsOverride,
                                  String discoveryPeriodOverride,
                                  String jmxLog4jOverride)
        {
            this.serviceStateDir = serviceStateDir;
            this.ensembleSizeOverride = ensembleSizeOverride;
            this.clientPortOverride = clientPortOverride;
            this.peerPortOverride = peerPortOverride;
            this.electionPortOverride = electionPortOverride;
            this.persistenceOverride = persistenceOverride;
            this.joinGroupsOverride = joinGroupsOverride;
            this.discoveryPeriodOverride = discoveryPeriodOverride;
            this.jmxLog4jOverride = jmxLog4jOverride;
        }

        public void run() {
            logger.log(Level.INFO, "quorumConfig="+quorumConfig);
            logger.log(Level.INFO, "quorumClasspath="+quorumClasspath);
            logger.log(Level.INFO, "quorumCodebase="+quorumCodebase);
            logger.log(Level.INFO, "quorumImplName="+quorumImplName);
            logger.log(Level.INFO, "ensembleSizeOverride="
                                   +ensembleSizeOverride);
            logger.log(Level.INFO, "clientPortOverride="+clientPortOverride);
            logger.log(Level.INFO, "peerPortOverride="+peerPortOverride);
            logger.log(Level.INFO, "electionPortOverride="
                                   +electionPortOverride);
            logger.log(Level.INFO, "persistenceOverride="+persistenceOverride);
            logger.log(Level.INFO, "joinGroupsOverride="+joinGroupsOverride);
            logger.log(Level.INFO, "discoveryPeriodOverride="
                                   +discoveryPeriodOverride);
            logger.log(Level.INFO, "jmxLog4jOverride="+jmxLog4jOverride);

            ArrayList<String> overrideList = new ArrayList<String>();
            overrideList.add(quorumConfig);
            overrideList.add(ensembleSizeOverride);
            overrideList.add(clientPortOverride);
            overrideList.add(peerPortOverride);
            overrideList.add(electionPortOverride);
            overrideList.add(persistenceOverride);
            overrideList.add(joinGroupsOverride);
            overrideList.add(discoveryPeriodOverride);
            overrideList.add(jmxLog4jOverride);
            String[] argsArray = (String[])(overrideList).toArray
                                            (new String[overrideList.size()]);
            NonActivatableServiceDescriptor serviceDescriptor =
                       new NonActivatableServiceDescriptor(quorumCodebase,
                                                           policy,
                                                           quorumClasspath,
                                                           quorumImplName,
                                                           argsArray);
            try {
                refMap.put
                ( serviceStateDir,
                  (Created)serviceDescriptor.create
                        (new ServiceStarterConfig(new BasicProxyPreparer())) );
            } catch(Exception e) {
                logger.log(Level.WARN, "exception creating service "
                           +"["+serviceStateDir+"]", e);
                return;
            }
        }
    }
}
