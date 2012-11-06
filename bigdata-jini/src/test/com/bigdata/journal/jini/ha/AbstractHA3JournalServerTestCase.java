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
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.rmi.Remote;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.AssertionFailedError;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import sun.security.jca.ServiceId;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.RunState;
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.JavaServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration.AbstractServiceStarter;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.jini.ha.HAJournalServer.ConfigurationOptions;
import com.bigdata.quorum.AbstractQuorumClient;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumClient;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.util.InnerCause;
import com.bigdata.zookeeper.DumpZookeeper;
import com.bigdata.zookeeper.ZooHelper;
import com.bigdata.zookeeper.ZooKeeperAccessor;

/**
 * Class layers in support to start and stop the {@link HAJournalServer}
 * processes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class AbstractHA3JournalServerTestCase extends
        AbstractHAJournalServerTestCase implements DiscoveryListener {

    public AbstractHA3JournalServerTestCase() {
    }

    public AbstractHA3JournalServerTestCase(final String name) {
        super(name);
    }

    /**
     * The timeout in milliseconds to await the discovery of a service if there
     * is a cache miss (default {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
     */
    static final protected long cacheMissTimeout = 2000;
    
    /**
     * The timeout used to await quorum meet or break.
     */
    protected final static long awaitQuorumTimeout = 5000;
    
    /**
     * Implementation listens for the death of the child process and can be used
     * to decide when the child process is no longer executing.
     */
    private static class ServiceListener implements IServiceListener {

        private volatile HAGlue haGlue;
        private volatile ProcessHelper processHelper;
        private volatile boolean dead = false;
        
        public ServiceListener() {
            
        }

        public void setService(final HAGlue haGlue) {

            if (haGlue == null)
                throw new IllegalArgumentException();

            this.haGlue = haGlue;
        }

        @SuppressWarnings("unused")
        public HAGlue getHAGlue() {

            return haGlue;
            
        }

        public void add(final ProcessHelper processHelper) {

            if (processHelper == null)
                throw new IllegalArgumentException();

            this.processHelper = processHelper;

        }

        public void remove(final ProcessHelper processHelper) {

            if (processHelper == null)
                throw new IllegalArgumentException();

            if (processHelper != this.processHelper)
                throw new AssertionError();

            dead = true;

        }

        public ProcessHelper getProcessHelper() {
        
            return processHelper;
            
        }
        
        public boolean isDead() {

            return dead;

        }

    }

    /**
     * The {@link Remote} interfaces for these services (if started and
     * successfully discovered).
     */
    private HAGlue serverA = null, serverB = null, serverC = null;

    /**
     * {@link UUID}s for the {@link HAJournalServer}s.
     */
    private UUID serverAId = UUID.randomUUID(), serverBId = UUID.randomUUID(),
            serverCId = UUID.randomUUID();

    /**
     * These {@link IServiceListener}s are used to reliably detect that the
     * corresponding process starts and (most importantly) that it is really
     * dies once it has been shutdown or destroyed.
     */
    private ServiceListener serviceListenerA = null, serviceListenerB = null,
            serviceListenerC = null;
    
    private LookupDiscoveryManager lookupDiscoveryManager = null;

    private ServiceDiscoveryManager serviceDiscoveryManager = null;

    private HAJournalDiscoveryClient discoveryClient = null;

    /**
     * The {@link ZooKeeperAccessor} used by the {@link #quorum}.
     */
    private ZooKeeperAccessor zka = null;
    
    /**
     * The logicalServiceId (without the zroot prefix).
     */
    private String logicalServiceId = null;
    
    /**
     * The zpath of the logical service.
     */
    private String logicalServiceZPath = null;
    
    /** Quorum client used to monitor (or act on) the logical service quorum. */
    protected Quorum<HAGlue, QuorumClient<HAGlue>> quorum = null;
    
    @Override
    protected void setUp() throws Exception {
        
        /*
         * Destroy the test directory structure.
         * 
         * Note: This is done before we run the test rather than after so we can
         * look at the end state of the data after running the test.
         */
        {

            final File testDir = new File(TGT_PATH);
            
            if (testDir.exists()) {

                recursiveDelete(testDir);
                
            }

        }

        super.setUp();

        // Unique for each test.
        logicalServiceId = "CI-HAJournal-" + getName() + "-" + UUID.randomUUID();
        
        /*
         * Read the jini/river configuration file. We need this to setup the
         * clients that we will use to lookup the services that we start.
         */
        final Configuration config = ConfigurationProvider
                .getInstance(new String[] { SRC_PATH + "jiniClient.config" });

        final JiniClientConfig jiniClientConfig = new JiniClientConfig(
                JiniClientConfig.Options.NAMESPACE, config);

        /*
         * Note: This class will perform multicast discovery if ALL_GROUPS
         * is specified and otherwise requires you to specify one or more
         * unicast locators (URIs of hosts running discovery services). As
         * an alternative, you can use LookupDiscovery, which always does
         * multicast discovery.
         */
        lookupDiscoveryManager = new LookupDiscoveryManager(
                jiniClientConfig.groups, jiniClientConfig.locators,
                this /* DiscoveryListener */, config);

        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the data server is listening.
         */
        serviceDiscoveryManager = new ServiceDiscoveryManager(
                lookupDiscoveryManager, new LeaseRenewalManager(), config);

        // Setup discovery for HAGlue clients.
        discoveryClient = new HAJournalDiscoveryClient(serviceDiscoveryManager,
                null/* serviceDiscoveryListener */, cacheMissTimeout);

        // Setup quorum client.
        quorum = newQuourm();
        
    }
    
    @Override
    protected void tearDown() throws Exception {

        if (quorum != null && log.isInfoEnabled()) {

            /*
             * Echo the final quorum state (as currently reflected).
             */

            log.info(quorum.toString());

        }
        
        if (zka != null && log.isInfoEnabled()) {

            /*
             * Dump the final zookeeper state for the logical service.
             */
            
            final StringWriter sw = new StringWriter();

            final PrintWriter w = new PrintWriter(sw);

            new DumpZookeeper(zka.getZookeeper()).dump(w, true/* showData */,
                    logicalServiceZPath, 0/* depth */);

            w.flush();
            w.close();
            
            sw.flush();

            log.info("Zookeeper State for logical service: \n" + sw.toString());

        }

        if (serverA != null) {
            safeDestroy(serverA, serviceListenerA);
            serverA = null;
            serviceListenerA = null;
        }

        if (serverB != null) {
            safeDestroy(serverB, serviceListenerB);
            serverB = null;
            serviceListenerB = null;
        }

        if (serverC != null) {
            safeDestroy(serverC, serviceListenerC);
            serverC = null;
            serviceListenerC = null;
        }

        if (serviceDiscoveryManager != null) {
            serviceDiscoveryManager.terminate();
            serviceDiscoveryManager = null;
        }

        if (lookupDiscoveryManager != null) {
            lookupDiscoveryManager.terminate();
            lookupDiscoveryManager = null;
        }

        if (discoveryClient != null) {
            discoveryClient.terminate();
            discoveryClient = null;
        }

        if (quorum != null) {
            quorum.terminate();
            quorum = null;
        }

        if (zka != null) {
            final String zroot = logicalServiceZPath;
            final ZooKeeper zookeeper = zka.getZookeeper();
            destroyZNodes(zroot, zookeeper);
            zka.close();
            zka = null;
        }

        logicalServiceId = null;
        logicalServiceZPath = null;
        serverAId = serverBId = serverCId = null;

        super.tearDown();

    }

    /**
     * Clear out everything in zookeeper for the specified zpath.
     */
    private void destroyZNodes(final String zpath, final ZooKeeper zookeeper) {

        if (log.isInfoEnabled())
            log.info("zpath=" + zpath);
        
        try {

            if (zookeeper.exists(zpath, false/* watch */) != null) {

                ZooHelper.destroyZNodes(zookeeper, zpath, 0/* depth */);
            }

        } catch (InterruptedException ex) {

            log.warn(ex);

        } catch (SessionExpiredException ex) {

            log.warn(ex);

        } catch (ConnectionLossException ex) {

            log.warn(ex);

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Safely destroy the service.
     * 
     * @param haGlue
     *            The service.
     */
    private void safeDestroy(final HAGlue haGlue,
            final ServiceListener serviceListener) {

        if (haGlue == null)
            return;

        try {
        
            if (log.isInfoEnabled())
                log.info("Destroying service: " + haGlue);

            final UUID serviceId = haGlue.getServiceUUID();

            haGlue.destroy();

            awaitServiceGone(serviceId, haGlue, serviceListener);

        } catch (Throwable t) {
            
            if (InnerCause.isInnerCause(t, java.net.ConnectException.class)) {
            
                log.warn("Service is down: " + t);
                
            } else {
                
                // Some other problem.
                log.error(t, t);
                
            }
            
        }

    }

    private void safeShutdown(final HAGlue haGlue,
            final ServiceListener serviceListener) {

        if (haGlue == null)
            return;

        try {

            final UUID serviceId = haGlue.getServiceUUID();
            
            // Shutdown the remote service.
            ((RemoteDestroyAdmin) haGlue).shutdown();
            
            awaitServiceGone(serviceId, haGlue, serviceListener);
            
        } catch (Throwable t) {
            
            if (InnerCause.isInnerCause(t, java.net.ConnectException.class)) {
            
                log.warn("Service is down: " + t);
                
            } else {
                
                // Some other problem.
                log.error(t, t);
                
            }
            
        }

    }

    /**
     * Await positive indication that the service is shutdown. This can mean
     * looking for the service {@link RunState} change, looking for the service
     * to no longer be discoverable, looking for the service to no longer accept
     * RMI requests, etc.
     * <p>
     * Note: If the child does not shutdown normally, then this will force a
     * kill of the child process and throw out an {@link AssertionFailedError}.
     * This {@link AssertionFailedError} is a good indication that there is a
     * problem with the process shutdown / destroy logic. You should obtain
     * thread dumps when this happens and examine them for the root cause of the
     * process failing to terminate normally.
     */
    private void awaitServiceGone(final UUID serviceId, final HAGlue haGlue,
            final ServiceListener serviceListener) {

        assertCondition(new Runnable() {
            public void run() {
                try {
                    haGlue.getRunState();
                    fail();// still answering RMI requests.
                } catch (IOException e) {
                    // Service is down.
                    return;
                }
            }
        });

        assertCondition(new Runnable() {
            public void run() {

                // try to discover the service item.
                final ServiceItem serviceItem = discoveryClient
                        .getServiceItem(serviceId);
                
                if (serviceItem != null) {
                 
                    // still registered.
                    fail();
               
                }
                
            }
        });

        try {
            
            assertCondition(new Runnable() {
                public void run() {
                    // Wait for the process death.
                    assertTrue(serviceListener.isDead());
                }
            });

        } catch (AssertionFailedError err) {
        
            /*
             * If we do not observe a normal process death, then attempt to kill
             * the child process.
             */

            log.error("Forcing kill of child process.");
            
            try {

                serviceListener.getProcessHelper()
                        .kill(true/* immediateShutdown */);
                
            } catch (InterruptedException e) {
                
                // Ignore.
                
            }

            throw err;
            
        }
        
    }

    /**
     * Return Zookeeper quorum that can be used to reflect (or act on) the
     * distributed quorum state for the logical service.
     * 
     * @throws ConfigurationException
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    protected Quorum<HAGlue, QuorumClient<HAGlue>> newQuourm()
            throws ConfigurationException, InterruptedException,
            KeeperException {

        final Configuration config = ConfigurationProvider
                .getInstance(new String[] { SRC_PATH + "zkClient.config" });

        final ZookeeperClientConfig zkClientConfig = new ZookeeperClientConfig(
                config);

        final List<ACL> acl = zkClientConfig.acl;
        final String zoohosts = zkClientConfig.servers;
        final int sessionTimeout = zkClientConfig.sessionTimeout;

        // Note: Save reference.
        zka = new ZooKeeperAccessor(zoohosts, sessionTimeout);

        // znode name for the logical service.
//        final String logicalServiceId = (String) config.getEntry(
//                ZookeeperClientConfig.Options.NAMESPACE,
//                ConfigurationOptions.LOGICAL_SERVICE_ID, String.class);
        final String logicalServiceId = getLogicalServiceId();

        final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                + HAJournalServer.class.getName();

        // zpath for the logical service (save reference).
        logicalServiceZPath = logicalServiceZPathPrefix + "/"
                + logicalServiceId;

        final int replicationFactor = (Integer) config.getEntry(
                ZookeeperClientConfig.Options.NAMESPACE,
                ConfigurationOptions.REPLICATION_FACTOR, Integer.TYPE);        

        if (!zka.awaitZookeeperConnected(10, TimeUnit.SECONDS)) {

            throw new RuntimeException("Could not connect to zk");

        }

        if (log.isInfoEnabled()) {
            log.info("Connected to zookeeper");
        }

        /*
         * Ensure key znodes exist.
         */
        try {
            zka.getZookeeper().create(zkClientConfig.zroot,
                    new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            zka.getZookeeper().create(logicalServiceZPathPrefix,
                    new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            zka.getZookeeper().create(logicalServiceZPath,
                    new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }

        // Quorum that can be used to monitor the distributed quorum state.
        final Quorum<HAGlue, QuorumClient<HAGlue>> quorum = new ZKQuorumImpl<HAGlue, QuorumClient<HAGlue>>(
                replicationFactor, zka, acl);

        quorum.start(new MockQuorumClient<HAGlue>(logicalServiceZPath));
        
        return quorum;
        
    }
    
    /**
     * Return the logicalServiceId. This is overridden to be include name of the
     * test and a {@link UUID} in order to keep HAJournalServer processes that
     * do not die nicely from causing crosstalk between the unit tests.
     */
    private String getLogicalServiceId() {
    
        return logicalServiceId;

    }

    private class MockQuorumClient<S extends Remote> extends
            AbstractQuorumClient<S> {

        protected MockQuorumClient(String logicalServiceId) {

            super(logicalServiceId);

        }

        /**
         * Resolve an {@link HAGlue} object from its Service UUID.
         */
        @Override
        public S getService(final UUID serviceId) {

            final ServiceItem serviceItem = discoveryClient
                    .getServiceItem(serviceId);

            if (serviceItem == null) {

                // Not found (per the API).
                throw new QuorumException("Service not found: uuid="
                        + serviceId);

            }

            @SuppressWarnings("unchecked")
            final S service = (S) serviceItem.service;

            return service;

        }

    }

    abstract private class StartServerTask implements Callable<HAGlue> {

        private final String name;
        private final String configName;
        private final UUID serverId;
        private final ServiceListener serviceListener;
        protected final boolean restart;

        public StartServerTask(final String name, final String configName,
                final UUID serverId, final ServiceListener serviceListener,
                final boolean restart) {

            this.name = name;
            this.configName = configName;
            this.serverId = serverId;
            this.serviceListener = serviceListener;
            this.restart = restart;

        }
        
        final public HAGlue start() throws Exception {

            return startServer(name, configName, serverId, serviceListener,
                    restart);

        }
        
    }

    protected class StartATask extends StartServerTask {

        public StartATask(final boolean restart) {

            super("A", "HAJournal-A.config", serverAId,
                    serviceListenerA = new ServiceListener(), restart);

        }

        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverA, serviceListenerA);
                
                serverA = null;
                
            }
            
            return serverA = start();

        }

    }

    protected class StartBTask extends StartServerTask {

        public StartBTask(final boolean restart) {

            super("B", "HAJournal-B.config", serverBId,
                    serviceListenerB = new ServiceListener(), restart);

        }

        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverB, serviceListenerB);
                
                serverB = null;
                
            }
            
            return serverB = start();

        }

    }

    protected class StartCTask extends StartServerTask {

        public StartCTask(final boolean restart) {

            super("C", "HAJournal-C.config", serverCId,
                    serviceListenerC = new ServiceListener(), restart);

        }

        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverC, serviceListenerC);
                
                serverC = null;
                
            }
            

            return serverC = start();

        }

    }

    protected HAGlue startA() throws Exception {

        return new StartATask(false/* restart */).call();

    }

    protected HAGlue startB() throws Exception {

        return new StartBTask(false/* restart */).call();

    }

    protected HAGlue startC() throws Exception {

        return new StartCTask(false/* restart */).call();

    }

    protected HAGlue restartA() throws Exception {

        return new StartATask(true/* restart */).call();

    }

    protected HAGlue restartB() throws Exception {

        return new StartBTask(true/* restart */).call();

    }

    protected HAGlue restartC() throws Exception {

        return new StartCTask(true/* restart */).call();

    }

    private HAGlue startServer(final String name,
            final String sourceConfigFileName, final UUID serviceId,
            final ServiceListener serviceListener,
            final boolean restart) throws Exception {

        final String configFile = SRC_PATH + sourceConfigFileName;

        final File serviceDir = new File(TGT_PATH, name);

        final String installedConfigFileName = "HAJournal.config";

        if (!serviceDir.exists()) {

            if (restart)
                fail("Not found: " + serviceDir);

            // Create service directory.
            serviceDir.mkdirs();

        }
        
        /*
         * Copy various configuration and accessory files into the service
         * directory.
         */
        if (!restart) {

            // security policy
            copyFile(new File("policy.all"), new File(serviceDir, "policy.all"));

            // log4j configuration.
            copyFile(new File(
                    "bigdata/src/resources/logging/log4j-dev.properties"),
                    new File(serviceDir, "log4j.properties"));

            // java logging configuration.
            copyFile(new File(
                    "bigdata/src/resources/logging/logging.properties"),
                    new File(serviceDir, "logging.properties"));

            // HAJournalServer configuration
            copyFile(new File(SRC_PATH, sourceConfigFileName), //
                    new File(serviceDir, installedConfigFileName));

        }
        
        /*
         * Read jini configuration.
         */
        final Configuration config = ConfigurationProvider
                .getInstance(new String[] { configFile });
        
        final ServiceConfiguration serviceConfig = new HAJournalServerConfiguration(
                name, config, serviceId, serviceDir,
                new String[] { installedConfigFileName });

        final AbstractServiceStarter<?> serviceStarter = serviceConfig
                .newServiceStarter(serviceListener);

        final ProcessHelper processHelper = serviceStarter.call();

        try {

            if (log.isInfoEnabled())
                log.info("Awaiting service discovery: "
                        + processHelper.name);

            final ServiceID serviceID = JiniUtil.uuid2ServiceID(serviceId);

            final ServiceItem[] items = serviceDiscoveryManager.lookup(
                    new ServiceTemplate(//
                            serviceID, //
                            null, // iface[]
                            null //new Entry[]{new Name(name)}
                    ), // template
                    1, // minMatches
                    1, // maxMatches
                    null, // filter
                    5000 // timeout (ms)
                    );

            assertNotNull(items);

            assertTrue(items.length == 1);

            assertNotNull(items[0]);

            final ServiceItem serviceItem = items[0];

            final HAGlue haGlue = (HAGlue) serviceItem.service;

            // Set the HAGlue interface on the ServiceListener.
            serviceListener.setService(haGlue);
            
            /*
             * Wait until the server is running.
             */
            assertCondition(new Runnable() {
                public void run() {

                    try {

                        assertEquals(RunState.Running, haGlue.getRunState());
                        
                    } catch (IOException e) {
                        
                        throw new RuntimeException(e);
                        
                    }

                }
            });

            return haGlue;

        } catch (Throwable t) {

            log.error(t, t);

            processHelper.kill(true/* immediateShutdown */);

            throw new RuntimeException("Could not start/locate service: name="
                    + name + ", configFile=" + configFile, t);

        }

    }

    /**
     * Copy a file
     * 
     * @param src
     *            The source file (must exist).
     * @param dst
     *            The target file.
     *            
     * @throws IOException
     */
    static private void copyFile(final File src, final File dst)
            throws IOException {

        if (!src.exists())
            throw new FileNotFoundException(src.getAbsolutePath());

        if (log.isInfoEnabled())
            log.info("src=" + src + ", dst=" + dst);
        
        FileInputStream is = null;
        FileOutputStream os = null;
        try {
            is = new FileInputStream(src);
            os = new FileOutputStream(dst, false/* append */);
            copyStream(is, os);
            os.flush();
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException ex) {
                }
            if (os != null)
                try {
                    os.close();
                } catch (IOException ex) {
                }
        }
    }

    /**
     * Copy the input stream to the output stream.
     * 
     * @param content
     *            The input stream.
     * @param outstr
     *            The output stream.
     *            
     * @throws IOException
     */
    static private void copyStream(final InputStream content,
            final OutputStream outstr) throws IOException {

        final byte[] buf = new byte[1024];

        while (true) {
        
            final int rdlen = content.read(buf);
            
            if (rdlen <= 0) {
            
                break;
                
            }
            
            outstr.write(buf, 0, rdlen);
            
        }

    }

//    /**
//     * Filter for the specific service item based on the Name Entry.
//     */
//    private static class NameItemFilter implements ServiceItemFilter {
//
//        final private String name;
//
//        public NameItemFilter(final String name) {
//
//            if (name == null)
//                throw new IllegalArgumentException();
//
//            this.name = name;
//            
//        }
//        
//        @Override
//        public boolean check(final ServiceItem serviceItem) {
//
//            final Entry[] entries = serviceItem.attributeSets;
//
//            String theName = null;
//
//            for (Entry e : entries) {
//
//                if (e instanceof Name && theName == null) {
//
//                    // found a name.
//                    theName = ((Name) e).name;
//
//                }
//            }
//
//            if (theName.equals(name))
//                log.info("Found: " + serviceItem);
//
//            return true;
//
//        }
//
//    }

    /**
     * Utility class for configuring and starting an {@link HAJournalServer}
     * under test suite control.
     */
    private class HAJournalServerConfiguration extends
            JavaServiceConfiguration {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        private final String serviceName;
        private final UUID serviceId;
        private final File serviceDir;
        private final String[] args;
        
        public HAJournalServerConfiguration(final String serviceName,
                final Configuration config, final UUID serviceId,
                final File serviceDir, final String[] args)
                throws ConfigurationException {

            // Note: ignored! args[] is used instead.
            super(HAJournalServer.ConfigurationOptions.COMPONENT, config);

            if (serviceName == null)
                throw new IllegalArgumentException();

            if (serviceId == null)
                throw new IllegalArgumentException();

            if (serviceDir == null)
                throw new IllegalArgumentException();

            if (args == null)
                throw new IllegalArgumentException();

            if (args.length == 0)
                throw new IllegalArgumentException();

            if (args[0] == null) // the configuration file name.
                throw new IllegalArgumentException();

            this.serviceName = serviceName;

            this.serviceId = serviceId;
            
            this.serviceDir = serviceDir;
            
            this.args = args;
            
        }

        @Override
        @SuppressWarnings("rawtypes")
        public HAJournalServerStarter newServiceStarter(
                final IServiceListener listener) throws Exception {

            return new HAJournalServerStarter(listener);

        }

        private class HAJournalServerStarter<V extends ProcessHelper> extends
                JavaServiceStarter<V> {

           /**
            * The {@link ServiceItem} iff discovered.
            */
            private ServiceItem serviceItem = null;

            /**
             * Used to override the service directory in the deployed
             * configuration.
             */
            private final String TEST_SERVICE_DIR = "test.serviceDir";
            
            /**
             * Used to override the {@link ServiceId} in the deployed
             * configuration.
             */
            private final String TEST_SERVICE_ID = "test.serviceId";
            
            /**
             * Used to override the logicalServiceId in the deployed
             * configuration.
             */
            private final String TEST_LOGICAL_SERVICE_ID = "test.logicalServiceId";
            
            /**
             * The absolute effective path of the service directory. This is
             * overridden on the {@link #TEST_SERVICE_DIR} environment variable
             * and in the deployed HAJournal.config file in order to have the
             * service use the specified service directory when it gets
             * deployed.
             */
            private final String servicePath = HAJournalServerConfiguration.this.serviceDir
                    .getAbsolutePath();

            protected HAJournalServerStarter(final IServiceListener listener) {

                super(listener);
                
            }
            
//            /**
//             * Adds <code>serviceDir</code> into the environment.
//             */
//            @Override
//            protected void setUpEnvironment(final Map<String, String> env) {
//
//                super.setUpEnvironment(env);
//
//                env.put(TEST_SERVICE_DIR, servicePath);
//
//            }
            
            /**
             * Extended to add the configuration file on the command line after
             * the class name.
             */
            @Override
            protected void addCommandArgs(final List<String> cmds) {

                cmds.add("-D" + TEST_SERVICE_DIR + "=" + servicePath);

                cmds.add("-D" + TEST_SERVICE_ID + "=" + serviceId);

                cmds.add("-D" + TEST_LOGICAL_SERVICE_ID + "="
                        + getLogicalServiceId());

                super.addCommandArgs(cmds);
                
                for (String arg : args) {

                    // the configuration file, etc.
                    cmds.add(arg);
                    
                }
                
            }

            /**
             * Overridden to monitor for the jini join of the service and the
             * creation of the znode corresponding to the physical service
             * instance.
             * 
             * @todo we could also verify the service using its proxy, e.g., by
             *       testing for a normal run state.
             */
            @Override
            protected void awaitServiceStart(final V processHelper,
                    final long timeout, final TimeUnit unit) throws Exception,
                    TimeoutException, InterruptedException {

                final long begin = System.nanoTime();

                long nanos = unit.toNanos(timeout);

                // wait for the service to be discovered
                serviceItem = awaitServiceDiscoveryOrDeath(processHelper,
                        nanos, TimeUnit.NANOSECONDS);

//                // proxy will be used for destroy().
//                processHelper.setServiceItem(serviceItem);

                // subtract out the time we already waited.
                nanos -= (System.nanoTime() - begin);

//                // TODO (restore) wait for the ephemeral znode for the service to be created
//                awaitZNodeCreatedOrDeath(serviceItem, processHelper, nanos,
//                        TimeUnit.NANOSECONDS);

            }

            /**
             * Waits up to timeout units for the service to either by discovered
             * by jini or to die.
             * <p>
             * Note: We recognize the service by the present of the assigned
             * {@link ServiceToken} attribute. If a service with that
             * {@link ServiceToken} can not be discovered by jini after a
             * timeout, then we presume that the service could not start and
             * throw an exception. The {@link ServiceToken} provides an
             * attribute which is assigned by the service starter while the
             * {@link ServiceID} is assigned by jini only after the service has
             * joined with a jini registrar.
             * 
             * @param processHelper
             * @param timeout
             * @param unit
             * @return The {@link ServiceItem} for the discovered service.
             * @throws Exception
             */
            protected ServiceItem awaitServiceDiscoveryOrDeath(
                    final ProcessHelper processHelper, long timeout,
                    final TimeUnit unit) throws Exception, TimeoutException,
                    InterruptedException {

                // convert to ms for jini lookup() waitDur.
                timeout = unit.toMillis(timeout);

                final long begin = System.currentTimeMillis();

                ServiceDiscoveryManager serviceDiscoveryManager = null;
                try {

                    serviceDiscoveryManager = new ServiceDiscoveryManager(
//                            fed.getDiscoveryManagement(),
                            lookupDiscoveryManager,// Note: This is a reference on the TestCase!
                            new LeaseRenewalManager());

                    if (log.isInfoEnabled())
                        log.info("Awaiting service discovery: "
                                + processHelper.name);

                    final ServiceID serviceID = JiniUtil
                            .uuid2ServiceID(serviceId);

                    final ServiceItem[] items = serviceDiscoveryManager.lookup(
                            new ServiceTemplate(//
                                    serviceID, //
                                    null, // iface[]
                                    null // new Entry[]{new Name(serviceName)}
                            ), // template
                            1, // minMatches
                            1, // maxMatches
                            null, // filter
                            timeout//
                            );

                    final long elapsed = System.currentTimeMillis() - begin;

                    if (items.length == 0) {

                        throw new Exception("Service did not start: elapsed="
                                + elapsed + ", name=" + serviceName);

                    }

                    if (items.length != 1) {

                        throw new Exception("Duplicate ServiceTokens? name="
                                + serviceName + ", found="
                                + Arrays.toString(items));

                    }

                    if (log.isInfoEnabled())
                        log.info("Discovered service: elapsed=" + elapsed
                                + ", name=" + processHelper.name + ", item="
                                + items[0]);

                    return items[0];

                } finally {

                    if (serviceDiscoveryManager != null) {

                        serviceDiscoveryManager.terminate();

                    }

                }

            }

//            /**
//             * Waits up to timeout units for the znode for the physical service
//             * to be created or the process to die.
//             * 
//             * @param processHelper
//             * @param timeout
//             * @param unit
//             * 
//             * @throws TimeoutException
//             * @throws InterruptedException
//             * @throws KeeperException
//             */
//            private void awaitZNodeCreatedOrDeath(
//                    final ServiceItem serviceItem,
//                    final ProcessHelper processHelper, final long timeout,
//                    final TimeUnit unit) throws KeeperException,
//                    InterruptedException, TimeoutException {
//
//                // // convert to a standard UUID.
//                // final UUID serviceUUID =
//                // JiniUtil.serviceID2UUID(serviceItem.serviceID);
//
//                // this is the zpath that the service will create.
//                final String physicalServiceZPath = logicalServiceZPath + "/"
//                        + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER + "/"
//                        + serviceUUID;
//
//                // @todo this should pass in the ZooKeeperAccessor.
//                if (!ZNodeCreatedWatcher.awaitCreate(fed.getZookeeper(),
//                        physicalServiceZPath, timeout, unit)) {
//
//                    throw new TimeoutException("zpath does not exist: "
//                            + physicalServiceZPath);
//
//                }
//
//                if (log.isInfoEnabled())
//                    log.info("znode exists: zpath=" + physicalServiceZPath);
//
//                // success.
//                return;
//
//            }

       } // class HAJournalServerStarter
       
    } // class // HAJournalServerConfiguration
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. 
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles();//getFileFilter());

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }
    
    @Override
    public void discarded(DiscoveryEvent arg0) {
        // NOP
    }

    @Override
    public void discovered(DiscoveryEvent arg0) {
        // NOP
    }

    /**
     * Wait until we have a met quorum (not necessarily a fully met quorum).
     * 
     * @return The quorum token for that met quorum.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws AsynchronousQuorumCloseException
     */
    protected long awaitMetQuorum() throws IOException,
            AsynchronousQuorumCloseException, InterruptedException,
            TimeoutException {

        // Wait for a quorum met.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        return token;

    }

    
    /**
     * Wait until we have a fully met quorum.
     * 
     * @return The quorum token for that fully met quorum.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws AsynchronousQuorumCloseException
     */
    protected long awaitFullyMetQuorum() throws IOException,
            AsynchronousQuorumCloseException, InterruptedException,
            TimeoutException {

        // Wait for a quorum met.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout * 2,
                TimeUnit.MILLISECONDS);

        // Wait for a fully met quorum.
        assertCondition(new Runnable() {
            public void run() {
                try {
                    // Verify quorum is FULLY met for that token.
                    assertTrue(quorum.isQuorumFullyMet(token));
                } catch (Exception e) {
                    // Quorum is not fully met.
                    fail();
                }
            }
        });
        
        return token;

    }

}
