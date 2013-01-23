package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Server;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAGlueDelegate;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.ha.RunState;
import com.bigdata.ha.althalog.HALogManager;
import com.bigdata.ha.althalog.IHALogWriter;
import com.bigdata.ha.msg.HALogRequest;
import com.bigdata.ha.msg.HALogRootBlocksRequest;
import com.bigdata.ha.msg.HARebuildRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.MonitoredFutureTask;
import com.bigdata.util.config.NicUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import com.sun.jini.start.LifeCycle;

/**
 * An administratable server for an {@link HAJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/530"> Journal
 *      HA </a>
 */
public class HAJournalServer extends AbstractServer {

    private static final Logger log = Logger.getLogger(HAJournalServer.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * Configuration options for the {@link HAJournalServer}.
     */
    public interface ConfigurationOptions extends
            AbstractServer.ConfigurationOptions {

        String COMPONENT = HAJournalServer.class.getName();
        
        /**
         * The target replication factor (k).
         */
        String REPLICATION_FACTOR = "replicationFactor";
        
        /**
         * The {@link InetSocketAddress} at which the managed {@link HAJournal}
         * exposes its write pipeline interface (required).
         */
        String WRITE_PIPELINE_ADDR = "writePipelineAddr";

        /**
         * The logical service identifier for this highly available journal.
         * There may be multiple logical highly available journals, each
         * comprised of <em>k</em> physical services. The logical service
         * identifier is used to differentiate these different logical HA
         * journals. The service {@link UUID} is used to differentiate the
         * physical service instances. By assigning a logical service identifier
         * to an {@link HAJournalServer} you associate that server instance with
         * the specified logical highly available journal.
         * <p>
         * The identifier may be any legal znode node.
         * 
         * TODO This needs to be reconciled with the federation. The federation
         * uses ephemeral sequential to create the logical service identifiers.
         * Here they are being assigned manually. This is basically the "flex"
         * versus "static" issue.
         */
        String LOGICAL_SERVICE_ID = "logicalServiceId";
        
    }
    
    /**
     * Configuration options for the {@link NanoSparqlServer}.
     */
    public interface NSSConfigurationOptions extends ConfigParams {
        
        String COMPONENT = NanoSparqlServer.class.getName();
        
        /**
         * The port at which the embedded {@link NanoSparqlServer} will respond
         * to HTTP requests (default {@value #DEFAULT_PORT}). This MAY be ZERO
         * (0) to use a random open port.
         */
        String PORT = "port";

        int DEFAULT_PORT = 8080;
        
    }

    /**
     * FIXME RESYNC : FLAG CONDITIONALLY ENABLES THE HA LOG AND RESYNC PROTOCOL. Disabled
     * in committed code until we have this running properly.
     */
    private static final boolean HA_LOG_ENABLED = true;
    
    /**
     * Caching discovery client for the {@link HAGlue} services.
     */
    private HAJournalDiscoveryClient discoveryClient;

    /**
     * The journal.
     */
    private HAJournal journal;
    
    private UUID serviceUUID;

    private ZookeeperClientConfig zkClientConfig;
    
    private ZooKeeperAccessor zka;
    
    /**
     * An executor used to handle events that were received in the zk watcher
     * event thread. We can not take actions that could block in the watcher
     * event thread. Therefore, a task for the event is dropped onto this
     * service where it will execute asynchronously with respect to the watcher
     * thread.
     * <p>
     * Note: This executor will be torn down when the backing
     * {@link AbstractJournal#getExecutorService()} is torn down. Tasks
     * remaining on the backing queue for the {@link LatchedExecutor} will be
     * unable to execute successfuly and the queue will be drained as attempts
     * to run those tasks result in {@link RejectedExecutionException}s.
     */
    private LatchedExecutor singleThreadExecutor;
    
    private HAGlue haGlueService;
    
    /**
     * The znode name for the logical service.
     * 
     * @see ConfigurationOptions#LOGICAL_SERVICE_ID
     */
    private String logicalServiceId;
    
    /**
     * The zpath for the logical service.
     * 
     * @see ConfigurationOptions#LOGICAL_SERVICE_ID
     */
    private String logicalServiceZPath;
    
    /**
     * The {@link Quorum} for the {@link HAJournal}.
     */
    private Quorum<HAGlue, QuorumService<HAGlue>> quorum;

//    /**
//     * Class models the invariants for an attempt to join a met quorum.
//     */
//    private static class SyncState {
//        private final IRootBlockView expected;
//        private final long quorumToken;
//        private long nextBlockSeq;
//        private boolean didCommit;
//
//        public SyncState(final IRootBlockView expected, final long quorumToken,
//                final long nextBlockSeq, final boolean didCommit) {
//            this.expected = expected;
//        }
//    }
//    private final Lock syncLock = new ReentrantLock();
    
    /**
     * An embedded jetty server exposing the {@link NanoSparqlServer} webapp.
     * The {@link NanoSparqlServer} webapp exposes a SPARQL endpoint for the
     * Journal, which is how you read/write on the journal (the {@link HAGlue}
     * interface does not expose any {@link Remote} methods to write on the
     * {@link HAJournal}.
     */
    private volatile Server jettyServer;

    /**
     * Caching discovery client for the {@link HAGlue} services.
     */
    public HAJournalDiscoveryClient getDiscoveryClient() {

        return discoveryClient;
        
    }

    public HAJournalServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);

    }
    
    @Override
    protected void terminate() {

        if (discoveryClient != null) {
        
            discoveryClient.terminate();
            
            discoveryClient = null;
            
        }

        super.terminate();
    
    }

    @Override
    protected HAGlue newService(final Configuration config)
            throws Exception {

        /*
         * Verify discovery of at least one ServiceRegistrar.
         */
        {
            final long begin = System.currentTimeMillis();

            ServiceRegistrar[] registrars = null;

            long elapsed = 0;

            while ((registrars == null || registrars.length == 0)
                    && elapsed < TimeUnit.SECONDS.toMillis(10)) {

                registrars = getDiscoveryManagement().getRegistrars();

                Thread.sleep(100/* ms */);

                elapsed = System.currentTimeMillis() - begin;

            }

            if (registrars == null || registrars.length == 0) {

                throw new RuntimeException(
                        "Could not discover ServiceRegistrar(s)");

            }

            if (log.isInfoEnabled()) {
                log.info("Found " + registrars.length + " service registrars");
            }

        }

        // Setup discovery for HAGlue clients.
        discoveryClient = new HAJournalDiscoveryClient(
                getServiceDiscoveryManager(),
                null/* serviceDiscoveryListener */, cacheMissTimeout);

        // Jini/River ServiceID.
        final ServiceID serviceID = getServiceID();

        if (serviceID == null)
            throw new AssertionError("ServiceID not assigned?");
        
        // UUID variant of that ServiceID.
        serviceUUID = JiniUtil.serviceID2UUID(serviceID);
        
        /*
         * Setup the Quorum / HAJournal.
         */

        zkClientConfig = new ZookeeperClientConfig(config);

        // znode name for the logical service.
        logicalServiceId = (String) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.LOGICAL_SERVICE_ID, String.class); 

        final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                + HAJournalServer.class.getName();
        
        // zpath for the logical service.
        logicalServiceZPath = logicalServiceZPathPrefix + "/"
                + logicalServiceId;

        final int replicationFactor = (Integer) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.REPLICATION_FACTOR, Integer.TYPE);        

        {

            // The address at which this service exposes its write pipeline.
            final InetSocketAddress writePipelineAddr = (InetSocketAddress) config
                    .getEntry(ConfigurationOptions.COMPONENT,
                            ConfigurationOptions.WRITE_PIPELINE_ADDR,
                            InetSocketAddress.class);

            /*
             * Configuration properties for this HAJournal.
             */
            final Properties properties = JiniClient.getProperties(
                    HAJournal.class.getName(), config);

            // Force the writePipelineAddr into the Properties.
            properties.put(HAJournal.Options.WRITE_PIPELINE_ADDR,
                    writePipelineAddr);

            /*
             * Zookeeper quorum.
             */
            {
                final List<ACL> acl = zkClientConfig.acl;
                final String zoohosts = zkClientConfig.servers;
                final int sessionTimeout = zkClientConfig.sessionTimeout;

                zka = new ZooKeeperAccessor(zoohosts, sessionTimeout);

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
                    zka.getZookeeper()
                            .create(zkClientConfig.zroot,
                                    new byte[] {/* data */}, acl,
                                    CreateMode.PERSISTENT);
                } catch (NodeExistsException ex) {
                    // ignore.
                }
                try {
                    zka.getZookeeper()
                            .create(logicalServiceZPathPrefix,
                                    new byte[] {/* data */}, acl,
                                    CreateMode.PERSISTENT);
                } catch (NodeExistsException ex) {
                    // ignore.
                }
                try {
                    zka.getZookeeper()
                            .create(logicalServiceZPath,
                                    new byte[] {/* data */}, acl,
                                    CreateMode.PERSISTENT);
                } catch (NodeExistsException ex) {
                    // ignore.
                }

                quorum = new ZKQuorumImpl<HAGlue, QuorumService<HAGlue>>(
                        replicationFactor, zka, acl);
            }

            // The HAJournal.
            this.journal = new HAJournal(properties, quorum);
            
        }

        // executor for events received in the watcher thread.
        singleThreadExecutor = new LatchedExecutor(
                journal.getExecutorService(), 1/* nparallel */);
        
        // our external interface.
        haGlueService = journal.newHAGlue(serviceUUID);

        // wrap the external interface, exposing administrative functions.
        final AdministrableHAGlueService administrableService = new AdministrableHAGlueService(
                this, haGlueService);

        // return that wrapped interface.
        return administrableService;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle initialization that must wait until the
     * {@link ServiceItem} is registered.
     */
    @Override
    protected void startUpHook() {

        if (log.isInfoEnabled())
            log.info("Starting server.");

        quorum.addListener(new QuorumListener() {

            @Override
            public void notify(final QuorumEvent e) {
                if (log.isTraceEnabled())
                    System.err.println("QuorumEvent: " + e); // TODO LOG @ TRACE
            }
        });

        // Set the quorum client aka quorum service and start the quorum.
        quorum.start(newQuorumService(logicalServiceZPath, serviceUUID,
                haGlueService, journal));

        /*
         * Note: This CAN NOT moved into QuorumServiceImpl.start(Quorum). I
         * suspect that this is because the quorum watcher would not be running
         * at the moment that we start doing things with the actor, but I have
         * not verified that rationale in depth.
         */
        doConditionalCastVote(this/* server */,
                (Quorum<HAGlue, QuorumService<HAGlue>>) quorum, journal);

        /*
         * The NSS will start on each service in the quorum. However,
         * only the leader will create the default KB (if that option is
         * configured).
         */
        try {

            startNSS();

        } catch (Exception e1) {

            log.error("Could not start NanoSparqlServer: " + e1, e1);

        }

    }
    
    /**
     * Attempt to add the service as a member, add the service to the pipeline,
     * and conditionally cast a vote for the last commit time. If the service is
     * already a quorum member or already in the pipeline, then those are NOPs.
     * If the quorum is already met, then the service DOES NOT cast a vote for
     * its lastCommitTime - it will need to resynchronize instead.
     * 
     * FIXME RESYNC : There needs to be an atomic decision whether to cast a
     * vote and then join or to start synchronizing. We need to synchronize if a
     * quorum is already met. We need to cast our vote iff a quorum has not met.
     * However, I can not see any (easy) way to make this operation atomic.
     * Maybe the leader simply needs to verify the actual lastCommitTime of each
     * service when the quorum meets, or maybe a service can only join the
     * quorum if it can verify that the leader has the same lastCommitTime for
     * its current root block. Once writes start, we need to be resynchronizing
     * rather than joining the quorum.
     */
    static private void doConditionalCastVote(
            final HAJournalServer server,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum,
            final HAJournal journal) {

        try {
        final QuorumActor<?, ?> actor = quorum.getActor();
        
        if(!server.isRunning())
            return;
        
        // ensure member.
        actor.memberAdd();

        if(!server.isRunning())
            return;

        // ensure in pipeline.
        actor.pipelineAdd();

        if (!quorum.isQuorumMet()) {

            /*
             * Cast a vote for our lastCommitTime.
             * 
             * Note: If the quorum is already met, then we MUST NOT cast a vote
             * for our lastCommitTime since we can not join the met quorum
             * without attempting to synchronize.
             */

            if(!server.isRunning())
                return;

            actor.castVote(journal.getLastCommitTime());

//            final long token;
//            try {
//                
//                // Block for a bit and see if the quorum meets.
//                token = quorum.awaitQuorum(1000, TimeUnit.MILLISECONDS);
//
//                // The quorum is met.
//                if (!quorum.getClient().isJoinedMember(token)) {
//
//                    /*
//                     * If we are not joined with the met quorum, then we will
//                     * need to withdraw our vote and synchronize.
//                     */
//                    
//                }
//
//            } catch (AsynchronousQuorumCloseException e) {
//                
//                // Shutdown. Rethrow exception.
//                throw e;
//                
//            } catch (InterruptedException e) {
//                
//                // Shutdown. Propagate interrupt.
//                Thread.currentThread().interrupt();
//                
//            } catch (TimeoutException e) {
//                
//                /*
//                 * We were able to cast a vote for our lastCommitTime and the
//                 * quorum did not meet.
//                 */
//                
//                return;
//                
//            }

        }
        
        } catch(Throwable t) {

            /*
             * Log and ignore any problems. Problems could include the shutdown
             * of the service, loss of zookeeper, etc.
             */
 
            log.error(t,t);
            
            return;

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to tear down the {@link NanoSparqlServer}, the {@link Quorum},
     * and the {@link HAJournal}.
     */
    @Override
    protected void beforeShutdownHook(final boolean destroy) {

        if (log.isInfoEnabled())
            log.info("destroy=" + destroy);

        if (jettyServer != null) {

            try {

                // Shutdown the embedded NSS.
                jettyServer.stop();

                // Wait for it to terminate.
                jettyServer.join();

                // Clear reference.
                jettyServer = null;

            } catch (Exception e) {

                log.error(e, e);

            }

        }

        if (quorum != null) {
            
            try {

//                // Notify the quorum that we are leaving.
//                quorum.getActor().serviceLeave();

                /*
                 * Terminate the watchers and threads that are monitoring and
                 * maintaining our reflection of the global quorum state.
                 */
                quorum.terminate();

                /*
                 * Close our zookeeper connection, invalidating all ephemeral
                 * znodes for this service.
                 * 
                 * Note: This provides a decisive mechanism for removing this
                 * service from the joined services, the pipeline, withdrawing
                 * its vote, and removing it as a quorum member.
                 */
                if (haLog.isInfoEnabled())
                    haLog.warn("FORCING UNCURABLE ZOOKEEPER DISCONNECT");
                
                if (zka != null) {

                    zka.close();
                    
                }

            } catch (Throwable t) {

                log.error(t, t);

            }
            
        }
        
        if (journal != null) {

            if (destroy) {

                journal.destroy();

            } else {

                journal.close();

            }

        }

    }

    /**
     * Factory for the {@link QuorumService} implementation.
     * 
     * @param logicalServiceZPath
     * @param serviceId
     * @param remoteServiceImpl
     *            The object that implements the {@link Remote} interfaces
     *            supporting HA operations.
     * @param store
     *            The {@link HAJournal}.
     */
    private QuorumServiceBase<HAGlue, HAJournal> newQuorumService(
            final String logicalServiceZPath,
            final UUID serviceId, final HAGlue remoteServiceImpl,
            final HAJournal store) {

        return new HAQuorumService<HAGlue, HAJournal>(logicalServiceZPath,
                serviceId, remoteServiceImpl, store, this);

    }

    /**
     * Concrete {@link QuorumServiceBase} implementation for the
     * {@link HAJournal}.
     */
    static private class HAQuorumService<S extends HAGlue, L extends HAJournal>
            extends QuorumServiceBase<S, L> {

        private final L journal;
        private final HAJournalServer server;

        /**
         * Lock to guard the HALogWriter.
         */
        private final Lock logLock = new ReentrantLock();
        
        /**
         * Future for task responsible for resynchronizing a node with
         * a met quorum.
         */
        private FutureTask<Void> resyncFuture = null;

        /**
         * @param logicalServiceZPath
         * @param serviceId
         * @param remoteServiceImpl
         *            The object that implements the {@link Remote} interfaces
         *            supporting HA operations.
         * @param store
         *            The {@link HAJournal}.
         */
        public HAQuorumService(final String logicalServiceZPath,
                final UUID serviceId, final S remoteServiceImpl, final L store,
                final HAJournalServer server) {

            super(logicalServiceZPath, serviceId, remoteServiceImpl, store);

            this.journal = store;
            
            this.server = server;

        }

        /**
         * Resolve an {@link HAGlue} object from its Service UUID.
         */
        @Override
        public S getService(final UUID serviceId) {
            
            final HAJournalDiscoveryClient discoveryClient = server
                    .getDiscoveryClient();

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

        @Override
        public void start(final Quorum<?,?> quorum) {
            
            if (haLog.isTraceEnabled())
                log.trace("START");
            
            super.start(quorum);

            // TODO It appears to be a problem to do this here. Maybe because
            // the watcher is not running yet? Could submit a task that could
            // await an appropriate condition to start....
//            final QuorumActor<?, ?> actor = quorum.getActor();
//            actor.memberAdd();
//            actor.pipelineAdd();
//            actor.castVote(journal.getLastCommitTime());

            // Inform the Journal about the current token (if any).
            journal.setQuorumToken(quorum.token());
            
        }
        
        @Override
        public void quorumBreak() {

            super.quorumBreak();

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new QuorumBreakTask()));

        }

        /**
         * Task to handle a quorum break event.
         */
        private class QuorumBreakTask implements Callable<Void> {

            public Void call() throws Exception {
                
                // Inform the Journal that the quorum token is invalid.
                journal.setQuorumToken(Quorum.NO_QUORUM);

                if (HA_LOG_ENABLED) {

                    try {

                        journal.getHALogManager().disable();

                    } catch (IOException e) {

                        haLog.error(e, e);

                    }

                }

                // if (server.isRunning()) {
                // /*
                // * Attempt to cast a vote for our lastCommitTime.
                // *
                // * FIXME BOUNCE : May need to trigger when we re-connect with
                // * zookeeper if this event was triggered by a zk session
                // * expiration.
                // */
                // doConditionalCastVote(server,
                // (Quorum<HAGlue, QuorumService<HAGlue>>) this
                // .getQuorum(),
                // journal);
                // }

                // Done.
                return null;
            }

        } // class QuorumBreakTask
        
        @Override
        public void quorumMeet(final long token, final UUID leaderId) {

            super.quorumMeet(token, leaderId);

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new QuorumMeetTask(token, leaderId)));

        }

        /**
         * Task to handle a quorum meet event.
         */
        private class QuorumMeetTask implements Callable<Void> {

            private final long token;
            private final UUID leaderId;

            public QuorumMeetTask(final long token, final UUID leaderId) {
                this.token = token;
                this.leaderId = leaderId;
            }
            
            public Void call() throws Exception {
                
                // Inform the journal that there is a new quorum token.
                journal.setQuorumToken(token);

                if (HA_LOG_ENABLED) {

                    if (isJoinedMember(token)) {

                        try {

                        	journal.getHALogManager().createLog(
                                    journal.getRootBlockView());

                        } catch (IOException e) {

                            /*
                             * We can not remain in the quorum if we can not write
                             * the HA Log file.
                             */
                            haLog.error("CAN NOT OPEN LOG: " + e, e);

                            getActor().serviceLeave();

                        }

                    } else {

                        /*
                         * The quorum met, but we are not in the met quorum.
                         * 
                         * Note: We need to synchronize in order to join an already
                         * met quorum. We can not just vote our lastCommitTime. We
                         * need to go through the synchronization protocol in order
                         * to make sure that we actually have the same durable state
                         * as the met quorum.
                         */

                        conditionalStartResync(token);

                    }
                    
                }

                // Done.
                return null;
                
            } // call()
            
        } // class QuorumMeetTask
            
        @Override
        public void pipelineAdd() {

            super.pipelineAdd();

            final Quorum<?, ?> quorum = getQuorum();

            final long token = quorum.token();

            conditionalStartResync(token);

        }

        /**
         * Conditionally start resynchronization of this service with the met
         * quorum.
         * 
         * @param token
         */
        private void conditionalStartResync(final long token) {
            
            if (isPipelineMember() && !isJoinedMember(token)
                    && getQuorum().isQuorumMet()
                    && (resyncFuture == null || resyncFuture.isDone())) {

                /*
                 * Start the resynchronization protocol.
                 */

                if (resyncFuture != null) {

                    // Cancel future if already running (paranoia).
                    resyncFuture.cancel(true/* mayInterruptIfRunning */);

                }

                resyncFuture = new MonitoredFutureTask<Void>(new ResyncTask());

                journal.getExecutorService().submit(resyncFuture);

            }

        }

        /**
         * Rebuild the backing store from scratch.
         * <p>
         * If we can not replicate ALL log files for the commit points that we
         * need to make up on this service, then we can not incrementally
         * resynchronize this service and we will have to do a full rebuild of
         * the service instead.
         * <p>
         * A rebuild begins by pinning the history on the quorum by asserting a
         * read lock (a read-only tx against then current last commit time).
         * This prevents the history from being recycled, but does not prevent
         * concurrent writes on the existing backing store extent, or extension
         * of the backing store.
         * <p>
         * While holding that read lock, we need to make a copy of the bytes in
         * the backing store. This copy can be streamed. It must start at the
         * first valid offset beyond the root blocks since we do not want to
         * update the root blocks until we have caught up with and replayed the
         * existing HA Log files. If the file is extended, we do not need to
         * copy the extension. Note that the streamed copy does not represent
         * any coherent commit point. However, once we apply ALL of the HA Log
         * files up to the last commit time that we pinned with a read lock,
         * then the local backing file will be binary consistent with that
         * commit point and we apply both the starting and ending root block for
         * that commit point, and finally release the read lock.
         * <p>
         * At this point, we are still not up to date. However, the HALog files
         * required to bring us up to date should exist and we can enter the
         * normal resynchronization logic.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * 
         *         TODO We are not actually binary consistent (even if we are
         *         data consistent) until we have either (A) joined the met
         *         quourm; or (B) replayed the HA Logs up to commitCounter+1 for
         *         the commitCounter on the leader as of the moment that we
         *         finished streaming the leader's backing file to this node.
         * 
         *         Blow off the root blocks (zero commit counters). Then install
         *         when known synched to specific commit point and enter
         *         resync.+
         */
        private class RebuildTask implements Callable<Void> {

            private final long token;
            private final S leader;
            
            public RebuildTask() {

                // run while quorum is met.
                token = getQuorum().token();

                // The leader for that met quorum (RMI interface).
                leader = getLeader(token);

            }

            @Override
            public Void call() throws Exception {

                if (true)
                    throw new UnsupportedOperationException();
                
//                final long readLock = leader.newTx(ITx.READ_COMMITTED);
                
                try {

//                    doRun(readLock);
                    doRun();

                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        log.info("Interrupted.");

                    }

                    log.error(t, t);

                } finally {
                    
//                    // release the read lock.
//                    leader.abort(readLock);
                    
                }

                return null;

            }
            
            private void doRun(/*final long readLock*/) throws Exception {

                haLog.warn("REBUILD: " + server.getServiceName());
                
                /*
                 * Note: We need to discard any writes that might have been
                 * buffered before we start the resynchronization of the local
                 * store.
                 */

                journal.doLocalAbort();

                /*
                 * Make a note of the root block that is in effect when we being
                 * the rebuild procedure. Once we have replicated the backing
                 * store from the leader, we will need to replay all HALog files
                 * starting with commitCounter+1 for this root block. Once we
                 * catch up, we can atomically join and lay down the root blocks
                 * from the leader for the most recent commit point.
                 */
                final IRootBlockView rootBlockAtStartOfRebuild = leader
                        .getRootBlock(
                                new HARootBlockRequest(null/* storeUUID */))
                        .getRootBlock();

                /*
                 * Replicate the backing store of the leader.
                 * 
                 * Note: We are holding a read lock, so committed allocations
                 * will not be overwritten. However, the replicated backing file
                 * will not be logically consistent until we apply all HALog
                 * files since the commit point noted above.
                 * 
                 * Note: This remoteFuture MUST be cancelled if the RebuildTask
                 * is interrupted.
                 */
                final Future<Void> remoteFuture = leader
                        .sendHAStore(new HARebuildRequest(getServiceId()));

                try {

                    // Wait for the raw store to be replicated.
                    remoteFuture.get();
                    
                } finally {

                    // Ensure remoteFuture is cancelled.
                    remoteFuture.cancel(true/* mayInterruptIfRunning */);
                    
                }
 
                /*
                 * FIXME REBUILD: Apply all HALogs (request each log starting
                 * with the commit point identified above, and apply as
                 * received, but DO NOT lay down root blocks or go through
                 * commit points). [Refactor this code from the RESYNC task.]
                 * 
                 * Note: The quorum WILL be maintaining HALog files since (a)
                 * they are maintained for every active write set; and (b) the
                 * quorum is not fully met since this service is not joined with
                 * the met quorum.
                 */
                {
                    
                    // The last commit point that has been captured.
                    long commitCounter = rootBlockAtStartOfRebuild
                            .getCommitCounter();

                    // Until joined with the met quorum.
                    while (!getQuorum().getMember().isJoinedMember(token)) {

                        // Abort if the quorum breaks.
                        getQuorum().assertQuorum(token);

                        // Replicate and apply the next write set
                        replicateAndApplyWriteSet(leader, token,
                                commitCounter + 1, false/* incremental */);

                        // Replicate the next write set.
                        commitCounter++;

                    }

                }

            }
            
        } // class RebuildTask
        
        /**
         * This class handles the resynchronization of a node that is not at the
         * same commit point as the met quorum. The task will replicate write
         * sets (HA Log files) from the services in the met quorum, and apply
         * those write sets (in pure commit sequence) in order to advance its
         * own committed state to the same last commit time as the quorum
         * leader. Once it catches up with the quorum, it still needs to
         * replicate logged writes from a met services in the quorum until it
         * atomically can log from the write pipeline rather than replicating a
         * logged write. At that point, the service can vote its lastCommitTime
         * and will join the met quorum.
         * <p>
         * Note: In fact, the service can always begin logging from the write
         * pipeline as soon as it observes (or acquires) the root block and
         * observes the seq=0 write cache block (or otherwise catches up with
         * the write pipeline). However, it can not write those data onto the
         * local journal until it is fully caught up. This optimization might
         * allow the service to catch up slightly faster, but the code would be
         * a bit more complex.
         */
        private class ResyncTask implements Callable<Void> {

            /**
             * The quorum token in effect when we began the resync.
             */
            private final long token;

            /**
             * The quorum leader. This is fixed until the quorum breaks or the
             * resync ends.
             */
            private final S leader;
            
            public ResyncTask() {

                // run while quorum is met.
                token = getQuorum().token();

                // The leader for that met quorum (RMI interface).
                leader = getLeader(token);

            }

            @Override
            public Void call() throws Exception {

                try {

                    doRun();

                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        log.info("Interrupted.");

                    }

                    log.error(t, t);

                } finally {
                    
                    haLog.warn("RESYNCH: exit.");

                }

                return null;

            }

            /**
             * Replicate each write set for commit points GT the current commit
             * point on this service. As each write set is replicated, is also
             * applied and we advance to another commit point. This method loops
             * until we have all write sets locally replicated and the service
             * is able to begin accepting write cache blocks from the write
             * pipeline rather than through the resynchronization protocol.
             * <p>
             * Note: This task will be interrupted when the service catches up
             * and is able to log and write the write cache block from the write
             * pipeline. At that point, we no longer need to replicate the write
             * sets from the leader.
             * 
             * @throws Exception
             */
            private void doRun() throws Exception {

                haLog.warn("RESYNCH: " + server.getServiceName());

                /*
                 * Note: We need to discard any writes that might have been
                 * buffered before we start the resynchronization of the local
                 * store. Otherwise they could wind up flushed through by the
                 * RWStore (for example, when handling a file extension).
                 * 
                 * Note: This is necessary. We do a low-level abort when we
                 * install the root blocks from the quorum leader before we sync
                 * the first commit point, but we do not do the low-level abort
                 * if we already have the correct root blocks in place.
                 */

                journal.doLocalAbort();

                // Until joined with the met quorum.
                while (!getQuorum().getMember().isJoinedMember(token)) {

                    // Abort if the quorum breaks.
                    getQuorum().assertQuorum(token);
                    
                    // The current commit point on the local store.
                    final long commitCounter = journal.getRootBlockView()
                            .getCommitCounter();

                    // Replicate and apply the next write set
                    replicateAndApplyWriteSet(leader, token, commitCounter + 1,
                            true/* incremental */);

                }

            }

        } // class ResyncTask

        /**
         * Replicate the write set having the specified commit counter, applying
         * each {@link WriteCache} block as it is received and eventually going
         * through a local commit when we receiving the closing
         * {@link IRootBlockView} for that write set.
         * 
         * @param leader
         *            The quorum leader.
         * @param quorumToken
         *            The quorum token that must remain valid throughout this
         *            operation.
         * @param closingCommitCounter
         *            The commit counter for the <em>closing</em> root block of
         *            the write set to be replicated.
         * @param incremental
         *            When <code>true</code> this is an incremental
         *            re-synchronization based on replicated HALog files. When
         *            <code>false</code>, this is a ground up rebuild (disaster
         *            recovery). For the ground up rebuild we DO NOT go through
         *            a local commit with each HALog file that we replicate and
         *            apply.
         * 
         * @throws IOException
         * @throws FileNotFoundException
         * @throws ExecutionException
         * @throws InterruptedException
         */
        private void replicateAndApplyWriteSet(final S leader,
                final long quorumToken, final long closingCommitCounter,
                final boolean incremental) throws FileNotFoundException,
                IOException,
                InterruptedException, ExecutionException {

            if (haLog.isInfoEnabled())
                haLog.info("RESYNC: commitCounter=" + closingCommitCounter
                        + ", incremental=" + incremental);

            final IHALogRootBlocksResponse resp;
            try {
                
                // Request the root blocks for the write set.
                resp = leader
                        .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                closingCommitCounter));

            } catch (FileNotFoundException ex) {

                /*
                 * Oops. The leader does not have that log file.
                 * 
                 * We will have to rebuild the service from scratch since we
                 * do not have the necessary HA Log files to synchronize
                 * with the existing quorum.
                 * 
                 * TODO RESYNC : It is possible to go to another service in
                 * the met quorum for the same log file, but it needs to be
                 * a service that is UPSTREAM of this service.
                 */

                final String msg = "HA Log not available: commitCounter="
                        + closingCommitCounter;

                if (incremental) {

                    log.error(msg);

                    final FutureTask<Void> ft = new FutureTaskMon<Void>(
                            new RebuildTask());

                    try {

                        // Run service rebuild task.
                        journal.getExecutorService().submit(ft);

                        ft.get();

                    } finally {

                        ft.cancel(true/* mayInterruptIfRunning */);

                    }

                    // Re-enter the resync protocol.
                    return;

                } else {
                    
                    /*
                     * If we are already doing a rebuild (versus resync) then it
                     * is a fatal error if we can not locate the necessary HALog
                     * files.
                     * 
                     * Note: This could be caused by a bug or if someone deleted
                     * the necessary HALog file on the leader. Either way,
                     * manual intervention is required.
                     */
                    
                    log.fatal(msg);
 
                    // Shutdown.
                    server.shutdownNow(false/* destroy */);

                    // Rethrow the exception.
                    throw new IOException(ex);
                    
                }

            }

            // root block when the quorum started that write set.
            final IRootBlockView openRootBlock = resp.getOpenRootBlock();

            if (openRootBlock.getCommitCounter() != closingCommitCounter - 1) {
                
                /*
                 * The opening root block for the write set must have a
                 * commit counter that is ONE less than the requested commit
                 * point.
                 */
                
                throw new AssertionError(
                        "Should start at the previous commit point: requested commitCounter="
                                + closingCommitCounter + ", openRootBlock="
                                + openRootBlock);
            }
            
            /*
             * If the local journal is empty, then we need to replace both
             * of it's root blocks with the opening root block.
             */
            if (journal.getRootBlockView().getCommitCounter() == 0
                    && incremental) {

                // Install the initial root blocks.
                installRootBlocksFromQuorum(openRootBlock);

            }

            // Make sure we have the correct HALogWriter open.
            logLock.lock();
            try {
                final HALogManager logManager = journal.getHALogManager();
                logManager.disable();
                logManager.createLog(openRootBlock);
            } finally {
                logLock.unlock();
            }

            /*
             * We need to transfer and apply the write cache blocks from the
             * HA Log file on some service in the met quorum. This code
             * works with the leader because it is known to be upstream from
             * all other services in the write pipeline.
             * 
             * Note: Because we are multiplexing the write cache blocks on
             * the write pipeline, there is no advantage to gathering
             * different write sets from different nodes. A slight advantage
             * could be had by reading off of the immediate upstream node,
             * but understandability of the code is improved by the
             * assumption that we are always replicating these data from the
             * quorum leader.
             */

            Future<Void> ft = null;
            try {

                ft = leader
                        .sendHALogForWriteSet(new HALogRequest(
                                server.serviceUUID, closingCommitCounter,
                                true/* incremental */));

                try {
                
                    /*
                     * Wait up to one second
                     * 
                     * TODO Lift out timeout (configure).
                     */
                    
                    ft.get(1000, TimeUnit.MILLISECONDS);
                    
                } catch (TimeoutException ex) {

                    /*
                     * Nothing was received before the timeout. Attempt to
                     * join with the met quorum.
                     */
                    
                    if (conditionalJoinWithMetQuorum(leader, quorumToken,
                            closingCommitCounter - 1, incremental)) {

                        /*
                         * We are caught up and have joined the met quorum.
                         * 
                         * Note: Future will be canceled in finally clause.
                         */
                     
                        return;
                        
                    }

                }

                /*
                 * Wait until all write cache blocks are received.
                 */
                if (log.isDebugEnabled())
                    log.debug("WAITING ON SEND HALOG FUTURE");

                ft.get();

            } finally {

                
                if (ft != null) {

                    if (log.isDebugEnabled())
                        log.debug("HALOG FUTURE: isDone=" + ft.isDone());

                    ft.cancel(true/* mayInterruptIfRunning */);

                    ft = null;

                }

            }

            /*
             * Figure out the closing root block. If this HALog file was
             * active when we started reading from it, then the open and
             * close root blocks would have been identical in the [resp] and
             * we will need to grab the root blocks again now that it has
             * been closed.
             */
            final IRootBlockView closeRootBlock;
            {

                // root block when the quorum committed that write set.
                IRootBlockView tmp = resp.getCloseRootBlock();

                if (openRootBlock.getCommitCounter() == tmp
                        .getCommitCounter()) {

                    /*
                     * The open and close commit counters were the same when
                     * we first requested them, so we need to re-request the
                     * close commit counter now that we are done reading on
                     * the file.
                     */
                    
                    // Re-request the root blocks for the write set.
                    final IHALogRootBlocksResponse resp2 = leader
                            .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                    closingCommitCounter));
                    
                    tmp = resp2.getCloseRootBlock();

                }

                closeRootBlock = tmp;

                if (closeRootBlock.getCommitCounter() != closingCommitCounter) {

                    throw new AssertionError(
                            "Wrong commitCounter for closing root block: expected commitCounter="
                                    + closingCommitCounter
                                    + ", but closeRootBlock="
                                    + closeRootBlock);

                }
                
            }

            if (incremental) {

                // Local commit.
                journal.doLocalCommit(
                        (QuorumService<HAGlue>) HAQuorumService.this,
                        closeRootBlock);

            }

            // Close out the current HALog writer.
            logLock.lock();
            try {
                final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();
                logWriter.close(closeRootBlock);
            } finally {
                logLock.unlock();
            }

            if (haLog.isInfoEnabled())
                haLog.info("RESYNC: caught up to commitCounter="
                        + closingCommitCounter);

        }

        /**
         * Conditional join of a service attempting to synchronize with the met
         * quorum. If the current commit point that is being replicated (as
         * indicated by the <i>commitCounter</i>) is thought to be the most
         * current root block on the leader AND we have not received any writes
         * on the HALog, then we assume that the leader is quiescent (no write
         * activity) and we attempt to join the qourum.
         * 
         * @param leader
         *            The quorum leader.
         * @param openingCommitCounter
         *            The commit counter for the <em>opening</em> root block of
         *            the write set that is currently being replicated.
         * @param incremental
         *            When <code>true</code> this is an incremental
         *            re-synchronization based on replicated HALog files. When
         *            <code>false</code>, this is a ground up rebuild (disaster
         *            recovery). For the ground up rebuild we DO NOT go through
         *            a local commit with each HALog file that we replicate and
         *            apply.
         * 
         * @return <code>true</code> iff we were able to join the met quorum.
         */
        private boolean conditionalJoinWithMetQuorum(final S leader,
                final long quorumToken, final long openingCommitCounter,
                final boolean incremental) throws IOException {

            // Get the current root block from the quorum leader.
            final IRootBlockView currentRootBlockOnLeader = leader
                    .getRootBlock(new HARootBlockRequest(null/* storeId */))
                    .getRootBlock();

            final boolean sameCommitCounter = currentRootBlockOnLeader
                    .getCommitCounter() == openingCommitCounter;

            if (haLog.isDebugEnabled())
                haLog.debug("sameCommitCounter=" + sameCommitCounter
                        + ", openingCommitCounter=" + openingCommitCounter
                        + ", currentRootBlockOnLeader="
                        + currentRootBlockOnLeader + ", incremental="
                        + incremental);

            if (!sameCommitCounter) {

                /*
                 * We can not join. We are not at the same commit point as
                 * the quorum leader.
                 */

                return false;

            }

            /*
             * This is the same commit point that we are trying to replicate
             * right now. Check the HALog. If we have not observed any write
             * cache blocks, then we can attempt to join the met quorum.
             * 
             * Note: We can not accept replicated writes while we are
             * holding the logLock (the lock is required to accept
             * replicated writes).
             */
            logLock.lock();

            try {

                if(!incremental) {
                 
                    /*
                     * FIXME REBUILD : When this is a ground up service rebuild,
                     * we need to lay down both root blocks atomically when we
                     * are ready to join the met quorum (or perhaps at the next
                     * commit point after we join the met quorum).
                     */
                    
                    throw new UnsupportedOperationException();
                    
                }
                
                final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();

                if (haLog.isDebugEnabled())
                    haLog.debug("HALog.commitCounter="
                            + logWriter.getCommitCounter()
                            + ", HALog.getSequence="
                            + logWriter.getSequence());

                if (logWriter.getCommitCounter() != openingCommitCounter
                        || logWriter.getSequence() != 0L) {

                    return false;

                }
                
                if (haLog.isInfoEnabled())
                    haLog.info("Will attempt to join met quorum.");

                // The vote cast by the leader.
                final long lastCommitTimeOfQuorumLeader = getQuorum()
                        .getCastVote(leader.getServiceId());

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(quorumToken);

                // Cast that vote.
                getActor().castVote(lastCommitTimeOfQuorumLeader);

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(quorumToken);

                // Attempt to join the met quorum.
                getActor().serviceJoin();

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(quorumToken);

                haLog.warn("RESYNC : joined met quorum. commitCounter="
                        + openingCommitCounter + ", lastCommitTimeOfLeader="
                        + lastCommitTimeOfQuorumLeader + ", nextBlockSeq="
                        + logWriter.getSequence());

                return true;

            } finally {

                logLock.unlock();

            }
            
        }

        @Override
        protected void handleReplicatedWrite(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws Exception {

            logLock.lock();
            try {

                if (haLog.isDebugEnabled())
                    haLog.debug("msg=" + msg + ", buf=" + data);

                // The current root block on this service.
                final long commitCounter = journal.getRootBlockView()
                        .getCommitCounter();

                if (req != null && !req.isIncremental()) {

                    /*
                     * This message and payload are part of a ground up service
                     * rebuild (disaster recovery from the quorum) rather than
                     * an incremental resynchronization.
                     * 
                     * Note: HALog blocks during rebuild are written onto the
                     * appropriate HALog file using the same rules that apply to
                     * resynchronization. We also capture the root blocks for
                     * those replicate HALog files. However, during a REBUILD,
                     * we DO NOT go through a local commit for the replicated
                     * HALog files until the service is fully synchronized. This
                     * prevents the service from having root blocks that are
                     * "non-empty" when the file state is not fully consistent
                     * with the leader.
                     * 
                     * Note: Replicaed backing store write blocks MUST be
                     * written directly onto the backing FileChannel after
                     * correcting the offset (which is relative to the root
                     * block). Even for the RWStore, the replicated backing
                     * store blocks represent a contiguous extent on the file
                     * NOT scattered writes.
                     * 
                     * FIXME REBUILD : Handle replicated HALog write blocks
                     * 
                     * FIXME REBUILD : Handle replicated backing store write
                     * blocks.
                     */

                    throw new UnsupportedOperationException();

                }
                
                if (HA_LOG_ENABLED) {

                    final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();

                    if (msg.getCommitCounter() == logWriter.getCommitCounter()
                            && msg.getSequence() == (logWriter.getSequence() - 1)) {

                        /*
                         * Duplicate message. This can occur due retrySend() in
                         * QuorumPipelineImpl#replicate(). retrySend() is used
                         * to make the pipeline robust if a service (other than
                         * the leader) drops out and we need to change the
                         * connections between the services in the write
                         * pipeline in order to get the message through.
                         */

                        if (log.isInfoEnabled())
                            log.info("Ignoring message (dup): " + msg);

                        return;

                    }

                }

                if (resyncFuture != null && !resyncFuture.isDone()) {

                    /*
                     * If we are resynchronizing, then pass ALL messages (both
                     * live and historical) into handleResyncMessage().
                     */
                    
                    handleResyncMessage((IHALogRequest) req, msg, data);

                } else if (commitCounter == msg.getCommitCounter()
                        && isJoinedMember(msg.getQuorumToken())) {

                    /*
                     * We are not resynchronizing this service. This is a
                     * message for the current write set. The service is joined
                     * with the quorum.
                     */

                    // write on the log and the local store.
                    acceptHAWriteMessage(msg, data);

                } else {
                    
                    if (log.isInfoEnabled())
                        log.info("Ignoring message: " + msg);
                    
                    /*
                     * Drop the pipeline message.
                     * 
                     * Note: There are two cases here.
                     * 
                     * (A) It is a historical message that is being ignored on
                     * this node;
                     * 
                     * (B) It is a live message, but this node is not caught up
                     * and therefore can not log the message yet.
                     */
                    
                }

            } finally {

                logLock.unlock();

            }

        }
        
        /**
         * Adjust the size on the disk of the local store to that given in the
         * message.
         * <p>
         * Note: When historical messages are being replayed, the caller needs
         * to decide whether the message should applied to the local store. If
         * so, then the extent needs to be updated. If not, then the message
         * should be ignored (it will already have been replicated to the next
         * follower).
         */
        private void setExtent(final IHAWriteMessage msg) throws IOException {

            try {

                ((IHABufferStrategy) journal.getBufferStrategy())
                        .setExtentForLocalStore(msg.getFileExtent());

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            } catch (RuntimeException t) {

                // Wrap with the HA message.
                throw new RuntimeException("msg=" + msg + ": " + t, t);
                
            }

        }

        /**
         * Handle a replicated write requested to resynchronize this service
         * with the quorum. The {@link WriteCache} messages for HA Logs are
         * delivered over the write pipeline, along with messages for the
         * current write set. This method handles those that are for historical
         * write sets (replayed from HA Log files) as well as those that are
         * historical writes for the current write set (that is, messages that
         * this service missed because it joined the write pipeline after the
         * first write message for the current write set and was thus not able
         * to log and/or apply the write message even if it did observe it).
         * <p>
         * Note: The quorum token associated with historical message needs to be
         * ignored. The quorum could have broken and met again since, in which
         * case any attempt to use that old token will cause a QuorumException.
         * 
         * @throws InterruptedException
         * @throws IOException
         */
        private void handleResyncMessage(final IHALogRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws IOException, InterruptedException {

            logLock.lock();

            try {

                /*
                 * TODO RESYNC : Review when (and where) we open and close log
                 * files.
                 */

                final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();

                final long journalCommitCounter = journal.getRootBlockView()
                        .getCommitCounter();

                if (req == null) {
                    
                    /*
                     * Live message.
                     */

                    if (msg.getCommitCounter() == journalCommitCounter
                            && msg.getSequence() + 1 == logWriter.getSequence()) {

                        /*
                         * We just received the last resync message that we need
                         * to join the met quorum.
                         */

                        resyncTransitionToMetQuorum(msg, data);

                        return;

                    } else {

                        /*
                         * Drop live messages since we are not caught up.
                         */

                        if (haLog.isDebugEnabled())
                            log.debug("Ignoring write cache block: msg=" + msg);

                        return;

                    }

                } else {

                    /*
                     * A historical message (replay of an HALog file).
                     * 
                     * Note: We will see ALL messages. We can only log the
                     * message if it is for our commit point.
                     */

                    if (!server.serviceUUID.equals(req.getServiceId())) {

                        /*
                         * Not our request. Drop the message.
                         */
                        
                        if (haLog.isDebugEnabled())
                            log.debug("Ignoring write cache block: msg=" + msg);

                        return;

                    }

                    // log and write cache block.
                    acceptHAWriteMessage(msg, data);

                }
                
            } finally {

                logLock.unlock();

            }

        }

        /**
         * Atomic transition to the met quorum, invoked when we receive the same
         * sequence number for some {@link WriteCache} block in the current
         * write set twice. This happens when we get it once from the explicit
         * resynchronization task and once from the normal write pipeline
         * writes. In both cases, the block is transmitted over the write
         * pipeline. The double-presentation of the block is our signal that we
         * are caught up with the normal write pipeline writes.
         */
        private void resyncTransitionToMetQuorum(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();
            
            final IRootBlockView rootBlock = journal.getRootBlockView();

            if (logWriter.getCommitCounter() != rootBlock.getCommitCounter()) {

                throw new AssertionError("HALogWriter.commitCounter="
                        + logWriter.getCommitCounter() + ", but rootBlock="
                        + rootBlock);

            }

            if (msg.getCommitCounter() != rootBlock.getCommitCounter()
                    || msg.getLastCommitTime() != rootBlock.getLastCommitTime()) {

                throw new AssertionError("msg=" + msg + ", but rootBlock="
                        + journal.getRootBlockView());

            }

            /*
             * Service is not joined but is caught up with the write
             * pipeline and is ready to join.
             */

            if (resyncFuture != null) {

                // Caught up.
                resyncFuture.cancel(true/* mayInterruptIfRunning */);

            }

            // Accept the message - log and apply.
            acceptHAWriteMessage(msg, data);

            // Vote our lastCommitTime.
            getActor().castVote(rootBlock.getLastCommitTime());

            log.warn("Service voted for lastCommitTime of quorum, is receiving pipeline writes, and should join the met quorum");

            // /*
            // * TODO RESYNC : If there is a fully met quorum, then we can purge
            // * all HA logs *EXCEPT* the current one. However, in order
            // * to have the same state on each node, we really need to
            // * make this decision when a service observes the
            // * SERVICE_JOIN event that results in a fully met quorum.
            // * That state change will be globally visible. If we do
            // this
            // * here, then only the service that was resynchronizing
            // will
            // * wind up purging its logs.
            // */
            // if (getQuorum().isQuorumFullyMet()) {
            //
            // purgeHALogs(false/* includeCurrent */);
            //
            // }

        }
        
        /**
         * Verify commitCounter in the current log file and the message are
         * consistent, then log and apply the {@link WriteCache} block.
         */
        private void acceptHAWriteMessage(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            if (HA_LOG_ENABLED) {

                final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();

                if (msg.getCommitCounter() != logWriter.getCommitCounter()) {

                    throw new AssertionError();

                }

                /*
                 * Log the message and write cache block.
                 */
                logWriteCacheBlock(msg, data);
                
            }

            setExtent(msg);
            writeWriteCacheBlock(msg, data);

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Writes the {@link IHAWriteMessage} and the data onto the
         * {@link HALogWriter}
         */
        @Override
        public void logWriteCacheBlock(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException {

            if (!HA_LOG_ENABLED)
                return;

            logLock.lock();

            try {

                final IHALogWriter logWriter = journal.getHALogManager().getOpenLogFile().getWriter();

                logWriter.write(msg, data);
                
            } finally {
                
                logLock.unlock();
                
            }
            
        }

        /**
         * Write the raw {@link WriteCache} block onto the backing store.
         */
        private void writeWriteCacheBlock(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            /*
             * Note: the ByteBuffer is owned by the HAReceiveService. This just
             * wraps up the reference to the ByteBuffer with an interface that
             * is also used by the WriteCache to control access to ByteBuffers
             * allocated from the DirectBufferPool. However, release() is a NOP
             * on this implementation since the ByteBuffer is owner by the
             * HAReceiveService.
             */
            
            final IBufferAccess b = new IBufferAccess() {

                @Override
                public void release(long timeout, TimeUnit unit)
                        throws InterruptedException {
                    // NOP
                }

                @Override
                public void release() throws InterruptedException {
                    // NOP
                }

                @Override
                public ByteBuffer buffer() {
                    return data;
                }
            };

            ((IHABufferStrategy) journal.getBufferStrategy())
                    .writeRawBuffer(msg, b);
            
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Writes the root block onto the {@link HALogWriter} and closes the log
         * file. A new log is then opened, using the given root block as the
         * starting point for that log file.
         */
        @Override
        public void logRootBlock(final IRootBlockView rootBlock) throws IOException {

            if (!HA_LOG_ENABLED)
                return;

            logLock.lock();

            try {
            	final HALogManager logManager = journal.getHALogManager();
                final IHALogWriter logWriter = logManager.getOpenLogFile().getWriter();
                
                // Close off the old log file with the root block.
                logWriter.close(rootBlock);
                
                // Open up a new log file with this root block.
                logManager.createLog(rootBlock);
                
            } finally {
                
                logLock.unlock();
                
            }
            
        }

        /**
         * {@inheritDoc}
         * <p>
         * Destroys the HA log files in the HA log directory.
         */
        @Override
        public void purgeHALogs(final boolean includeCurrent) {

            if (!HA_LOG_ENABLED)
                return;

            logLock.lock();

            try {
                journal.getHALogManager().removeAllLogFiles(includeCurrent);
            } finally {

                logLock.unlock();

            }

        }

        @SuppressWarnings("unchecked")
        @Override
        public void installRootBlocksFromQuorum(final IRootBlockView rootBlock) {

            journal.installRootBlocksFromQuorum((QuorumService<HAGlue>) this,
                    rootBlock);

        }

        @Override
        public File getServiceDir() {
            return server.getServiceDir();
        }
        
    }

//    /**
//     * Conditionally create the default KB instance.
//     * 
//     * @see NSSConfigurationOptions
//     * 
//     * @throws Exception
//     */
//    private void conditionalCreateDefaultKB() throws Exception {
//        
//        final String COMPONENT = NSSConfigurationOptions.COMPONENT;
//
//        final String namespace = (String) config.getEntry(COMPONENT,
//                NSSConfigurationOptions.NAMESPACE, String.class,
//                NSSConfigurationOptions.DEFAULT_NAMESPACE);
//
//        final boolean create = (Boolean) config.getEntry(COMPONENT,
//                NSSConfigurationOptions.CREATE, Boolean.TYPE,
//                NSSConfigurationOptions.DEFAULT_CREATE);
//
//    }
    
    /**
     * Setup and start the {@link NanoSparqlServer}.
     * <p>
     * Note: We need to wait for a quorum meet since this will create the KB
     * instance if it does not exist and we can not write on the
     * {@link HAJournal} until we have a quorum meet.
     */
    private void startNSS() throws Exception {

        final String COMPONENT = NSSConfigurationOptions.COMPONENT;

        final String namespace = (String) config.getEntry(COMPONENT,
                NSSConfigurationOptions.NAMESPACE, String.class,
                NSSConfigurationOptions.DEFAULT_NAMESPACE);

        final Integer queryPoolThreadSize = (Integer) config.getEntry(
                COMPONENT, NSSConfigurationOptions.QUERY_THREAD_POOL_SIZE,
                Integer.TYPE,
                NSSConfigurationOptions.DEFAULT_QUERY_THREAD_POOL_SIZE);

        final boolean create = (Boolean) config.getEntry(COMPONENT,
                NSSConfigurationOptions.CREATE, Boolean.TYPE,
                NSSConfigurationOptions.DEFAULT_CREATE);

        final Integer port = (Integer) config.getEntry(COMPONENT,
                NSSConfigurationOptions.PORT, Integer.TYPE,
                NSSConfigurationOptions.DEFAULT_PORT);

        log.warn("Starting NSS: port=" + port);

        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, namespace);

            initParams.put(ConfigParams.QUERY_THREAD_POOL_SIZE,
                    queryPoolThreadSize.toString());

            // Note: Create will be handled by the QuorumListener (above).
            initParams.put(ConfigParams.CREATE, Boolean.toString(create));

        }
        
        if (jettyServer != null && jettyServer.isRunning()) {
        
            throw new RuntimeException("Already running");
            
        }

        // Setup the embedded jetty server for NSS webapp.
        jettyServer = NanoSparqlServer.newInstance(port, journal, initParams);

        // Start the server.
        jettyServer.start();

        /*
         * Report *an* effective URL of this service.
         * 
         * Note: This is an effective local URL (and only one of them, and even
         * then only one for the first connector). It does not reflect any
         * knowledge about the desired external deployment URL for the service
         * end point.
         */
        final String serviceURL;
        {

            final int actualPort = jettyServer.getConnectors()[0].getLocalPort();

            String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                    true/* loopbackOk */);

            if (hostAddr == null) {

                hostAddr = "localhost";

            }

            serviceURL = new URL("http", hostAddr, actualPort, ""/* file */)
                    .toExternalForm();

            System.out.println("logicalServiceZPath: " + logicalServiceZPath);
            System.out.println("serviceURL: " + serviceURL);

        }

    }
    
    /**
     * Start an {@link HAJournal}.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file.
     */
    public static void main(final String[] args) {

        if (args.length == 0) {

            System.err.println("usage: <config-file> [config-overrides]");

            System.exit(1);

        }

        final HAJournalServer server = new HAJournalServer(args,
                new FakeLifeCycle());

        // Wait for the HAJournalServer to terminate.
        server.run();
        
//        try {
//            final Server tmp = server.jettyServer;
//            if (tmp != null) {
//                // Wait for the jetty server to terminate.
//                tmp.join();
//            }
//        } catch (InterruptedException e) {
//            log.warn(e);
//        }

        System.exit(0);

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link HAGlue} interface
     * exposed by the {@link HAJournal}.
     * 
     * @see HAJournal.HAGlueService
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class AdministrableHAGlueService extends HAGlueDelegate
            implements RemoteAdministrable, RemoteDestroyAdmin {

        final protected HAJournalServer server;

        public AdministrableHAGlueService(final HAJournalServer server,
                final HAGlue service) {

            super(service);

            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info("serviceID=" + server.getServiceID());

            return server.proxy;
            
        }
        
        /**
         * Sets up the {@link MDC} logging context. You should do this on every
         * client facing point of entry and then call
         * {@link #clearLoggingContext()} in a <code>finally</code> clause. You
         * can extend this method to add additional context.
         * <p>
         * This implementation adds the following parameters to the {@link MDC}.
         * <dl>
         * <dt>serviceName</dt>
         * <dd>The serviceName is typically a configuration property for the
         * service. This datum can be injected into log messages using
         * <em>%X{serviceName}</em> in your log4j pattern layout.</dd>
         * <dt>serviceUUID</dt>
         * <dd>The serviceUUID is, in general, assigned asynchronously by the
         * service registrar. Once the serviceUUID becomes available it will be
         * added to the {@link MDC}. This datum can be injected into log
         * messages using <em>%X{serviceUUID}</em> in your log4j pattern layout.
         * </dd>
         * <dt>hostname</dt>
         * <dd>The hostname statically determined. This datum can be injected
         * into log messages using <em>%X{hostname}</em> in your log4j pattern
         * layout.</dd>
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request.</dd>
         * </dl>
         * Note: {@link InetAddress#getHostName()} is used. This method makes a
         * one-time best effort attempt to resolve the host name from the
         * {@link InetAddress}.
         */
        private void setupLoggingContext() {

            try {

                // Note: This _is_ a local method call.
                final ServiceID serviceUUID = server.getServiceID();

                // Will be null until assigned by the service registrar.

                if (serviceUUID != null) {

                    MDC.put("serviceUUID", serviceUUID);

                }

                MDC.put("serviceName", server.getServiceName());

                MDC.put("hostname", server.getHostName());

                try {

                    final InetAddress clientAddr = ((ClientHost) ServerContext
                            .getServerContextElement(ClientHost.class))
                            .getClientHost();

                    MDC.put("clientname", clientAddr.getHostName());

                } catch (ServerNotActiveException e) {

                    /*
                     * This exception gets thrown if the client has made a
                     * direct (vs RMI) call so we just ignore it.
                     */

                }

            } catch (Throwable t) {

                /*
                 * Ignore.
                 */

            }

        }

        /**
         * Clear the logging context.
         */
        protected void clearLoggingContext() {
            
            MDC.remove("serviceName");

            MDC.remove("serviceUUID");

            MDC.remove("hostname");
            
            MDC.remove("clientname");

        }

        /*
         * DestroyAdmin
         */

        @Override
        public void destroy() {

            server.runShutdown(true/* destroy */);

        }

        @Override
        public void shutdown() {

            server.runShutdown(false/* destroy */);

        }

        @Override
        public void shutdownNow() {

            server.runShutdown(false/* destroy */);

        }

//        /**
//         * Extends the base behavior to return a {@link Name} of the service
//         * from the {@link Configuration}. If no name was specified in the
//         * {@link Configuration} then the value returned by the base class is
//         * returned instead.
//         */
//        @Override
//        public String getServiceName() {
//
//            String s = server.getServiceName();
//
//            if (s == null)
//                s = super.getServiceName();
//
//            return s;
//
//        }

        @Override
        public int getNSSPort() {

            final String COMPONENT = NSSConfigurationOptions.COMPONENT;

            try {

                final Integer port = (Integer) server.config.getEntry(
                        COMPONENT, NSSConfigurationOptions.PORT, Integer.TYPE,
                        NSSConfigurationOptions.DEFAULT_PORT);

                return port;

            } catch (ConfigurationException e) {

                throw new RuntimeException(e);
                
            }

        }

        @Override
        public RunState getRunState() {
            
            return server.getRunState();
            
        }
        
    }

}
