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
 * Created on Jan 16, 2009
 */

package com.bigdata.service.jini.master;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.bigdata.counters.CounterSet;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.BigdataZooDefs;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.AbstractScaleOutFederation;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.IBigdataFederation;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.IClientService;
import com.bigdata.service.IDataServiceCallable;
//BTM - PRE_FRED_3481 import com.bigdata.service.IMetadataService;
//BTM - PRE_FRED_3481 import com.bigdata.service.IRemoteExecutor;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.util.DumpFederation.ScheduledDumpTask;
//BTM - PRE_FRED_3481 import com.bigdata.service.ndx.pipeline.KVOLatch;
import com.bigdata.util.concurrent.ExecutionExceptions;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZLockImpl;
import com.bigdata.zookeeper.ZooHelper;

//BTM - PRE_FRED_3481
import com.bigdata.service.CallableExecutor;
import com.bigdata.service.IClientServiceCallable;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.jini.IJiniDiscoveryManagement;
import com.bigdata.journal.IScaleOutIndexManager;
import com.bigdata.service.ForceOverflowTask;
import com.bigdata.service.PurgeResourcesTask;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import org.apache.zookeeper.data.ACL;
import java.util.ArrayList;
import java.util.concurrent.ScheduledFuture;

/**
 * Utility class that can be used to execute a distributed job. The master
 * creates a set of tasks, submits each task to a shard service for
 * execution, and awaits their {@link Future}s. There are a variety of
 * {@link ConfigurationOptions}. In order to execute a master, you specify a
 * concrete instance of this class and {@link ConfigurationOptions} using the
 * fully qualified class name of that master implementation class. You specify
 * the client task using {@link TaskMaster#newClientTask(int)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 *            The generic type of the {@link JobState}.
 * @param <T>
 *            The generic type of the client task.
 * @param <U>
 *            The generic type of the value returned by the client task.
 * 
 * @todo could refactor the task to a task sequence easily enough, perhaps using
 *       some of the rule step logic. That would be an interesting twist on a
 *       parallel datalog.
 */
//BTM - PRE_FRED_3481 abstract public class TaskMaster<S extends TaskMaster.JobState, T extends Callable<U>, U>
//BTM - PRE_FRED_3481        implements Callable<Void> {
abstract public class TaskMaster<S extends TaskMaster.JobState,
                                 T extends IClientServiceCallable<U>, U>
        implements Callable<Void> {

    final private static Logger log = Logger.getLogger(TaskMaster.class);

//BTM - FOR_CLIENT_SERVICE - BEGIN
    protected IScaleOutIndexManager scaleOutIndexManager;
    protected ILocalResourceManagement localResourceManager;
    protected IJiniDiscoveryManagement discoveryManager;
    protected Configuration config;

    private ZooKeeperAccessor zookeeperAccessor;
    private List<ACL> zookeeperAcl;
    private String zookeeperRoot;
//BTM - FOR_CLIENT_SERVICE - END

    /**
     * {@link Configuration} options for the {@link TaskMaster} and derived
     * classes. The "component" for these options is the name of the concrete
     * master class to be executed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

		/**
		 * When <code>true</code> as an after action on the job, the
		 * shard services in the federation will be made to undergo
		 * asynchronous overflow processing, a compacting merge will be
		 * requested for all shards, and the live journals will be truncated so
		 * that the total size on disk of the federation is at its minimum
		 * footprint for the given history retention policy (default
		 * <code>false</code>). The master will block during this operation so
		 * you can readily tell when it is finished. Note that this option only
		 * makes sense in benchmark environments where you can control the total
		 * system otherwise asynchronous writes may continue.
		 * 
		 * @see AbstractScaleOutFederation#forceOverflow(boolean, boolean)
		 */
        String FORCE_OVERFLOW = "forceOverflow";

        /**
         * The path to the directory in where {@link ScheduledDumpTask}s will
         * write metadata about the state, size, and other aspects of the index
         * partitions throughout the run (optional).
         * 
         * @see #INDEX_DUMP_NAMESPACE
         */
        String INDEX_DUMP_DIR = "indexDumpDir";

        /**
         * The namespace to be used for the {@link ScheduledDumpTask}s
         * (optional).
         * 
         * @see #INDEX_DUMP_DIR
         */
        String INDEX_DUMP_NAMESPACE = "indexDumpNamespace";

        /**
         * Boolean option may be used to delete the exiting job with the same
         * name during startup (default <code>false</code>). This can be used
         * if the last job terminated abnormally and you want to re-run the job.
         */
        String DELETE_JOB = "deleteJob";

        /**
         * The #of clients to start. The clients will be distributed across the
         * discovered callable executor services in the federation matching the
         * {@link #CLIENTS_TEMPLATE}.
         */
        String NCLIENTS = "nclients";

        /**
         * An integer describing minimum #of client services to which
         * the clients will be submitted for execution.
         */
        String CLIENT_SERVICE_COUNT = "clientServiceCount";

        /**
         * An array of zero or more {@link ServicesTemplate} describing the
         * types of services, and the minimum #of services of each type, that
         * must be discovered before the job may begin.
         */
        String SERVICES_TEMPLATES = "servicesTemplates";

        /**
         * The timeout in milliseconds to await the discovery of the various
         * services described by the {@link #SERVICES_TEMPLATES} and
         * {@link #CLIENTS_TEMPLATE}.
         */
        String SERVICES_DISCOVERY_TIMEOUT = "awaitServicesTimeout";

        /**
         * The job name is used to identify the job within zookeeper. A znode
         * with this name will be created as follows:
         * 
         * <pre>
         * zroot (of the federation)
         *    / jobs
         *      / TaskMaster (fully qualified name of the concrete master class).
         *        / jobName
         * </pre>
         * 
         * If the client will store state in zookeeper or use {@link ZLock}s,
         * it must create a znode under the jobName whose name is the assigned
         * <em>client#</em>. This znode may be used by the client to store
         * its state in zookeeper. The client may also create {@link ZLock}s
         * which are children of this znode.
         * 
         * <pre>
         *          / client# (where # is the client#; the data of this znode is typically the client's state).
         *            / locknode (used to elect the client that is running if there is contention).
         *            / ...
         * </pre>
         * 
         * @see JobState#getClientZPath(JiniFederation, int)
         */
        String JOB_NAME = "jobName";

    }

    /**
     * State describing the job to be executed. The various properties are all
     * defined by {@link ConfigurationOptions}.
     */
    public static class JobState implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -340273551639560974L;

        /**
         * Set to <code>true</code> iff a pre-existing instance of the same
         * job should be delete before starting this job.
         * 
         * @see ConfigurationOptions#DELETE_JOB
         */
        private transient final boolean deleteJob;

        /**
         * Set <code>true</code> iff an existing job is being resumed
         * (defaults to <code>false</code> until proven otherwise).
         */
        private boolean resumedJob = false;

        /**
         * Return <code>true</code> iff an existing job is being resumed.
         */
        public boolean isResumedJob() {

            return resumedJob;

        }

        /**
         * The time at which the job started to execute and 0L if the job has
         * not started to execute.
         */
        private transient long beginMillis = 0L;

        /**
         * The time at which the job was done executing and 0L if the job has
         * not finished executing.
         */
        private transient long endMillis = 0L;

        /**
         * Elapsed run time for the job in milliseconds. This is ZERO (0L) until
         * the job starts. Once the job is done executing the elapsed time will
         * no longer increase.
         */
        public long getElapsedMillis() {

            if (beginMillis == 0L)
                return 0L;

            if (endMillis == 0L) {

                return System.currentTimeMillis() - beginMillis;

            }

            return endMillis - beginMillis;

        }

        /**
         * A map giving the {@link Future} for each client. The keys of the map
         * are the client numbers in [0:N-1].
         * 
         * @see #startClients()
         * @see #awaitAll(Map)
         * @see #cancelAll(Map, boolean)
         */
        protected transient Map<Integer/* client# */, Future<?/* U */>> futures;

        /*
         * Public options and configuration information.
         */

        /**
         * The name <em>component</em> in the jini {@link Configuration} whose
         * values will be used to configure the {@link JobState}. This defaults
         * to the name of the concrete {@link TaskMaster} instance. You may
         * override this value using <code>-Dbigdata.component=foo</code> on
         * the command line. This makes it possible to have multiple
         * parameterizations for the same master class in a single
         * {@link Configuration} file.
         */
        public final String component;

        /**
         * The job name.
         * 
         * @see ConfigurationOptions#JOB_NAME
         */
        public final String jobName;

        /**
         * The #of client tasks.
         * 
         * @see ConfigurationOptions#NCLIENTS
         */
        public final int nclients;

        /**
         * The minimum number of client services to which the clients will
         * be distributed for remote execution.
         * 
         * @see ConfigurationOptions#CLIENTS_TEMPLATE
         */
        public final int clientServiceCount;

        /**
         * An array of zero or more {@link ServicesTemplate} describing the
         * types of services, and the minimum #of services of each type, that
         * must be discovered before the job may begin.
         * 
         * @see ConfigurationOptions#SERVICES_TEMPLATES
         */
        public final ServicesTemplate[] servicesTemplates;

        /**
         * @see ConfigurationOptions#AWAIT_SERVICES_TIMEOUT}
         */
        public final long servicesDiscoveryTimeout;

        /*
         * Debugging and benchmarking options.
         */

        /**
         * <code>true</code> iff overflow will be forced on the data services
         * after the client tasks are done.
         * 
         * @see ConfigurationOptions#FORCE_OVERFLOW
         */
        public final boolean forceOverflow;

        /**
         * The directory into which scheduled dumps of the index partition
         * metadata will be written by a {@link ScheduledDumpTask} (optional).
         * 
         * @see ConfigurationOptions#INDEX_DUMP_DIR
         */
        public final File indexDumpDir;

        /**
         * The namespace to be used for the scheduled dumps of the index
         * partition metadata (optional).
         * 
         * @see ConfigurationOptions#INDEX_DUMP_NAMESPACE
         */
        public final String indexDumpNamespace;

        /**
         * Allows extension of {@link #toString()}
         * 
         * @param sb
         */
        protected void toString(StringBuilder sb) {

        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getName());

            sb.append("{ resumedJob=" + isResumedJob());

            /*
             * General options.
             */

            sb.append(", component=" + component);

            sb.append(", " + ConfigurationOptions.JOB_NAME + "=" + jobName);

            sb.append(", " + ConfigurationOptions.NCLIENTS + "=" + nclients);

            sb.append(", " + ConfigurationOptions.CLIENT_SERVICE_COUNT + "="
                    + clientServiceCount);

            sb.append(", " + ConfigurationOptions.SERVICES_TEMPLATES + "="
                    + Arrays.toString(servicesTemplates));

            sb.append(", " + ConfigurationOptions.SERVICES_DISCOVERY_TIMEOUT
                    + "=" + servicesDiscoveryTimeout);

            /*
             * Debugging and benchmarking options.
             */

            sb.append(", " + ConfigurationOptions.FORCE_OVERFLOW + "="
                    + forceOverflow);

            /*
             * Run state stuff.
             */

            /*
             * Subclass's options.
             */
            toString(sb);

            sb.append("}");

            return sb.toString();

        }

        protected JobState(final String component, final Configuration config)
                throws ConfigurationException {

            if (component == null)
                throw new IllegalArgumentException();

            if (config == null)
                throw new IllegalArgumentException();

            this.component = component;

            /*
             * general options.
             */

            jobName = (String) config.getEntry(component,
                    ConfigurationOptions.JOB_NAME, String.class);

            deleteJob = (Boolean) config.getEntry(component,
                    ConfigurationOptions.DELETE_JOB, Boolean.TYPE,
                    Boolean.FALSE);

            nclients = (Integer) config.getEntry(component,
                    ConfigurationOptions.NCLIENTS, Integer.TYPE);

            clientServiceCount = (Integer) config.getEntry(component,
                    ConfigurationOptions.CLIENT_SERVICE_COUNT, Integer.TYPE);

            servicesTemplates = (ServicesTemplate[]) config.getEntry(component,
                    ConfigurationOptions.SERVICES_TEMPLATES,
                    ServicesTemplate[].class);

            servicesDiscoveryTimeout = (Long) config.getEntry(component,
                    ConfigurationOptions.SERVICES_DISCOVERY_TIMEOUT, Long.TYPE);

            /*
             * Benchmarking and debugging options.
             */

            forceOverflow = (Boolean) config.getEntry(component,
                    ConfigurationOptions.FORCE_OVERFLOW, Boolean.TYPE,
                    Boolean.FALSE);

            indexDumpDir = (File) config.getEntry(component,
                    ConfigurationOptions.INDEX_DUMP_DIR, File.class, null);

            indexDumpNamespace = (String) config.getEntry(component,
                    ConfigurationOptions.INDEX_DUMP_NAMESPACE, String.class,
                    null);

            /*
             * Client/service maps.
             */

            clientServiceMap = new ServiceMap(nclients);
        }

        /**
         * The mapping of clients onto the callable executor services on which
         * that client will execute.
         */
        final public ServiceMap clientServiceMap;

        /**
         * Return the zpath of the node for all jobs which are instances of the
         * configured master's class.
         * 
         * @see #component
         */
//BTM - PRE_CLIENT_SERVICE        final public String getJobClassZPath(final JiniFederation fed) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            return fed.getZooConfig().zroot + "/" + BigdataZooDefs.JOBS + "/" + component;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        }
        final public String getJobClassZPath(final String zookeeperRoot) {
            return zookeeperRoot + "/" + BigdataZooDefs.JOBS + "/" + component;
        }

        /**
         * Return the zpath to the znode which corresponds to the job which is
         * being executed. The data for this znode is this {@link JobState}.
         */
//BTM - PRE_CLIENT_SERVICE        final public String getJobZPath(final JiniFederation fed) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            return getJobClassZPath(fed) + "/" + jobName;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        }
        final public String getJobZPath(final String zookeeperRoot) {
            return getJobClassZPath(zookeeperRoot) + "/" + jobName;
        }

        /**
         * Return the zpath to the node which corresponds to the specified
         * client task. This znode is a direct child of the znode for the job.
         * The client is responsible for creating this zpath if they wish to
         * store state in zookeeper. Any {@link ZLock}s used by the client and
         * scoped to its work should be created as children of this zpath.
         * 
         * @param clientNum
         *            The client number.
         * 
         * @see ConfigurationOptions#JOB_NAME
         */
//BTM - PRE_CLIENT_SERVICE        final public String getClientZPath(final JiniFederation fed, final int clientNum) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            return getJobZPath(fed) + "/" + "client" + clientNum;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        }
        final public String getClientZPath(final String zookeeperRoot,
                                           final int clientNum)
        {
            return getJobZPath(zookeeperRoot) + "/" + "client" + clientNum;
        }

        /**
         * Return the zpath of the locknode for the specified client task. Any
         * tasks running with that clientNum MUST contend for a {@link ZLock}
         * which permits it to run the task. This prevents concurrent execution
         * of the task for the specified client in the event that more than one
         * master is running for the same {@link JobState}.
         * 
         * @param clientNum
         *            The client number.
         */
//BTM - PRE_CLIENT_SERVICE        final public String getLockNodeZPath(final JiniFederation fed, final int clientNum) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            return getClientZPath(fed, clientNum) + "/" + "locknode";
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        }
        final public String getLockNodeZPath(final String zookeeperRoot,
                                             final int clientNum)
        {
            return getClientZPath(zookeeperRoot, clientNum) + "/" + "locknode";
        }

    }

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE     /**
//BTM - PRE_CLIENT_SERVICE      * The federation (from the ctor).
//BTM - PRE_CLIENT_SERVICE      */
//BTM - PRE_CLIENT_SERVICE     protected final JiniFederation<?> fed;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     /**
//BTM - PRE_CLIENT_SERVICE      * The federation (from the ctor).
//BTM - PRE_CLIENT_SERVICE      */
//BTM - PRE_CLIENT_SERVICE     public JiniFederation<?> getFederation() {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         return fed;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     }
    public IScaleOutIndexManager getScaleOutIndexManager() {
        return scaleOutIndexManager;
    }
//BTM - PRE_CLIENT_SERVICE - END

    /**
     * The {@link JobState} which is either set from the {@link Configuration}
     * (new job) or read from zookeeper (existing job) and thereafter
     * unchanging.
     */
    public S getJobState() {

        return jobState;

    }

    private S jobState;

    /**
     * Runs the master. SIGTERM (normal kill or ^C) will cancel the job,
     * including any running clients.
     * 
     * @return The {@link Future} for the master. Use {@link Future#get()} to
     *         await the outcome of the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    final protected Future<Void> innerMain() {

//BTM - PRE_CLIENT_SERVICE         final Future<Void> future = fed.getExecutorService().submit(this);
        final Future<Void> future = localResourceManager.getThreadPool().submit(this);

        /*
         * Install a shutdown hook so that the master will cancel any running
         * clients if it is interrupted (normal kill will trigger this hook, but
         * it is also triggered for a normal exit from main()).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {

                future.cancel(true/* mayInterruptIfRunning */);

                System.err.println("Shutting down: " + new Date());

            }

        });

        return future;

    }

    /**
     * 
     * @param fed
     * 
     * @throws ConfigurationException
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    protected TaskMaster(final JiniFederation<?> fed)
//BTM - PRE_CLIENT_SERVICE            throws ConfigurationException {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (fed == null)
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        this.fed = fed;
    protected TaskMaster(final IScaleOutIndexManager scaleOutIndexManager,
                         final IJiniDiscoveryManagement discoveryManager,
                         final ILocalResourceManagement localResourceManager,
                         final ZooKeeperAccessor zookeeperAccessor,
                         final List<ACL> zookeeperAcl,
                         final String zookeeperRoot,
                         final Configuration config)
            throws ConfigurationException
    {
        if (scaleOutIndexManager == null) {
            throw new NullPointerException("null scaleOutIndexManager");
        }
        if (discoveryManager == null) {
            throw new NullPointerException("null discoveryManager");
        }
        if (localResourceManager == null) {
            throw new NullPointerException("null localResourceManager");
        }
        if (zookeeperAccessor == null) {
            throw new NullPointerException("null zookeeperAccessor");
        }
        if (config == null) {
            throw new NullPointerException("null config");
        }

        this.scaleOutIndexManager = scaleOutIndexManager;
        this.discoveryManager = discoveryManager;
        this.localResourceManager = localResourceManager;
        this.zookeeperAccessor = zookeeperAccessor;
        this.zookeeperAcl = zookeeperAcl;
        this.zookeeperRoot = zookeeperRoot;
        this.config = config;
//BTM - PRE_CLIENT_SERVICE - END

        /*
         * Use the name of the concrete instance of this class by default but
         * permit override of the component name on the command line.
         */
        final String component = System.getProperty("bigdata.component",
                getClass().getName());

        // The jini configuration specified on the command line.
//BTM - PRE_CLIENT_SERVICE        final Configuration config = fed.getClient().getConfiguration();

        // Initialize the job state.
        jobState = newJobState(component, config);

    }

    /**
     * Execute the master. If the master is interrupted, including by the signal
     * handler installed by {@link #innerMain()}, then the client tasks will be
     * cancelled. A simple <code>main()</code> can be written as follows:
     * 
     * <pre>
     * public static void main(final String[] args) {
     * 
     *     final JiniFederation fed = new JiniClient(args).connect();
     * 
     *     try {
     * 
     *         final TaskMaster task = new MyMaster(fed);
     * 
     *         task.execute();
     *         
     *     } finally {
     *     
     *         fed.shutdown();
     *         
     *     }
     * 
     * }
     * </pre>
     * 
     * Where <code>MyMaster</code> is a concrete subclass of
     * {@link TaskMaster}.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void execute() throws InterruptedException, ExecutionException {

        // execute master wait for it to finish.
        try {

            innerMain().get();

        } finally {

            // always write the date when the master terminates.
            System.err.println("Done: " + new Date());

        }

    }

    /**
     * Wait a bit to discover some minimum #of data services. Then allocate the
     * clients to the data services. There can be more than one per data
     * service.
     * 
     * @return <code>null</code>
     * 
     * @see #execute()
     * 
     * @todo In my experience zookeeper (at least 3.0.1 and 3.1.0) has a
     *       tendency to drop sessions for the java client when under even
     *       moderate swapping. Because of this I am not verifying that the
     *       {@link TaskMaster} retains the {@link ZLock} for the job throughout
     *       its run. Doing so at this point is just begging for an aborted run.
     */
    final public Void call() throws Exception {

        /*
         * Setup the jobState.
         * 
         * Note: [jobState] will be replaced as a side-effect if there is an
         * existing instance of the job in zookeeper (same component and
         * jobName).
         */
        final ZLock zlock = setupJob();

        try {

            // note: take timestamp after discovering services!
            jobState.beginMillis = System.currentTimeMillis();

            // callback for overrides.
            beginJob(getJobState());

            try {
                
                // run the clients and wait for them to complete.
                runJob();

            } catch (CancellationException ex) {

                error(jobState, ex);
                
                throw ex;

            } catch (InterruptedException ex) {

                error(jobState, ex);
                
                throw ex;

            } catch (ExecutionException ex) {

                error(jobState, ex);
                
                throw ex;
        
            }
        
            success(jobState);

        } finally {

            tearDownJob(jobState, zlock);

        }

        if (jobState.forceOverflow) {

            forceOverflow();

        }

        return null;

    }

    /**
     * Start the client tasks and await their futures.
     * 
     * @throws Exception
     *             Client execution problem.
     * @throws InterruptedException
     *             Master interrupted awaiting clients.
     */
    protected void runJob() throws Exception, InterruptedException {

        final long begin = System.currentTimeMillis();

        // unless successful.
        boolean failure = true;

        try {

            startClients();

            awaitAll();

            failure = false;

        } finally {

            final long elapsed = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled())
                log.info("Done: " + (failure ? "failure" : "success")
                        + ", elapsed=" + elapsed);

        }

    }

    /**
     * Distributes the clients to the services on which they will execute and
     * returns a map containing their {@link Future}s. The kind of service on
     * which the clients are run is determined by
     * {@link JobState#clientsTemplate} but must implement
     * {@link CallableExecutor}. Clients are assigned to the services using a
     * stable ordered assignment {@link JobState#clientServiceUUIDs}. If there
     * are more clients than services, then some services will be tasked with
     * more than one client. If there is a problem submitting the clients then
     * any clients already submitted will be canceled and the original exception
     * will be thrown out of this method.
     * 
     * @throws IOException
     *             If there is an RMI problem submitting the clients to the
     *             {@link CallableExecutor} services.
     * @throws ConfigurationException
     * 
     * @see {@link JobState#futures}
     */
    protected void startClients() throws IOException {

        if (log.isInfoEnabled())
            log.info("Will run " + jobState.nclients);

        jobState.futures = new LinkedHashMap<Integer, Future<?/* U */>>(
                jobState.nclients/* initialCapacity */);

        // #of clients that were started successfully.
        int nstarted = 0;
        try {

            for (int clientNum = 0; clientNum < jobState.nclients; clientNum++) {

//BTM - PRE_CLIENT_SERVICE  final IClientService service = jobState.clientServiceMap.getService(clientNum);
                final CallableExecutor service =
                    jobState.clientServiceMap.getService(clientNum);

                final IClientServiceCallable<U> clientTask = newClientTask(clientNum);

                if (log.isInfoEnabled())
                    log.info("Running client#=" + clientNum + " on " + service);

                jobState.futures.put(clientNum, (Future<U>) service
                        .submit(clientTask));

                nstarted++;

            } // start the next client.

        } finally {

            if (nstarted < jobState.nclients) {

                log.error("Aborting : could not start client(s): nstarted="
                        + nstarted + ", nclients=" + jobState.nclients);

                cancelAll(true/* mayInterruptIfRunning */);

            }

        }

    }

    /**
     * Await the completion of the {@link Future}. If any client fails then the
     * remaining clients will be canceled.
     * 
     * @throws IllegalStateException
     *             if {@link JobState#futures} is <code>null</code>.
     * @throws ExecutionException
     *             for the first client whose failure is noticed.
     * @throws InterruptedException
     *             if the master is interrupted while awaiting the
     *             {@link Future}s.
     */
    protected void awaitAll() throws ExecutionException, InterruptedException {

        try {

            while (!allDone()) {

                final int nremaining = jobState.futures.size();

                if (log.isDebugEnabled())
                    log.debug("#remaining futures=" + nremaining);

                if (nremaining < 10)
                    // sleep a bit before rechecking the futures.
                    Thread.sleep(1000/* ms */);
                else
                    // sleep longer if there are more clients.
                    Thread.sleep(10000/* ms */);

            }

        } catch (InterruptedException t) {

            /*
             * Cancel all futures on error.
             */

            log.error("Cancelling job: cause=" + t);

            try {

                cancelAll(true/* mayInterruptIfRunning */);

            } catch (Throwable t2) {

                log.error(t2);

            }

            throw new RuntimeException(t);

        } catch (ExecutionException t) {

            /*
             * Cancel all futures on error.
             */

            log.error("Cancelling job: cause=" + t);

            try {

                cancelAll(true/* mayInterruptIfRunning */);

            } catch (Throwable t2) {

                log.error(t2);

            }

            throw new RuntimeException(t);

        }

    }

    /**
     * Callback invoked when the job is done executing (normal completion) but
     * is still holding the {@link ZLock} for the {@link JobState}. The default
     * implementation destroys the znodes for the job since it is done
     * executing.
     * 
     * @throws Exception
     */
    protected void success(final S jobState) throws Exception {

        // timestamp after the job is done.
        jobState.endMillis = System.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Clients done: elapsed=" + jobState.getElapsedMillis());

        /*
         * This is the commit point corresponding to the end of the job.
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        System.out.println("commit point: "
//BTM - PRE_CLIENT_SERVICE                + getFederation().getLastCommitTime());
        System.out.println("commit point: "
                + scaleOutIndexManager.getLastCommitTime());
//BTM - PRE_CLIENT_SERVICE - END
        /*
         * Delete zookeeper state when the job completes successfully.
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        ZooHelper.destroyZNodes(fed.getZookeeperAccessor().getZookeeper(),
//BTM - PRE_CLIENT_SERVICE                jobState.getJobZPath(fed), 0/* depth */);
        ZooHelper.destroyZNodes(zookeeperAccessor.getZookeeper(),
                                jobState.getJobZPath(zookeeperRoot),
                                0);//depth
//BTM - PRE_CLIENT_SERVICE - END
    }

    /**
     * Callback invoked if an exception is thrown during the job execution. The
     * {@link JobState#endMillis} is set by this method, the client tasks are
     * canceled, and an error message is logged. By default, the znode for the
     * job is not destroyed. You can destroy a terminated job using zookeeper or
     * automatically destroy a pre-existing job when re-submitting the job using
     * {@link ConfigurationOptions#DELETE_JOB}.
     * <p>
     * Note: This method should not throw anything since that could cause the
     * root cause of the job error to be masked.
     * 
     * @param jobState
     *            The {@link JobState}.
     * @param t
     *            The exception.
     */
    protected void error(final S jobState, final Throwable t) {

        /*
         * Defensive implementation designed to be safe(r) if there is a log
         * configuration issue, etc.
         */
        
        try {

            // timestamp after the job is done.
            jobState.endMillis = System.currentTimeMillis();

            log.error("Abort: elapsed=" + jobState.getElapsedMillis()
                    + " : cause=" + t, t);
            
        } finally {

            try {

                // cancel any running clients.
                cancelAll(true/* mayInterruptIfRunning */);

            } catch (Throwable t2) {

                t2.printStackTrace(System.err);

            }

        }

    }
    
    /**
     * Callback invoked when the job is done executing (any completion) but has
     * not yet release the {@link ZLock} for the {@link JobState}. The default
     * releases the {@link ZLock}. It may be extended to handle other cleanup.
     * 
     * @throws Exception
     */
    protected void tearDownJob(final S jobState, final ZLock zlock)
            throws Exception {

        zlock.unlock();

    }

    /**
     * Return a {@link JobState}.
     * 
     * @param component
     *            The component.
     * @param config
     *            The configuration.
     * 
     * @return The {@link JobState}.
     */
    abstract protected S newJobState(String component, Configuration config)
            throws ConfigurationException;

    /**
     * Return a client to be executed on a remote shard service. The client can
     * obtain access to the {@link IBigdataFederation} when it executes on the
     * remote shard service if it implements {@link IDataServiceCallable}. You
     * can use {@link AbstractClientTask} as a starting point.
     * 
     * @param clientNum
     *            The client number.
     * 
     * @return The client task.
     * 
     * @see AbstractClientTask
     */
    abstract protected T newClientTask(final int clientNum);

    /**
     * Callback invoked when the job is ready to execute and is holding the
     * {@link ZLock} for the {@link JobState}. This may be extended to register
     * indices, etc. The default implementation handles the setup of the
     * optional index partition metadata dumps.
     * 
     * @throws Exception
     * 
     * @see ConfigurationOptions#INDEX_DUMP_DIR
     * @see ConfigurationOptions#INDEX_DUMP_NAMESPACE
     */
    protected void beginJob(final S jobState) throws Exception {

        if (jobState.indexDumpDir != null) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            // runs @t0, 1m, 2m, ... 9m.
//BTM - PRE_CLIENT_SERVICE            fed.addScheduledTask(new ScheduledDumpTask(fed,
//BTM - PRE_CLIENT_SERVICE                    jobState.indexDumpNamespace, 10/* nruns */,
//BTM - PRE_CLIENT_SERVICE                    jobState.indexDumpDir, "indexDump", TimeUnit.MINUTES),
//BTM - PRE_CLIENT_SERVICE                    0/* initialDelay */, 1/* delay */, TimeUnit.MINUTES);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            // runs @t10m, 20m, 30m, ... 50m.
//BTM - PRE_CLIENT_SERVICE            fed.addScheduledTask(new ScheduledDumpTask(fed,
//BTM - PRE_CLIENT_SERVICE                    jobState.indexDumpNamespace, 5/* nruns */,
//BTM - PRE_CLIENT_SERVICE                    jobState.indexDumpDir, "indexDump", TimeUnit.MINUTES),
//BTM - PRE_CLIENT_SERVICE                    10/* initialDelay */, 10/* delay */, TimeUnit.MINUTES);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            // runs @t1h, 2h, ... until cancelled.
//BTM - PRE_CLIENT_SERVICE            fed.addScheduledTask(new ScheduledDumpTask(fed,
//BTM - PRE_CLIENT_SERVICE                    jobState.indexDumpNamespace, Integer.MAX_VALUE/* nruns */,
//BTM - PRE_CLIENT_SERVICE                    jobState.indexDumpDir, "indexDump", TimeUnit.MINUTES),
//BTM - PRE_CLIENT_SERVICE                    1/* initialDelay */, 1/* delay */, TimeUnit.HOURS);
            // runs @t0, 1m, 2m, ... 9m.
            this.addScheduledTask
                (new ScheduledDumpTask(scaleOutIndexManager,
                                       discoveryManager,
                                       localResourceManager,
                                       jobState.indexDumpNamespace,
                                       10, //nruns
                                       jobState.indexDumpDir,
                                       "indexDump",
                                       TimeUnit.MINUTES),
                 0, //initialDelay
                 1, //period
                 TimeUnit.MINUTES);

            // runs @t10m, 20m, 30m, ... 50m.
            this.addScheduledTask
                (new ScheduledDumpTask(scaleOutIndexManager,
                                       discoveryManager,
                                       localResourceManager,
                                       jobState.indexDumpNamespace,
                                       5, //nruns
                                       jobState.indexDumpDir,
                                       "indexDump",
                                       TimeUnit.MINUTES),
                 10, //initialDelay
                 10, //period
                 TimeUnit.MINUTES);

            // runs @t1h, 2h, ... until cancelled.
            this.addScheduledTask
                (new ScheduledDumpTask(scaleOutIndexManager,
                                       discoveryManager,
                                       localResourceManager,
                                       jobState.indexDumpNamespace,
                                       Integer.MAX_VALUE, //nruns
                                       jobState.indexDumpDir,
                                       "indexDump",
                                       TimeUnit.MINUTES),
                 1,//initialDelay
                 1,//period
                 TimeUnit.HOURS);
//BTM - PRE_CLIENT_SERVICE - END
        }
    }

    /**
     * Sets up the {@link JobState} in zookeeper, including the assignment of
     * service {@link UUID}s to each client. {@link #jobState} will be replaced
     * with the {@link JobState} read from zookeeper if a pre-existing job is
     * found in zookeeper.
     * 
     * @return The global lock for the master running the job.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    protected ZLock setupJob() throws KeeperException, InterruptedException,
            TimeoutException {

//BTM - PRE_CLIENT_SERVICE        final ZooKeeper zookeeper = fed.getZookeeperAccessor().getZookeeper();
        final ZooKeeper zookeeper = zookeeperAccessor.getZookeeper();

        try {
            // ensure znode exists.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            zookeeper.create(fed.getZooConfig().zroot + "/"
//BTM - PRE_CLIENT_SERVICE                    + BigdataZooDefs.JOBS, new byte[0], fed.getZooConfig().acl,
//BTM - PRE_CLIENT_SERVICE                    CreateMode.PERSISTENT);
            zookeeper.create(zookeeperRoot + "/" + BigdataZooDefs.JOBS,
                             new byte[0],
                             zookeeperAcl,
                             CreateMode.PERSISTENT);
//BTM - PRE_CLIENT_SERVICE - END
        } catch (NodeExistsException ex) {
            // ignore.
        }

//BTM - PRE_CLIENT_SERVICE  final String jobClassZPath = jobState.getJobClassZPath(fed);
        final String jobClassZPath = jobState.getJobClassZPath(zookeeperRoot);

        try {
            // ensure znode exists.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            zookeeper.create(jobClassZPath, new byte[0],
//BTM - PRE_CLIENT_SERVICE                    fed.getZooConfig().acl, CreateMode.PERSISTENT);
            zookeeper.create(jobClassZPath,
                             new byte[0],
                             zookeeperAcl,
                             CreateMode.PERSISTENT);
//BTM - PRE_CLIENT_SERVICE - END

        } catch (NodeExistsException ex) {
            // ignore.
        }

        /*
         * Use a global lock to protect the job.
         * 
         * Note: We just created parent of this lock node (or at any rate,
         * ensured that it exists).
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        final ZLock zlock = ZLockImpl.getLock(zookeeper, jobClassZPath + "/"
//BTM - PRE_CLIENT_SERVICE                + "locknode_" + jobState.jobName, fed.getZooConfig().acl);
        final ZLock zlock =
              ZLockImpl.getLock
                  (zookeeper,
                   jobClassZPath + "/" + "locknode_" + jobState.jobName,
                   zookeeperAcl);
//BTM - PRE_CLIENT_SERVICE - END

        zlock.lock();
        try {

//BTM - PRE_CLIENT_SERVICE  final String jobZPath = jobState.getJobZPath(fed);
            final String jobZPath = jobState.getJobZPath(zookeeperRoot);

            if (jobState.deleteJob
                    && zookeeper.exists(jobZPath, false/* watch */) != null) {

                /*
                 * Delete old job.
                 */

                log.warn("Deleting old job: " + jobZPath);

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE                ZooHelper.destroyZNodes(fed.getZookeeperAccessor()
//BTM - PRE_CLIENT_SERVICE                        .getZookeeper(), jobZPath, 0/* depth */);
                ZooHelper.destroyZNodes(zookeeperAccessor.getZookeeper(),
                                        jobZPath,
                                        0);//depth
//BTM - PRE_CLIENT_SERVICE - END

                // detach the performance counters for the old job.
                detachPerformanceCounters();
                
            }

            try {

                // create znode that is the root for the job.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE                zookeeper.create(jobZPath, SerializerUtil.serialize(jobState),
//BTM - PRE_CLIENT_SERVICE                        fed.getZooConfig().acl, CreateMode.PERSISTENT);
                zookeeper.create(jobZPath,
                                 SerializerUtil.serialize(jobState),
                                 zookeeperAcl,
                                 CreateMode.PERSISTENT);
//BTM - PRE_CLIENT_SERVICE - END

                if (log.isInfoEnabled())
                    log.info("New job: " + jobState);

                try {

                    /*
                     * Assign clients to services.
                     */
                    final ServiceItem[] discoveredServices =
                            new DiscoverServicesWithPreconditionsTask().call();

                    jobState.clientServiceMap
                            .assignClientsToServices(discoveredServices);

                    // write those assignments into zookeeper.
                    zookeeper.setData(jobZPath, SerializerUtil
                            .serialize(jobState), -1/* version */);

                    if (log.isInfoEnabled())
                        log.info("Wrote client assignments into zookeeper.");

                } catch (Throwable t) {

                    /*
                     * Since we created the jobState znode, delete the jobState
                     * while we are still holding the zlock.
                     */
                    try {
                        zookeeper.delete(jobZPath, -1/* version */);
                    } catch (Throwable t2) {
                        log.error(t2);
                    }

                    throw new RuntimeException(t);

                }

            } catch (NodeExistsException ex) {

                /*
                 * Resuming a job already in progress and/or providing backup
                 * clients for a job that is currently running.
                 * 
                 * Note: We use the client to data service UUID assignments read
                 * from the znode data which are part of the jobState
                 * 
                 * @todo stable assignments are only required when clients will
                 * read or write local data. This is not the common case and
                 * should be a declarative configuration option.
                 */

                jobState = (S) SerializerUtil.deserialize(zookeeper.getData(
                        jobZPath, false, new Stat()));

//BTM - PRE_CLIENT_SERVICE                jobState.clientServiceMap.resolveServiceUUIDs(fed);
                jobState.clientServiceMap.resolveServiceUUIDs
                                              (discoveryManager);

                jobState.resumedJob = true;

                log.warn("Pre-existing job: " + jobZPath);

            }

        } catch (KeeperException t) {

            zlock.unlock();

            throw t;

        } catch (InterruptedException t) {

            zlock.unlock();

            throw t;

        } catch (Throwable t) {

            zlock.unlock();

            throw new RuntimeException(t);

        }

        return zlock;

    }

    /**
     * Detach the performance counters for the job.
     * 
     * @todo does not remove the counters on the LBS, just in local memory so
     *       this is not much help. It would only be useful if we re-ran the
     *       same job within the same JVM instance.
     */
    protected void detachPerformanceCounters() {
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        getFederation().getServiceCounterSet().makePath("Jobs").detach(
//BTM - PRE_CLIENT_SERVICE                jobState.jobName);
        localResourceManager.getServiceCounterSet(true)
                            .makePath("Jobs").detach(jobState.jobName);
//BTM - PRE_CLIENT_SERVICE - END
    }
    
    /**
     * Attach to the counters reported by the client to the LBS.
     */
    protected void attachPerformanceCounters(final CounterSet counterSet) {

        if(counterSet == null) {
            
            throw new IllegalArgumentException();
            
        }
//BTM - PRE_CLIENT_SERVICE - BEGIN  
//BTM - PRE_CLIENT_SERVICE        getFederation().getServiceCounterSet().makePath("Jobs").makePath(
//BTM - PRE_CLIENT_SERVICE                getJobState().jobName).attach(counterSet, true/* replace */);
        localResourceManager.getServiceCounterSet(true)
                            .makePath("Jobs").makePath
                              (getJobState().jobName).attach
                                                      (counterSet,
                                                       true);//replace
//BTM - PRE_CLIENT_SERVICE - END

    }
    
    /**
     * Class awaits discovery of all services required by the {@link JobState}
     * up to the {@link JobState#servicesDiscoveryTimeout} and returns the
     * {@link ServiceItem}s for the services on which the clients should be
     * executed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class DiscoverServicesWithPreconditionsTask implements
            Callable<ServiceItem[]> {

        public DiscoverServicesWithPreconditionsTask() {

        }

        /**
         * Await discovery of the services described by
         * {@link JobState#servicesTemplates} and by
         * {@link JobState#clientsTemplate}.
         * 
         * @return An object reporting the discovered services which match the
         *         {@link JobState#clientsTemplate}.
         */
        public ServiceItem[] call() throws Exception {

            if (jobState == null)
                throw new IllegalArgumentException();

            if (jobState.servicesTemplates == null)
                throw new IllegalArgumentException();

            if (jobState.servicesDiscoveryTimeout <= 0)
                throw new IllegalArgumentException();

            /*
             * This is the task that will give us the services on which the
             * clients will execute.
             */
            ServicesTemplate clientsTemplate =
                new ServicesTemplate
                    (jobState.clientServiceCount, // minMatches
		     new ServiceTemplate
                        (null, //serviceID
//BTM - PRE_FRED_3481    new Class[]{ com.bigdata.service.IClientService.class },
                         new Class[]{ com.bigdata.service.CallableExecutor.class },
		         null),
                     null);

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            final Future<ServiceItem[]> discoverClientServicesFuture = fed
//BTM - PRE_CLIENT_SERVICE                    .getExecutorService().submit(
//BTM - PRE_CLIENT_SERVICE                            new DiscoverServices(fed, clientsTemplate,
//BTM - PRE_CLIENT_SERVICE                                    jobState.servicesDiscoveryTimeout));
            final Future<ServiceItem[]> discoverClientServicesFuture =
                  localResourceManager.getThreadPool().submit
                      ( new DiscoverServices
                                (discoveryManager.getServiceDiscoveryManager(),
                                 clientsTemplate,
                                 jobState.servicesDiscoveryTimeout) );
//BTM - PRE_CLIENT_SERVICE - END

            /*
             * Additional tasks for the other services which must be discovered
             * as pre-conditions before the job can execute.
             */
            final List<Callable<ServiceItem[]>> tasks = new LinkedList<Callable<ServiceItem[]>>();

            for (ServicesTemplate t : jobState.servicesTemplates) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE                tasks.add(new DiscoverServices(fed, t,
//BTM - PRE_CLIENT_SERVICE                        jobState.servicesDiscoveryTimeout));
                tasks.add
                    ( new DiscoverServices
                              (discoveryManager.getServiceDiscoveryManager(),
                               t,
                               jobState.servicesDiscoveryTimeout) );
//BTM - PRE_CLIENT_SERVICE - END

            }

            // submit all tasks in parallel.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            final Future<ServiceItem[]>[] futures = fed.getExecutorService()
//BTM - PRE_CLIENT_SERVICE                    .invokeAll(tasks).toArray(new Future[tasks.size()]);
            final Future<ServiceItem[]>[] futures =
                      localResourceManager.getThreadPool().invokeAll(tasks)
                                          .toArray(new Future[tasks.size()]);
//BTM - PRE_CLIENT_SERVICE - END

            // Assemble a list of errors.
            final List<Throwable> causes = new LinkedList<Throwable>();

            /*
             * Get the future, which gives the services on which we will execute
             * the clients.
             */
            final ServiceItem[] clientServiceItems = discoverClientServicesFuture
                    .get();

            if (clientServiceItems.length < jobState.clientServiceCount) {

                final String msg = "Not enough services to run clients: found="
                        + clientServiceItems.length + ", required="
                        + jobState.clientServiceCount;

                log.error(msg);

                causes.add(new RuntimeException(msg));

            }

            /*
             * Check the other pre-conditions for discovered services.
             */

            for (int i = 0; i < futures.length; i++) {

                final Future<ServiceItem[]> f = futures[i];

                final ServicesTemplate servicesTemplate = jobState.servicesTemplates[i];

                try {

                    final ServiceItem[] a = f.get();

                    if (a.length < servicesTemplate.minMatches) {

                        final String msg = "Not enough services: found="
                                + a.length + ", required="
                                + servicesTemplate.minMatches + ", template="
                                + servicesTemplate;

                        // log error w/ specific cause of rejected run.
                        log.error(msg);

                        // add msg to list of causes.
                        causes.add(new RuntimeException(msg));

                    }

                } catch (Throwable ex) {

                    // add thrown exception to list of causes.
                    causes.add(ex);

                }

            }

            if (!causes.isEmpty()) {

                throw new ExecutionExceptions(causes);

            }

            return clientServiceItems;

        }

    }

    /**
     * Check the futures.
     * 
     * @return <code>true</code> when no more tasks are running.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected boolean allDone() throws InterruptedException, ExecutionException {

        if (jobState.futures == null)
            throw new IllegalStateException();

        // Note: used to avoid concurrent modification of [futures].
        final List<Integer> finished = new LinkedList<Integer>();

        int nremaining = jobState.futures.size();

        for (Map.Entry<Integer, Future<?/* U */>> entry : jobState.futures
                .entrySet()) {

            final int clientNum = entry.getKey();

            final Future<?/* U */> future = entry.getValue();

            if (future.isDone()) {

                /*
                 * Note: test the client's future and halt if the client fails.
                 */
                final Object value = future.get();

                nremaining--;

                System.out.println("Done: " + new Date() + " : clientNum="
                        + clientNum + " of " + jobState.nclients + " with "
                        + nremaining + " remaining : result=" + value);

                try {
                    notifyOutcome(clientNum, (U) value);
                } catch (Throwable t) {
                    log.error("Ignoring thrown exception: " + t);
                }

                finished.add(clientNum);

            }

        }

        for (int clientNum : finished) {

            jobState.futures.remove(clientNum);

        }

        // finished iff no more futures.
        return jobState.futures.isEmpty();

    }

    /**
     * Cancel the futures.
     * 
     * @param futures
     *            The futures.
     * @param mayInterruptIfRunning
     *            If the tasks for the futures may be interrupted.
     */
    synchronized// since is invoked from execute() and runClients()
    protected void cancelAll(final boolean mayInterruptIfRunning) {

        if (jobState.futures == null) {

            /*
             * Note: This is ignored since it is possible that cancelAll() is
             * invoked from execute() before the client tasks have been assigned
             * their futures.
             */

            return;

        }

        log.warn("Cancelling all futures: nfutures=" + jobState.futures.size());

        final Iterator<Future<?/* U */>> itr = jobState.futures.values()
                .iterator();

        while (itr.hasNext()) {

            final Future<?/* U */> f = itr.next();

            if (!f.isDone()) {

                f.cancel(mayInterruptIfRunning);

            }

            itr.remove();

        }

    }

    /**
     * Force overflow on all discovered shard services.
     * 
     * @see ConfigurationOptions#FORCE_OVERFLOW
     * 
     * @todo This is an operation that we would like to run once by the master
     *       which actually executes the clients even if there are multiple
     *       masters (multiple master support is not really all there yet and
     *       there are interactions with how the client tasks handle multiple
     *       instances of themselves so this is all forward looking).
     */
    protected void forceOverflow() {

        System.out.println("Forcing overflow: now=" + new Date());

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE		fed.forceOverflow(true/* compactingMerge */, true/* truncateJournal */);
        forceOverflowAllDataServices(true,  //compactingMerge,
                                     true); //truncateJournal
//BTM - PRE_CLIENT_SERVICE - END

        System.out.println("Forced overflow: now=" + new Date());

    }

    /**
     * Callback for the master to consume the outcome of the client's
     * {@link Future} (default is NOP).
     * 
     * @param clientNum
     *            The client number.
     * @param value
     *            The value returned by the {@link Future}.
     */
    protected void notifyOutcome(final int clientNum, final U value) {

    }

//BTM - FOR_CLIENT_SERVICE - BEGIN - methods added by BTM ---------------------

    private ScheduledFuture addScheduledTask(Runnable task,
                                             long initialDelay,
                                             long delay,
                                             TimeUnit unit)
    {
        if (task == null) {
            throw new IllegalArgumentException("null task");
        }
        if (log.isInfoEnabled()) {
            log.info("Scheduling task: task=" + task.getClass()
                    + ", initialDelay=" + initialDelay + ", delay=" + delay
                    + ", unit=" + unit);
        }
        return (localResourceManager.getScheduledExecutor())
                                    .scheduleWithFixedDelay
                                         (task, initialDelay, delay, unit);
    }

//BTM - Adapted from AbstractScaleOutFederation

    /**
     * Force overflow of each data service in the scale-out federation (only
     * scale-out federations support overflow processing). This method is
     * synchronous. It will not return until all {@link ShardService}s have
     * initiated and completed overflow processing. Any unused resources (as
     * determined by the {@link StoreManager}) will have been purged.
     * <p>
     * This is a relatively fast operation when
     * <code>compactingMerge := false</code>. By specifying both
     * <code>compactingMerge := false</code> and
     * <code>truncateJournal := false</code> you can cause the data services to
     * close out their current journals against further writes. While this is
     * not a global synchronous operation, it can provide a basis to obtain a
     * "near synchronous" snapshot from the federation consisting of all writes
     * up to the point where overflow was triggered on each data service.
     * 
     * @param compactingMerge
     *            When <code>true</code>, each shard on each
     *            {@link ShardService} will undergo a compacting merge.
     *            Synchronous parallel compacting merge of all shards is an
     *            expensive operation. This parameter shoudl normally be
     *            <code>false</code> unless you are requesting a compacting
     *            merge for specific purposes, such as benchmarking when all
     *            data is known to exist in one {@link IndexSegment} per shard.
     * @param truncateJournal
     *            When <code>true</code>, the live journal will be truncated to
     *            its minimum extent (all writes will be preserved but there
     *            will be no free space left in the journal). This may be used
     *            to force the {@link ShardService} to its minimum possible
     *            footprint.
     * 
     * @todo when overflow processing is enabled for the shard locator service
     *       we will have to modify this to also trigger overflow for those
     *       services.
     */
    private void forceOverflowAllDataServices(boolean compactingMerge,
                                              boolean truncateJournal)
    {
        // find UUID for each data service.
        final UUID[] dataServiceUUIDs =
                     discoveryManager.getDataServiceUUIDs(Integer.MAX_VALUE);
        final int ndataServices = dataServiceUUIDs.length;

        log.warn("forcing overflow [# of dataServices="+ndataServices
                 +", now="+new Date()+"]");

        final List<Callable<Void>> tasks =
                  new ArrayList<Callable<Void>>(ndataServices);
        for (UUID serviceUUID : dataServiceUUIDs) {
            tasks.add(new ForceOverflowTask
                              (discoveryManager.getDataService(serviceUUID),
                               compactingMerge,
                               truncateJournal) );
        }

        if(truncateJournal) {
            // @todo
            //
            // The metadata service does not yet support overflow (it does
            // not support partitioned metadata indices) so it only has a live
            // journal. Therefore all that can be done until overflow
            // processing is enabled in the metadata service is to 
            // truncate the live journal for the metadata service.

            tasks.add( new PurgeResourcesTask
                               (discoveryManager.getMetadataService(),
                                truncateJournal) );
        }
        final List<Future<Void>> futures;
        try {
            futures = localResourceManager.getThreadPool().invokeAll(tasks);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        int nok = 0;
        for (Future f : futures) {
            try {
                f.get();
                nok++;
            } catch (InterruptedException ex) {
                log.warn(ex.getLocalizedMessage());
                continue;
            } catch (ExecutionException ex) {
                log.error(ex.getLocalizedMessage(), ex);
            }
        }
        log.warn("overflow complete [# of ok="+nok+", # of dataServices="
                 +ndataServices+",now="+new Date()+"]");
        if (nok != tasks.size()) {
            throw new RuntimeException
                          ("overflow processing failed [# of ok="+nok
                            +", # of tasks="+tasks.size()+"]");
        }
    }
//BTM - FOR_CLIENT_SERVICE - END ----------------------------------------------

}
