/**

 The Notice below must appear in each file of the Source Code of any
 copy you distribute of the Licensed Product.  Contributors to any
 Modifications may add their own copyright notices to identify their
 own contributions.

 License:

 The contents of this file are subject to the CognitiveWeb Open Source
 License Version 1.1 (the License).  You may not copy or use this file,
 in either source code or executable form, except in compliance with
 the License.  You may obtain a copy of the License from

 http://www.CognitiveWeb.org/legal/license/

 Software distributed under the License is distributed on an AS IS
 basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 the License for the specific language governing rights and limitations
 under the License.

 Copyrights:

 Portions created by or assigned to CognitiveWeb are Copyright
 (c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
 information for CognitiveWeb is available at

 http://www.CognitiveWeb.org

 Portions Copyright (c) 2002-2003 Bryan Thompson.

 Acknowledgements:

 Special thanks to the developers of the Jabber Open Source License 1.0
 (JOSL), from which this License was derived.  This License contains
 terms that differ from JOSL.

 Special thanks to the CognitiveWeb Open Source Contributors for their
 suggestions and support of the Cognitive Web.

 Modifications:

 */
/*
 * Created on Mar 17, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ForceEnum;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.EmbeddedBigdataFederation.Options;
import com.bigdata.service.mapReduce.MapService.EmbeddedMapService;
import com.bigdata.service.mapReduce.ReduceService.EmbeddedReduceService;


/**
 * <p>
 * The master for running parallel map/reduce jobs distributed across a cluster.
 * </p>
 * <p>
 * Map/reduce is a functional programming style in which a program is broken
 * down into a <i>map</i> and a <i>reduce</i> operation. Those operations are
 * trivially parallelized and distributed across one or more worker tasks on
 * available hosts. In general, there are M map tasks and N reduce tasks for
 * each map/reduce job. The map operation, the intermediate store, and the
 * reduce tasks are distinct services. In particular, this means that the job
 * state resides with the master while the intermediate state for the reduce
 * tasks is decoupled from the reduce logic.
 * </p>
 * <p>
 * Logically, each map operation processes a key-value pair, writing a set of
 * intermediate key-value pairs as its output. In practice, the inputs are often
 * drawn from files in a (networked) file system but other possible sources
 * include chaining map/reduce jobs or reading from an index. It is important
 * that the keys output by the map operation capture all necessary "uniqueness"
 * distinctions. For example, the key is often formed from an "application key"
 * followed the map task identifier, and finally the map task local tuple
 * counter. The map operation outputs are buffered and automatically partitioned
 * into N temporary stores (one per reduce task) using a hash function of the
 * key module the #of reduce tasks.
 * </p>
 * <p>
 * Each reduce task reads the intermediate store having data for the
 * intermediate key-value partition assigned to that reduce task. The keys in
 * those partitions are essentially random since they are assigned to partitions
 * by the (user-defined) hash function. However, the intermediate store
 * guarentees that the key-value pairs will be fully ordered. The reduce task is
 * then run on the total order, writing its outputs onto a local output file or
 * loading the data into scale-out indices.
 * </p>
 * <p>
 * The map/reduce master selects map and reduce services on which to run a
 * map/reduce job and is responsible for re-assigning tasks that fail or that
 * take are taking too long. The master attempts to use reduce services that are
 * "close" to the intermediate data (in terms of the network topology) but not
 * necessarily on the same host - this decouples the reduce operation from the
 * input state and lets us failover to another reduce service as necessary.
 * </p>
 * <h4>Design alternatives</h4>
 * Note: I considered several design alternatives for reduce state and behavior,
 * including:
 * <ol>
 * 
 * <li>Reduce client uses local temporary stores. These are created when a
 * map/reduce job starts and released when it ends.</li>
 * 
 * <li>Reduce client uses {@link DataService}s as the store files. A set of
 * {@link DataService}s would either be dedicated to map/reduce tasks or
 * multiplexed with other operations.</li>
 * 
 * </ol>
 * The advantages of using {@link DataService}s are (a) reuse of failover for
 * data services; and (b) reduce behavior is separated from reduce state. The
 * advantage of using local temporary stores is that it is faster since there is
 * no network IO for the reduce task (there is always going to be network IO for
 * the map task). Local stores also simplifies management of temporary state.
 * However the use of local stores tightly couples the reduce task execution
 * with the reduce state and requires that you re-execute the entire map/reduce
 * job in the reduce task fails - this is unacceptable.
 * </p>
 * <p>
 * Another design choice was the use of B+Trees for the intermediate stores. The
 * B+Tree automatically forces the tuples into a total order within each reduce
 * partition. The use of buffering (potentially across multiple map tasks for a
 * single map job) and group commit help to improve bulk insert performance on
 * the index. The most obvious alternative would be to use an external sort
 * program to sort the tuples in a reduce partition before the reduce job
 * executes. The keys are variable length (unsigned) byte[]s and the values are
 * variable length byte[]s, so the standard Un*x sort utilities will not work.
 * While many sort algorithms preserve duplicate keys, the B+Tree does not.
 * Therefore we make sure that the application key is extended to include a map
 * task identifier and map task local tuple counter so that key collision can
 * only arise when the same map task executes more than once - for example, when
 * retrying a failed or long running map task. In such cases the most recent run
 * of the task will simply overwrite the earlier run.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Explore the parameter space (m,n,# map services, # reduce services,
 * buffer mode, flush on commit, overflow, group commit, job size, # of
 * machines). Look at job completion rate, scaling, bytes read/written per
 * second, etc.
 * 
 * @todo Offer options for the behavior in the face of failed map tasks.
 *       <p>
 *       One option is that the task is simply retried, partial data may be
 *       present if the task never succeeds, and if the task eventually succeeds
 *       then the data will reflect the full execution of that task (a purely
 *       additive model with atomic append only on the individual reduce stores
 *       and not across the reduce stores on which a given map task writes).
 *       <p>
 *       Another alternative is that the output from map tasks that do not
 *       complete within this many retries will not participate in the reduce
 *       task. In order to guarentee this we need to do a
 *       <em>distributed transactional</em> atomic "append" of tuples across
 *       all reduce stores on which a map task writes. Since we know that there
 *       will never be collision from different map tasks (the map task UUID is
 *       part of the key), we can could optimize this distributed transaction in
 *       a number of ways. First, write-write conflicts can not arise. Second,
 *       we could avoid the use of real transactions (since there will never be
 *       conflicts) if we support the concept of a distributed "unisolated"
 *       atomic commit.
 * 
 * @todo Generalize {@link IMapSource} so that we can read input 2-tuples from a
 *       variety of sources, including indices, other map reduce jobs, etc. This
 *       interface currently assumes that the inputs for the map operations are
 *       files in a (networked) file system.
 * 
 * @todo Iff the write on the reduce output files is to be atomic (so the map
 *       task has no effect unless it completes successfully), then we need to
 *       use a transaction (since we are actually writing onto N distinct reduce
 *       input files). (The writes on the individual reduce input files will
 *       always be atomic, but a failure of some operations would leave part of
 *       the effect of the map task in some of the reduce input files.)
 * 
 * @todo A good example of a map/reduce job would be bulk loading a large Lehigh
 *       Benchmark data set. The map job would parse the files that were local
 *       on each host, distributing triples across the cluster using a hash
 *       function. The reduce job would sort the triplets arriving at each host
 *       and then bulk load those triplets into the indices.
 *       <p>
 *       Another example would be to bulk index terms in documents, etc.
 * 
 * @todo consider a BFS - a bigdata file system using the local file system to
 *       store files and a bigdata federation to store metadata. The file system
 *       would support atomic append, read, write, and delete operations. This
 *       should be trivial to implement over the existing infrastructure
 *       (failover would require sending the files about as well as the file
 *       metadata so that would be a little more complex). BFS could then be
 *       used as a distributed file system for fan-in to map/reduce jobs.
 * 
 * @todo Support a job schedule in the master and master failover?. Expose an
 *       HTML monitor if the master is made into a service?
 */
public class MapReduceMaster {

    public static final transient Logger log = Logger
            .getLogger(MapReduceMaster.class);

    /**
     * A map reduce job.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class MapReduceJob implements IMapReduceJob {

        // the UUID of this job.
        private final UUID uuid;

        // #of map tasks to run.
        private final int m;

        // #of reduce tasks to run.
        private final int n;

        // object responsible for enumerating the inputs to the map task.

        private final IMapSource mapSource;

        private final Class<? extends IMapTask> mapTaskClass;

        private final Constructor<? extends IMapTask> mapTaskCtor;
        
        private final Class<? extends IReduceTask> reduceTaskClass;

        private final Constructor<? extends IReduceTask> reduceTaskCtor;
        
        private final IHashFunction hashFunction;

        public MapReduceJob(int m, int n,
                IMapSource mapSource,
                Class<? extends IMapTask> mapTaskClass,
                Class<? extends IReduceTask> reduceTaskClass,
                IHashFunction hashFunction
                ) {

            if (m <= 0)
                throw new IllegalArgumentException();

            if (n <= 0)
                throw new IllegalArgumentException();

            if (mapSource == null)
                throw new IllegalArgumentException();

            if (mapTaskClass == null)
                throw new IllegalArgumentException();

            if (reduceTaskClass == null)
                throw new IllegalArgumentException();

            if (hashFunction == null)
                throw new IllegalArgumentException();

            // assign a job UUID.
            this.uuid = UUID.randomUUID();

            this.m = m;

            this.n = n;

            this.mapSource = mapSource;

            this.mapTaskClass = mapTaskClass;

            try {
                this.mapTaskCtor = mapTaskClass.getConstructor(new Class[] {
                        UUID.class, Integer.class, IHashFunction.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } 
            
            this.reduceTaskClass = reduceTaskClass;

            try {
                this.reduceTaskCtor = reduceTaskClass
                        .getConstructor(new Class[] { UUID.class, UUID.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } 

            this.hashFunction = hashFunction;

        }

        public UUID getUUID() {

            return uuid;

        }

        public int getMapTaskCount() {

            return m;

        }

        public int getReduceTaskCount() {

            return n;

        }

        public IMapSource getMapSource() {

            return mapSource;

        }

        public IHashFunction getHashFunction() {

            return hashFunction;

        }

        public IMapTask getMapTask() {
            
            return getMapTask( UUID.randomUUID() );
           
        }
        
        public IMapTask getMapTask(UUID task) {

            try {

                return mapTaskCtor.newInstance(new Object[] {
                        task,
                        Integer.valueOf(getReduceTaskCount()),
                        getHashFunction() }
                );
                
            } catch (InstantiationException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalAccessException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalArgumentException e) {

                throw new RuntimeException(e);
                
            } catch (InvocationTargetException e) {
                
                throw new RuntimeException(e);

            }

        }

        public IReduceTask getReduceTask(UUID task, UUID dataService) {

            try {

                return reduceTaskCtor.newInstance(new Object[] {
                        task,
                        dataService}
                );
                
            } catch (InstantiationException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalAccessException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalArgumentException e) {

                throw new RuntimeException(e);
                
            } catch (InvocationTargetException e) {
                
                throw new RuntimeException(e);

            }

        }

    }

    /**
     * The state of a map/reduce job.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class JobState {
        
        /**
         * The job to be run.
         */
        final MapReduceJob job;
        /**
         * The client for the {@link BigdataFederation} used to store the
         * intermediate state of the map/reduce operation.
         */
        final IBigdataClient client;
        /**
         * The map services.
         */
        IMapService[] mapServices;
        /**
         * The reduce services.
         */
        IReduceService[] reduceServices;
        /**
         * The UUID assigned to each reduce task.
         */
        UUID[] reduceTasks;
        /**
         * The data services for each reduce partition. The index on the data
         * service is named by the UUID of the corresponding reduce task.
         */
        UUID[] dataServices;
        
        public JobState(MapReduceJob job, IBigdataClient client) {

            if (job == null)
                throw new IllegalArgumentException();

            if (client == null)
                throw new IllegalArgumentException();

            this.job = job;
            
            this.client = client;
        
            setUp();
            
        }

        /**
         * Select the map, reduce, and data services to be used by the job.
         * 
         * @todo When we convert to distributed map/reduce, we will need to
         *       perform service discovery for the map/reduce clients and use no
         *       more than are required by the job. If there are fewer available
         *       than the input parameters for the job then we will submit more
         *       than one task to each service. In general, a map service can
         *       run 100s of tasks in parallel while a reduce service can run
         *       10s of tasks in parallel.
         * 
         * @todo cleanly separate the notion of M and N (#of map tasks and #of
         *       reduce tasks) from the notion of the #of map services and the
         *       #of reduce services and from the notion of the number of map
         *       inputs (fan-in to the map operations). N is the fan-in to the
         *       reduce operations.
         */
        protected void setUp() {

            /**
             * Since we are running everything in process, this is the #of map
             * services to be start. The actual #of map tasks to run is based on
             * the #of input files and is discovered dynamically. The #of map
             * services to be started is based on an expectation that we will
             * distribute 100 concurrent map tasks to each map service. Since we
             * do not know the fan in (the #of input files) we are not able to
             * tell how many sets of map tasks will be run through the map
             * services before the input has been consumed.
             */
            final int numMapServicesToStart = 1; //Math.max(1, job.m / 100);

            /**
             * Since we are running everything in process, this is the #of
             * reduce services to start. The #of reduce tasks to be run is given
             * by job.n - this is a fixed input parameter from the user.
             */
            final int numReduceServicestoStart = 1;// Math.max(1, job.n/10);

            /**
             * The map services. Each services is capable of running a large #of
             * map tasks in parallel.
             */
            mapServices = new IMapService[numMapServicesToStart];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numMapServicesToStart; i++) {

                    mapServices[i] = new EmbeddedMapService(UUID.randomUUID(),
                            properties, client);

                }

            }

            /**
             * The reduce services. Each service is capable of running a few
             * reduce tasks in parallel.
             */
            reduceServices = new IReduceService[numReduceServicestoStart];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numReduceServicestoStart; i++) {

                    reduceServices[i] = new EmbeddedReduceService(UUID
                            .randomUUID(), properties, client);
                    
                }

            }

            /**
             * The reduce task identifiers.
             * 
             * Note: The #of reduce tasks is determined by the caller.
             * 
             * Note: We pre-generate the UUIDs for these tasks since they are
             * also used to name the intermediate stores.
             */
            reduceTasks = new UUID[job.n];
            {

                for(int i=0; i<reduceTasks.length; i++) {

                    reduceTasks[i] = UUID.randomUUID();
                    
                }
                
            }
            
        }
        
        /**
         * Notify everyone that the job is starting.
         * 
         * @todo parallelize notice so that we can start/end jobs faster.
         * 
         * @todo handle map service and reduce service failover (take the
         *       service off line and perhaps find a new one to use).
         * 
         * @todo handle data service failover
         */
        public void start() {
            
            /*
             * Setup intermediate indices on the data services (one per reduce
             * task). The same data service MAY be used for more than one reduce
             * task.
             * 
             * @todo in order for the map and reduce tasks to have automatic
             * failover they need to query the metadata service if the given
             * data service fails and find the replacement data service (this
             * presumes that a media redundency chain is in effect).
             */
            dataServices = new UUID[reduceTasks.length];
            {
                IMetadataService metadataService = client.getMetadataService();
                for (int i = 0; i < reduceTasks.length; i++) {
                    try {
                        dataServices[i] = metadataService.getUnderUtilizedDataService();
                    } catch (IOException e) {
                        log.warn("Metadata service not available: " + e);
                    }
                    try {
                        // the index name (local on the data service).
                        String name = reduceTasks[i].toString();
                        String className = UnisolatedBTree.class.getName();
                        Object config = null;
                        client.getDataService(dataServices[i]).registerIndex(
                                name, reduceTasks[i], className, config);
                    } catch (Exception e) {
                        log.warn("Could not create intermediate store: " + e);
                    }
                }
            }
            
            // notify map services of new job.
            {
                final int nmapServices = mapServices.length;
                for (int i = 0; i < nmapServices; i++) {
                    try {
                        mapServices[i].startJob(job.uuid, reduceTasks,
                                dataServices);
                    } catch (IOException e) {
                        log.warn("Map service not available: " + e);
                    }
                }
            }
            
            // notify reduce services of new job.
            {
                final int nreduceServices = reduceServices.length;
                for (int i = 0; i < nreduceServices; i++) {
                    try {
                        reduceServices[i].startJob(job.uuid);
//                        , reduceTasks[i], dataServices[i]);
                    } catch (IOException e) {
                        log.warn("Reduce service not available: " + e);
                    }
                }
            }
            
        }

        /**
         * Notify everyone that the job is done.
         */
        public void end() {
            
            for(int i=0; i<mapServices.length; i++) {
                try {
                    mapServices[i].endJob(job.uuid);
                } catch(IOException e) {
                    log.warn("Map service: "+e);
                }
            }
            
            for(int i=0; i<reduceServices.length; i++) {
                try {
                    reduceServices[i].endJob(job.uuid);
                } catch(IOException e) {
                    log.warn("Reduce service: "+e);
                }
            }
            
            for(int i=0; i<dataServices.length; i++) {
                try {
                    IDataService ds = client.getDataService(dataServices[i]);
                    if(ds==null) {
                        log.warn("Could not locate data service: "+dataServices[i]);
                    }
                    // the index name (local on the data service).
                    String name = reduceTasks[i].toString();
                    ds.dropIndex(name);
                } catch(Exception e) {
                    log.warn("Data service: "+e);
                }
            }
            
        }

        /**
         * Distribute the map tasks and wait for them to complete.
         * <p>
         * Note: This presumes that we are using network storage for the input
         * sources. We simply assign each file to one map task.
         * 
         * @param delay
         *            The maximum time (in milliseconds) that we will wait for a
         *            task to complete.
         * 
         * @param maxTasks
         *            When non-zero, this is the maximum #of input files that
         *            will be mapped.
         * 
         * @param maxRetry
         *            The maximum #of times that we will retry a task that fails
         *            to complete successfully within the <i>delay</i>. A value
         *            of ZERO (0) disables retry.
         * 
         * @param clients
         *            The clients on which the map tasks will be run.
         * 
         * @todo run the map task on the client that is 'closest' to the data
         *       that it will read while also making sure that we distribute the
         *       map tasks among the available clients.
         * 
         * FIXME [m] should limit the #of active map tasks for this job. This
         * helps to give the user control over the parallelism and also makes
         * the "delay" interpretable in terms of the time since the job was
         * submitted. Right now all inputs are mapped immediately and just queue
         * up in the map service(s) so the map services themselves are limiting
         * the parallelism. The logic here needs to be modified to have no more
         * than [m] map tasks "out there" for execution at any given time. This
         * is going to make the logic more complicated since we have to notice
         * tasks that finish asynchronously. Try to refactor the logic for
         * running the map and reduce ops first into shared code and then
         * redesign so as to limit the maximum concurrency. Since the executor
         * service can already do this, maybe we can queue up the input files
         * (blocking queue of at least m) and drain them through an executor
         * service (m concurrent tasks). the executor service just submits the
         * task to the remove service. tasks that fail are re-queued on the
         * local input queue unless their retry count has been exceeded.
         * 
         * @todo Long running tasks should be replicated once we are "mostly"
         *       done.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @todo Can the return value mislead the caller? E.g., is it possible
         *       for a task to succeed twice, resulting in double counting such
         *       that we can not rely on the return to indicate whether all map
         *       tasks completed successfully?
         */
        public double map(long delay, long maxTasks, int maxRetry) {

            final int nservices = mapServices.length;

            // #of input sources assigned to map tasks.
            int nstarted = 0;
            
            int nretries = 0;

            // the files to be mapped over the clients.
            Iterator<File> itr = job.getMapSource().getSources();

            /*
             * A queue of map tasks that have been submitted and on which we are
             * waiting for completion.
             */
            DelayQueue<MapTaskState> waiting = new DelayQueue<MapTaskState>();

            while (itr.hasNext()) {

                // the input source.
                File file = itr.next();
                
                // do not map more than this many input files.
                if(maxTasks!=0L && nstarted>=maxTasks) break;

                // choose a client to run this input source.
                final int index = (int) (nstarted % nservices);

                log
                        .info("Mapping file " + file.toString() + " on client #"
                                + index);

                // the client to which this input source will be assigned.
                IMapService client = mapServices[index];

                // run the map task.
                Future future;
                final IMapTask mapTask = job.getMapTask();
                try {
                    // @todo the Future will go away with a remote service refactor.
                    future = client.submit(job.uuid, file, mapTask);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                // add to the queue of tasks on which we will wait.
                waiting.add(new MapTaskState(delay, mapTask.getUUID(), 0,
                        future, file));

                nstarted++;

            }

            /*
             * Wait for the queue to drain.
             */
            int nended = 0;
            int nfailed = 0; // #of tasks that do not succeed (retries also fail).
            
            while (nended < nstarted) {

                MapTaskState taskState;
                
                try {
                    
                    taskState = waiting.poll(1, TimeUnit.SECONDS);

                    if (taskState == null) {

                        // still waiting on a task to complete.
                        continue;
                        
                    }
                    
                } catch (InterruptedException ex) {

                    log.warn("Interrupted: "+ex,ex);

                    // @todo ignore or abort processing?
                    continue;
                    
                }

                nended++;
                
                taskState.cancelIfStillRunning();

                if (taskState.error() == null) {
                    log.info("Done: "
                            + taskState.uuid
                            + " "
                                + (taskState.success() ? "success"
                                        : (taskState.future.isCancelled() ? "cancelled"
                                                : taskState.error())) + "("
                                                + nended + " of " + nstarted + ")");
                } else {
                    log.warn("Error: " + taskState.uuid + " " + "(" + nended
                            + " of " + nstarted + ")", taskState.error());

                }

                /*
                 * If the task was not successful, then re-try it up to N times.
                 */
                if (!taskState.success()) {

                    if (taskState.ntries < maxRetry) {

                        final int ntries2 = taskState.ntries + 1;

                        // choose a client to run this input source.
                        final int index = (int) (nstarted % nservices);

                        log.info("Mapping file " + taskState.file.toString()
                                + " on client #" + index + "(ntries=" + ntries2
                                + ")");

                        // the client to which this input source will be
                        // assigned.
                        IMapService client = mapServices[index];

                        // run the map task.
                        Future future;
                        final IMapTask mapTask = job.getMapTask(taskState.uuid);
                        try {
                            // @todo the Future will go away with a remote
                            // service refactor.
                            future = client.submit(job.uuid, taskState.file,
                                    mapTask);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }

                        // add to the queue of tasks on which we will wait.
                        waiting
                                .add(new MapTaskState(taskState.delay,
                                        taskState.uuid, ntries2, future,
                                        taskState.file));

                        nstarted++;

                        nretries++;

                    } else {

                        /*
                         * A task has failed more times than we are willing to
                         * retry so mark this as a permanent failure.
                         */
                        
                        log.warn("Could not map: " + taskState.file);

                        nfailed++;

                    }

                }

            }

            System.err.println("Ran " + nstarted + " map tasks ("+nretries+" retries, "+nfailed+" failed).");

            return (double)(nstarted-nfailed)/nstarted;
            
        }

        /**
         * Distribute the reduce tasks and wait for them to complete.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @todo Can the return value mislead the caller? E.g., is it possible
         *       for a task to succeed twice, resulting in double counting such
         *       that we can not rely on the return to indicate whether all
         *       tasks completed successfully?
         */
        public double reduce(long delay, int maxRetry) {

            final int nservices = reduceServices.length;

            // #of reduce tasks run.
            int nstarted = 0;

            // #of reduce tasks that get retried.
            int nretries = 0;
            
            /*
             * A queue of map tasks that have been submitted and on which we are
             * waiting for completion.
             */
            DelayQueue<ReduceTaskState> waiting = new DelayQueue<ReduceTaskState>();

            final int n = job.getReduceTaskCount();

            for (int i = 0; i < n; i++) {

                // the service on which this reduce task will be executed.
                IReduceService reduceService = reduceServices[i % reduceServices.length];

                // the service from which it will read its data.
                UUID dataService = dataServices[i % reduceTasks.length];

                // run the task.
                // @todo the Future will go away with the remove service refactor.
                Future<Object> future;
                IReduceTask reduceTask = job.getReduceTask(reduceTasks[i],dataService);
                try {
                    future = reduceService.submit(job.getUUID(), reduceTask);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // add to the queue of tasks on which we will wait.
                waiting.add(new ReduceTaskState(delay, reduceTask.getUUID(), 0, future, dataService));

                nstarted++;

            }

            /*
             * Wait for the queue to drain.
             */
            int nended = 0;
            int nfailed = 0;
            
            while (nended < nstarted) {

                ReduceTaskState taskState;
                
                try {
                    
                    taskState = waiting.poll(1, TimeUnit.SECONDS);

                    if (taskState == null) {

                        // still waiting on a task to complete.
                        continue;
                        
                    }
                    
                } catch (InterruptedException ex) {
                    
                    log.warn("Interrupted: "+ex,ex);
                    
                    // @todo ignore or abort processing?
                    continue;
                    
                }

                nended++;
                
                taskState.cancelIfStillRunning();

                if (taskState.error() == null) {
                    log.info("Done: "
                            + taskState.uuid
                            + " "
                                + (taskState.success() ? "success"
                                        : (taskState.future.isCancelled() ? "cancelled"
                                                : taskState.error())) + "("
                                                + nended + " of " + nstarted + ")");
                } else {
                    log.warn("Error: " + taskState.uuid + " " + "(" + nended
                            + " of " + nstarted + ")", taskState.error());

                }

                /*
                 * If the task was not successful, then re-try it up to N times.
                 */
                if (!taskState.success()) {

                    if (taskState.ntries < maxRetry) {

                        final int ntries2 = taskState.ntries + 1;

                        // choose a client to run this input source.
                        final int index = (int) (nstarted % nservices);

                        log.info("Reducing " + taskState.dataService
                                + " on client #" + index + "(ntries=" + ntries2
                                + ")");

                        /*
                         * The service on which this reduce task will be
                         * executed.
                         */
                        IReduceService client = reduceServices[index];

                        // run the map task.
                        Future future;
                        IReduceTask reduceTask = job.getReduceTask(
                                taskState.uuid, taskState.dataService);
                        try {
                            // @todo the Future will go away with a remote
                            // service refactor.
                            future = client.submit(job.uuid, reduceTask);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }

                        // add to the queue of tasks on which we will wait.
                        waiting.add(new ReduceTaskState(taskState.delay,
                                taskState.uuid, ntries2, future,
                                taskState.dataService));

                        nstarted++;

                        nretries++;

                    } else {

                        /*
                         * A task has failed more times than we are willing to
                         * retry so mark this as a permanent failure.
                         */

                        log.warn("Could not map: " + taskState.uuid);

                        nfailed++;

                    }

                }

            }

            System.err.println("Ran " + nstarted + " reduce tasks (" + nretries
                    + " retries, " + nfailed + " failed).");

            return (double)(nstarted-nfailed)/nstarted;

        }

    }
    
    /**
     * Run a map/reduce task.
     * 
     * @param args
     * 
     * FIXME describe the job using an XML file and refactor
     * {@link ExtractKeywords}, {@link CountKeywords}, and
     * {@link TestCountKeywordJob} into some test suites for this class.
     * 
     * @todo Normally the #of map clients will exceed the actual number of
     *       available {@link MapService}s. In this case, each client will be
     *       expected to execute multiple map task and those tasks will be
     *       parallelized both across the clients and within each clients thread
     *       pool. Likewise, there can be more reduce tasks than
     *       {@link MapService}s. However, reduce tasks tend to be heavier so
     *       the proportion of reduce tasks to clients might be on the order of
     *       2:1 while the proportion of map tasks to clients might be on the
     *       order of 100:1. Does this suggest that we should have separate map
     *       and reduce thread pools in the clients?
     *       <p>
     *       The #of reduce tasks MUST be choosen such that the intermediate
     *       file may be handled on a single machine. (There must be enough free
     *       disk to store the file and enough resources to sort the file,
     *       producing a sorted version. So you need 2X disk plus lots of RAM.
     *       The sort should really be in C so that we can leverage more than 2G
     *       of RAM.)
     * 
     * FIXME There is confusion in reporting M vs the #of map operations
     * actually run vs the #of map services available vs the #of map services
     * used. (Likewise for reduce).
     */
    public static void main(String[] args) {

        /*
         * FIXME  Map processing is faster when N is smaller - why?
         * 
         * Given M=100
         * 
         * N = 1 : 12 seconds maptime; 2 seconds reduce time.
         * N = 2 : 16 seconds maptime; 2 seconds reduce time.
         * N = 3 : 23 seconds maptime; 3 seconds reduce time.
         * N = 4 : 29 seconds maptime; 3 seconds reduce time.
         * 
         * This is measured with 2 data services and bufferMode=Transient.
         * 
         * With bufferMode=Disk and forceOnCommit=No the results are:
         * 
         * N = 1 : 23 seconds maptime; 3 seconds reduce time.
         * N = 2 : 19 seconds maptime; 3 seconds reduce time.
         * N = 3 : 25 seconds maptime; 3 seconds reduce time.
         * N = 4 : 28 seconds maptime; 3 seconds reduce time.
         * 
         * which is even stranger.
         * 
         * I would expect the average write queue length on the data
         * services for the reduce partitions to be M/P, where P is
         * min(#of data services available,N) (or P=N if using group
         * commit).  So, if N = P = 1 and M = 100 then 100 tasks will
         * be trying to write on 1 data service concurrently.  However,
         * the #s above appear to go the other way.
         * 
         * FIXME setting m here has no effect since the size of the thread
         * pool in the MapService is actually limiting concurrency since we
         * are not throttling the #of concurrent jobs submitted to the map
         * service.
         * 
         * Note: the above is also true for the reduce service but there is
         * never the less a clear effect which must be due to the #of data
         * services in use (the fewer data services in use the better the
         * system is performing above).
         * 
         * Ok. The answers to the above observations are: (a) we write 
         * smaller batches on each partition as there are more partitions.
         * If we extract on the order of 1000 keywords per task and have
         * 10 partitions then a map task writes 100 keywords per partition.
         * Smaller writes are more expensive; (b) we are doing one commit
         * per map task per partition - more commits are more expensive;
         * (c) each commit makes the nodes and leaves of the btree immutable,
         * so we are doing more (de-)serialization of nodes and leaves.
         * 
         * Group commit will fix most of these issues - perhaps all.  However
         * there is going to be a practical limit on the #of reduce tasks to
         * run based on the expected #of tuples output per map task.  At some
         * point each map task will write zero or one tuples per reduce task.
         * Still, that might be necessary for very large map fan ins.  Consider
         * if you have billions of documents to process.  Each document is still
         * going to be very small, but the #of reduce partitions must be high
         * enough to make the intermediate files fit in local storage and to
         * make the reduce computation not too long.  The #of reduce tasks may
         * also be set by the #of machines that you are going to use to serve
         * the resulting data, e.g., a cluster of 100 machines serving a large
         * text index.
         */
        MapReduceJob job = new TestCountKeywordJob(100/* m */, 2/* n */);
//        MapReduceJob job = new TestCountKeywordJob(1/* m */, 2/* n */);

        /**
         * Setup the client that will connect to federation on which the
         * intermediate results will be written.
         * 
         * @todo this is using embedded federation - use a distributed one but
         *       keep an option for testing that uses an embedded federation.
         *       For this we will need to specify the federation identifier so
         *       that the client can connect.
         * 
         * @todo when used for map/reduce a larger disk-only journal and no
         *       overflow would improve performance since we expect to only read
         *       back the data once (in a given reduce task) and the bulk index
         *       build is at least that expensive. There really may be a role
         *       for "temporary" data services -- data services optimized for
         *       short-term but robust data storage -- for both map/reduce and
         *       for intermediate results in data processing such as closure of
         *       an RDFS store or join results.
         */
        final IBigdataClient client;
        {
            
            Properties properties = new Properties();

            /*
             * A distributed federation should be using bufferMode=Disk since
             * the intermediate results can be quite large.
             */
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            // Note: using temp files for testing purposes w/ an embedded federation.
            properties.setProperty(Options.CREATE_TEMP_FILE, Boolean.TRUE.toString());
            
            // Note: No disk at all, but consumes more RAM to buffer the data.
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                    .toString());
          
            /*
             * #of data services to run in the federation.
             * 
             * Note: At most one data service will be used per reduce fan in
             * (N). Excess data services will not be used.  There should not
             * be much overhead to unused data services since even the fully
             * buffered modes extend the buffer in response to use.
             */
//            properties.setProperty(Options.NDATA_SERVICES, "10");

            properties.setProperty(Options.FORCE_ON_COMMIT, ForceEnum.No.toString());

            client = new EmbeddedBigdataClient(properties);
            
            client.connect();
            
        }

        JobState jobState = new JobState(job,client);
        
        // The minimum success rate required for the map stage.
        final double minSuccessRate = .9d;
        
        try {

            /*
             * Notify everyone that this job is starting.
             */
            jobState.start();

            /*
             * Distribute the map tasks and wait for them to complete.
             */
            final long elapsedMap;
            {

                final long beginMap = System.currentTimeMillis();

                // the maximum time a map task may run.
                final long delay = 60 * 1000;

                // non-zero to run at most that many input files.
                final long maxTasks = 0L;
//                final long maxTasks = 1L; // @todo remove

                // the maximum #of times a map task will be retried (zero disables retry).
//                final int maxRetry = 3;
                final int maxRetry = 0; // @todo remove

                final double percentSuccess = jobState.map(delay, maxTasks, maxRetry);
                
                System.out.println("Map operation "+percentSuccess+" success.");
                
                if(percentSuccess<minSuccessRate) {
                    
                    log.warn("Success rate less than threshold - aborting");
                    
                    System.exit(1);
                    
                }

                elapsedMap = System.currentTimeMillis() - beginMap;
                
            }

            /*
             * Distribute the reduce tasks and wait for them to complete.
             */
            final long elapsedReduce;
            {
            
                final long beginReduce = System.currentTimeMillis();

                // the maximum time a reduce task may run.
                final long delay = 60 * 1000;

                // the maximum #of times a reduce task will be retried (zero disables retry).
//                final int maxRetry = 3;
                final int maxRetry = 0; // @todo remove

                final double percentSuccess = jobState.reduce(delay, maxRetry);
               
                System.out.println("Reduce operation "+percentSuccess+" success.");

                if(percentSuccess<minSuccessRate) {
                    
                    log.warn("Success rate less than threashold - aborting");
                    
                    System.exit(1);
                    
                }
                
                elapsedReduce = System.currentTimeMillis() - beginReduce;

            }

            System.out.println("Done: map( " + elapsedMap + " ms, " + job.m
                    + " tasks ), reduce( " + elapsedReduce + " ms" + ", "
                    + job.n + " tasks )");
            
        } catch (Throwable t) {
            
            log.error( "Problem running job: "+t, t );
            
        } finally {

            /*
             * Notify everyone that this job is over.
             */

            jobState.end();

            /*
             * Disconnect from the federation.
             */
            
            client.terminate();
            
        }

    }

    /**
     * A task that has been submitted and on which we are awaiting completion.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract static class TaskState implements Delayed {

        // maximum grace period (ms).
        final long delay;

        // task UUID.
        final UUID uuid;

        // #of times that this task has been tried.
        final int ntries;

        // start time.
        final long begin = System.currentTimeMillis();

        /*
         * @todo this needs to be handled differently for a remote service.
         */
        final Future<Object> future;

        /**
         * @param uuid
         *            The task UUID.
         * @param delay
         *            The length of time in milliseconds that we are willing
         *            to wait for this task to complete.
         * @param ntries
         *            The #of times that this task has already been
         *            attempted.
         * @param future
         */
        protected TaskState(long delay, UUID uuid, int ntries,
                Future<Object> future) {

            this.delay = delay;

            this.uuid = uuid;

            this.future = future;

            this.ntries = ntries;

        }

        /**
         * Returns the remaining delay associated with this object
         * 
         * @return 0L if the task is done (whether an error or success).
         *         Otherwise, the remaining time for this task to run.
         */
        public long getDelay(TimeUnit unit) {

            if (future.isDone()) {

                // The task is done (any kind of completion).
                return 0;

            }

            // the remaining delay (it is actually in milliseconds).
            long elapsed = System.currentTimeMillis() - begin;

            long remaining = delay - elapsed;

            return unit.convert(remaining, TimeUnit.MILLISECONDS);

        }

        public int compareTo(Delayed arg0) {

            if (this == arg0)
                return 0;

            return uuid.compareTo(((TaskState) arg0).uuid);

        }

        // /**
        // * Return true iff the task is still running at the time that this
        // * method was invoked.
        // */
        // public boolean running() {
        //                
        // return ! future.isDone();
        //                
        // }

        /**
         * Return true iff the task had completed successfully at the time
         * that this method was invoked
         */
        public boolean success() {

            if (future.isDone() && !future.isCancelled()) {

                try {

                    future.get();

                    return true;

                } catch (InterruptedException ex) {
                    // will not happen since the task is done.
                    throw new AssertionError();
                } catch (CancellationException ex) {
                    // will not happen since we checked this above.
                    throw new AssertionError();
                } catch (ExecutionException ex) {
                    return false;
                }

            }

            // not successful (might not be done yet).
            return false;

        }

        /**
         * Return the {@link Throwable} object take caused the task to fail
         * or <code>null</code> if the task has not completed or if the
         * task has completed by no error was generated.
         */
        public Throwable error() {

            if (future.isDone() && !future.isCancelled()) {

                try {

                    future.get();

                    // no exception.
                    return null;

                } catch (InterruptedException ex) {
                    // will not happen since the task is done.
                    throw new AssertionError();
                } catch (CancellationException ex) {
                    // will not happen since we checked this above.
                    throw new AssertionError();
                } catch (ExecutionException ex) {
                    // the cause of the exception.
                    return ex.getCause();
                }

            }

            // not done yet or cancelled.
            return null;

        }

        /**
         * Cancel the task if it is still running.
         * 
         * @todo this implementation is synchronous since it relies on
         *       {@link Future#cancel(boolean)}.  It will block the code
         *       that is polling the queue of waiting tasks, but it will
         *       not block the running tasks themselves.
         */
        public void cancelIfStillRunning() {

            if (!future.isDone()) {

                future.cancel(true/* mayInterruptIfRunning */);

            }

        }

    }
    
    /**
     * The state for a running {@link IMapTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MapTaskState extends TaskState {

        // the input file to the map task.
        final File file;

        /**
         * @param delay
         * @param uuid
         * @param ntries
         * @param future

         * @param file
         *            The input file for the map task.
         */
        protected MapTaskState(long delay, UUID uuid, int ntries, Future<Object> future, File file) {
            super(delay, uuid, ntries, future);

            this.file = file;
        
        }

    }

    /**
     * The state for a running {@link IReduceTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ReduceTaskState extends TaskState {

        // the data service from which the reduce task will read.
        final UUID dataService;

        /**
         * @param delay
         * @param uuid
         * @param ntries
         * @param future
         * 
         * @param dataService
         *            The data service from which the reduce task will read its
         *            input.
         */
        protected ReduceTaskState(long delay, UUID uuid, int ntries, Future<Object> future, UUID dataService) {
            
            super(delay, uuid, ntries, future);

            this.dataService = dataService;
        
        }

    }

    public static class TestCountKeywordJob extends MapReduceJob {

        public TestCountKeywordJob(int m, int n) {

            super(m, n, new FileSystemMapSource(new File("."),
                    new FileFilter() {

                        public boolean accept(File pathname) {

                            if (pathname.isDirectory()) {

                                // i.e., recursive processing of directories.

                                return true;

                            }

                            String name = pathname.getName();

                            return name.endsWith(".java");

                        }

                    }),
                    
                    // map task
//                    new NopMapTask(),
//                    new ReadOnlyMapTask(),
                    ExtractKeywords.class,
                    
                    // reduce task.
                    CountKeywords.class,
                    
                    DefaultHashFunction.INSTANCE);

        }

    }

}
