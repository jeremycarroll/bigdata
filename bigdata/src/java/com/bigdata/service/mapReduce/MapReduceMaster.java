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

import java.io.IOException;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

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

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

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
 * 
 * <h4>Design alternatives</h4>
 * 
 * Note: There are several design alternatives for reduce state and behavior,
 * including:
 * <ol>
 * 
 * <li>Map writes on stores local to the reduce service via RMI. The reduce
 * local stores are created when a map/reduce job starts and released when it
 * ends.</li>
 * 
 * <li>Map and reduce use {@link DataService}s as the store files. A set of
 * {@link DataService}s would either be dedicated to map/reduce tasks or
 * multiplexed with other operations. If dedicated data services are used, then
 * use {@link BufferMode#Disk} and set the overflow threshold quite high since
 * large temporary data sets are to be expected. (The advantage of this design
 * is that state and behavior are isolated: if a service fails you can just
 * re-run the specific tasks since the state is not lost; you can have state
 * failover using the existing {@link DataService} infrastructure.)</li>
 * 
 * <li>Map writes sorted data for each reduce partition on local disk; reduce
 * copies the data from the fan-in map services and either uses a merge sort or
 * a fused view to provide a total ordering. The map output stores are created
 * when a map operation starts and released when the reduce operation ends. The
 * reduce store (if any) is created when the reduce operation starts and
 * released when it ends. (The advantage here is that you mostly read and write
 * on local storage, except for copying data from the map service outputs to the
 * reduce service inputs. The disadvantage is that state is local to services
 * such that a map server failure during reduce setup could lead to the
 * re-execution of the entire job.)</li>
 * 
 * </ol>
 * </p>
 * 
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
 * FIXME get group commit working.
 * 
 * FIXME get working with a distributed federation as well.
 * 
 * @todo offer option to buffer map outputs across map tasks within the service
 *       to increase the size of write operations and improve performance.
 *       <p>
 *       Note: Any time that we buffer output tuples across tasks we are faced
 *       with the possibility that we must re-execute those tasks if the service
 *       fails since their state has not been transferred to the reduce worker.
 *       <p>
 *       If the intermediate state is not too large, then we could do a sort on
 *       each reduce partition split from each map worker (in memory) and then a
 *       merge sort of the reduce partition splits.
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
 * @todo map/reduce with suitable temporary storage could likely be reused to do
 *       distributed joins. Take a look at the client index view for how
 *       iterators are being range partitioned and see if this could be
 *       refactored for a distributed join.
 * 
 * @todo Map and reduce could be abstracted decomposed tasks reading and writing
 *       data in parallel. Map/Reduce becomes a 2-stage chain with slight
 *       specialization for its stages (e.g., hash partitioning output tuples
 *       and writing onto a sorted store for map while reduce reads from a
 *       sorted store). This generalization might require letting the user write
 *       the run() method that is executed by the task worker on the service so
 *       that more input and output options are enabled.
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
 *       <p>
 *       While Java clients for BFS might be easy enough, what challenges are
 *       involved in supporting BFS to Windows and Un*x clients?
 */
public class MapReduceMaster {

    public static final transient Logger log = Logger
            .getLogger(MapReduceMaster.class);

    /**
     * The state of a map/reduce job that is being executed by the master.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract static class JobState {
        
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
        IJobAndTaskService<MapJobMetadata, AbstractMapTask>[] mapServices;
        /**
         * The reduce services.
         */
        IJobAndTaskService<ReduceJobMetadata,AbstractReduceTask>[]reduceServices;
        /**
         * The UUID assigned to each reduce task.
         */
        UUID[] reduceTasks;
        /**
         * The data services for each reduce partition. The index on the data
         * service is named by the UUID of the corresponding reduce task.
         */
        UUID[] dataServices;

        /**
         * Metadata for the map operation.
         */
        MapJobMetadata mapJobMetadata;
        
        /**
         * Metadata for the reduce operation.
         */
        ReduceJobMetadata reduceJobMetadata;

        /**
         * The object that runs the map tasks.
         */
        RemoteTaskRunner<MapJobMetadata, AbstractMapTask> mapTaskRunner;
        
        /**
         * The object that runs the reduce tasks.
         */
        RemoteTaskRunner<ReduceJobMetadata, AbstractReduceTask> reduceTaskRunner;
        
        /**
         * The #of map tasks submitted so far (retries are not counted).
         */
        public long getMapTaskCount() {
            
            return mapTaskRunner == null ? 0 : mapTaskRunner.ntasks;
            
        }
        
        /**
         * The #of reduce tasks submitted so far (retries are not counted).
         */
        public long getReduceTaskCount() {
            
            return reduceTaskRunner == null ? 0 : reduceTaskRunner.ntasks;
            
        }
        
        /**
         * The elapsed time for the map operation (the clock stops once
         * the map operation is over).
         */
        public long getMapElapsedTime() {
            
            return mapTaskRunner == null ? 0 : mapTaskRunner.elapsed();
            
        }
        
        /**
         * The elapsed time for the reduce operation (the clock stops once the
         * reduce operation is over).
         */
        public long getReduceElapsedTime() {
            
            return reduceTaskRunner == null ? 0 : reduceTaskRunner.elapsed();
            
        }
        
        /**
         * The #of map tasks that eventually succeeded.
         */
        public long getMapSuccessCount() {
            
            return mapTaskRunner == null ? 0 : mapTaskRunner.nsuccess;
            
        }

        /**
         * The #of reduce tasks that eventually succeeded.
         */
        public long getReduceSuccessCount() {
            
            return reduceTaskRunner == null ? 0 : reduceTaskRunner.nsuccess;
            
        }

        /**
         * The #of map tasks that were retried (this counts each retry of each
         * task).
         */
        public long getMapRetryCount() {

            return mapTaskRunner == null ? 0 : mapTaskRunner.nretried;

        }

        /**
         * The #of reduce tasks that were retried (this counts each retry of
         * each task).
         */
        public long getReduceRetryCount() {
            
            return reduceTaskRunner == null ? 0 : reduceTaskRunner.nretried;
            
        }

        /**
         * The #of map tasks that permanently failed.
         */
        public long getMapFailedCount() {
            
            return mapTaskRunner == null ? 0 : mapTaskRunner.nfailed;
            
        }

        /**
         * The #of reduce tasks that permanently failed.
         */
        public long getReduceFailedCount() {
            
            return reduceTaskRunner == null ? 0 : reduceTaskRunner.nfailed;
            
        }

        public JobState(MapReduceJob job, IBigdataClient client) {

            if (job == null)
                throw new IllegalArgumentException();

            if (client == null)
                throw new IllegalArgumentException();

            this.job = job;
            
            this.client = client;
        
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
         */
        protected void setUp() {

            /*
             * Note: concrete implementation MUST initialize [mapServices] and
             * [reduceServices] before calling this method.
             */
            if(mapServices==null||reduceServices==null) {
                
                throw new IllegalStateException(
                        "map/reduce services are not initialized");
                
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

            setUpDataStores();
            
            mapJobMetadata = new MapJobMetadata(job.uuid,reduceTasks,dataServices);
            
            reduceJobMetadata = new ReduceJobMetadata(job.uuid);
            
        }
        
        /**
         * Setup intermediate indices on the data services (one per reduce
         * task). The same data service MAY be used for more than one reduce
         * task.
         * 
         * @todo in order for the map and reduce tasks to have automatic
         *       failover they need to query the metadata service if the given
         *       data service fails and find the replacement data service (this
         *       presumes that a media redundency chain is in effect).
         */
        protected void setUpDataStores() {

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

        }

        /**
         * Tear down the embedded services.
         */
        protected void tearDown() {

            for(int i=0; i<mapServices.length; i++) {
                
                if(mapServices[i] instanceof EmbeddedMapService) {
                
                    ((EmbeddedMapService)mapServices[i]).shutdown();
                    
                }
                
            }
            
            for(int i=0; i<reduceServices.length; i++) {

                if(reduceServices[i] instanceof EmbeddedReduceService) {
                    
                    ((EmbeddedReduceService)reduceServices[i]).shutdown();
                    
                }

            }
            
        }
        
        /**
         * Start the job.
         */
        protected void start() {

            setUp();
            
        }

        /**
         * Terminate the job.
         */
        protected void terminate() {

            /*
             * Release the intermediate stores.
             */

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

            tearDown();
            
        }

        /**
         * Distribute the map tasks and wait for them to complete.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @throws InterruptedException
         *             if the excecuting map tasks are interrupted.
         */
        protected double map() throws InterruptedException { 

            Iterator<AbstractMapTask> tasks = new Striterator(job
                    .getMapSource().getSources()).addFilter(new Resolver() {

                        protected Object resolve(Object arg0) {

                            return job.getMapTask(arg0);
                            
                        }

            });
            
            mapTaskRunner =
                new RemoteTaskRunner<MapJobMetadata, AbstractMapTask>(
                        mapServices,
                        mapJobMetadata,
                        tasks,
                        job.getMaxMapTasks(),
                        job.m, //maxConcurrency,
                        job.getMaxMapTaskRetry(),
                        job.getMapTaskTimeout()
                        );
            
            mapTaskRunner.awaitTermination(/*maxMapOpRuntime(ms)*/);

            return ((double)mapTaskRunner.nsuccess)/mapTaskRunner.ntasks;
            
        }

        /**
         * Distribute the reduce tasks and wait for them to complete.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @exception InterruptedException
         *                if the executing tasks are interrupted.
         */
        protected double reduce() throws InterruptedException {

            /*
             * There is a fixed fan-in for the reduce operation.  It is the #of
             * reduce partitions declared by the map/reduce job.  Therefore we
             * put all of the tasks in an executor service with a maximum parallism
             * set by the caller.
             */
            
            AbstractReduceTask[] tasks = new AbstractReduceTask[job.n];
            
            for(int i=0; i<job.n; i++) {
                
                tasks[i] = (AbstractReduceTask) job.getReduceTask(reduceTasks[i], dataServices[i]);
                
            }
            
            reduceTaskRunner =
                new RemoteTaskRunner<ReduceJobMetadata, AbstractReduceTask>(
                        reduceServices,
                        reduceJobMetadata,
                        Arrays.asList(tasks).iterator(),
                        0L /* maxTasks */,
                        job.n, // maxConcurrency,
                        job.getMaxReduceTaskRetry(),
                        job.getReduceTaskTimeout()
                        );
            
            reduceTaskRunner.awaitTermination(/*timeout(ms)*/);

            return ((double)reduceTaskRunner.nsuccess)/reduceTaskRunner.ntasks;
            
        }
        
        /**
         * A summary of the current job state.
         */
        public String status() {

            final double mapPercent = (mapTaskRunner == null ? 0d
                    : ((double) mapTaskRunner.nsuccess) / mapTaskRunner.ntasks);

            final double reducePercent = (reduceTaskRunner == null ? 0d
                    : ((double) reduceTaskRunner.nsuccess)
                            / reduceTaskRunner.ntasks);
            
            final String map = "map( m=" + job.m + ", ntasks="
                    + getMapTaskCount() + ", nretried=" + getMapRetryCount()
                    + ", success=" + percent.format(mapPercent) + ", elapsed="
                    + getMapElapsedTime() + "ms )";

            final String reduce = "reduce( n=" + job.n + ", ntasks="
                    + getReduceTaskCount() + ", nretried="
                    + getReduceRetryCount() + ", success="
                    + percent.format(reducePercent) + ", elapsed="
                    + getReduceElapsedTime() + "ms )";

            return map + "\n" + reduce;
            
        }
        
        // used to format percentages.
        final protected Format percent = NumberFormat.getPercentInstance();
        
        /**
         * Run the job.
         * 
         * @return <i>this</i>
         */
        public JobState run(double minMapSuccessRate, double minReduceSuccessRate) {
            
            try {

                /*
                 * Notify everyone that this job is starting.
                 */
                start();

                /*
                 * Distribute the map tasks and wait for them to complete.
                 */
                final double percentMapSuccess;
                {

                    percentMapSuccess = map();
                    
                    System.out.println("Map operation "+percentMapSuccess+" success.");
                    
                    if(percentMapSuccess<minMapSuccessRate) {
                        
                        log.warn("Success rate less than threshold - aborting");
                        
                        return this;
                        
                    }
                    
                }

                /*
                 * Distribute the reduce tasks and wait for them to complete.
                 */
                final double percentReduceSuccess;
                {

                    percentReduceSuccess = reduce();
                   
                    System.out.println("Reduce operation "+percentReduceSuccess+" success.");

                    if(percentReduceSuccess<minReduceSuccessRate) {
                        
                        log.warn("Success rate less than threashold - aborting");
                        
                        return this;
                        
                    }

                }

                log.info("Done: "+status());
                
            } catch (Throwable t) {
                
                log.error( "Problem running job: "+t, t );
                
            } finally {

                /*
                 * Terminate the job.
                 */

                terminate();
                
            }

            return this;
            
        }
        
    }

    /**
     * Sets up embedded map and reduce services.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class EmbeddedJobState extends JobState {
        
        public EmbeddedJobState(MapReduceJob job, IBigdataClient client) {

            super(job,client);
            
        }
        
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
             * The map services. Each services is capable of running map tasks
             * in parallel.
             */
            mapServices = new IJobAndTaskService[numMapServicesToStart];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numMapServicesToStart; i++) {

                    mapServices[i] = new EmbeddedMapService(UUID.randomUUID(),
                            properties, client);

                }

            }

            /**
             * The reduce services. Each service is capable of running reduce
             * tasks in parallel.
             */
            reduceServices = new IJobAndTaskService[numReduceServicestoStart];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numReduceServicestoStart; i++) {

                    reduceServices[i] = new EmbeddedReduceService(UUID
                            .randomUUID(), properties, client);
                    
                }

            }

            super.setUp();
            
        }
        
    }
    
    /**
     * Run a map/reduce task.
     * 
     * @param args
     * 
     * @todo Support description and execution of a map/reduce using XML.
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
        MapReduceJob job = new CountKeywordJob(100/* m */, 1/* n */);
//        MapReduceJob job = new TestCountKeywordJob(1/* m */, 1/* n */);

        // non-zero to submit no more than this many map inputs.
        job.setMaxMapTasks( 10 );

        // the timeout for a map task.
//        job.setMapTaskTimeout(2*1000/*millis*/);
        
        // the timeout for a reduce task.
//        job.setReduceTaskTimeout(2*1000/*millis*/);
        
        // the maximum #of times a map task will be retried (zero disables retry).
//        job.setMaxMapTaskRetry(3);

        // the maximum #of times a reduce task will be retried (zero disables retry).
//        job.setMaxReduceTaskRetry(3);

        /**
         * Setup the client that will connect to federation on which the
         * intermediate results will be written.
         * 
         * @todo this is using embedded federation - use a distributed one but
         *       keep an option for testing that uses an embedded federation.
         *       For this we will need to specify the federation identifier so
         *       that the client can connect.
         */
        final IBigdataClient client;
        {
            
            Properties properties = new Properties();

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
            properties.setProperty(Options.NDATA_SERVICES, ""+Math.min(10,job.n));

            properties.setProperty(Options.FORCE_ON_COMMIT, ForceEnum.No.toString());

            client = new EmbeddedBigdataClient(properties);
            
            client.connect();
            
        }

        /*
         * Run the map/reduce operation.
         * 
         * Note: This is using embedded services to test map/reduce.
         */
        System.out.println(new EmbeddedJobState(job, client).run(.9d, .9d)
                .status());
        
        /*
         * Disconnect from the federation.
         */
        
        client.terminate();

    }

}
