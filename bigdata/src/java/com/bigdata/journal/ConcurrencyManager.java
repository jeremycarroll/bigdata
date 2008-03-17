package com.bigdata.journal;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Counters;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.concurrent.LockManager;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.Instrument.InstrumentDelta;
import com.bigdata.counters.Instrument.InstrumentInstantaneousAverage;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.DataService;
import com.bigdata.service.DataService.StatusTask;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Supports concurrent operations against named indices. The
 * {@link ConcurrencyManager} a {@link LockManager} to administer exclusive
 * locks on unisolated named indices and thereby identify a schedule of
 * operations such that access to an unisolated named index is always single
 * threaded while access to distinct unisolated named indices MAY be concurrent.
 * <p>
 * There are several thread pools that facilitate concurrency. They are:
 * <dl>
 * 
 * <dt>{@link #txWriteService}</dt>
 * <dd>This is used for the "active" phrase of transaction. Transactions read
 * from historical states of named indices during their active phase and buffer
 * the results on isolated indices. Since transactions never write on the
 * unisolated indices during their "active" phase distinct transactions may be
 * run with arbitrary concurrency. However, concurrent tasks for the same
 * transaction must obtain an exclusive lock on the isolated index that is used
 * to buffer their writes. A transaction that requests a commit using the
 * {@link ITransactionManagerService} results in a task being submitted to the
 * {@link #writeService}. Transactions are selected to commit once they have
 * acquired a lock on the corresponding unisolated indices, thereby enforcing
 * serialization of their write sets both among other transactions and among
 * unisolated writers. The commit itself consists of the standard validation and
 * merge phrases.</dd>
 * 
 * <dt>{@link #readService}</dt>
 * <dd>Concurrent unisolated readers running against the <strong>historical</strong>
 * state of a named index. No locking is imposed. Concurrency is limited by the
 * size of the thread pool, but large thread pools can reduce overall
 * performance..</dd>
 * 
 * <dt>{@link #writeService}</dt>
 * <dd>Concurrent unisolated writers running against the <strong>current</strong>
 * state of (or more more) named index(s) (the "live" or "mutable" index(s)).
 * The underlying {@link BTree} is NOT thread-safe. Therefore writers MUST
 * predeclare their locks, which allows us to avoid deadlocks altogether. This
 * is also used to schedule the commit phrase of transactions. </dd>
 * 
 * <dt>{@link #commitService}</dt>
 * <dd>Schedules periodic group commits. </dd>
 * 
 * <dt>{@link #statusService}</dt>
 * <dd>Periodically {@link StatusTask#log}s counters.</dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConcurrencyManager implements IConcurrencyManager {

    protected static final Logger log = Logger.getLogger(ConcurrencyManager.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    /**
     * Options for the {@link ConcurrentManager}.
     * <p>
     * Note: The main factors that influence the throughput of group commit are
     * {@link #WRITE_SERVICE_CORE_POOL_SIZE} and
     * {@link #WRITE_SERVICE_QUEUE_CAPACITY}. So far, more is better (at least
     * up to 1000 each) and you always want the queue capacity to be at least as
     * large as the core pool size. Pre-starting the core pool threads appears
     * to offer a minor advantage on startup. Note that there is also a strong
     * effect as the JVM performs optimizations on the running code, so
     * randomize your tests.
     * <p>
     * See {@link StressTestGroupCommit} for performance tuning.
     * 
     * @todo add option for retryCount, timeout (from submission until the group
     *       commit).
     * 
     * @todo add options: (a) to prestart all core threads; (b) set the maximum
     *       #of threads; and (c) set the maximum queue capacity for the read
     *       and tx thread pools.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {
    
        /**
         * <code>txServicePoolSize</code> - The #of threads in the pool
         * handling concurrent transactions.
         * 
         * @see #DEFAULT_TX_SERVICE_CORE_POOL_SIZE
         */
        public static final String TX_SERVICE_CORE_POOL_SIZE = "txServiceCorePoolSize";
    
        /**
         * The default #of threads in the transaction service thread pool.
         */
        public final static String DEFAULT_TX_SERVICE_CORE_POOL_SIZE = "50";
    
        /**
         * <code>readServicePoolSize</code> - The #of threads in the pool
         * handling concurrent unisolated read requests on named indices.
         * 
         * @see #DEFAULT_READ_SERVICE_CORE_POOL_SIZE
         */
        public static final String READ_SERVICE_CORE_POOL_SIZE = "readServiceCorePoolSize";
    
        /**
         * The default #of threads in the read service thread pool.
         * 
         * @see #READ_SERVICE_CORE_POOL_SIZE
         */
        public final static String DEFAULT_READ_SERVICE_CORE_POOL_SIZE = "50";
    
        /**
         * The target for the #of threads in the pool handling concurrent
         * unisolated write on named indices.
         * 
         * @see #DEFAULT_WRITE_SERVICE_CORE_POOL_SIZE
         */
        public final static String WRITE_SERVICE_CORE_POOL_SIZE = "writeServiceCorePoolSize";
    
        /**
         * The default #of threads in the write service thread pool (200).
         * 
         * @todo This SHOULD automatically increase up to
         *       {@link #WRITE_SERVICE_MAXIMUM_POOL_SIZE} under demand but I
         *       have not been observing that. If it does, then drop the default
         *       value down to something much smaller, e.g., 10-50.
         * 
         * @todo revisit this value after I modify the
         *       {@link WriteExecutorService} to checkpoint indices so that it
         *       does not need to wait for nrunning to reach zero and so that it
         *       can abort individual tasks rather than discarding entire commit
         *       groups. Use {@link StressTestConcurrentUnisolatedIndices} to
         *       examine the behavior for just the writeService, but choose the
         *       default in terms of a more complete test that loads all three
         *       queues (read, write, and tx).
         */
        public final static String DEFAULT_WRITE_SERVICE_CORE_POOL_SIZE = "200";
        
        /**
         * The maximum #of threads allowed in the pool handling concurrent
         * unisolated write on named indices.
         * 
         * @see #DEFAULT_WRITE_SERVICE_MAXIMUM_POOL_SIZE
         */
        public final static String WRITE_SERVICE_MAXIMUM_POOL_SIZE = "writeServiceMaximumPoolSize";
    
        /**
         * The default for the maximum #of threads in the write service thread
         * pool.
         */
        public final static String DEFAULT_WRITE_SERVICE_MAXIMUM_POOL_SIZE = "1000";
        
        /**
         * When true, the write service will be prestart all of its worker
         * threads (default false).
         * 
         * @see #DEFAULT_WRITE_SERVICE_PRESTART_ALL_CORE_THREADS
         */
        public final static String WRITE_SERVICE_PRESTART_ALL_CORE_THREADS = "writeServicePrestartAllCoreThreads";
    
        /**
         * The default for {@link #WRITE_SERVICE_PRESTART_ALL_CORE_THREADS}.
         */
        public final static String DEFAULT_WRITE_SERVICE_PRESTART_ALL_CORE_THREADS = "false";
    
        /**
         * The maximum depth of the write service queue before newly submitted
         * tasks will block the caller -or- ZERO (0) to use a queue with an
         * unlimited capacity.
         * 
         * @see #WRITE_SERVICE_CORE_POOL_SIZE
         * @see #DEFAULT_WRITE_SERVICE_QUEUE_CAPACITY
         */
        public static final String WRITE_SERVICE_QUEUE_CAPACITY = "writeServiceQueueCapacity";
    
        /**
         * The default maximum depth of the write service queue (1000).
         */
        public static final String DEFAULT_WRITE_SERVICE_QUEUE_CAPACITY = "1000";
        
        /**
         * The maximum time in milliseconds that
         * {@link ConcurrencyManager#shutdown()} will wait termination of the
         * various services -or- ZERO (0) to wait forever (default is to wait
         * forever).
         * <p>
         * Note: The journal will no longer accept begin to execute queued tasks
         * once shutdown begins so this primarily effects whether or not tasks
         * that are already executing will be allowed to run until completion.
         * <p>
         * Note: You can use {@link ConcurrencyManager#shutdownNow()} to
         * terminate the journal immediately.
         * <p>
         * Note: Abrupt shutdown of the journal is always safe, but changes that
         * have not been committed will not be there on restart.
         * 
         * @todo who should define this parameter? It should effect all
         *       shutdowns for a {@link Journal} or {@link DataService}.
         * 
         * @see #DEFAULT_SHUTDOWN_TIMEOUT
         */
        public final static String SHUTDOWN_TIMEOUT = "shutdownTimeout";

        /**
         * The default timeout for {@link IConcurrencyManager#shutdown()}.
         */
        public final static String DEFAULT_SHUTDOWN_TIMEOUT = "0";
        
    }

    /**
     * The properties specified to the ctor.
     */
    final private Properties properties;

    /**
     * The object managing local transactions. 
     */
    final private ILocalTransactionManager transactionManager;
    
    /**
     * The object managing the resources on which the indices are stored.
     */
    final private IResourceManager resourceManager;
    
    /**
     * The local time at which this service was started.
     */
    final private long serviceStartTime = System.currentTimeMillis();
    
    /**
     * <code>true</code> until the service is shutdown.
     */
    private boolean open = true;
    
    /**
     * Pool of threads for handling concurrent read/write transactions on named
     * indices. Distinct transactions are not inherently limited in their
     * concurrency, but concurrent operations within a single transaction MUST
     * obtain an exclusive lock on the isolated index(s) on the temporary store.
     * The size of the thread pool for this service governs the maximum
     * practical concurrency for transactions.
     * <p>
     * Transactions always read from historical data and buffer their writes
     * until they commit. Transactions that commit MUST acquire unisolated
     * writable indices for each index on which the transaction has written.
     * Once the transaction has acquired those writable indices it then runs its
     * commit phrase as an unisolated operation on the {@link #writeService}.
     */
    final protected ThreadPoolExecutor txWriteService;

    /**
     * Pool of threads for handling concurrent unisolated read operations on
     * named indices using <strong>historical</strong> data. Unisolated
     * read operations from historical data are not inherently limited in
     * their concurrency and do not conflict with unisolated writers. The
     * size of the thread pool for this service governs the maximum
     * practical concurrency for unisolated readers.
     * <p>
     * Note that unisolated read operations on the <strong>current</strong>
     * state of an index DO conflict with unisolated writes and such tasks
     * must be run as unisolated writers.
     * <p>
     * Note: unisolated readers of historical data do require the rention of
     * historical commit records (which may span more than one logical
     * journal) until the reader terminates.
     */
    final protected ThreadPoolExecutor readService;

    /**
     * Pool of threads for handling concurrent unisolated write operations on
     * named indices. Unisolated writes are always performed against the current
     * state of the named index. Unisolated writes for the same named index (or
     * index partition) conflict and must be serialized. The size of this thread
     * pool and the #of distinct named indices together govern the maximum
     * practical concurrency for unisolated writers.
     * <p>
     * Serialization of access to unisolated named indices is acomplished by
     * gaining an exclusive lock on the unisolated named index.
     */
    final protected WriteExecutorService writeService;

    /**
     * The timeout for {@link #shutdown()} -or- ZERO (0L) to wait for ever.
     */
    final long shutdownTimeout;

    /**
     * Manages 2PL locking for writes on unisolated named indices.
     */
    final protected LockManager<String> lockManager;

    /**
     * An object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {
        
        return new Properties(properties);
        
    }
    
    protected void assertOpen() {
        
        if (!open)
            throw new IllegalStateException();
        
    }
    
    public WriteExecutorService getWriteService() {
        
        assertOpen();
        
        return writeService;
        
    }
    
    public LockManager<String> getLockManager() {
        
        assertOpen();
        
        return lockManager;
        
    }
    
    public ILocalTransactionManager getTransactionManager() {
        
        assertOpen();
        
        return transactionManager;
        
    }
    
    public IResourceManager getResourceManager() {
        
        assertOpen();
        
        return resourceManager;
        
    }
    
    /**
     * Shutdown the journal (running tasks will run to completion, but no new
     * tasks will start).
     * <p>
     * Note: You SHOULD use this method rather than {@link #close()} for normal
     * shutdown of the journal.
     * 
     * @see #shutdownNow()
     */
    public void shutdown() {

        assertOpen();

        open = false;
        
        log.info("begin");

        // time when shutdown begins.
        final long begin = System.currentTimeMillis();

        /*
         * Note: when the timeout is zero we approximate "forever" using
         * Long.MAX_VALUE.
         */

        final long shutdownTimeout = this.shutdownTimeout == 0L ? Long.MAX_VALUE
                : this.shutdownTimeout;
        
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        
        txWriteService.shutdown();

        readService.shutdown();

        writeService.shutdown();

        try {

            log.info("Awaiting transaction service termination");
            
            long elapsed = System.currentTimeMillis() - begin;
            
            if(!txWriteService.awaitTermination(shutdownTimeout-elapsed, unit)) {
                
                log.warn("Transaction service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting transaction service termination.", ex);
            
        }

        try {

            log.info("Awaiting read service termination");

            long elapsed = System.currentTimeMillis() - begin;
            
            if(!readService.awaitTermination(shutdownTimeout-elapsed, unit)) {
                
                log.warn("Read service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting read service termination.", ex);
            
        }

        try {

            long elapsed = System.currentTimeMillis() - begin;
            
            long timeout = shutdownTimeout-elapsed;

            log.info("Awaiting write service termination: will wait "+timeout+"ms");

            if(!writeService.awaitTermination(timeout, unit)) {
                
                log.warn("Write service termination : timeout");
                
            }
            
        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting write service termination.", ex);
            
        }
    
        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");
        
    }

    /**
     * Immediate shutdown (running tasks are cancelled rather than being
     * permitted to complete).
     * 
     * @see #shutdown()
     */
    public void shutdownNow() {

        assertOpen();

        open = false;
        
        log.info("begin");
        
        final long begin = System.currentTimeMillis();
        
        txWriteService.shutdownNow();

        readService.shutdownNow();

        writeService.shutdownNow();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");

    }

    /**
     * (Re-)open a journal supporting concurrent operations.
     * 
     * @param properties
     *            See {@link ConcurrencyManager.Options}.
     * @param transactionManager
     *            The object managing the local transactions.
     * @param resourceManager
     *            The object managing the resources on which the indices are
     *            stored.
     */
    public ConcurrencyManager(Properties properties,
            ILocalTransactionManager transactionManager,
            IResourceManager resourceManager) {

        if (properties == null)
            throw new IllegalArgumentException();

        if (transactionManager == null)
            throw new IllegalArgumentException();

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.properties = properties;
        
        this.transactionManager = transactionManager; 
         
        this.resourceManager = resourceManager;
        
        String val;

        final int txServicePoolSize;
        final int readServicePoolSize;

        // txServicePoolSize
        {

            val = properties.getProperty(ConcurrencyManager.Options.TX_SERVICE_CORE_POOL_SIZE,
                    ConcurrencyManager.Options.DEFAULT_TX_SERVICE_CORE_POOL_SIZE);

            txServicePoolSize = Integer.parseInt(val);

            if (txServicePoolSize < 1) {

                throw new RuntimeException("The '"
                        + ConcurrencyManager.Options.TX_SERVICE_CORE_POOL_SIZE
                        + "' must be at least one.");

            }

            log.info(ConcurrencyManager.Options.TX_SERVICE_CORE_POOL_SIZE + "=" + txServicePoolSize);

        }

        // readServicePoolSize
        {

            val = properties.getProperty(ConcurrencyManager.Options.READ_SERVICE_CORE_POOL_SIZE,
                    ConcurrencyManager.Options.DEFAULT_READ_SERVICE_CORE_POOL_SIZE);

            readServicePoolSize = Integer.parseInt(val);

            if (readServicePoolSize < 1) {

                throw new RuntimeException("The '"
                        + ConcurrencyManager.Options.READ_SERVICE_CORE_POOL_SIZE
                        + "' must be at least one.");

            }

            log
                    .info(ConcurrencyManager.Options.READ_SERVICE_CORE_POOL_SIZE + "="
                            + readServicePoolSize);

        }

        // shutdownTimeout
        {

            val = properties.getProperty(ConcurrencyManager.Options.SHUTDOWN_TIMEOUT,
                    ConcurrencyManager.Options.DEFAULT_SHUTDOWN_TIMEOUT);

            shutdownTimeout = Long.parseLong(val);

            if (shutdownTimeout < 0) {

                throw new RuntimeException("The '" + ConcurrencyManager.Options.SHUTDOWN_TIMEOUT
                        + "' must be non-negative.");

            }

            log.info(ConcurrencyManager.Options.SHUTDOWN_TIMEOUT + "=" + shutdownTimeout);

        }

        // setup thread pool for concurrent transactions.
        txWriteService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                txServicePoolSize, DaemonThreadFactory.defaultThreadFactory());

        // setup thread pool for unisolated read operations.
        readService = (ThreadPoolExecutor) Executors
                .newFixedThreadPool(readServicePoolSize, DaemonThreadFactory
                        .defaultThreadFactory());

        // setup thread pool for unisolated write operations.
        {
            
            final int writeServiceCorePoolSize;
            final int writeServiceMaximumPoolSize;
            final int writeServiceQueueCapacity;
            final boolean writeServicePrestart;

            // writeServiceCorePoolSize
            {

                writeServiceCorePoolSize = Integer.parseInt(properties.getProperty(
                        ConcurrencyManager.Options.WRITE_SERVICE_CORE_POOL_SIZE,
                        ConcurrencyManager.Options.DEFAULT_WRITE_SERVICE_CORE_POOL_SIZE));

                if (writeServiceCorePoolSize < 1) {

                    throw new RuntimeException("The '"
                            + ConcurrencyManager.Options.WRITE_SERVICE_CORE_POOL_SIZE
                            + "' must be at least one.");

                }

                log.info(ConcurrencyManager.Options.WRITE_SERVICE_CORE_POOL_SIZE + "="
                        + writeServiceCorePoolSize);

            }

            // writeServiceMaximumPoolSize
            {

                writeServiceMaximumPoolSize = Integer.parseInt(properties.getProperty(
                        ConcurrencyManager.Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
                        ConcurrencyManager.Options.DEFAULT_WRITE_SERVICE_MAXIMUM_POOL_SIZE));

                if (writeServiceMaximumPoolSize < writeServiceCorePoolSize) {

                    throw new RuntimeException("The '"
                            + ConcurrencyManager.Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE
                            + "' must be greater than the core pool size.");

                }

                log.info(ConcurrencyManager.Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE + "="
                        + writeServiceMaximumPoolSize);

            }

            // writeServiceQueueCapacity
            {

                writeServiceQueueCapacity = Integer.parseInt(properties.getProperty(
                        ConcurrencyManager.Options.WRITE_SERVICE_QUEUE_CAPACITY,
                        ConcurrencyManager.Options.DEFAULT_WRITE_SERVICE_QUEUE_CAPACITY));

                if (writeServiceQueueCapacity < 0) {

                    throw new RuntimeException("The '"
                            + ConcurrencyManager.Options.WRITE_SERVICE_QUEUE_CAPACITY
                            + "' must be non-negative.");

                }

                if (writeServiceQueueCapacity<writeServiceCorePoolSize) {

                    throw new RuntimeException("The '"
                            + ConcurrencyManager.Options.WRITE_SERVICE_QUEUE_CAPACITY
                            + "' must be greater than the "
                            + ConcurrencyManager.Options.WRITE_SERVICE_CORE_POOL_SIZE);

                }

                log.info(ConcurrencyManager.Options.WRITE_SERVICE_QUEUE_CAPACITY+ "="
                        + writeServiceQueueCapacity);

            }

            // writeServicePrestart
            {
                
                writeServicePrestart = Boolean.parseBoolean(properties.getProperty(
                        ConcurrencyManager.Options.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS,
                        ConcurrencyManager.Options.DEFAULT_WRITE_SERVICE_PRESTART_ALL_CORE_THREADS));
                
                log.info(ConcurrencyManager.Options.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS + "="
                        + writeServicePrestart);

            }
            
            final BlockingQueue<Runnable> queue =
                ((writeServiceQueueCapacity == 0 || writeServiceQueueCapacity > 5000)
                        ? new LinkedBlockingQueue<Runnable>()
                        : new ArrayBlockingQueue<Runnable>(writeServiceQueueCapacity)
                        );
            
            writeService = new WriteExecutorService(resourceManager,
                    writeServiceCorePoolSize, writeServiceMaximumPoolSize,
                    queue, DaemonThreadFactory.defaultThreadFactory());

            if (writeServicePrestart) {

                writeService.prestartAllCoreThreads();
                
            }
            
            // Setup the lock manager used by the write service.
            {

                /*
                 * Note: this means that any operation that writes on unisolated
                 * indices MUST specify in advance those index(s) on which it
                 * will write.
                 */

                final boolean predeclareLocks = true;

                /*
                 * Create the lock manager. The capacity is only used if we need to
                 * detect deadlocks if we are NOT pre-declaring locks. When we
                 * pre-declare locks deadlocks are NOT possible.
                 */

                lockManager = new LockManager<String>(
                        writeServiceMaximumPoolSize, predeclareLocks);

            }
            
        }
        
    }

    private CounterSet countersRoot;
    
    /**
     * Return the {@link CounterSet}.
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null){

            countersRoot = new CounterSet();

            // elapsed time since the service started (milliseconds).
            countersRoot.addCounter("elapsed", new Instrument<Long>(){
                public Long getValue() {
                    return System.currentTimeMillis() - serviceStartTime;
                }
            });
            
            /*
             * task statistics by class.
             */
            {

                countersRoot.makePath("/Tasks/Unisolated").attach(countersU
                        .createCounters());

                countersRoot.makePath("/Tasks/ReadCommitted").attach(countersRC
                        .createCounters());

                countersRoot.makePath("/Tasks/Historical Read").attach(countersHR
                        .createCounters());

                countersRoot.makePath("/Tasks/Transaction").attach(countersTX
                        .createCounters());

            }
            // readService
            {
                
                final CounterSet tmp = countersRoot.makePath("Read Service");
                
                final ThreadPoolExecutor service = readService;
            
                tmp.addCounter("#active",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getActiveCount();
                            }
                        });
                
                tmp.addCounter("#queued",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getQueue().size();
                            }
                        });

                tmp.addCounter("#completed",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getCompletedTaskCount();
                            }
                        });
                
                tmp.addCounter("poolSize",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getPoolSize();
                            }
                        });

                tmp.addCounter("largestPoolSize",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getLargestPoolSize();
                            }
                        });

            }

            // txWriteService
            {
                
                final CounterSet tmp = countersRoot.makePath("Transaction Write Service");
                
                final ThreadPoolExecutor service = txWriteService;
            
                tmp.addCounter("#active",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getActiveCount();
                            }
                        });
                
                tmp.addCounter("#queued",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getQueue().size();
                            }
                        });

                tmp.addCounter("#completed",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getCompletedTaskCount();
                            }
                        });
                
                tmp.addCounter("poolSize",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getPoolSize();
                            }
                        });

                tmp.addCounter("largestPoolSize",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getLargestPoolSize();
                            }
                        });

            }
            
            // writeService
            {
                
                final CounterSet tmp = countersRoot.makePath("Unisolated Write Service");
                
                final WriteExecutorService service = writeService;
            
                tmp.addCounter("#active",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getActiveCount();
                            }
                        });
                
                tmp.addCounter("#queued",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getQueue().size();
                            }
                        });

                tmp.addCounter("#completed",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getCompletedTaskCount();
                            }
                        });
                
                tmp.addCounter("poolSize",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getPoolSize();
                            }
                        });

                tmp.addCounter("largestPoolSize",
                        new Instrument<Integer>() {
                            public Integer getValue() {
                                return service.getLargestPoolSize();
                            }
                        });

                /*
                 * data only available for the write service.
                 */
                tmp.addCounter("#commits",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getGroupCommitCount();
                            }
                        });

                tmp.addCounter("#aborts",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getAbortCount();
                            }
                        });

                tmp.addCounter("overflowCount",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getOverflowCount();
                            }
                        });

                tmp.addCounter("failedTaskCount",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getFailedTaskCount();
                            }
                        });

                tmp.addCounter("successTaskCount",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getSuccessTaskCount();
                            }
                        });

                tmp.addCounter("committedTaskCount",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getCommittedTaskCount();
                            }
                        });

                tmp.addCounter("maxLatencyUntilCommit",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getMaxLatencyUntilCommit();
                            }
                        });

                tmp.addCounter("maxCommitLatency",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getMaxCommitLatency();
                            }
                        });

                tmp.addCounter("maxRunning",
                        new Instrument<Long>() {
                            public Long getValue() {
                                return service.getMaxRunning();
                            }
                        });

            }
            
        }
        
        return countersRoot;
        
    }

    /**
     * Submit a task (asynchronous). Tasks will execute asynchronously in the
     * appropriate thread pool with as much concurrency as possible.
     * <p>
     * Note: Unisolated write tasks will NOT return before the next group commit
     * (exceptions may be thrown if the task fails or the commit fails). The
     * purpose of group commits is to provide higher throughput for writes on
     * the store by only syncing the data to disk periodically rather than after
     * every write. Group commits are scheduled by the {@link #commitService}.
     * The trigger conditions for group commits may be configured using
     * {@link ConcurrencyManager.Options}. If you are using the store in a single threaded context
     * then you may set {@link Options#WRITE_SERVICE_CORE_POOL_SIZE} to ONE (1)
     * which has the effect of triggering commit immediately after each
     * unisolated write. However, note that you can not sync a disk more than ~
     * 30-40 times per second so your throughput in write operations per second
     * will never exceed that for a single-threaded application writing on a
     * hard disk. (Your mileage can vary if you are writing on a transient store
     * or using a durable medium other than disk).
     * <p>
     * Note: The isolated indices used by a {@link IsolationEnum#ReadWrite}
     * transaction are NOT thread-safe. Therefore a partial order is imposed
     * over concurrent tasks for the <strong>same</strong> transaction that
     * seek to read or write on the same index(s). Full concurrency is allowed
     * when different transactions access the same index(s), but write-write
     * conflicts MAY be detected during commit processing.
     * <p>
     * Note: The following exceptions MAY be wrapped by {@link Future#get()} for
     * tasks submitted via this method:
     * <dl>
     * <dt>{@link ValidationError}</dt>
     * <dd>An unisolated write task was attempting to commit the write set for
     * a transaction but validation failed. You may retry the entire
     * transaction.</dd>
     * <dt>{@link InterruptedException}</dt>
     * <dd>A task was interrupted during execution and before the task had
     * completed normally. You MAY retry the task, but note that this exception
     * is also generated when tasks are cancelled when the journal is being
     * {@link #shutdown()} after the timeout has expired or
     * {@link #shutdownNow()}. In either of these cases the task will not be
     * accepted by the journal.</dd>
     * <dt></dt>
     * <dd></dd>
     * </dl>
     * 
     * @param task
     *            The task.
     * 
     * @return The {@link Future} that may be used to resolve the outcome of the
     *         task.
     * 
     * @exception RejectedExecutionException
     *                if task cannot be scheduled for execution (typically the
     *                queue has a limited capacity and is full)
     * @exception NullPointerException
     *                if task null
     */
    public Future<Object> submit(AbstractTask task) {

        assertOpen();
        
        // Note that time the task was submitted for execution.
        task.nanoTime_submitTask = System.nanoTime();
        
        if( task.readOnly ) {

            /*
             * Reads against historical data do not require concurrency control.
             * 
             * The only distinction between a transaction and an unisolated read
             * task is the choice of the historical state from which the task
             * will read. A ReadOnly transaction reads from the state of the
             * index as of the start time of the transaction. A ReadCommitted
             * transaction and an unisolated reader both read from the last
             * committed state of the index.
             */

            log.info("Submitted to the read service: "
                    + task.getClass().getName() + ", timestamp="
                    + task.timestamp);

            return submitWithDynamicLatency(task, readService);

        } else {

            if (task.isTransaction) {

                /*
                 * A task that reads from historical data and writes on isolated
                 * indices backed by a temporary store. Concurrency control is
                 * required for the isolated indices on the temporary store, but
                 * not for the reads against the historical data.
                 */

                log.info("Submitted to the transaction service: "
                        + task.getClass().getName() + ", timestamp="
                        + task.timestamp);

                return submitWithDynamicLatency(task, txWriteService);

            } else {

                /*
                 * A task that reads from and writes on "live" indices. The live
                 * indices are NOT thread-safe. Concurrency control provides a
                 * partial order over the executing tasks such that there is
                 * never more than one task with access to a given live index.
                 */

                log.info("Submitted to the write service: "
                        + task.getClass().getName() + ", timestamp="
                        + task.timestamp);

                return submitWithDynamicLatency(task, writeService);

            }

        }
        
    }
    
    /**
     * Submit a task to a service, dynamically imposing latency on the caller
     * based on the #of tasks already in the queue for that service.
     * 
     * @param task
     *            The task.
     * @param service
     *            The service.
     * 
     * @return The {@link Future}.
     */
    private Future<Object> submitWithDynamicLatency(AbstractTask task,ExecutorService service) {

        if(service instanceof ThreadPoolExecutor) {

            BlockingQueue<Runnable> queue = ((ThreadPoolExecutor)service).getQueue();
        
            final int queueCapacity = queue.remainingCapacity();
            
            // @todo what is the right thing to do here from queuing theory for backing off?
            if (queue.size() * 1.10 >= queueCapacity) {
                
                try {
                    
                    /*
                     * Note: Any delay here what so ever causes the #of tasks in
                     * a commit group to be governed primarily by the CORE pool
                     * size.
                     */

                    if(INFO) System.err.print("z");
                    
                    Thread.sleep(50/*ms*/);
                    
                } catch (InterruptedException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }
            
        }

        return service.submit(task);

    }

    /**
     * Executes the given tasks, returning a list of Futures holding their
     * status and results when all complete. Note that a completed task could
     * have terminated either normally or by throwing an exception. The results
     * of this method are undefined if the given collection is modified while
     * this operation is in progress.
     * <p>
     * Note: Contract is per {@link ExecutorService#invokeAll(Collection)}
     * 
     * @param tasks
     *            The tasks.
     * 
     * @return Their {@link Future}s.
     * 
     * @exception InterruptedException
     *                if interrupted while waiting, in which case unfinished
     *                tasks are cancelled.
     * @exception NullPointerException
     *                if tasks or any of its elements are null
     * @exception RejectedExecutionException
     *                if any task cannot be scheduled for execution
     */
    public List<Future<Object>> invokeAll(Collection<AbstractTask> tasks)
            throws InterruptedException {

        assertOpen();
        
        List<Future<Object>> futures = new LinkedList<Future<Object>>();

        boolean done = false;

        try {

            // submit all.
            
            for (AbstractTask task : tasks) {

                futures.add(submit(task));

            }

            // await all futures.
            
            for (Future<Object> f : futures) {

                if (!f.isDone()) {

                    try {

                        f.get();

                    } catch (ExecutionException ex) {

                        // ignore.
                        
                    } catch (CancellationException ex) {

                        // ignore.

                    }

                }
                
            }

            done = true;
            
            return futures;
            
        } finally {
            
            if (!done) {

                // At least one future did not complete.
                
                for (Future<Object> f : futures) {

                    if(!f.isDone()) {

                        f.cancel(true/* mayInterruptIfRunning */);
                        
                    }
                    
                }
                
            }
        
        }

    }
    
    /**
     * Executes the given tasks, returning a list of Futures holding their
     * status and results when all complete or the timeout expires, whichever
     * happens first. Note that a completed task could have terminated either
     * normally or by throwing an exception. The results of this method are
     * undefined if the given collection is modified while this operation is in
     * progress.
     * <p>
     * Note: Contract is based on
     * {@link ExecutorService#invokeAll(Collection, long, TimeUnit)} but only
     * the {@link Future}s of the submitted tasks are returned.
     * 
     * @param tasks
     *            The tasks.
     * 
     * @return The {@link Future}s of all tasks that were
     *         {@link #submit(AbstractTask) submitted} prior to the expiration
     *         of the timeout.
     * 
     * @exception InterruptedException
     *                if interrupted while waiting, in which case unfinished
     *                tasks are cancelled.
     * @exception NullPointerException
     *                if tasks or any of its elements are null
     * @exception RejectedExecutionException
     *                if any task cannot be scheduled for execution
     */
    public List<Future<Object>> invokeAll(Collection<AbstractTask> tasks,
            long timeout, TimeUnit unit) throws InterruptedException {

        assertOpen();
        
        List<Future<Object>> futures = new LinkedList<Future<Object>>();

        boolean done = false;
        
        long nanos = unit.toNanos(timeout);
        
        long lastTime = System.nanoTime();
        
        try {

            // submit all.
            
            for (AbstractTask task : tasks) {

                long now = System.nanoTime();
                
                nanos -= now - lastTime;
                
                lastTime = now;
                
                if (nanos <= 0) {

                    // timeout.
                    
                    return futures;
                    
                }
                
                futures.add(submit(task));

            }

            // await all futures.
            
            for (Future<Object> f : futures) {

                if (!f.isDone()) {

                    if (nanos <= 0) { 
                     
                        // timeout
                        
                        return futures;
                        
                    }
                    
                    try {

                        f.get(nanos, TimeUnit.NANOSECONDS);

                    } catch (TimeoutException ex) {

                        return futures;

                    } catch (ExecutionException ex) {

                        // ignore.

                    } catch (CancellationException ex) {

                        // ignore.

                    }

                    long now = System.nanoTime();
                    
                    nanos -= now - lastTime;
                    
                    lastTime = now;

                }

            }

            done = true;

            return futures;

        } finally {

            if (!done) {

                // At least one future did not complete.

                for (Future<Object> f : futures) {

                    if (!f.isDone()) {

                        f.cancel(true/* mayInterruptIfRunning */);

                    }

                }
                
            }
        
        }
        
    }

    /*
     * Per task counters.
     */

    /**
     * Class captures various data about the execution of tasks. These data are
     * collected by the {@link ConcurrencyManager} in four groups corresponding
     * to {@link ITx#UNISOLATED}, {@link ITx#READ_COMMITTED}, read historical,
     * and transaction tasks.
     */
    static public class TaskCounters {

        /** #of tasks executed. */
        public long taskCount;
        
        /** #of tasks that failed. */
        public long failCount;

        /** #of tasks that succeeded. */
        public long successCount;
        
        /**
         * Cumulative elapsed time while tasks await assignment to a worker
         * thread.
         * <p>
         * Note: Since this is aggregated over concurrent tasks the reported
         * elapsed time MAY exceed the actual elapsed time during which those
         * tasks were executed.
         */
        public long workerNanoLatency;

        /**
         * Cumulative elapsed time consumed by tasks while assigned to a worker
         * thread.
         * <p>
         * Note: Since this is aggregated over concurrent tasks the reported
         * elapsed time MAY exceed the actual elapsed time during which those
         * tasks were executed.
         */
        public long workerNanoTime;

        /**
         * Cumulative elapsed time consumed by tasks while waiting for an
         * resource lock.
         * <p>
         * Note: this value will only be non-zero for {@link ITx#UNISOLATED}
         * tasks since they are the only tasks that wait for locks.
         */
        public long workerLockLatency;
        
        /** Ctor */
        public TaskCounters() {
            
        }
        
        /**
         * Adds counters to this set.
         */
        public void add(TaskCounters c) {
            
            taskCount += c.taskCount;
            
            failCount += c.failCount;
            
            successCount += c.successCount;
            
            workerNanoTime += c.workerNanoTime;
            
            workerNanoLatency += c.workerNanoLatency;

            workerLockLatency += c.workerLockLatency;
                
        }
        
        /**
         * Creates an {@link CounterSet} that reports on the counters in this
         * {@link TaskCounters} object.
         * 
         * @return The new {@link CounterSet}.
         */
        public CounterSet createCounters() {

            CounterSet root = new CounterSet();

            /*
             * direct reporting counters.
             */
            
            root.addCounter("taskCount", new Instrument<Long>() {
                public Long getValue() {
                    return taskCount;
                }
            });

            root.addCounter("failCount", new Instrument<Long>() {
                public Long getValue() {
                    return failCount;
                }
            });

            root.addCounter("successCount", new Instrument<Long>() {
                public Long getValue() {
                    return successCount;
                }
            });

            /*
             * counter per second.
             */
            
            root.addCounter("Average Tasks Per Second", new InstrumentDelta() {
                protected long sample() {
                    return taskCount;
                }
            });

            /*
             * The sample period.
             * 
             * @todo this is one second. A longer sample period is going to
             * produce more stable results, e.g., 60 seconds, but you have to be
             * running the service for at least that long to get your first
             * sample.
             */
            final long samplePeriod = 1;
            final TimeUnit samplePeriodUnits = TimeUnit.SECONDS;
            
            // Scaling factor converts nanoseconds to milliseconds.
            final double scalingFactor = 1d / TimeUnit.NANOSECONDS.convert(1,
                    TimeUnit.MILLISECONDS);
            
            /*
             * Instantaneous average latency in milliseconds until a task begins
             * to execute in a given sample period.
             */
            root.addCounter("Average Task Latency", new InstrumentInstantaneousAverage(
                    samplePeriod, samplePeriodUnits, scalingFactor) {
                protected long sampleEventCounter() {return taskCount;}
                protected long sampleCumulativeEventTime() {return workerNanoLatency;}
            });

            /*
             * Instantaneous average latency in milliseconds of the time that a
             * task is waiting for exclusive locks (zero unless the task is
             * unisolated).
             */
            root.addCounter("Average Lock Latency", new InstrumentInstantaneousAverage(
                    samplePeriod, samplePeriodUnits, scalingFactor) {
                protected long sampleEventCounter() {return taskCount;}
                protected long sampleCumulativeEventTime() {return workerLockLatency;}
            });

            /*
             * Instantaneous average latency in milliseconds of the time that a
             * task is assigned to a worker thread.
             */
            root.addCounter("Average Task Time", new InstrumentInstantaneousAverage(
                    samplePeriod, samplePeriodUnits, scalingFactor) {
                protected long sampleEventCounter() {return taskCount;}
                protected long sampleCumulativeEventTime() {return workerNanoTime;}
            });

            return root;

        }
        
    }

    /** Counters for {@link ITx#UNISOLATED} tasks. */
    public TaskCounters countersU  = new TaskCounters();
    
    /** Counters for {@link ITx#READ_COMMITTED} tasks. */
    public TaskCounters countersRC = new TaskCounters();
    
    /** Counters for transactions tasks. */
    public TaskCounters countersTX = new TaskCounters();
    
    /** Counters for historical read tasks. */
    public TaskCounters countersHR = new TaskCounters();
    
    /*
     * Per index counters.
     */
    
    private Map<String/* name */, Counters> indexCounters = new HashMap<String, Counters>();

    private Counters totalCounters = new Counters();
    
    /**
     * Return the aggregated counters for all named indices accessed by an
     * {@link AbstractTask} with either {@link ITx#UNISOLATED} or
     * {@link ITx#READ_COMMITTED} isolation since the {@link #resetCounters()}
     * was last invoked (typically at the last journal overflow event).
     * 
     * @see ResourceManager#overflow()
     */
    public Counters getTotalCounters() {
        
        synchronized(totalCounters) {

            return new Counters(totalCounters);
            
        }
        
    }

    /**
     * Return the aggregated counters for the named index as accessed by an
     * {@link AbstractTask} with either {@link ITx#UNISOLATED} or
     * {@link ITx#READ_COMMITTED} isolation.
     * <p>
     * Note: The per-index counters are reset by {@link #resetCounters()}
     * 
     * @param name
     *            The name of the index.
     *            
     * @return The counters for that index -or- <code>null</code> if the index
     *         has not been accessed by an {@link AbstractTask} with either
     *         {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} isolation
     *         since the counters were last reset.
     */
    public Counters getCounters(String name) {
        
        return indexCounters.get(name);
        
    }

    /**
     * Return and then reset the total and per-index counters for each named
     * indices accessed by an {@link AbstractTask} with either
     * {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} isolation.
     */
    public Map<String/* name */, Counters> resetCounters() {

        final Map<String,Counters> tmp;
        
        synchronized (totalCounters) {

            tmp = indexCounters;
            
            indexCounters = new HashMap<String,Counters>();

            totalCounters = new Counters();

        }
        
        return tmp;
        
    }
    
    /**
     * Adds the counters to the global total and to the total for the named
     * index.
     * 
     * @param name
     *            The index name.
     * @param ndx
     *            An {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} index
     *            view.
     * 
     * @todo this does not account for writes isolated by a transaction.
     */
    protected void addCounters(String name, IIndex ndx) {
        
        final Counters c;
        if(ndx instanceof AbstractBTree) {
            
            c = ((AbstractBTree)ndx).counters;
            
        } else {
            
            FusedView view = (FusedView)ndx;
            
            AbstractBTree[] sources = view.getSources();
            
            c = new Counters();
            
            for(int i=0; i<sources.length; i++) {
                
                c.add(sources[i].counters);
                
            }
            
        }
                    
        synchronized(totalCounters) {
            
            totalCounters.add(c);

            Counters tmp = indexCounters.get(name);
            
            if (tmp == null) {
                
                tmp = new Counters();
                
                indexCounters.put(name, tmp);
                
            }
            
            tmp.add(c);
            
        }
        
    }
    
}
