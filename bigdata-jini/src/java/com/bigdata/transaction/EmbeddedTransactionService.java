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

package com.bigdata.transaction;

//BTM
import static com.bigdata.transaction.Constants.*;
import static com.bigdata.journal.TxState.*;

//BTM - original imports from DistributedTransactionService
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.zip.Adler32;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.concurrent.LockManager;
import com.bigdata.concurrent.LockManagerTask;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.RunState;
import com.bigdata.util.concurrent.ExecutionExceptions;

//BTM
import com.bigdata.config.LongValidator;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.TransactionService;
import com.bigdata.journal.TxState;
import com.bigdata.journal.ValidationError;
import com.bigdata.service.CommitTimeIndex;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.IServiceShutdown.ShutdownType;
import com.bigdata.service.ITxCommitProtocol;
import com.bigdata.service.Service;
import com.bigdata.service.TxServiceRunState;
import com.bigdata.util.InnerCause;
import com.bigdata.util.MillisecondTimestampFactory;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.config.LogUtil;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

//BTM - replace with ShardService
//BTM import com.bigdata.service.DataService;
//BTM import com.bigdata.service.IDataService;
//BTM import com.bigdata.service.IMetadataService;
import com.bigdata.service.ShardService;

public class EmbeddedTransactionService implements TransactionService,
                                                   Service
{
//BTM - BEGIN
public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedTransactionService.class).getName());

private UUID serviceUUID;
private String hostname;

private ServiceDiscoveryManager sdm;
private LookupCache shardCache;
private Map<UUID, ShardService> embeddedDataServiceMap;//change to ShardService

    public interface Options {

        /**
         * The directory in which the persistent state of this service will be
         * stored.
         */
        String DATA_DIR = 
            EmbeddedTransactionService.class.getName()+".dataDir";

        /**
         * The interval in milliseconds between writing a snapshot of
         * the index of accessible commit points into the {@link #DATA_DIR}
         * ({@value #DEFAULT_SNAPSHOT_INTERVAL}).
         * <p>
         * Two snapshots are retained of the commit time index so that
         * those historical commit times required for reading on
         * committed states of the database whose values are greater
         * than the <i>releaseTime</i> may be on hand after a service
         * restart. Two snapshots are maintained, with the older snapshot
         * being overwritten each time. A snapshot is written every N
         * milliseconds, where N is configured using this property,
         * and also when the service is shutdown.
         * <p>
         * The value of this property MAY be ZERO (0L) to disable snapshots;
         * a feature that is used by the {@link EmbeddedFederation} when
         * run in a diskless mode.
         */
        String SNAPSHOT_INTERVAL = 
            EmbeddedTransactionService.class.getName()+".snapshotInterval";

        /** 5 minutes (in millseconds). */
        String DEFAULT_SNAPSHOT_INTERVAL = ""+ (5 * 60 * 1000);

//BTM - BEGIN - added options
    String THREAD_POOL_SIZE = "threadPoolSize";
    String DEFAULT_THREAD_POOL_SIZE = 
           new Integer(Constants.DEFAULT_THREAD_POOL_SIZE).toString();
//BTM - END   - added options

//BTM - BEGIN - from AbstractTransactionService -------------------------------------
         /**
          * How long to hold onto the database history (in milliseconds).
          * If set to {@link Long#MAX_VALUE}, then the database is an
          * (effectively) immortal database. The transaction service
          * tracks the timestamp corresponding to the earliest running
          * transaction (if any). When such a transaction exists, the
          * actual release time is computed as follows:
          * 
          * <pre>
          * releaseTime =
          *     min(lastCommitTime - 1, 
          *         min(earliestRunningTx, now - minimumReleaseAge))
          * </pre>
          * 
          * This ensures that history in use while under a running
          * transaction is not released even when the minimumReleaseAge
          * is ZERO (0).
          * <p>
          * When no transactions exist, the actual release time is
          * computed as follows:
          * 
          * <pre>
          * releaseTime = min(commitTime - 1, now - minimumReleaseAge)
          * </pre>
          * 
          * This ensures that the the release time advances when no
          * transactions are in use, but that the minimum release age
          * is still respected.
          */
        String MIN_RELEASE_AGE = 
            EmbeddedTransactionService.class.getName() + ".minReleaseAge";

        /**
         * Minimum release age is zero (0). A value of ZERO (0) implies
         * that any history not required for the read-committed view is
         * released each time the {@link ResourceManager} overflows.
         */
        String MIN_RELEASE_AGE_NO_HISTORY = "0";

        /** Minimum release age is one minutes. */
        String MIN_RELEASE_AGE_1M = "" + 1/* mn */* 60/* sec */* 1000/* ms */;

        /** Minimum release age is five minutes. */
        String MIN_RELEASE_AGE_5M = "" + 5/* mn */* 60/* sec */* 1000/* ms */;

        /** Minimum release age is one hour. */
        String MIN_RELEASE_AGE_1H = "" + 1/* hr */* 60/* mn */* 60/* sec */
                * 1000/* ms */;

        /** Minimum release age is one day. */
        String MIN_RELEASE_AGE_1D = "" + 24/* hr */* 60/* mn */* 60/* sec */
                * 1000/* ms */;

        /** Minimum release age is one week. */
        String MIN_RELEASE_AGE_1W = "" + 7/* d */* 24/* hr */* 60/* mn */
                * 60/* sec */
                * 1000/* ms */;

        /** Immortal database */
        String MIN_RELEASE_AGE_NEVER = "" + Long.MAX_VALUE;

        /**
         * Default minimum release age is ZERO(0L) milliseconds (only the last
         * commit point will be retained after a full compacting merge).
         */
        String DEFAULT_MIN_RELEASE_AGE = MIN_RELEASE_AGE_NO_HISTORY;
//BTM - END - from AbstractTransactionService -------------------------------------
    }

    /**
     * A map<tx --> state> of the distributed transactions that are
     * currently committing.
     * 
     * @todo config for initial capacity and concurrency?
     */
    private final ConcurrentHashMap<Long, DistributedTxCommitTask> commitList
                  = new ConcurrentHashMap<Long, DistributedTxCommitTask>();

    /**
     * The {@link LockManager} used to impose a partial ordering on the prepare
     * phase of distributed transaction commits using index partition names as
     * the named resources for which the tasks must contend.
     */
    private final LockManager<String> indexLockManager = 
        new LockManager<String>
                (0/* maxConcurrencyIsIgnored */, true/* predeclareLocks */);

    /**
     * The {@link LockManager} used to impose a partial ordering on the commit
     * phase of distributed transaction commits using {@link ShardService}
     * {@link UUID}s as the named resources for which the tasks must contend.
     */
    private final LockManager<UUID> dataServiceLockManager = 
        new LockManager<UUID>
                (0/* maxConcurrencyIsIgnored */, true/* predeclareLocks */);

    /**
     * A {@link BTree} containing a log of the historical commit points.
     * <p>
     * The main things that it gives us are (a) the half-open ranges within
     * which we can allocate read-historical transactions; and (b) the last
     * commit time on record. It seems that creating an image of the log every N
     * seconds should be sufficient.
     * <p>
     * Note: Read and write operations on this index MUST be synchronized on the
     * index object.
     */
    protected final CommitTimeIndex commitTimeIndex;

    /**
     * True iff the service does not write any state on the disk.
     */
    private final boolean isTransient;
    
    /**
     * The data directory -or- <code>null</code> iff the service is transient.
     */
    protected final File dataDir;

    /**
     * The interval in milliseconds between logging an image of the
     * {@link #commitTimeIndex}.
     * 
     * @see Options#COMMIT_TIME_INDEX_SNAPSHOT_INTERVAL
     */
    private final long snapshotInterval;
    
    /**
     * The last (known) commit time.
     */
    private volatile long lastCommitTime = 0L;

    /**
     * The minimum age in milliseconds before history may be released.
     * 
     * @see Options#MIN_RELEASE_AGE
     */
    final private long minReleaseAge;

//BTM - BEGIN - fields from AbstractTransactionService ------------------------
    /**
     * A copy of the callers properties. 
     */
    private final Properties properties;

    /**
     * A lock used to serialize certain operations that must be atomic with
     * respect to the state of the transaction service. Mostly this is used to
     * serialize the assignment of transaction identifiers and the update of the
     * release time as transactions complete.
     * <p>
     * Note: To avoid lock ordering problems DO NOT acquire this {@link #lock}
     * if you are already holding a {@link TxState#lock}. This causes a lock
     * ordering problem and can result in deadlock.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The run state for the transaction service.
     */
    private volatile TxServiceRunState runState;

    private volatile long releaseTime = 0L;

    private CounterSet countersRoot;

    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has completed (aborted
     * or committed).
     * 
     * @todo config param for the initial capacity of the map.
     * @todo config for the concurrency rating of the map.
     */
    final private ConcurrentHashMap<Long, TxState> activeTx = new ConcurrentHashMap<Long, TxState>();

    /** #of transactions started. */
    private long startCount = 0L;

    /** #of transactions committed. */
    private long commitCount = 0L;

    /** #of transactions aborted. */
    private long abortCount = 0L;

    /** #of active read-only transactions. */
    private final AtomicLong readOnlyActiveCount = new AtomicLong(0L);

    /** #of active read-write transactions. */
    private final AtomicLong readWriteActiveCount = new AtomicLong(0L);

    /**
     * A transient index holding the <strong>absolute value</strong> of the
     * start times of all active transactions.
     * <p>
     * Note: The absolute value constraint is imposed so that the
     *       earliest active transaction in the index can be 
     *       identified by its position (position zero); which
     *       would not work if negative start times were allowed.
     * <p>
     * Note: In order to support this, {@link #findUnusedTimestamp(long, long)}
     * will not return a timestamp whose absolute value corresponds to an active
     * transaction.
     */
    private final CommitTimeIndex startTimeIndex = 
                      CommitTimeIndex.createTransient();

    /**
     * Signalled by {@link #deactivateTx(TxState)} and based on {@link #lock}.
     */
    protected final Condition txDeactivate = lock.newCondition();

//BTM - END - fields from AbstractTransactionService --------------------------


//BTM - BEGIN - fields from AbstractFederation --------------------------------
    /**
     * Used to run application tasks.
     */
    private final ThreadPoolExecutor threadPool;

    /**
     * Used to sample and report on the queue associated with the
     * {@link #threadPool}.
     */
    private final ScheduledExecutorService scheduledExecutorService =
                      Executors.newSingleThreadScheduledExecutor
                          (new DaemonThreadFactory
                                   (getClass().getName()+".sampleService"));
//BTM - END   - fields from AbstractFederation --------------------------------


    public EmbeddedTransactionService
               (final UUID serviceUUID,
                final String hostname,
                final ServiceDiscoveryManager sdm,
                final Map<UUID, ShardService> embeddedDataServiceMap,
                final String persistenceDir,
                final int threadPoolSize,
                final Properties properties)
    {

//BTM        super(properties);
//BTM - BEGIN - from AbstractTransactionService

        this.serviceUUID = serviceUUID;
        this.hostname = hostname;
        this.properties = (Properties) properties.clone();

        this.minReleaseAge = 
            LongValidator.GTE_ZERO.parse
                (Options.MIN_RELEASE_AGE, 
                 properties.getProperty
                     (Options.MIN_RELEASE_AGE,
                      Options.DEFAULT_MIN_RELEASE_AGE));
        if (logger.isInfoEnabled()) {
            logger.info(Options.MIN_RELEASE_AGE+"="+minReleaseAge);
        }
        this.runState = TxServiceRunState.Starting;
//BTM - END - from AbstractTransactionService

//BTM
//BTM        if (properties.getProperty(Options.DATA_DIR) == null) {
//BTM
//BTM            throw new RuntimeException("Required property: " + Options.DATA_DIR);
//BTM
//BTM        }
if(persistenceDir == null) {
    throw new RuntimeException
                  ("required property not set ["+Options.DATA_DIR+"]");
}

        snapshotInterval = LongValidator.GTE_ZERO.parse(
                Options.SNAPSHOT_INTERVAL, properties.getProperty(
                        Options.SNAPSHOT_INTERVAL,
                        Options.DEFAULT_SNAPSHOT_INTERVAL));

        if (logger.isInfoEnabled())
            logger.info(Options.SNAPSHOT_INTERVAL + "=" + snapshotInterval);

        isTransient = snapshotInterval == 0;

        if (isTransient) {
            dataDir = null;
        } else {
//BTM            dataDir = new File(properties.getProperty(Options.DATA_DIR));
this.dataDir = new File(persistenceDir);

            if (logger.isInfoEnabled())
                logger.info(Options.DATA_DIR + "=" + dataDir);
        }
        
        // Create transient BTree for the commit time logger.
        commitTimeIndex = CommitTimeIndex.createTransient();

        setup();
        
        if (logger.isInfoEnabled())
            logger.info("lastCommitTime=" + lastCommitTime + ", #commitTimes="
                    + commitTimeIndex.getEntryCount());


//BTM - BEGIN - from AbstractFederation.shutdownNow
        if (threadPoolSize == 0) {
            this.threadPool = 
                (ThreadPoolExecutor) Executors.newCachedThreadPool
                    (new DaemonThreadFactory
                         (getClass().getName()+".executorService"));
        } else {
            this.threadPool = 
                (ThreadPoolExecutor) Executors.newFixedThreadPool
                    (threadPoolSize, 
                     new DaemonThreadFactory
                         (getClass().getName()+".executorService"));
        }
//BTM - END   - from AbstractFederation.shutdownNow

//BTM - BEGIN
this.embeddedDataServiceMap = embeddedDataServiceMap;
if(sdm != null) {
    Class[] shardType = new Class[] {ShardService.class};
    ServiceTemplate shardTmpl = new ServiceTemplate(null, shardType, null);
    ServiceItemFilter shardFilter = null;//new IDataServiceOnlyFilter();//BTM - change to null
    try {
        this.shardCache = sdm.createLookupCache
                                     ( shardTmpl, 
                                       shardFilter,
                                       null );
    } catch(RemoteException e) {
        logger.warn(e.getMessage(), e);
    }
}

start();
//BTM - END
    }

//BTM
// Required by Service interface (extended by TransactionService)

    public UUID getServiceUUID() {
        return this.serviceUUID;
    }

    public Class getServiceIface() {
        return SERVICE_TYPE;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public String getHostname() {
        return this.hostname;
    }

//BTM
// Required by IServiceShutdown interface (shutdown & shutdownNow were provided by DistributedTransactionService)

    public boolean isOpen() {
        return runState != TxServiceRunState.Halted;
    }

//BTM - original methods of DistributedTransactionService

    /**
     * Either creates the data directory or reads the {@link #commitTimeIndex}
     * from files in an existing data directory.
     */
    private void setup() {

        if(isTransient) {
            // nothing committed yet.
            lastCommitTime = 0L;
            return;
        }
        
        if (!dataDir.exists()) {// new service if dataDir does not exist yet

            if (!dataDir.mkdirs() && !dataDir.mkdirs()) {
                throw new RuntimeException
                              ("failure on dataDir creation ["+dataDir+"]");
            }
            // nothing committed yet.
            lastCommitTime = 0L;
            return;
        }

        {
            // the files on which the images should have been written.
            final File file0 = new File(dataDir, BASENAME + "0" + EXT);

            final File file1 = new File(dataDir, BASENAME + "1" + EXT);

            if (!file0.exists() && !file1.exists()) {

                logger.warn("No commit time logs - assuming new service: dataDir="
                        + dataDir);

                // nothing committed yet.
                lastCommitTime = 0L;

                return;

            }
            // timestamps on those files (zero if the file does not exist)
            final long time0 = file0.lastModified();
            final long time1 = file1.lastModified();

            // true iff file0 is more recent.
            final boolean isFile0 = (time0 != 0L && time1 != 0L) //
                ? (time0 > time1 ? true: false)// Note: both files exist.
                : (time0 != 0L ? true: false)// Note: only one file exists
                ;

            final File file = isFile0 ? file0 : file1;

//            System.err.println("file0: "+file0.lastModified());
//            System.err.println("file1: "+file1.lastModified());
//            System.err.println("isFile0="+isFile0);

            /*
             * Note: On restart the value of this counter is set to either
             * ONE(1) or TWO(1) depending on which snapshot file is more
             * current.
             * 
             * It is ONE(1) if we read file0 since the counter would be ONE(1)
             * after we write file0 for the first time.
             * 
             * It is TWO(2) if we read file1 since the counter would be TWO(2)
             * after we write file1 for the first time.
             */
            snapshotCount = isFile0 ? 1 : 2;

            try {

                // read most recent image.
                final int entryCount = SnapshotHelper.read(commitTimeIndex,
                        file);

                logger.warn("Read snapshot: entryCount=" + entryCount + ", file="
                        + file);
                
            } catch (IOException ex) {

                throw new RuntimeException("Could not read file: " + file, ex);

            }

        }

        if (commitTimeIndex.getEntryCount() == 0) {

            // nothing in the commit time logger.
            lastCommitTime = 0;

        } else {

            // the last commit time in the logger. @todo write unit test to
            // verify on restart.
            lastCommitTime = commitTimeIndex.decodeKey(commitTimeIndex
                    .keyAt(commitTimeIndex.getEntryCount() - 1));

        }
    }

    /**
     * Basename for the files written in the {@link #dataDir} containing images
     * of the {@link #commitTimeIndex}.
     */
    static protected final String BASENAME = "commitTime";
    
    /**
     * Extension for the files written in the {@link #dataDir} containing
     * snapshots of the {@link #commitTimeIndex}.
     */
    static protected final String EXT = ".snapshot";
    
    /**
     * #of times we have written a snapshot of the {@link #commitTimeIndex}.
     */
    private long snapshotCount = 0L;
    
    /**
     * Runs the {@link SnapshotTask} once.
     */
    public void snapshot() {
        
        new SnapshotTask().run();

    }
    
    /**
     * A task that writes a snapshot of the commit time index onto a pair of
     * alternating files. This is in the spirit of the Challis algorithm, but
     * the approach is less rigorous.
     */
    private class SnapshotTask implements Runnable {
      
        /**
         * Note: Anything thrown out of this method will cause the task to no
         * longer be scheduled!
         */
        public void run() {
            
            if(isTransient) {

                // snapshot not supported for transient service.
                throw new RuntimeException("Service is transient");
                
            }
            
            lock.lock();

            try {

                final long begin = System.currentTimeMillis();

                // either 0 or 1.
                final int i = (int) snapshotCount % 2;

                final File file = new File(dataDir, BASENAME + i + EXT);
                
                if (!dataDir.exists()) {

                    if (!dataDir.mkdirs() && !dataDir.mkdirs()) {

                        throw new RuntimeException("Could not create: " + dataDir);

                    }
                    
                }

                final int entryCount;
                synchronized (commitTimeIndex) {

                    entryCount = SnapshotHelper.write(commitTimeIndex, file);

                }

                // increment counter iff successful.
                snapshotCount++;

                final long elapsed = System.currentTimeMillis() - begin;

                logger.warn("snapshot: snapshotCount=" + snapshotCount
                        + ", entryCount=" + entryCount + ", file=" + file
                        + ", elapsed=" + elapsed);

            } catch (Throwable t) {

                logger.error(t.getMessage(), t);

                return;

            } finally {

                lock.unlock();

            }

        }
        
    };
    
    /**
     * A helper class for reading and writing snapshots of the commit time
     * index. The image contains the commit timestamps in order.
     * <p>
     * Note: The caller must prevent concurrent changes to the index.
     * 
     * @todo write counters into the files since the system clock could be
     *       messed with on before a restart but the counters will always be
     *       valid. we would then either read both and choose one, or have a
     *       method to report the header with the earlier counter.
     * 
     * @todo Checksum the commit time log file? this is easily done either using
     *       a {@link ByteBuffer} or using {@link Adler32}.
     */
    public static class SnapshotHelper {

        static public int read(CommitTimeIndex ndx, File file)
                throws IOException {

            final FileInputStream is = new FileInputStream(file);

            try {

                final BufferedInputStream bis = new BufferedInputStream(is);

                final DataInputStream dis = new DataInputStream(bis);

                return SnapshotHelper.read(ndx, dis);

            } finally {

                is.close();

            }

        }
        
        static public int read(CommitTimeIndex ndx, DataInputStream is)
                throws IOException {

            final int n = is.readInt();

            for (int i = 0; i < n; i++) {

                ndx.add(is.readLong());

            }

            return n;
            
        }

        static public int write(CommitTimeIndex ndx, File file)
                throws IOException {

            final FileOutputStream os = new FileOutputStream(file);

            try {

                final BufferedOutputStream bos = new BufferedOutputStream(os);

                final DataOutputStream dos = new DataOutputStream(bos);

                // write the image on the file.
                final int entryCount = SnapshotHelper.write(ndx, dos);

                dos.flush();
                
                bos.flush();

                return entryCount;
                
            } finally {

                os.close();

            }

        }
        
        static public int write(CommitTimeIndex ndx, DataOutputStream os)
                throws IOException {

            final int entryCount = ndx.getEntryCount();
            
            os.writeInt(entryCount);
            
            final ITupleIterator itr = ndx.rangeIterator();

            int n = 0;
            
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final long commitTime = ndx.decodeKey(tuple.getKey());

                os.writeLong(commitTime);

                n++;
                
            }
            
            if (n != entryCount) {
                
                /*
                 * Note: probable error is the caller not preventing concurrent
                 * modification.
                 */
                
                throw new AssertionError();
                
            }
            
            return entryCount;
            
        }
        
    }
    
//BTM    public DistributedTransactionService start() {
private void start() {

        /*
         * Note: lock makes operation _mostly_ atomic even though the
         *       the runState is changed. For example, new transactions
         *       can not start without this lock.
         */
        lock.lock();
        try {
//BTM            super.start();
//BTM - BEGIN - from AbstractTransactionService ---------------------------------        

        if(logger.isInfoEnabled()) 
            logger.info("");

//BTM - no need for additional lock here
//BTM        lock.lock();
//BTM        try {

            switch (getRunState()) {
            case Starting:
                break;
            default:
                throw new IllegalStateException();
            }
            
            final long timestamp = _nextTimestamp();

            final long lastCommitTime = getLastCommitTime();

            if (timestamp < lastCommitTime) {

                throw new RuntimeException(
                        "Clock reporting timestamps before lastCommitTime: now="
                                + new Date(timestamp) + ", lastCommitTime="
                                + new Date(lastCommitTime));

            }

            /*
             * Note: This computes the releaseTime on startup.
             * 
             * Note: While nextTimestamp() is not really a transaction, it is LT
             * any possible transaction identifier (since there are no running
             * transactions).
             */
            updateReleaseTime(timestamp);

            setRunState(TxServiceRunState.Running);

//BTM        } finally {
//BTM            lock.unlock();
//BTM        }
//BTM - END -------------------------------------------------------------------------------        

            addScheduledTasks();
            
//BTM            return this;

        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Adds the scheduled tasks.
     */
    protected void addScheduledTasks() {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

//BTM        final AbstractFederation fed = (AbstractFederation) getFederation();

        // @todo config options (verify units).
//BTM        notifyFuture = fed.addScheduledTask(new NotifyReleaseTimeTask(),
notifyFuture = addScheduledTask(new NotifyReleaseTimeTask(),
                60/* initialDelay */, 60/* delay */, TimeUnit.SECONDS);

        if (snapshotInterval != 0L) {

            // start the snapshot task.

//BTM            writeFuture = fed.addScheduledTask(
writeFuture = addScheduledTask(
                    new SnapshotTask(),
                    snapshotInterval/* initialDelay */,
                    snapshotInterval/* delay */,
                    TimeUnit.MILLISECONDS);
        }
    }
    
    private ScheduledFuture notifyFuture = null;
    private ScheduledFuture writeFuture = null;
    
    public void shutdown() {
logger.warn("YYYYY TRANSACTION SERVICE: embeddedTransactionService.shutdown");
        
        lock.lock();
        try {
            switch (getRunState()) {
                case Shutdown:
                case ShutdownNow:
                case Halted:
                    return;
            }

            /*
             * First make sure that all tx are terminated - this is important
             * otherwise we will write the commit time index image before we
             * have the last commit times on hand.
             */
//BTM            super.shutdown();
//BTM - BEGIN - from AbstractTransactionService.shutdown
            // Do not allow new transactions to start.
            setRunState(TxServiceRunState.Shutdown);
            try {
                // wait for running transactions to complete.
                awaitRunningTx(10/* logTimeout */, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                // convert to fast shutdown.
                logger.warn("Interrupted during shutdown - will do fast shutdown: "+ex, ex);

                shutdownNow();
                return;
            }
            // Service is halted.
            setRunState(TxServiceRunState.Halted);
//BTM - END - from AbstractTransactionService.shutdown

            /*
             * No need to interrupt this task. It will complete soon enough.
             * However, we do want to cancel it so it will stop running.
             */
            if (notifyFuture != null) {
                notifyFuture.cancel(false/* mayInterruptIfRunning */);
            }

            /*
             * Cancel this task, but DO NOT interrupt it to avoid a partial
             * write if there is a write in progress. If there is a write in
             * progress, then we will wind up writing it again immediately since
             * we do that below. This is Ok. We will just have a current image
             * and a nearly current image.
             */
            if (writeFuture != null) {
                writeFuture.cancel(false/* mayInterruptIfRunning */);
            }

            if (snapshotInterval != 0L) {
                // write a final image during shutdown.
                new SnapshotTask().run();
            }
        } finally {
            lock.unlock();
        }
    }

    public void shutdownNow() {
logger.warn("YYYYY TRANSACTION SERVICE >>> embeddedTransactionService.shutdownNow");

        lock.lock();
        try {
            switch (getRunState()) {
                case ShutdownNow:
                case Halted:
                    return;
            }

            /*
             * First make sure that all tx are terminated - this is important
             * otherwise we will write the commit time index image before we
             * have the last commit times on hand.
             */
//BTM            super.shutdownNow();
//BTM - BEGIN - from AbstractTransactionService.shutdownNow
            setRunState(TxServiceRunState.ShutdownNow);

            for (long tx : activeTx.keySet()) {
                final TxState state = activeTx.get(tx);

                if (state == null) {
                    /*
                     * Note: concurrent removal or clearing of the weak
                     * reference is possible.
                     */
                    continue;
                }

                state.lock.lock();
                try {
                    if (state.isActive()) {
                        // if (!state.isReadOnly()) {
                        try {
                            abortImpl(state);
                            assert state.isAborted() : state.toString();
                        } catch(Throwable t) {
                            logger.error(state.toString(), t);
                        } finally {
                            deactivateTx(state);
                        }
                    }
                } finally {
                    state.lock.unlock();
                    /*
                     * Note: We are already holding the outer lock so we do not
                     * need to acquire it here.
                     */
                    updateReleaseTime(Math.abs(state.tx));
                }
            }// foreach tx in activeTx

            // signal once now that we are run.
            txDeactivate.signalAll();

            final int activeCount = getActiveCount();

            if (activeCount != 0) {
                logger.warn("Service shutdown with active transactions: #nactive="
                        + activeTx.size());
            }
            setRunState(TxServiceRunState.Halted);
//BTM - END - from AbstractTransactionService.shutdownNow

            /*
             * Cancel and interrupt if running.
             */
            if (notifyFuture != null) {
                notifyFuture.cancel(true/* mayInterruptIfRunning */);
            }

            /*
             * Cancel this task and interrupt if running. Interrupting this will
             * leave a partial snapshot on the disk, but we do not advance the
             * counter unless the snapshot is successful so we will overwrite
             * that partial snapshot below when we write a final snapshot.
             */
            if (writeFuture != null) {
                writeFuture.cancel(true/* mayInterruptIfRunning */);
            }

            if (snapshotInterval != 0L) {
                // write a final snapshot during shutdown.
                snapshot();
            }
        } finally {
            lock.unlock();
        }

//BTM - BEGIN - from AbstractFederation.shutdownNow
        threadPool.shutdownNow();
//BTM - END - from AbstractFederation.shutdownNow

    }
    
    public void destroy() {
logger.warn("YYYYY TRANSACTION SERVICE >>> embeddedTransactionService.destroy");

        lock.lock();
        try {
//BTM            super.destroy();
//BTM - BEGIN - from AbstractTransactionService.destroy
            shutdownNow();
//BTM - END - from AbstractTransactionService.destroy

            if (!isTransient) {
logger.warn("YYYYY TRANSACTION SERVICE EmbeddedTransactionService.destroy >>> DELETING "+(new File(dataDir, BASENAME + "0" + EXT)));
                // delete the commit time index log files.
                new File(dataDir, BASENAME + "0" + EXT).delete();
                new File(dataDir, BASENAME + "1" + EXT).delete();

                // delete the data directory (works iff it is empty).
                dataDir.delete();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Extended to truncate the head of the {@link #commitTimeIndex} such only
     * the commit times requires for reading on timestamps GTE to the new
     * releaseTime are retained.
     */
    protected void setReleaseTime(long releaseTime) {
        
//BTM        super.setReleaseTime(releaseTime);
//BTM - BEGIN - from AbstractTransactionService.setReleaseTime
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (logger.isInfoEnabled())
            logger.info("new releaseTime="+releaseTime);
        this.releaseTime = releaseTime;
//BTM - END - from AbstractTransactionService.setReleaseTime

        /*
         * Truncate the head of the commit time index since we will no longer
         * grant transactions whose start time is LTE the new releaseTime.
         */
        
        // Note: Use the current value.
        releaseTime = getReleaseTime();
        
        if (releaseTime > 0) {
        
            synchronized (commitTimeIndex) {

                /*
                 * The exclusive upper bound is the timestamp of the earliest
                 * commit point on which we can read with this [releaseTime].
                 */
                final long toKey = commitTimeIndex.find(releaseTime + 1);

                final ITupleIterator itr = commitTimeIndex.rangeIterator(0L,
                        toKey, 0/* capacity */, IRangeQuery.KEYS
                                | IRangeQuery.CURSOR, null/* filter */);

                while (itr.hasNext()) {

                    itr.next();

                    // remove the tuple from the index.
                    itr.remove();

                }
            }
        }
    }
    
    /**
     * Return the proxies for the services participating in a distributed
     * transaction commit or abort.
     * <p>
     * Note: This method is here so that it may be readily overriden for unit
     * tests.
     * 
     * @param uuids
     *            The {@link UUID}s of the participating services.
     * 
     * @return The corresponding service proxies.
     */
    protected ITxCommitProtocol[] getDataServices(UUID[] uuids) {
        
//BTM        return getFederation().getDataServices(uuids);
//BTM - BEGIN
if(uuids == null) return null;
ArrayList<ITxCommitProtocol> retList = new ArrayList<ITxCommitProtocol>();
Map<UUID, ShardService> serviceMap = getDiscoveredDataServices();
for(int i=0; i<uuids.length; i++) {
    ITxCommitProtocol serviceRef = serviceMap.get(uuids[i]);
    if(serviceRef == null) continue;
    retList.add( serviceRef );
}
return retList.toArray(new ITxCommitProtocol[retList.size()]);
//BTM - END
    }

//BTM - BEGIN
//BTM - change all IDataServices to ShardServices
    private ShardService getDataService(UUID uuid) {
        Map<UUID, ShardService> serviceMap = getDiscoveredDataServices();
        return serviceMap.get(uuid);
    }

    private ShardService[] getDataServices() {
        Map<UUID, ShardService> serviceMap = getDiscoveredDataServices();
        Collection<ShardService> serviceRefs = serviceMap.values();
        return serviceRefs.toArray(new ShardService[serviceRefs.size()]);
    }

    private Map<UUID, ShardService> getDiscoveredDataServices() {
        Map<UUID, ShardService> serviceMap = new HashMap<UUID, ShardService>();
        if(shardCache != null) {
            ServiceItem[] shardItems = shardCache.lookup(null, Integer.MAX_VALUE);
            for(int i=0; i<shardItems.length; i++) {
                ServiceItem item = shardItems[i];
                if( (item == null) || (item.service == null) ) continue;
                UUID uuid = JiniUtil.serviceID2UUID(item.serviceID);
                ShardService serviceRef = (ShardService)(item.service);
                serviceMap.put(uuid, serviceRef);
            }
        } else {
            Set<Map.Entry<UUID, ShardService>> entrySet =
                                       embeddedDataServiceMap.entrySet();
            Iterator<Map.Entry<UUID, ShardService>> itr = entrySet.iterator();
            for(int i=0; itr.hasNext(); i++ ) {
                Map.Entry<UUID, ShardService> pair = itr.next();
                UUID uuid = pair.getKey();
                ShardService serviceRef = pair.getValue();
                serviceMap.put(uuid, serviceRef);
            }
        }
        return serviceMap;
    }
//BTM - END

    /**
     * Task runs {@link ITxCommitProtocol#abort(long)}.
     */
    private static class AbortTask implements Callable<Void> {
        
        private final ITxCommitProtocol service;
        private final TxState state;
        
        public AbortTask(final ITxCommitProtocol service, final TxState state) {
            
            if (service == null)
                throw new IllegalArgumentException();
            
            if (state == null)
                throw new IllegalArgumentException();
            
            this.service = service;
            
            this.state = state;
            
        }
        
        public Void call() throws Exception {
            
            service.abort(state.tx);

            return null;
            
        }
        
    }
    
    protected void abortImpl(final TxState state) throws Exception {

        if(!state.lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (!state.isActive())
            throw new IllegalStateException();

        if(state.isReadOnly()) {
    
            /*
             * Note: There is no local state for read-only tx so we do not need
             * to message the data services.
             */
            
            state.setRunState(RunState.Aborted);
            
            return;
            
        }

        final UUID[] uuids = state.getDataServiceUUIDs();

        final ITxCommitProtocol[] services = getDataServices(uuids);

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                uuids.length);
        
        for (ITxCommitProtocol dataService : services) {

            tasks.add(new AbortTask(dataService, state));

        }

//BTM        final List<Future<Void>> futures = getFederation().getExecutorService().invokeAll(tasks);
final List<Future<Void>> futures = threadPool.invokeAll(tasks);

        List<Throwable> causes = null;

        for (Future<Void> f : futures) {

            try {

                // verify no errors.
                f.get();

            } catch (Throwable t) {

                /*
                 * Collect all causes and always log an error if any data
                 * service abort fails.
                 * 
                 * Note: If an exception is thrown here the transaction will be
                 * aborted regardless. Howwever, the data service which threw
                 * the exception may still have local state on hand for the tx.
                 */

                logger.error(t, t);

                if (causes == null) {

                    causes = new LinkedList<Throwable>();

                }

                causes.add(t);
                
            }

        }
        
        state.setRunState(RunState.Aborted);
        
        if (causes != null) {
            
            throw new ExecutionExceptions(state.toString(), causes);
            
        }
        
    }

    /**
     * There are two distinct commit protocols depending on whether the
     * transaction write set is distributed across more than one
     * {@link ShardService}. When write set of the transaction lies entirely on
     * a single {@link ShardService}, an optimized commit protocol is used.
     * When the write set of the transaction is distributed, a 3-phase commit is
     * used with most of the work occurring during the "prepare" phase and a
     * very rapid "commit" phase. If a distributed commit fails, even during the
     * "commit", then the transaction will be rolled back on all participating
     * {@link ShardService}s.
     * 
     * <h3>Single phase commits</h3>
     * 
     * A simple commit protocol is used when the write set of the transaction
     * resides entirely on a single {@link ShardService}. Such commits DO NOT
     * contend for named resource locks (either on the index names or on the
     * {@link ShardService} {@link UUID}s). Since such transactions DO NOT have
     * dependencies outside of the specific {@link ShardService}, a necessary
     * and sufficient partial order will be imposed on the executing tasks
     * locally by the {@link ShardService} on which they are executing based
     * solely on the named resources which they declare. Without dependencies on
     * distributed resources, this can not deadlock.
     * 
     * <h3>Distributed commits</h3>
     * 
     * Transaction commits for a distributed database MUST be prepared in a
     * partial order so that they do not deadlock when acquiring the necessary
     * locks on the named indices on the local data services. That partial order
     * is imposed using the {@link #indexLockManager}. The named index locks
     * are pre-declared at the start of the distributed commit protocol and are
     * held through both the prepare and commit phases until the end of the
     * commit protocol. The distributed commit must obtain a lock on all of the
     * necessary named index resources before proceeding. If there is an
     * existing commit using some of those resources, then any concurrent commit
     * requiring any of those resources will block. The {@link LockManager} is
     * configured to require pre-declaration of locks. Deadlocks are NOT
     * possible when the locks are pre-declared.
     * <p>
     * A secondary partial ordering is established based on the
     * {@link ShardService} {@link UUID}s during the commit phase. This partial
     * order is necessary to avoid deadlocks for concurrently executing commit
     * phases of distributed transactions that DO NOT share named index locks.
     * Without a partial order over the participating {@link ShardService}s,
     * deadlocks could arise because each transaction will grab an exclusive
     * lock on the write service for each participating {@link ShardService}.
     * By ordering those lock requests, we again ensure that deadlocks can not
     * occur.
     * <p>
     * Note: The prepare phase for distributed commits allows the maximum
     * possible concurrency. This is especially important as validation and
     * merging down onto the unisolated indices can have significant length for
     * large transactions.
     * <p>
     * The commit phase should be very fast, with syncing the disk providing the
     * primary source of latency. All participating indices on the participating
     * data services have already been checkpointed. Once the commitTime is
     * assigned by the {@link DistributedTransactionService}, the group commit
     * need only update the root block on the live journal and sync to disk.
     * 
     * @todo Place timeout on the commit phase where the tx will abort unless
     *       all participants join at the "committed" barrier within ~ 250ms.
     *       That should be a generous timeout, but track aborts for this reason
     *       specifically since they may indicate interesting problems (heavy
     *       swapping, network issues, etc).
     * 
     * @todo make sure that we checkpoint the commit record index and
     *       {@link Name2Addr} before requesting the commitTime to remove even
     *       more latency.
     */
    protected long commitImpl(final TxState state) throws Exception {
        
        if (state.isReadOnly() || state.getDataServiceCount() == 0) {
            
            /*
             * Note: We do not maintain any transaction state on the client for
             * read-only transactionss.
             * 
             * Note: If the transaction was never started on any data service so
             * we do not need to notify any data service. In effect, the tx was
             * started but never used.
             */

            state.setRunState(RunState.Committed);
            
            return 0L;
            
        }
     

        if (!state.isDistributedTx()) {

            /*
             * The write set of the transaction is local to a single data
             * service. In this case we can do a much simpler commit protocol.
             */

            return singlePhaseCommit(state);
            
        }

        /*
         * The LockManagerTask will handle lock acquisition for the named
         * resources and then invoke our task to perform the commit.
         */
        final LockManagerTask<String, Long> delegate = new LockManagerTask<String, Long>(
                indexLockManager, state.getResources(), new DistributedTxCommitTask(state));

        /*
         * This queues the request until it holds the necessary locks (on the
         * named indices used by the transaction). It then prepares the
         * transaction and (if successfull) requests the necessary locks for the
         * commit phase (on the data service UUIDs) and then commits the tx.
         */

        return delegate.call();
        
    }

    /**
     * Prepare and commit a read-write transaction that has written on a single
     * data service.
     */
    protected long singlePhaseCommit(final TxState state) throws Exception {

        if(!state.lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        final UUID[] uuids = state.getDataServiceUUIDs();

        if (uuids.length != 1)
            throw new AssertionError();

        final UUID serviceUUID = uuids[0];

//BTM        final IDataService dataService = getFederation().getDataService(serviceUUID);
final ShardService dataService = getDataService(serviceUUID);//BTM - change to ShardService

        try {

            final long commitTime = dataService.singlePhaseCommit(state.tx);

            state.setRunState(RunState.Committed);

            return commitTime;

        } catch (Throwable t) {

            state.setRunState(RunState.Aborted);

            throw new RuntimeException(t);

        }

    }
    
    /**
     * <p>
     * Task runs the distributed commit protocol transaction.
     * </p>
     * Pre-conditions:
     * <p>
     * <ul>
     * <li>The transaction has a distributed write set (this does too much work
     * for a transaction whose write set is local to a single data service).</li>
     * <li>The caller holds the locks for the named index resources declared by
     * the transaction.</li>
     * <li>The transaction {@link TxState#isActive()}.</li>
     * </ul>
     * </p>
     * <p>
     * Post-conditions (success):
     * <ul>
     * <li>The transaction was assigned a <i>revisionTime</i>.</li>
     * <li>All participating data services validated the write set of the
     * transaction using that <i>revisionTime</i> and merge down the write set
     * of the transaction onto the corresponding unisolated indices.</li>
     * <li>The transaction was assigned a <i>commitTime</i>.</li>
     * <li>All participating data services have made the write set of the
     * transaction restart safe and marked the transaction as "committed" in
     * their local data.</li>
     * <li>The transaction {@link TxState#isCommitted()}.</li>
     * </ul>
     * </p>
     * <p>
     * Post-conditions (failure):
     * <ul>
     * <li>The transaction {@link TxState#isAborted()}.</li>
     * <li>Each participating data service has been notified that the
     * transaction was aborted.</li>
     * </ul>
     * </p>
     */
    private class DistributedTxCommitTask implements Callable<Long> {

        private final TxState state;

        /**
         * The {@link UUID}s of the participating shard services.
         */
        private final UUID[] uuids;

        /**
         * The proxies for the participating shard services.
         */
        private final ITxCommitProtocol[] services;

        /**
         * The #of participating shard services.
         */
        private final int nservices;

        /**
         * The revision time (assigned once the task begins to execute with all
         * locks held for the named index partitions).
         */
        private long revisionTime;

        /**
         * The commit time (assigned once the prepared barrier breaks and all
         * locks are held for the participating data services).
         * <p>
         * Note: This field is for debugging only.
         */
        private long commitTime;

        /**
         * The thread in which the {@link DistributedTxCommitTask} is executing.
         * This is the {@link Thread} that is used to obtain the locks for the
         * commit phase using the dataServiceLockManager.
         */
        final Thread commitThread;

        /**
         * Condition is signaled when the "prepared" barrier breaks.
         * <p>
         * Note: If the barrier does not break because a participate fails then
         * the {@link #commitThread} MUST be interrupted in order for it to awaken.
         */
        final Condition prepared;
        
        /**
         * Condition is signaled when the necessary locks are held for the
         * participating shard services.
         * 
         * @see EmbeddedTransactionService#dataServiceLockManager
         */
        final Condition locksHeld;
        
        /**
         * Condition is signaled when the "committed" barrier breaks.
         */
        final Condition committed;
        
        /**
         * Barrier used to await the
         * {@link ITransactionService#prepared(long, UUID)} messages during a
         * distributed read-write transaction commit.
         */
        CyclicBarrier preparedBarrier = null;

        /**
         * Barrier used to await the
         * {@link ITransactionService#committed(long, UUID)} messages during a
         * distributed read-write transaction commit.
         */
        CyclicBarrier committedBarrier = null;

        public DistributedTxCommitTask(final TxState state) {

            if (state == null)
                throw new IllegalArgumentException();

            /*
             * Note: If this thread is holding the lock on [TxState] then no
             * other thread can access that object. This issue is resolved by
             * creating [Condition]s on which this thread awaits based on
             * TxState.lock.
             */
            if(!state.lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            
            this.state = state;

            // The UUIDs of the participating (meta)dataServices.
            this.uuids = state.getDataServiceUUIDs();

            // The corresponding data services (resolve before acquiring locks).
            this.services = getDataServices(uuids);
            
            this.nservices = uuids.length;

            // Note: Same thread required for ctor and execution!
            this.commitThread = Thread.currentThread();
            
            this.prepared = state.lock.newCondition();

            this.locksHeld = state.lock.newCondition();

            this.committed = state.lock.newCondition();
            
        }

        /**
         * This method will be invoked by the {@link LockManagerTask} once it
         * holds all of the necessary named index resource locks. This is how we
         * impose a partial order for preparing the transaction. Deadlocks can
         * not arise because we predeclare the locks and {@link LockManager} can
         * guarantee no deadlocks in that case by sorting the requested
         * resources and acquiring the locks in the sorted order.
         */
        public Long call() throws Exception {

            assert this.commitThread == Thread.currentThread();
            
            return distributedCommit(state);

        }

        /**
         * Prepare and commit a read-write transaction that has written on more
         * than one data service.
         * <p>
         * Note: read-write transactions that have written on multiple journals
         * must use a distributed (2-/3-phase) commit protocol. As part of the
         * commit protocol, we obtain an exclusive write lock on each journal on
         * which the transaction has written. This is necessary in order for the
         * transaction as a whole to be assigned a single commit time. Latency
         * is critical in this commit protocol since the journals participating
         * in the commit will be unable to perform any unisolated operations
         * until the transaction either commits or aborts.
         * <p>
         * Note: There is an assumption that the revisionTime does not need to
         * be the commitTime. This allows us to get all the heavy work done
         * before we reach the "prepared" barrier, which means that the commit
         * phase should be very fast. The assumption is that validation of
         * different transactions writing on the same unisolated indices is in
         * fact serialized. The transaction services MUST enforce that
         * assumption by serializing distributed commits (at least those which
         * touch the same index partitions (necessary constraint), the same
         * indices (sufficient constraint) or the same {@link ShardService}s
         * (sufficient constraint)). If it did not serialize distributed commits
         * then <strong>deadlocks</strong> could arise where two distributed
         * commits were each seeking the exclusive write lock on resources, one
         * of which was already held by the other commit.
         * 
         * @throws Exception
         *             if anything goes wrong.
         * 
         * @return The commit time for the transaction.
         */
        protected long distributedCommit(final TxState state) throws Exception {

            if(!state.lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();

            // choose the revision timestamp.
            this.revisionTime = nextTimestamp();

            // add to map of concurrently committing distributed transactions.
            commitList.put(state.tx, this);

            try {

                /*
                 * Submit a task that will run issue the prepare(tx,rev)
                 * messages to each participating data service and await its
                 * future.
                 */
                call2();

                return commitTime;
                
            } finally {

                commitList.remove(state.tx);
                
            }
            
        }

        /**
         * Setups up the {@link TxState#preparedBarrier} and the
         * {@link TxState#committedBarrier} and then runs the
         * {@link PrepareTask} tasks.
         * <p>
         * Post-conditions: {@link TxState#isComplete()} will be true. The
         * transaction will either have been aborted or committed on all
         * {@link ShardService}s.
         * 
         * @return The assigned commit time.
         * 
         * @todo Allow interrupt of the data service committers if any task
         *       fails during prepare() rather than having to wait for all of
         *       those tasks to join at the {@link TxState#preparedBarrier}.
         *       This is only an optimization. We would cancel those tasks using
         *       the {@link TaskRunner}'s {@link Future}.
         */
        public Void call2() throws Exception {

            Future<?> taskRunnerFuture = null;
            try {

                setupPreparedBarrier();

                setupCommittedBarrier();

//BTM                taskRunnerFuture = getFederation().getExecutorService().submit(new TaskRunner());
taskRunnerFuture = threadPool.submit(new TaskRunner());

                /*
                 * Signaled when the prepared barrier breaks. Interrupted if
                 * the prepare phase fails.
                 */
                prepared.await();

                /**
                 * Runs an inner Callable once we have the data service UUID
                 * locks.
                 * <p>
                 * Note: The purpose of this task is to hold onto those locks
                 * until the commit is finished (either success or failure). The
                 * locks are automatically release once the inner Callable
                 * completes regardless of the outcome.
                 * <p>
                 * Note: This task will run in the same thread as the caller.
                 * This means that the task will already hold the
                 * {@link TxState#lock}.
                 */
                new LockManagerTask<UUID, Void>(dataServiceLockManager, state
                        .getDataServiceUUIDs(), new Callable<Void>() {

                    public Void call() throws Exception {

                        if (!state.lock.isHeldByCurrentThread()) {

                            /*
                             * Note: The task runs in its caller's thread and
                             * the caller should already be holding the TxState
                             * lock.
                             */
                            
                            throw new IllegalMonitorStateException();

                        }

                        /*
                         * Signal so that the task which caused the prepared
                         * barrier to break can resume. It turn, when the
                         * prepared runnable finishes, all tasks awaiting that
                         * barrier will continue to execute and will enter their
                         * "commit" phase.
                         */
                        locksHeld.signal();

                        // Signaled when the committed barrier breaks.
                        committed.await();

                        return null;

                    }

                }).call();

                // Done.
                return null;

            } finally {

                /*
                 * Reset the barriers in case anyone is waiting.
                 */

                if (preparedBarrier != null)
                    preparedBarrier.reset();

                if (committedBarrier != null)
                    committedBarrier.reset();

                /*
                 * Await the future on the task running the PrepareTasks.
                 * 
                 * Note: This task SHOULD complete very shortly after a
                 * successful commit.
                 * 
                 * Note: If any PrepareTask fails, then all PrepareTasks should
                 * abort shortly thereafter.
                 */
                if (taskRunnerFuture != null)
                    taskRunnerFuture.get();

            }
            
        }

        /**
         * Submits the {@link PrepareTask}s in a different thread, awaits their
         * {@link Future}s and logs any errors.
         * <p>
         * Note: The {@link PrepareTask}s are executed outside of the thread
         * that runs the {@link DistributedTxCommitTask} so that we may use the
         * thread running the {@link DistributedTxCommitTask} to obtain locks
         * from the {@link EmbeddedTransactionService#dataServiceLockManager}.
         */
        private class TaskRunner implements Callable<Void> {

            public TaskRunner() {
                
            }
            
            public Void call() throws Exception {

                // The task MUST NOT run in the commitThread.
                assert commitThread != Thread.currentThread();

                // This thread MUST NOT own the lock.
                assert !state.lock.isHeldByCurrentThread();
                
                /*
                 * The futures for the tasks used to invoke prepare(tx,rev) on
                 * each dataService.
                 */
                final List<Future<Void>> futures;
                final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                        nservices);

                for (ITxCommitProtocol dataService : services) {

                    tasks.add(new PrepareTask(dataService));

                }

                try {

                    /*
                     * Await all futures, returning once they are all done.
                     */

//BTM                    futures = getFederation().getExecutorService().invokeAll(tasks);
futures = threadPool.invokeAll(tasks);

                    // tx must be complete (either committed or aborted).
                    assert state.isComplete() : state.toString();

                } catch (Throwable t) {

                    /*
                     * If we can not invoke all tasks then abort
                     */

                    logger.error(t.getLocalizedMessage(), t);

                    state.setRunState(RunState.Aborted);

                    throw new RuntimeException(t);

                }

                List<Throwable> causes = null;
                for (Future f : futures) {

                    try {

                        f.get();

                    } catch (Throwable t) {

                        if (causes == null) {

                            causes = new LinkedList<Throwable>();

                        }

                        causes.add(t);

                        logger.error(t.getLocalizedMessage(), t);

                    }

                }

                if (causes != null) {

                    final int nfailed = causes.size();

                    state.setRunState(RunState.Aborted);

                    throw new ExecutionExceptions("Committer(s) failed: n="
                            + nservices + ", nfailed=" + nfailed, causes);

                }

                return null;
                
            } // call()

        } // class TaskRunner
    
        /**
         * Sets up the {@link TxState#preparedBarrier}. When the barrier action
         * runs it will change {@link RunState} to {@link RunState#Prepared} and
         * assign a <em>commitTime</em> to the transaction. When the barrier
         * breaks, the assigned <i>commitTime</i> will be reported back to the
         * {@link ShardService}s waiting in
         * {@link ITransactionService#prepared(long, UUID)} as the return value
         * for that method.
         */
        private void setupPreparedBarrier() {

            preparedBarrier = new CyclicBarrier(nservices,

            new Runnable() {

                /**
                 * Method runs when the "prepared" barrier breaks.
                 */
                public void run() {

                    state.lock.lock();

                    try {

                        state.setRunState(RunState.Prepared);

                        /*
                         * Wake up the main thread. It will obtain the necessary
                         * locks for the participating data services and then
                         * signal that we may continue.
                         */
                        prepared.signal();

                        try {
                            // wait until the necessary locks are held.
                            locksHeld.await();
                        } catch (InterruptedException ex) {
                            logger.warn("Interrupted", ex);
                            // re-throw the exception.
                            throw new RuntimeException(ex);
                        }
                        
                        // assign a commitTime to this tx.
                        final long commitTime = nextTimestamp();
                        
                        // Set the commitTime on the outer task.
                        DistributedTxCommitTask.this.commitTime = commitTime;
                        
                        // Set the commitTime on the tx.
                        state.setCommitTime(commitTime);

                    } finally {

                        state.lock.unlock();

                    }

                }

            });

        }

        /**
         * Sets up the {@link TxState#committedBarrier}. When the barrier
         * action runs it will change the {@link RunState} to
         * {@link RunState#Committed}.
         */
        protected void setupCommittedBarrier() {

            committedBarrier = new CyclicBarrier(nservices,

            new Runnable() {

                /**
                 * Method runs when the "committed" barrier breaks. At this
                 * point the transaction is fully committed on the participating
                 * data services.
                 */
                public void run() {

                    state.lock.lock();

                    try {

                        // wake up the main thread.
                        committed.signal();
                        
                        // Set the assigned commitTime on the TxState.
                        state.setCommitTime(commitTime);

                        // Change the tx run state.
                        state.setRunState(RunState.Committed);

                    } finally {

                        state.lock.unlock();

                    }

                }

            });

        }
        
        /**
         * Task issues {@link ITxCommitProtocol#prepare(long, long)} to an
         * {@link ShardService} participating in a distributed commit.
         */
        protected class PrepareTask implements Callable<Void> {

            final ITxCommitProtocol service;
            
            public PrepareTask(final ITxCommitProtocol service) {

                this.service = service;
                
            }

            public Void call() throws Exception {

                try {

                    service.prepare(state.tx, revisionTime);

                } catch (Throwable e) {

                    /*
                     * If an exception is thrown, then make sure that the tx
                     * is in the [Abort] state.
                     */
                    try {
                        logger.error(e.getLocalizedMessage(), e);
                    } catch (Throwable t) {
                        // ignored
                    }
                    
                    state.lock.lock();

                    try {

                        state.setRunState(RunState.Aborted);

                    } finally {

                        state.lock.unlock();

                    }
                    
                    throw new RuntimeException(e);

                }

                return null;

            }

        }

    }

    /**
     * Waits at "prepared" barrier. When the barrier breaks, examing the
     * {@link TxState}. If the transaction is aborted, then throw an
     * {@link InterruptedException}. Otherwise return the commitTime assigned
     * to the transaction.
     * 
     * @throws InterruptedException
     *             if the barrier is reset while the caller is waiting.
     */
    public long prepared(final long tx, final UUID dataService)
            throws IOException, InterruptedException, BrokenBarrierException {

        final DistributedTxCommitTask task = commitList.get(tx);
        
        if (task == null) {

            /*
             * Transaction is not committing.
             */
            
            throw new IllegalStateException();
            
        }
        
        final TxState state = task.state;
        
        state.lock.lock();
        
        try {
        
            if(!state.isStartedOn(dataService)) {
                
                throw new IllegalArgumentException();
                
            }
            
            // wait at the 'prepared' barrier.
            task.preparedBarrier.await();

            if (state.isAborted())
                throw new InterruptedException();

            return state.getCommitTime();
            
        } finally {
            
            state.lock.unlock();
            
        }
        
    }
    
    /**
     * Wait at "committed" barrier. When the barrier breaks, examing the
     * {@link TxState}. If the transaction is aborted, then return
     * <code>false</code>. Otherwise return true.
     * <p>
     * Note: The {@link TxState} will be aborted if any of the committers throws
     * an exception of their {@link ITxCommitProtocol#prepare(long, long)}
     * method.
     */
    public boolean committed(final long tx, final UUID dataService)
            throws IOException, InterruptedException, BrokenBarrierException {

        final DistributedTxCommitTask task = commitList.get(tx);
        
        if (task == null) {

            /*
             * Transaction is not committing.
             */
            
            throw new IllegalStateException();
            
        }        
        
        final TxState state = task.state;
        
        state.lock.lock();
        
        try {
        
            if(!state.isStartedOn(dataService)) {
                
                throw new IllegalArgumentException();
                
            }
            
            // wait at the 'committed' barrier.
            task.committedBarrier.await();

            if (state.isAborted())
                return false;

            return true;
            
        } finally {
            
            state.lock.unlock();
            
        }
                
    }

    protected long findCommitTime(final long timestamp) {
        
        synchronized(commitTimeIndex) {
            
            return commitTimeIndex.find(timestamp);
            
        }
        
    }

    protected long findNextCommitTime(long commitTime) {
        
        synchronized(commitTimeIndex) {
            
            return commitTimeIndex.findNext(commitTime);
            
        }

    }
    
    /**
     * @todo Is it a problem if the commit notices do not arrive in sequence?
     *       Because they will not. Unisolated operations will participate in
     *       group commits using timestamps obtained from the transaction
     *       service, but those commit operations will not be serialize and
     *       their reporting of the timestamps for the commits will likewise not
     *       be serialized.
     *       <p>
     *       The danger is that we could assign a read-historical transaction
     *       start time based on the {@link #commitTimeIndex} and then have
     *       commit timestamps arrive that are within the interval in which we
     *       made the assignment. Essentially, our interval was too large and
     *       the assigned start time may have been on either side of a
     *       concurrent commit. However, this can only occur for unisolated
     *       operations (non-transactional commits). The selected timestamp will
     *       always be coherent with respect to transaction commits since those
     *       are coordinated and use a shared commit time.
     *       <p>
     *       This issue can only arise when requesting historical reads for
     *       timestamps that are "close" to the most recent commit point since
     *       the latency involve would otherwise not effect the assignment of
     *       transaction start times. However, it can occur either when
     *       specifying the symbolic constant {@link ITx#READ_COMMITTED} to
     *       {@link #newTx(long)} or when specifying the exact commitTime
     *       reported by a transaction commit.
     *       <p>
     *       Simply stated, there is NO protection against concurrently
     *       unisolated operations committing. If such operations are used on
     *       the same indices as transactions, then it IS possible that the
     *       application will be unable to read from exactly the post-commit
     *       state of the transaction for a brief period (10s of milliseconds)
     *       until the unisolated commit notices have been propagated to the
     *       {@link DistributedTransactionService}. This issue will only occur
     *       when there is also a lot of contention for reading on the desired
     *       timestamp since otherwise the commitTime itself may be used as a
     *       transaction start time.
     * 
     * @todo depending on the latency involved and the issue described
     *       immediately above, it might be possible to simply queue these
     *       notices and consume them in an async thread. some operations (such
     *       as a distributed commit) might require that we catch up on the
     *       commit time notices in the queue. just thinking out loud here.
     */
    final public void notifyCommit(final long commitTime) {

        /*
         * Note: In order to avoid a deadlock, this must obtain the lock before
         * synchronizing on the commitTimeIndex since the method in the super
         * class will request that lock as well.
         */
        lock.lock();
        try {
            synchronized (commitTimeIndex) {
                /*
                 * Add all commit times
                 */
                commitTimeIndex.add(commitTime);

                /*
                 * Note: commit time notifications can be overlap such that they
                 * appear out of sequence with respect to their values. This is
                 * Ok. We just ignore any older commit times. However we do need
                 * to be synchronized here such that the commit time notices
                 * themselves are serialized so that we do not miss any.
                 */
                if (logger.isDebugEnabled())
                    logger.debug("commitTime="
                            + commitTime
                            + ", lastKnownCommitTime="
                            + lastCommitTime
                            + (lastCommitTime < commitTime ? " WILL UPDATE"
                                    : ""));

                if (lastCommitTime < commitTime) {
                    lastCommitTime = commitTime;

//BTM                    super.notifyCommit(commitTime);
updateReleaseTimeForBareCommit(commitTime);//BTM - from AbstractTransactionService.notifyCommit

                }
            }
        } finally {
            lock.unlock();
        }
    }
    
    final public long getLastCommitTime() {
        
        return lastCommitTime;
        
    }
    
    /**
     * Invokes {@link ITxCommitProtocol#setReleaseTime(long)} for a specific
     * shard service.
     */
    private static class SetReleaseTimeTask implements Callable<Void> {

        final ShardService dataService;

        final long releaseTime;

        public SetReleaseTimeTask(final ShardService dataService, final long releaseTime) {

            if (dataService == null)
                throw new IllegalArgumentException();

            if (releaseTime <= 0L)
                throw new IllegalArgumentException();

            this.dataService = dataService;

            this.releaseTime = releaseTime;

        }

        public Void call() throws Exception {

            dataService.setReleaseTime(releaseTime);

            return null;

        }

    }

    /**
     * Task periodically notifies the discovered {@link ShardService}s of the
     * new release time.
     * <p>
     * Note: Running a concurrent instance of this could cause release times to
     * be distributed that do not strictly advance. If you need to do this,
     * e.g., in order to immediately update the release time, then also
     * introduce a lock for this task so that instances of the task must
     * run in sequence.
     * 
     * @todo must also notify the metadata service once it is partitioned.
     * 
     * @todo We could monitor data service joins (for jini) and immediately
     *       notify newly joined data services of the current release time.
     * 
     * FIXME There is probably no reason to do this now that the resource
     * manager reaches out for the current release time before deciding which
     * resources it can release.
     */
    protected class NotifyReleaseTimeTask implements Runnable {

        private long lastReleaseTime = 0L;
        
        /**
         * Notifies all {@link ShardService}s of the current release time.
         * <p>
         * Note: An {@link ShardService} WILL NOT release its most current
         * commit point, regardless of the releaseTime that is sent to that
         * service.
         * <p>
         * Note: If this method throws an exception then the task will no longer
         * be scheduled!
         */
        public void run() {

            try {

                final long releaseTime = getReleaseTime();

                if (releaseTime == lastReleaseTime) {

                    // The release time has not been advanced.
                    return;
                    
                }
                
//BTM                final IBigdataFederation fed = getFederation();
//BTM
//BTM                final UUID[] a = fed.getDataServiceUUIDs(0/* maxCount */);
//BTM
//BTM                final IDataService[] services = getFederation().getDataServices(a);
final ShardService[] services = getDataServices();

//BTM                final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(a.length);
final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(services.length);

                for (ShardService dataService : services) {

                    tasks.add(new SetReleaseTimeTask(dataService, releaseTime));

                }

//BTM                logger.warn("Will set release time on " + a.length + " data services: releaseTime=" + releaseTime);
logger.info("setting release time on "+services.length+" shard service(s) [releaseTime="+releaseTime+"]");
                
//BTM                final List<Future<Void>> futures = getFederation().getExecutorService().invokeAll(tasks);
final List<Future<Void>> futures = threadPool.invokeAll(tasks);

                for (Future<Void> f : futures) {

                    try {

                        // verify no errors.
                        f.get();

                    } catch (Throwable t) {

                        /*
                         * Log an error if any data service can not be notified.
                         */

                        logger.error(t.getLocalizedMessage(), t);

                    }

                }

                // update the last release time.
                lastReleaseTime = releaseTime;
                
            } catch (Throwable t) {

                logger.error(t.getLocalizedMessage(), t);

            }

        }

    }
    
    /**
     * Adds counters for the {@link LockManager}.
     */
    synchronized public CounterSet getCounters() {

        if (countersRoot == null) {
            
            /*
             * Setup basic counters.
             */
//BTM            super.getCounters();
//BTM - BEGIN - from AbstractTransactionService.getCounters
        if (countersRoot == null) {
            countersRoot = new CounterSet();

            countersRoot.addCounter("runState", new Instrument<String>() {
                protected void sample() {
                    setValue(runState.toString());
                }
            });
            countersRoot.addCounter("#active", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getActiveCount());
                }
            });
            countersRoot.addCounter("lastCommitTime", new Instrument<Long>() {
                protected void sample() {
                    setValue(getLastCommitTime());
                }
            });
            countersRoot.addCounter("minReleaseAge", new Instrument<Long>() {
                protected void sample() {
                    setValue(getMinReleaseAge());
                }
            });
            countersRoot.addCounter("releaseTime", new Instrument<Long>() {
                protected void sample() {
                    setValue(getReleaseTime());
                }
            });
            countersRoot.addCounter("startCount", new Instrument<Long>() {
                protected void sample() {
                    setValue(getStartCount());
                }
            });
            countersRoot.addCounter("abortCount", new Instrument<Long>() {
                protected void sample() {
                    setValue(getAbortCount());
                }
            });
            countersRoot.addCounter("commitCount", new Instrument<Long>() {
                protected void sample() {
                    setValue(getCommitCount());
                }
            });
            countersRoot.addCounter("readOnlyActiveCount", new Instrument<Long>() {
                protected void sample() {
                    setValue(getReadOnlyActiveCount());
                }
            });
            countersRoot.addCounter("readWriteActiveCount", new Instrument<Long>() {
                protected void sample() {
                    setValue(getReadWriteActiveCount());
                }
            });

            /*
             * Reports the earliest transaction identifier -or- ZERO (0L) if
             * there are no active transactions.
             */
            countersRoot.addCounter("earliestTx", new Instrument<Long>() {
                protected void sample() {
                    synchronized (startTimeIndex) {
                        if (startTimeIndex.getEntryCount() == 0) {
                            // i.e., nothing running.
                            setValue(0L);
                        }
                        final long tx = startTimeIndex.find(1L);
                        setValue(tx);
                    }
                }
            });
        }
//BTM - END - from AbstractTransactionService.getCounters

            /**
             * The lock manager imposing a partial ordering on the prepare phase
             * of distributed transaction commits using the index partition
             * names as the named resources.
             */
            countersRoot.makePath("Index Lock Manager").attach(
                    ((EmbeddedTransactionService) this).indexLockManager
                            .getCounters());

            /**
             * The lock manager imposing a partial ordering on the commit phase
             * of distributed transaction commits using the data service UUIDs
             * as the named resources.
             */
            countersRoot.makePath("ShardService Lock Manager").attach(
                    ((EmbeddedTransactionService) this).dataServiceLockManager
                                    .getCounters());

            /**
             * The #of snapshots of the commit time index that have been written
             * to date.
             */
            countersRoot.addCounter("snapshotCount",
                    new Instrument<Long>() {
                        protected void sample() {
                            setValue(snapshotCount);
                        }
                    });

            /**
             * The #of distributed transaction commits that are currently in
             * progress.
             */
            countersRoot.addCounter("distributedCommitsInProgressCount",
                    new Instrument<Integer>() {
                        protected void sample() {
                            setValue(commitList.size());
                        }
                    });

            /**
             * The #of commit times that are currently accessible.
             */
            countersRoot.addCounter("commitTimesCount",
                    new Instrument<Integer>() {
                        protected void sample() {
                            synchronized (commitTimeIndex) {
                                setValue(commitTimeIndex.getEntryCount());
                            }
                        }
                    });

            countersRoot.addCounter("dataDir", new Instrument<String>() {
                protected void sample() {
                    setValue(dataDir.toString());
                }
            });

        }
        return countersRoot;
    }

    private void setupLoggingContext() {
        try {
            MDC.put("serviceUUID", this.serviceUUID);
            MDC.put("serviceName", SERVICE_NAME);
            MDC.put("hostname", this.hostname);
        } catch(Throwable t) { /* swallow */ }
    }


    /**
     * Clear the logging context.
     */
    private void clearLoggingContext() {
        MDC.remove("serviceName");
        MDC.remove("this.serviceUUID");
        MDC.remove("this.hostname");
    }

//BTM - NO LONGER NEEDED ???
//BTM    private class IDataServiceOnlyFilter 
//BTM                  implements ServiceItemFilter
//BTM    {
//BTM	public boolean check(ServiceItem item) {
//BTM            if((item == null) || (item.service == null)) {
//BTM                return false;
//BTM            }
//BTM            Class serviceType = (item.service).getClass();
//BTM            boolean isIDataService = 
//BTM             (IDataService.class).isAssignableFrom(serviceType);
//BTM            if( !isIDataService ) return false;
//BTM            boolean isIMetadataService = 
//BTM             (IMetadataService.class).isAssignableFrom(serviceType);
//BTM            return (isIDataService && !isIMetadataService);
//BTM        }
//BTM    }

//BTM - from AbstractTransactionService

    synchronized private final long _nextTimestamp() {
        return MillisecondTimestampFactory.nextMillis();
    }

    /**
     * This method MUST be invoked each time a transaction completes with the
     * absolute value of the transaction identifier that has just been
     * deactivated. The method will remove the transaction entry in the ordered
     * set of running transactions ({@link #startTimeIndex}). If the specified
     * timestamp corresponds to the earliest running transaction, then the
     * <code>releaseTime</code> will be updated and the new releaseTime will
     * be set using {@link #setReleaseTime(long)}.
     * <p>
     * Note that the {@link #startTimeIndex} contains the absolute value of the
     * transaction identifiers!
     * 
     * @param timestamp
     *            The absolute value of a transaction identifier that has just
     *            been deactivated.
     * 
     * @todo the {@link #startTimeIndex} could be used by
     *       {@link #findUnusedTimestamp(long, long)} so that it could further
     *       constrain its search within the half-open interval.
     */
    final private void updateReleaseTime(final long timestamp) {

        if (timestamp <= 0)
            throw new IllegalArgumentException();

        /*
         * Note: The calculation of the new release time needs to be atomic.
         * 
         * Note: This uses the same lock that we use to create new transactions
         * in order to prevent a new transaction from starting while we are
         * updating the release time.
         * 
         * @todo Should this also be used to serialize both handing out commit
         * times (for 2-phase commits) and acknowledging commit times (for
         * single phase commits)?
         */
        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        // current timestamp.
        final long now = _nextTimestamp();

        // current value for the releaseTime.
        final long oldReleaseTime = this.releaseTime;

        /*
         * true iff the tx specified by the caller was the earliest running
         * transaction.
         */
        final boolean isEarliestTx;

        /*
         * The earliest tx remaining now that the caller's tx is complete and
         * [now] if there are no more running transactions.
         */
        final long earliestTxStartTime;

        synchronized (startTimeIndex) {

			// Note: ZERO (0) is the first tuple in the B+Tree.
        	// Note: MINUS ONE (-1) means that the B+Tree is empty.
			final int indexOf = startTimeIndex.findIndexOf(timestamp);
			
			isEarliestTx = indexOf == 0;

			// remove start time from the index.
			if (indexOf != -1)
				startTimeIndex.remove(timestamp);

            if (!isEarliestTx) {

                // No change unless earliest tx terminates.
                return;

            }

            if (startTimeIndex.getEntryCount() > 0) {

                /*
                 * The start time associated with the earliest remaining tx.
                 */
                earliestTxStartTime = startTimeIndex.decodeKey(startTimeIndex
                        .keyAt(0));
                
            } else {

                /*
                 * There are no commit points and there are no active
                 * transactions.
                 */
                earliestTxStartTime = now;

            }

        } // synchronized(startTimeIndex)

        if (minReleaseAge == Long.MAX_VALUE) {

            return;
            
        }
        
        if (isEarliestTx) {

            // last commit time on the database.
            final long lastCommitTime = getLastCommitTime();

            // minimum milliseconds to retain history.
            final long minReleaseAge = getMinReleaseAge();

            /*
             * The release time will be the minimum of:
             * 
             * a) The timestamp BEFORE the lastCommitTime.
             * 
             * b) The timestamp BEFORE the earliestTxStartTime.
             * 
             * c) minReleaseAge milliseconds in the past.
             * 
             * Note: NEVER let go of the last commit time!
             * 
             * @todo there is a fence post here for [now-minReleaseAge] when
             * minReleaseAge is very large, e.g., Long#MAX_VALUE. This is caught
             * above for that specific value, but other very large values could
             * also cause problems.
             */
            final long releaseTime = Math.min(lastCommitTime - 1, Math.min(
                    earliestTxStartTime - 1, now - minReleaseAge));

            /*
             * We only update the release time if the computed time would
             * advance the releaseTime.
             * 
             * Note: The releaseTime MUST NOT go backwards since the database
             * may have already released history for any commit point whose
             * commitTime is LTE to the existing releaseTime.
             */
            if (this.releaseTime < releaseTime) {

                if (logger.isInfoEnabled())
                    logger.info("lastCommitTime=" + lastCommitTime
                            + ", earliestTxStartTime=" + earliestTxStartTime
                            + ", minReleaseAge=" + minReleaseAge + ", now="
                            + now + ", releaseTime(" + oldReleaseTime + "->"
                            + releaseTime + ")");

                // update.
                setReleaseTime(releaseTime);

            }

        }

    }

    /**
     * Change the {@link TxServiceRunState}.
     * 
     * @param newval
     *            The new value.
     * 
     * @throws IllegalStateException
     *             if the requested state is not a legal state change.
     */
    synchronized private void setRunState(final TxServiceRunState newval) {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (!runState.isTransitionLegal(newval)) {
            throw new IllegalStateException("runState=" + runState
                    + ", but newval=" + newval);
        }
        this.runState = newval;
        if (logger.isInfoEnabled()) {
            logger.info("runState=" + runState);
        }
    }

    /**
     * Adds the transaction to the local tables.
     * 
     * @param state
     *            The transaction.
     */
    private void activateTx(final TxState state) {
        if (state == null)
            throw new IllegalArgumentException();

        state.lock.lock();
        try {
            if (!state.isActive())

                throw new IllegalArgumentException();
            activeTx.put(state.tx, state);

            synchronized(startTimeIndex) {
                /*
                 * Note: Using the absolute value of the assigned timestamp so
                 * that the index is ordered earliest to most recent. This means
                 * that the absolute value of the timestamps must be unique,
                 * otherwise this will throw out an exception.
                 */
                startTimeIndex.add(Math.abs(state.tx));
            }

            startCount++;

            if(state.isReadOnly()) {
                readOnlyActiveCount.incrementAndGet();
            } else {
                readWriteActiveCount.incrementAndGet();   
            }
        } finally {
            state.lock.unlock();   
        }
    }

    /**
     * Removes the transaction from the local tables.
     * <p>
     * Note: The caller MUST own {@link TxState#lock} across this method and
     * MUST then do
     * 
     * <pre>
     * updateReleaseTime(long)
     * deactivateTx.signallAll()
     * </pre>
     * 
     * while holding the outer {@link #lock}.
     * <p>
     * Note: Normally this method is invoked without the outer {@link #lock}
     * which necessitates lifting those method calls out of this method and into
     * the caller.
     * 
     * @param state
     *            The transaction.
     * 
     * @throws IllegalMonitorStateException
     *             unless the caller is holding the {@link TxState#lock}.
     */
    private void deactivateTx(final TxState state) {
        if (state == null)
            throw new IllegalArgumentException();

        if (!state.lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (!state.isComplete())
            throw new IllegalArgumentException();

        if (state.isAborted()) {
            abortCount++;
        } else {
            commitCount++;
        }

        if (state.isReadOnly()) {
            readOnlyActiveCount.decrementAndGet();
        } else {
            readWriteActiveCount.decrementAndGet();
        }

        if (activeTx.remove(state.tx) == null) {
            logger.warn("Transaction not in table: " + state);
        }

        if (logger.isInfoEnabled())
            logger.info(state.toString());
    }

    /**
     * Assign a transaction identifier for a new transaction.
     * 
     * @param timestamp
     *            The timestamp.
     * 
     * @return The assigned transaction identifier.
     * 
     * @throws InterruptedException
     *             if interrupted while awaiting a start time which would
     *             satisify the request.
     * @throws InterruptedException
     *             if a timeout occurs while awaiting a start time which would
     *             satisify the request.
     */
    private long assignTransactionIdentifier(final long timestamp)
                     throws InterruptedException, TimeoutException
    {
        if (timestamp == ITx.UNISOLATED) {
            /*
             * When timestamp is ZERO (0L), this simply returns the next
             * distinct timestamp (with its sign bit flipped).
             * 
             * Note: This is guarenteed to be a valid start time since it
             *       is less than the next possible commit point for the
             *       database.
             * 
             * Note: When validation is performed, reads will be performed
             *       from [-startTime] and the journal will identify the
             *       1st commit point that is less than or equal to
             *       [-startTime]; which will be the most recent commit
             *       point on the database as of the moment when this
             *       transaction identifier was assigned.
             */
            return -nextTimestamp();
        }

        final long lastCommitTime = getLastCommitTime();

        if (timestamp > lastCommitTime) {
            /*
             * Can't request a historical read for a timestamp which has
             * not yet been issued by the service
             */
            throw new IllegalStateException
                          ("timestamp in the future [timestamp="+timestamp
                           + ", lastCommitTime="+lastCommitTime+"]");

        } else if (timestamp == lastCommitTime) {
            /*
             * Special case - just return the next timestamp.
             * 
             * Note: This is equivalent to a request using the symbolic
             *       constant READ_COMMITTED.
             */
            return nextTimestamp();
        }

        if (timestamp == ITx.READ_COMMITTED) {
            /*
             * This is a symbolic shorthand for a read-only transaction that
             * will read from the most recent commit point on the database.
             * 
             * Note: can't just issue a timestamp since it will be greater
             *       than lastCommitTime.
             * 
             * Note: If [lastCommitTime == 0], still issue the next timestamp.
             */
            return nextTimestamp();
        }
        final long releaseTime = getReleaseTime();

        if (timestamp <= releaseTime) {
            /*
             * This exception is thrown if there is an attempt to start a new
             * transaction that would read from historical data which has been
             * released. While the data MIGHT still be around, there is no way
             * to assert a read lock for that data since the releaseTime is
             * already in the future.
             */
            throw new IllegalStateException
                      ("timestamp less than or equal to release time"
                       +" [timestamp="+timestamp+", "
                       +"releaseTime="+releaseTime+"]");
        }
        return getStartTime(timestamp);
    }

    /**
     * Assign a distinct timestamp to a historical read that will read from the
     * commit point identified by the specified timestamp.
     * <p>
     * Note: Under some circumstances the assignment of a read-only transaction
     * identifier must be delayed until a distinct timestamp becomes available
     * between the designed start time and the next commit point.
     * 
     * @param timestamp
     *            The timestamp (identifies the desired commit point).
     * 
     * @return A distinct timestamp not in use by any transaction that will read
     *         from the same commit point.
     */
    private long getStartTime(final long timestamp)
                     throws InterruptedException, TimeoutException
    {
        /*
         * Find the commit time from which the tx will read (largest commitTime
         * LTE timestamp).
         */
        final long commitTime = findCommitTime(timestamp);

        if (commitTime == -1L) {
            /*
             * @todo I believe that this can only arise when there are no commit
             * points in the log.
             */
            throw new RuntimeException
                      ("no data for commit time [timestamp="+timestamp+"]");
        }

        /*
         * The commit time for the successor of that commit point (GT).
         */
        final long nextCommitTime = findNextCommitTime(commitTime);

        if (nextCommitTime == -1L) {
            /*
             * Note: If there is no successor of the desired commit point then
             *       the next timestamp can just be returned; which is 
             *       guarenteed to be greater than the desired commit time
             *       and less than the next commit point. [Note: this
             *       case is -- in fact -- handled above; so you should
             *       not get here.]
             */
            return nextTimestamp();
        }

        // Find a valid, unused timestamp.
        return findUnusedTimestamp(commitTime, nextCommitTime,
                                   1000/* timeout */, TimeUnit.MILLISECONDS);
    }

    /**
     * Find a valid, unused timestamp.
     * <p>
     * Note: Any timestamp in the half-open range [commitTime:nextCommitTime)
     * MAY be assigned as all such timestamps will read from the commit point
     * associated with [commitTime].
     * 
     * @param commitTime
     *            The commit time for the commit point on which the tx will read
     *            (this must be the exact timestamp associated with the desired
     *            commit point).
     * @param nextCommitTime
     *            The commit time for the successor of that commit point.
     * @param timeout
     *            The maximum length of time to await an available timestamp.
     * @param unit
     *            The unit in which <i>timeout</i> is expressed.
     */
    private long findUnusedTimestamp(final long commitTime,
                                     final long nextCommitTime,
                                     final long timeout,
                                     final TimeUnit unit)
                     throws InterruptedException, TimeoutException
    {
        long nanos = unit.toNanos(timeout);

        final long begin = System.nanoTime();
        
        while (nanos >= 0) {

            for (long t = commitTime; t < nextCommitTime; t++) {
                if (activeTx.containsKey(t) || activeTx.containsKey(-t)) {
                    /*
                     * Note: We do not accept an active read-only startTime.
                     * 
                     * Note: We do not accept a start time that corresponds to
                     * the absolute value of an active read-write transaction
                     * either. This latter constraint is imposed so that the
                     * keys in the [startTimeIndex] can be the absolute value of
                     * the assigned timestamp and still be unique.
                     * 
                     * @todo We could grab the timestamp using an atomic
                     * putIfAbsent and a special value and the replace the value
                     * with the desired one (or just construct the TxState
                     * object each time and discard it if the map contains that
                     * key). This might let us increase concurrency for newTx().
                     */
                    continue;
                }
                return t;
            }

            /*
             * Wait for a tx to terminate. If it is in the desired half-open
             * range it will be detected by the loop above.
             * 
             * Note: This requires that we use signalAll() since we could be
             * waiting on more than one half-open range.
             * 
             * @todo if we used a Condition for the half-open range then we
             * could signal exactly that condition.
             * 
             * Note: throws InterruptedException
             */
            nanos -= (System.nanoTime() - begin);

            if(!txDeactivate.await(nanos, TimeUnit.NANOSECONDS)) {
                throw new TimeoutException
                              ("EmbeddedTransactionService.findUnusedTimestamp "
                               +"- [!txDeactivate.await]");
            }
            nanos -= (System.nanoTime() - begin);
        }

        throw new TimeoutException
                      ("EmbeddedTransactionService.findUnusedTimestamp");
    }

    /**
     * Wait until active transactions complete.
     * 
     * @param logTimeout
     *            The timeout between {@link #logTimeout(long, TimeUnit)}
     *            messages.
     * @param unit
     *            The unit for that timeout.
     * 
     * @throws InterruptedException
     *             if this method is interrupted.
     */
    private void awaitRunningTx(long logTimeout, final TimeUnit unit)
                     throws InterruptedException
    {
        final long begin = System.nanoTime();
        long lastLogTime = begin;

        // convert to nanoseconds.
        logTimeout = unit.toNanos(logTimeout);

        long elapsed = 0L;

        if(logger.isInfoEnabled())
            logger.info("activeCount="+getActiveCount());

        while (getActiveCount() > 0) {
            // wait for a transaction to complete.
            if (txDeactivate.await(logTimeout, TimeUnit.NANOSECONDS)
                    && getActiveCount() == 0) {

                // no more tx are active.

                // update the elapsed time.
                elapsed = System.nanoTime() - begin;

                if(logger.isInfoEnabled())
                    logger.info("no transactions remaining "
                                +"[elapsed="+elapsed+"]");
                return;
            }
            
            // update the elapsed time.
            elapsed = System.nanoTime() - begin;

            {
                final long now = System.nanoTime();
                final long elapsedLogTime = now - lastLogTime;
                if (elapsedLogTime >= logTimeout) {
                    try {
                        logTimeout(elapsed, TimeUnit.NANOSECONDS);
                    } catch (Throwable t) {
                        logger.error("Ignored", t);
                    }
                }
                lastLogTime = now;
            }
        }// while(true)
    }

    /**
     * Logs periodic messages during shutdown.
     * 
     * @param elapsed
     *            The elapsed time since shutdown was requested.
     * @param unit
     *            The unit in which that time is measured.
     */
    private void logTimeout(final long elapsed, final TimeUnit unit) {
        
        logger.warn
              ("Waiting on task(s)" + ": elapsed="
                + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms, #active="
                + getActiveCount() + ", #readWrite="
                + getReadWriteActiveCount() + ", #readOnly="
                + getReadOnlyActiveCount());
    }

    /**
     * If there are NO active transactions and the current releaseTime
     * is less than (commitTime-1), then compute and set the new
     * releaseTime.
     * <p>
     * Note: This method was historically part of {@link #notifyCommit(long)}.
     * It was moved into its own method so it can be overriden for some
     * unit tests.
     * 
     * @throws IllegalMonitorStateException
     *             unless the caller is holding the lock.
     */
    protected void updateReleaseTimeForBareCommit(final long commitTime) {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        synchronized (startTimeIndex) {

            if (this.releaseTime < (commitTime - 1)
                    && startTimeIndex.getEntryCount() == 0)
            {
                final long lastCommitTime = commitTime;
                final long now = _nextTimestamp();
                final long releaseTime = 
                    Math.min(lastCommitTime - 1, now - minReleaseAge);

                if (this.releaseTime < releaseTime) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Advancing releaseTime (no active tx)"
                                + ": lastCommitTime=" + lastCommitTime
                                + ", minReleaseAge=" + minReleaseAge + ", now="
                                + now + ", releaseTime(" + this.releaseTime
                                + "->" + releaseTime + ")");
                    }
                    setReleaseTime(releaseTime);
                }
            }
        }

    }

    public long nextTimestamp() {

        /*
         * Note: This method is allowed in all run states (after startup)
         * since so much depends on the ability to obtain timestamps,
         * including the unisolated operations on individual journals or
         * shard services.
         */
        switch (runState) {
        case Starting:
//            case Halted:
            throw new IllegalStateException(ERR_SERVICE_NOT_AVAIL);
        default:
            break;
        }
        return _nextTimestamp();
    }

    /**
     * Return the {@link RunState}.
     */
    public TxServiceRunState getRunState() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        return runState;
    }

    /**
     * Note: Only those shard services on which a read-write transaction
     * has started will participate in the commit. If there is only a
     * single such shard service, then a single-phase commit will be used.
     * Otherwise a distributed transaction commit protocol will be used.
     * <p>
     * Note: The commits requests are placed into a partial order by sorting the
     * total set of resources which the transaction declares (via this method)
     * across all operations executed by the transaction and then contending
     * for locks on the named resources using a LockManager.
     */
    public void declareResources(final long tx, final UUID dataServiceUUID,
            final String[] resource) throws IllegalStateException {

        setupLoggingContext();

        lock.lock();
        try {

            switch (getRunState()) {
            case Running:
            case Shutdown:
                break;
            default:
                throw new IllegalStateException(ERR_SERVICE_NOT_AVAIL);
            }

            if (dataServiceUUID == null)
                throw new IllegalArgumentException();

            if (resource == null)
                throw new IllegalArgumentException();

            final TxState state = activeTx.get(tx);

            if (state == null) {

                throw new IllegalStateException(ERR_NO_SUCH);

            }

            state.lock.lock();

            try {

                if (state.isReadOnly()) {

                    throw new IllegalStateException(ERR_READ_ONLY);

                }

                if (!state.isActive()) {

                    throw new IllegalStateException(ERR_NOT_ACTIVE);

                }

                state.declareResources(dataServiceUUID, resource);

            } finally {

                state.lock.unlock();

            }

        } finally {

            lock.unlock();
            clearLoggingContext();

        }
        
    }

    /**
     * @see Options#MIN_RELEASE_AGE
     */
    public long getReleaseTime() {
        if (logger.isDebugEnabled())
            logger.debug("releaseTime=" + releaseTime + ", lastKnownCommitTime="
                    + getLastCommitTime());
        return releaseTime;
    }

    /**
     * Note: There is an upper bound of one read-write transaction that may be
     * created per millisecond (the resolution of {@link #nextTimestamp()}) and
     * requests for new read-write transactions contend with other requests for
     * {@link #nextTimestamp()}.
     * <p>
     * Note: The transaction service will refuse to start new transactions whose
     * timestamps are LTE to {@link #getEarliestTxStartTime()}.
     * 
     * @throws RuntimeException
     *             Wrapping {@link TimeoutException} if a timeout occurs
     *             awaiting a start time which would satisify the request for a
     *             read-only transaction (this can occur only for read-only
     *             transactions which must contend for start times which will
     *             read from the appropriate historical commit point).
     */
    public long newTx(final long timestamp) {

        setupLoggingContext();
        try {
            /*
             * Note: It may be possible to increase the concurrency of this
             * operation. Many cases do not allow contention since they will
             * just use the value returned by nextTimestamp(), which is always
             * distinct. Those cases which do allow contention involve search
             * for a start time that can read from a specific commit point. Even
             * then we may be able to reduce contention using atomic operations
             * on [activeTx], e.g., putIfAbsent().
             * 
             * However, pay attention to [lock]. Certainly it is serializing
             * newTx() at this point as well several other methods on this API.
             * Higher concurrency will require relaxing constraints on atomic
             * state transitions governed by [lock]. Perhaps by introducing
             * additional locks that are more specific. I don't want to relax
             * those constaints until I have a better sense of what must be
             * exclusive operations.
             */

            lock.lock();
            switch (getRunState()) {
            case Running:
                break;
            default:
                throw new IllegalStateException(ERR_SERVICE_NOT_AVAIL);
            }
            try {
                final long tx = assignTransactionIdentifier(timestamp);
                activateTx(new TxState(tx));
                return tx;
            } catch(TimeoutException ex) {
                throw new RuntimeException(ex);
            } catch(InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                lock.unlock();
            }
        } finally {
            clearLoggingContext();
        }
    }


    /**
     * Commit the transaction.
     */
     public long commit(final long tx) throws ValidationError {

        setupLoggingContext();
        try {
            switch (runState) {
            case Running:
            case Shutdown:
                break;
            default:
                throw new IllegalStateException(ERR_SERVICE_NOT_AVAIL);
            }

            final TxState state = activeTx.get(tx);
            if (state == null) {
                throw new IllegalStateException(ERR_NO_SUCH);
            }

            boolean wasActive = false;
            state.lock.lock();
            try {
                if (!state.isActive()) {
                    throw new IllegalStateException(ERR_NOT_ACTIVE);
                }
                wasActive = true;

                try {
                    final long commitTime = commitImpl(state);
                    assert state.isCommitted() : "tx=" + state;
                    return commitTime;
                } catch (Throwable t2) {
//                    logger.error(t2.getMessage(), t2);
                    assert state.isAborted() : "ex=" + t2 + ", tx=" + state;
                    if (InnerCause.isInnerCause(t2, ValidationError.class)) {
                        throw new ValidationError();
                    }
                    logger.error(t2.getMessage(), t2);
                    throw new RuntimeException(t2);
                }
            } finally {
                try {
                    if (wasActive) {
                        deactivateTx(state);
                    }
                } finally {
                    /*
                     * Note: This avoids a lock ordering problem by releasing
                     * the inner lock (state.lock) before acquiring the order
                     * lock.
                     */
                    state.lock.unlock();
                    if (wasActive) {
                        lock.lock();
                        try {
                            updateReleaseTime(Math.abs(state.tx));
                            /*
                             * Note: signalAll() is required. See code that
                             * searches the half-open range for a
                             * read-historical timestamp. It waits on this
                             * signal, but there can be more than one request
                             * waiting an requests can be waiting on different
                             * half-open ranges.
                             */
                            txDeactivate.signalAll();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
        } finally {
            clearLoggingContext();
        }
    }

    /**
     * Abort the transaction (asynchronous).
     */
    public void abort(final long tx) {

        setupLoggingContext();
        try {
            switch (runState) {
            case Running:
            case Shutdown:
                break;
            default:
                throw new IllegalStateException(ERR_SERVICE_NOT_AVAIL);
            }
            
            final TxState state = activeTx.get(tx);

            if (state == null)
                throw new IllegalStateException(ERR_NO_SUCH);

            boolean wasActive = false;
            state.lock.lock();

            try {
                if (!state.isActive()) {
                    throw new IllegalStateException(ERR_NOT_ACTIVE);
                }
                wasActive = true;
                try {
                    abortImpl(state);
                    assert state.isAborted() : state.toString();
                } catch (Throwable t) {
                    logger.error(state.toString(),t);
                }
            } finally {
                try {
                    if (wasActive) {
                        deactivateTx(state);
                    }
                } finally {
                    /*
                     * Note: This avoids a lock ordering problem by releasing
                     * the inner lock (state.lock) before acquiring the order
                     * lock.
                     */
                    state.lock.unlock();
                    if (wasActive) {
                        lock.lock();
                        try {
                            updateReleaseTime(Math.abs(state.tx));
                            /*
                             * Note: signalAll() is required. See code that
                             * searches the half-open range for a
                             * read-historical timestamp. It waits on this
                             * signal, but there can be more than one request
                             * waiting an requests can be waiting on different
                             * half-open ranges.
                             */
                            txDeactivate.signalAll();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
        } finally {
            clearLoggingContext();
        }
    }

    /**
     * The #of open transactions in any {@link RunState}.
     */
    final public int getActiveCount() {
        return activeTx.size();
    }

    /**
     * Return the minimum #of milliseconds of history that must be preserved.
     * 
     * @todo This centralizes the value for the minimum amount of history that
     *       will be preserved across the federation.
     *       <p>
     *       If minReleaseTime is increased, then the release time can be
     *       changed to match, but only by NOT advancing it until we are
     *       retaining enough history.
     *       <p>
     *       If minReleaseTime is decreased, then we can immediately release
     *       more history (or at least as soon as the task runs to notify the
     *       discovered data services of the new release time).
     */
    final public long getMinReleaseAge() {
        return minReleaseAge;
    }

    /** #of transaction started. */
    public long getStartCount() {
        return startCount;
    }        

    /** #of transaction committed. */
    public long getCommitCount() {
        return commitCount;
    }

    /** #of transactions aborted. */
    public long getAbortCount() {
        return abortCount;
    }

    public long getReadOnlyActiveCount() {
        return readOnlyActiveCount.get();
    }

    public long getReadWriteActiveCount() {
        return readWriteActiveCount.get();
    }


//BTM - BEGIN - from AbstractFederation ---------------------------------
    /**
     * Adds a task which will run until canceled, until it throws an
     * exception, or until the service is shut down.
     * <p>
     * Note: Tasks run on this service generally update sampled values on
     * {@link ICounter}s reported to the load balancer service. Basic
     * information on the threadPool is reported
     * automatically. Clients may add additional tasks to report on
     * client-side aspects of their application.
     * <p>
     * Note: Non-sampled counters are automatically conveyed to the
     * load balancer service once added to the basic {@link CounterSet}
     * returned by {@link #getCounterSet()}.
     * 
     * @param task
     *            The task.
     * @param initialDelay
     *            The initial delay.
     * @param delay
     *            The delay between invocations.
     * @param unit
     *            The units for the delay parameters.
     * 
     * @return The {@link ScheduledFuture} for that task.
     */
    private ScheduledFuture addScheduledTask(Runnable task,
                                             long initialDelay,
                                             long delay,
                                             TimeUnit unit)
    {
        if (task == null) {
            throw new IllegalArgumentException("null task");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Scheduling task: task=" + task.getClass()
                    + ", initialDelay=" + initialDelay + ", delay=" + delay
                    + ", unit=" + unit);
        }
        return scheduledExecutorService.scheduleWithFixedDelay
                                        (task, initialDelay, delay, unit);
    }

//BTM - END   - from AbstractFederation ---------------------------------

}
