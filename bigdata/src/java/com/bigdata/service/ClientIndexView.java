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
/*
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.AbstractKeyRangeIndexProcedure;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.LongAggregator;
import com.bigdata.btree.RangeCountProcedure;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.BatchContains.BatchContainsConstructor;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.BatchRemove.BatchRemoveConstructor;
import com.bigdata.btree.IIndexProcedure.IKeyArrayIndexProcedure;
import com.bigdata.btree.IIndexProcedure.IKeyRangeIndexProcedure;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IBigdataClient.Options;
import com.bigdata.util.InnerCause;

/**
 * <p>
 * A client-side view of a scale-out index as of some <i>timestamp</i>.
 * </p>
 * <p>
 * This view automatically handles the split, join, or move of index partitions
 * within the federation. The {@link IDataService} throws back a (sometimes
 * wrapped) {@link StaleLocatorException} when it does not have a registered
 * index as of some timestamp. If this exception is observed when the client
 * makes a request using a cached {@link PartitionLocator} record then the
 * locator record is stale. The client automatically fetches the locator
 * record(s) covering the same key range as the stale locator record and the
 * re-issues the request against the index partitions identified in those
 * locator record(s). This behavior correctly handles index partition split,
 * merge, and move scenarios. The implementation of this policy is limited to
 * exactly three places in the code: {@link AbstractDataServiceProcedureTask},
 * {@link PartitionedRangeQueryIterator}, and {@link DataServiceRangeIterator}.
 * </p>
 * <p>
 * Note that only {@link ITx#UNISOLATED} and {@link ITx#READ_COMMITTED}
 * operations are subject to stale locators since they are not based on a
 * historical committed state of the database. Historical read and
 * fully-isolated operations both read from historical committed states and the
 * locators are never updated for historical states (only the current state of
 * an index partition is split, joined, or moved - the historical states always
 * remain behind).
 * </p>
 * 
 * FIXME Add counters and report to the load balancer (a version for clients).
 * average responseTime, average queueLength, latency due to RMI (by also
 * obtaining the response time of the data service itself), #of procedures run
 * and the execution time for those procedures; #of splits; #of tuples in each
 * split; total #of tuples.
 * 
 * @todo If the index was dropped then that should cause the operation to abort
 *       (only possible for read committed or unisolated operations).
 *       <p>
 *       Likewise, if a transaction is aborted, then then index should refuse
 *       further operations.
 * 
 * @todo detect data service failure and coordinate cutover to the failover data
 *       services. ideally you can read on a failover data service at any time
 *       but it should not accept write operations unless it is the primary data
 *       service in the failover chain.
 *       <p>
 *       Offer policies for handling index partitions that are unavailable at
 *       the time of the request (continued operation during partial failure).
 * 
 * @todo We should be able to transparently use either a hash mod N approach to
 *       distributed index partitions or a dynamic approach based on overflow.
 *       This could even be decided on a per-index basis. The different
 *       approaches would be hidden by appropriate implementations of this
 *       class.
 *       <p>
 *       A hash partitioned index will need to enforce optional read-consistent
 *       semantics. This can be done by choosing a recent broadcast commitTime
 *       for the read or by re-issuing queries that come in with a different
 *       commitTime.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexView implements IClientIndex {

    protected static final transient Logger log = Logger
            .getLogger(ClientIndexView.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    private final IBigdataFederation fed;

    public IBigdataFederation getFederation() {
        
        return fed;
        
    }
    
    /**
     * The thread pool exposed by {@link IBigdataFederation#getThreadPool()}
     */
    protected ThreadPoolExecutor getThreadPool() {

        return (ThreadPoolExecutor)fed.getThreadPool();

    }

    /**
     * The timeout in milliseconds for tasks run on an {@link IDataService}.
     * 
     * @see Options#CLIENT_TASK_TIMEOUT
     */
    private final long taskTimeout;
    
    /**
     * 
     */
    protected static final String NON_BATCH_API = "Non-batch API";

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for locating code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     */
    private final boolean batchOnly;

    /**
     * The default capacity for the {@link #rangeIterator(byte[], byte[])}
     */
    private final int capacity;

    /**
     * The timestamp from the ctor.
     */
    private final long timestamp;

    public long getTimestamp() {
        
        return timestamp;
        
    }

    /**
     * The name of the scale-out index (from the ctor).
     */
    private final String name;
    
    public String getName() {
        
        return name;
        
    }

    /**
     * The {@link IndexMetadata} for the {@link MetadataIndex} that manages the
     * scale-out index. The metadata template for the managed scale-out index is
     * available as a field on this object.
     */
    private final MetadataIndexMetadata metadataIndexMetadata;
    
    /**
     * Obtain the proxy for a metadata service. if this instance fails, then we
     * can always ask for a new instance for the same federation (failover).
     */
    final protected IMetadataService getMetadataService() {
        
        return fed.getMetadataService();
        
    }
    
    /**
     * Return a view of the metadata index for the scale-out index as of the
     * timestamp associated with this index view.
     */
    final protected IMetadataIndex getMetadataIndex() {
        
        return fed.getMetadataIndex(name,timestamp);
        
    }
    
    /**
     * 
     * @see #getRecursionDepth()
     */
    private ThreadLocal<AtomicInteger> recursionDepth = new ThreadLocal<AtomicInteger>() {
        protected synchronized AtomicInteger initialValue() {
            return new AtomicInteger();
        }
    };

    /**
     * Return a {@link ThreadLocal} {@link AtomicInteger} whose value is the
     * recursion depth of the current {@link Thread}. This is initially zero
     * when the task is submitted by the application. The value incremented when
     * a task results in a {@link StaleLocatorException} and is decremented when
     * returning from the recursive handling of the
     * {@link StaleLocatorException}.
     * <p>
     * The recursion depth is used:
     * <ol>
     * <li>to limit the #of retries due to {@link StaleLocatorException}s for
     * a split of a task submitted by the application</li>
     * <li> to force execution of retried tasks in the caller's thread.</li>
     * </ol>
     * The latter point is critical - if the retry tasks are run in the client
     * {@link #getThreadPool() thread pool} then all threads in the pool can
     * rapidly become busy awaiting retry tasks with the result that the client
     * is essentially deadlocked.
     * 
     * @return The recursion depth.
     */
    protected AtomicInteger getRecursionDepth() {

        return recursionDepth.get();
        
    }
    
    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param name
     *            The index name.
     * @param timestamp
     *            Either the startTime of an active transaction,
     *            {@link ITx#UNISOLATED} for the current unisolated index view,
     *            {@link ITx#READ_COMMITTED} for a read-committed view, or
     *            <code>-timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * @param metadataIndexMetadata
     *            The metadata for the {@link MetadataIndex} as of that
     *            timestamp. This also contains the template
     *            {@link IndexMetadata} for the scale-out index partitions.
     */
    public ClientIndexView(IBigdataFederation fed, String name, long timestamp,
            MetadataIndexMetadata metadataIndexMetadata) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
        
        if (metadataIndexMetadata == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;

        this.name = name;

        this.timestamp = timestamp;
        
        this.metadataIndexMetadata = metadataIndexMetadata;
        
        this.capacity = fed.getClient().getDefaultRangeQueryCapacity();
        
        this.batchOnly = fed.getClient().getBatchApiOnly();

        this.taskTimeout = fed.getClient().getTaskTimeout();
        
    }

    /**
     * Metadata for the {@link MetadataIndex} that manages the scale-out index
     * (cached).
     */
    public MetadataIndexMetadata getMetadataIndexMetadata() {
     
        return metadataIndexMetadata;
        
    }
    
    /**
     * The metadata for the managed scale-out index. Among other things, this
     * gets used to determine how we serialize keys and values for
     * {@link IKeyArrayIndexProcedure}s when we serialize a procedure to be
     * sent to a remote {@link IDataService}.
     */
    public IndexMetadata getIndexMetadata() {

        return metadataIndexMetadata.getManagedIndexMetadata();

    }

    /**
     * FIXME Since scale-out indices can be very large this method should report
     * only on aspects of the clients access to the scale-out index rather than
     * attempting to aggregate the data from the various index partitions.
     */
    synchronized public ICounterSet getCounters() {

        if (counterSet == null) {

            counterSet = new CounterSet();
            
            counterSet.addCounter("name", new OneShotInstrument<String>(name));

            counterSet.addCounter("timestamp", new OneShotInstrument<Long>(timestamp));

        }
        
        return counterSet;
        
//        StringBuilder sb = new StringBuilder();
//
//        sb.append("scale-out index: name="+name);
//
//        /*
//         * Statistics for the metadata index.
//         */
//        try {
//            
//            String _name = MetadataService.getMetadataIndexName(name);
//            
//            sb.append("\n" + _name + " : "
//                    + getMetadataService().getStatistics(_name));
//
//        } catch (Exception ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//        /*
//         * Statistics for the index partitions (at least those that are cached
//         * by the client).
//         */
//        {
//            
//            final IMetadataIndex mdi = getMetadataIndex();
//            
//            final ITupleIterator itr = mdi.rangeIterator(null, null);
//            
//            while(itr.hasNext()) {
//                
//                final ITuple tuple = itr.next();
//                
//                final PartitionLocator pmd = (PartitionLocator) SerializerUtil.deserialize(tuple.getValue());
//                
//                final String _name = DataService.getIndexPartitionName(name, pmd.getPartitionId());
//                
//                sb.append("\npartition: " + _name);
//                sb.append("\ndataServices: " + Arrays.toString(pmd.getDataServices()));
//                
//                String _stats;
//                try {
//                    
//                    _stats = getDataService(pmd).getStatistics( _name);
//                
//                } catch (NoSuchIndexException e) {
//                    
//                    log.info("Index not found on data service, presumed moved: "+_name);
//
//                    continue;
//                    
//                } catch (Exception e) {
//                    
//                    _stats = "Could not obtain index partition statistics: "+e.toString();
//                    
//                }
//                
//                sb.append( "\nindexStats: "+_stats);
//                
//            }
//            
//        }
//        
//        return sb.toString();
        
    }
    private CounterSet counterSet;
    
    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public boolean contains(byte[] key) {
        
        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        
//        final BatchContains proc = new BatchContains(//
//                1, // n,
//                0, // offset
//                keys
//        );
//
//        final boolean[] ret = ((ResultBitBuffer) submit(key, proc)).getResult();
        
        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/*fromIndex*/,1/*toIndex*/, keys, null/*vals*/,BatchContainsConstructor.INSTANCE, resultHandler);

        return ((ResultBitBuffer) resultHandler.getResult()).getResult()[0];
        
    }
    
    public byte[] insert(byte[] key, byte[] value) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        final byte[][] vals = new byte[][] { value };
        
//        final BatchInsert proc = new BatchInsert(//
//                1, // n,
//                0, // offset
//                new byte[][] { key }, // keys
//                new byte[][] { value }, // vals
//                true // returnOldValues
//        );
//
//        final byte[][] ret = ((ResultBuffer) submit(key, proc)).getResult();
//
//        return ret[0];

        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/*fromIndex*/,1/*toIndex*/,keys,vals,BatchInsertConstructor.RETURN_OLD_VALUES,resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public byte[] lookup(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
//        final BatchLookup proc = new BatchLookup(//
//                1, // n,
//                0, // offset
//                new byte[][] { key } // keys
//        );
//
//        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();
//
//        return ret[0];

        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/*fromIndex*/,1/*toIndex*/,keys,null/*vals*/,BatchLookupConstructor.INSTANCE,resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public byte[] remove(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
//        final BatchRemove proc = new BatchRemove(//
//                1, // n,
//                0, // offset
//                new byte[][] { key }, // keys
//                true // returnOldValues
//        );
//
//        final byte[][] ret = ((ResultBuffer) submit(key, proc)).getResult();
//
//        return ret[0];

        final IResultHandler resultHandler = new IdentityHandler();
        
        submit(0/*fromIndex*/,1/*toIndex*/,keys,null/*vals*/,BatchRemoveConstructor.RETURN_OLD_VALUES,resultHandler);
     
        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    /*
     * All of these methods need to divide up the operation across index
     * partitions.
     */
    
    /**
     * Returns the sum of the range count for each index partition spanned by
     * the key range.
     */
    public long rangeCount(byte[] fromKey, byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
        
    }

    /**
     * An {@link ITupleIterator} that kinds the use of a series of
     * {@link ResultSet}s to cover all index partitions spanned by the key
     * range.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        
        return rangeIterator(fromKey, toKey, capacity, IRangeQuery.KEYS
                | IRangeQuery.VALS/* flags */, null/*filter*/);
        
    }

    /**
     * Identifies the index partition(s) that are spanned by the key range query
     * and maps an iterator across each index partition. The iterator buffers
     * responses up to the specified capacity and a follow up iterator request
     * is automatically issued if the iterator has not exhausted the key range
     * on a given index partition. Once the iterator is exhausted on a given
     * index partition it is then applied to the next index partition spanned by
     * the key range.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, ITupleFilter filter ) {

        if (capacity == 0) {

            capacity = this.capacity;

        }
        
        // @todo make this a ctor argument or settable property?
        final boolean readConsistent = (timestamp == ITx.UNISOLATED?false:true);
        
        return new PartitionedRangeQueryIterator(this, readConsistent, fromKey,
                toKey, capacity, flags, filter);
        
    }

    public Object submit(byte[] key, ISimpleIndexProcedure proc) {

        // Find the index partition spanning that key.
        final PartitionLocator locator = getMetadataIndex().find(key);

        /*
         * Submit procedure to that data service.
         */
        try {

            if (INFO) {

                log.info("Submitting " + proc.getClass() + " to partition"
                        + locator);

            }

            // required to get the result back from the procedure.
            final IResultHandler resultHandler = new IdentityHandler();

            // run on the thread pool in order to limit client parallelism
            final SimpleDataServiceProcedureTask task = new SimpleDataServiceProcedureTask(
                    key, new Split(locator, 0, 0), proc, resultHandler);

            // submit procedure and await completion.
            getThreadPool().submit(task).get(taskTimeout, TimeUnit.MILLISECONDS);

            // the singleton result.
            Object result = resultHandler.getResult();

            return result;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Returns an iterator that will visit the {@link PartitionLocator}s for
     * the specified scale-out index key range.
     * <p>
     * The method fetches a chunk of locators at a time from the metadata index.
     * Unless the #of index partitions spanned is very large, this will be an
     * atomic read of locators from the metadata index. When the #of index
     * partitions spanned is very large, then this will allow a chunked
     * approach.
     * <p>
     * The actual client parallelism is limited by
     * {@link Options#CLIENT_THREAD_POOL_SIZE}. This value is typically smaller
     * than the capacity of the chunked iterator used to read on the metadata
     * index.
     * <p>
     * Note: It is possible that a split or join could occur during the process
     * of mapping the procedure across the index partitions. When the view is
     * {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} this could make the
     * set of mapped index partitions inconsistent in the sense that it might
     * double count some parts of the key range or that it might skip some parts
     * of the key range. In order to avoid this problem the locator scan MUST
     * use <em>read-consistent</em> semantics and issue continuation queries
     * with respect to the commitTime for which the first locator
     * {@link ResultSet} was materialized.
     * 
     * @param fromKey
     *            The scale-out index first key that will be visited
     *            (inclusive). When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first scale-out index key that will NOT be visited
     *            (exclusive). When <code>null</code> there is no upper bound.
     * 
     * @return The iterator. The value returned by {@link ITuple#getValue()}
     *         will be a serialized {@link PartitionLocator} object.
     */
//    * @param parallel
//    *            <code>true</code> iff a parallelizable procedure will be
//    *            mapped over the index partitions (this effects the #of cached
//    *            locators).
    public ITupleIterator locatorScan(final byte[] fromKey, final byte[] toKey ) {
//            final boolean parallel) {
        
//        /*
//         * When the view is either unisolated or read committed we restrict the
//         * scan on the metadata index to buffer no more locators than can be
//         * processed in parallel (if the task is not parallelizable then we only
//         * read a few locators at a time). This keeps us from buffering locators
//         * that may be made stale not by index partition splits, joins, or moves
//         * but simply by concurrent writes of index entries since those writes
//         * will be visible immediate with either unisolated or read committed
//         * isolation.
//         */
//        final int capacity = (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED)//
//            ? (parallel ? MAX_PARALLEL_TASKS : 5) // unisolated or read-committed.
//            : 0 // historical read or fully isolated (default capacity)
//            ;
        
        if(INFO)
        log.info("Querying metadata index: name=" + name + ", fromKey="
                + BytesUtil.toString(fromKey) + ", toKey="
                + BytesUtil.toString(toKey) + ", capacity=" + capacity);
        
        final ITupleIterator itr;
        {
         
            final IMetadataIndex mdi = getMetadataIndex();
            
            /*
             * Note: The scan on the metadata index needs to start at the index
             * partition in which the fromKey would be located. Therefore when
             * the fromKey is specified we replace it with the leftSeparator of
             * the index partition which would contain that fromKey.
             */

            final byte[] _fromKey = fromKey == null //
                ? null //
                : mdi.find(fromKey).getLeftSeparatorKey()//
                ;

            itr = mdi.rangeIterator(_fromKey,//
                    toKey, //
                    0, // capacity, //
                    IRangeQuery.VALS,// the values are the locators.
                    null // filter
                    );

        }

        return itr;
        
    }
    
    /**
     * Maps an {@link IIndexProcedure} across a key range by breaking it down
     * into one task per index partition spanned by that key range.
     * <p>
     * Note: In order to avoid growing the task execution queue without bound,
     * an upper bound of {@link #MAX_PARALLEL_TASKS} tasks will be placed onto
     * the queue at a time. More tasks will be submitted once those tasks finish
     * until all tasks have been executed. When the task is not parallelizable
     * the tasks will be submitted to the corresponding index partitions at a
     * time and in key order.
     */
    public void submit(byte[] fromKey, byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler resultHandler) {

        if (proc == null)
            throw new IllegalArgumentException();

        // true iff the procedure is known to be parallelizable.
        final boolean parallel = proc instanceof IParallelizableIndexProcedure;
        
        if(INFO)
        log.info("Procedure " + proc.getClass().getName()
                + " will be mapped across index partitions in "
                + (parallel ? "parallel" : "sequence"));

        final int poolSize = ((ThreadPoolExecutor) getThreadPool())
                .getCorePoolSize();

        final int maxTasksPerRequest = fed.getClient().getMaxParallelTasksPerRequest();
        
        // max #of tasks to queue at once.
        final int maxTasks = poolSize == 0 ? maxTasksPerRequest : Math.min(
                poolSize, maxTasksPerRequest);

        // verify positive or the loop below will fail to progress.
        assert maxTasks > 0 : "maxTasks=" + maxTasks + ", poolSize=" + poolSize
                + ", maxTasksPerRequest=" + maxTasksPerRequest;
        
        // scan spanned index partition locators in key order.
        final ITupleIterator itr = locatorScan(fromKey, toKey);
        
        long nparts = 0;
        
        while (itr.hasNext()) {

            /*
             * Process the remaining locators a "chunk" at a time. The chunk
             * size is choosen to be the configured size of the client thread
             * pool. This lets us avoid overwhelming the thread pool queue when
             * mapping a procedure across a very large #of index partitions.
             * 
             * The result is an ordered list of the tasks to be executed. The
             * order of the tasks is determined by the natural order of the
             * index partitions - that is, we submit the tasks in key order so
             * that a non-parallelizable procedure will be mapped in the correct
             * sequence.
             */

            final ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>(maxTasks);
            
            for(int i=0; i<maxTasks && itr.hasNext(); i++) {
                
                final ITuple tuple = itr.next();
                
                final PartitionLocator locator = (PartitionLocator) SerializerUtil
                        .deserialize(tuple.getValue());

                final Split split = new Split(locator, 0/* fromIndex */, 0/* toIndex */);                

                tasks.add(new KeyRangeDataServiceProcedureTask(fromKey, toKey, split, proc,
                        resultHandler));
                
                nparts++;
                
            }

            runTasks(parallel, tasks );

            // next index partition(s)                     
        
        }

        if(INFO)
        log.info("Procedure " + proc.getClass().getName() + " mapped across "
                + nparts + " index partitions in "
                + (parallel ? "parallel" : "sequence"));

    }

    /**
     * The procedure will be transparently broken down and executed against each
     * index partitions spanned by its keys. If the <i>ctor</i> creates
     * instances of {@link IParallelizableIndexProcedure} then the procedure
     * will be mapped in parallel against the relevant index partitions.
     * <p>
     * Note: Unlike mapping an index procedure across a key range, this method
     * is unable to introduce a truely enourmous burden on the client's task
     * queue since the #of tasks arising is equal to the #of splits and bounded
     * by <code>n := toIndex - fromIndex</code>.
     * 
     * @return The aggregated result of applying the procedure to the relevant
     *         index partitions.
     */
    public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractIndexProcedureConstructor ctor, IResultHandler aggregator) {

        if (ctor == null) {

            throw new IllegalArgumentException();
        
        }
        
//        if (aggregator == null) {
//
//            throw new IllegalArgumentException();
//            
//        }
        
        /*
         * Break down the data into a series of "splits", each of which will be
         * applied to a different index partition.
         */

        final List<Split> splits = splitKeys(fromIndex, toIndex, keys);

        final int nsplits = splits.size();

        /*
         * Create the instances of the procedure for each split.
         */
        
        final ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                nsplits);
        
        // assume true until proven otherwise.
        boolean parallel = true;
        {
         
            final Iterator<Split> itr = splits.iterator();

            while (itr.hasNext()) {

                final Split split = itr.next();

                final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                        split.fromIndex, split.toIndex, keys, vals);

                if (!(proc instanceof IParallelizableIndexProcedure)) {

                    parallel = false;

                }

                tasks.add(new KeyArrayDataServiceProcedureTask(keys, vals,
                        split, proc, aggregator, ctor));
                
            }
            
        }

        if (INFO)
            log.info("Procedures created by " + ctor.getClass().getName()
                    + " will run on " + nsplits + " index partitions in "
                    + (parallel ? "parallel" : "sequence"));
        
        runTasks(parallel, tasks);
        
    }

    /**
     * Runs a set of tasks.
     * <p>
     * Note: If {@link #getRecursionDepth()} evaluates to a value larger than
     * zero then the task(s) will be forced to execute in the caller's thread.
     * <p>
     * {@link StaleLocatorException}s are handled by the recursive application
     * of <code>submit()</code>. These recursively submitted tasks are forced
     * to run in the caller's thread by incrementing the
     * {@link #getRecursionDepth()} counter. This is done to prevent the thread
     * pool from becoming deadlocked as threads wait on threads handling stale
     * locator retries. The deadlock situation arises as soon as all threads in
     * the thread pool are waiting on stale locator retries as there are no
     * threads remaining to process those retries.
     * 
     * @param parallel
     *            <code>true</code> iff the tasks MAY be run in parallel.
     * @param tasks
     *            The tasks to be executed.
     */
    protected void runTasks(final boolean parallel,
            final ArrayList<Callable<Void>> tasks) {

        if(tasks.isEmpty()) {
            
            log.warn("No tasks to run?",new RuntimeException("No tasks to run?"));
            
            return;
            
        }
        
        if (getRecursionDepth().get() > 0) {

            /*
             * Force sequential execution of the tasks in the caller's thread.
             */

            runInCallersThread(tasks);

        } else if (parallel) {

            /*
             * Map procedure across the index partitions in parallel.
             */

            runParallel(tasks);

        } else {

            /*
             * sequential execution against of each split in turn.
             */

            runSequence(tasks);

        }

    }
    
    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in parallel.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runParallel(ArrayList<Callable<Void>> tasks) {

        final long begin = System.currentTimeMillis();
        
        if(INFO)
        log.info("Running " + tasks.size() + " tasks in parallel (#active="
                + getThreadPool().getActiveCount() + ", queueSize="
                + getThreadPool().getQueue().size() + ") : "
                + tasks.get(0).toString());
        
        int nfailed = 0;
        
        final LinkedList<Throwable> causes = new LinkedList<Throwable>();
        
        try {

            final List<Future<Void>> futures = getThreadPool().invokeAll(tasks,
                    taskTimeout, TimeUnit.MILLISECONDS);
            
            final Iterator<Future<Void>> itr = futures.iterator();
           
            int i = 0;
            
            while(itr.hasNext()) {
                
                final Future<Void> f = itr.next();
                
                try {
                    
                    f.get();
                    
                } catch (ExecutionException e) {
                    
                    AbstractDataServiceProcedureTask task = (AbstractDataServiceProcedureTask) tasks
                            .get(i);
                    
                    if(INFO) log.info("Execution failed: task=" + task, e);
                    
                    causes.addAll(task.causes);
                    
                    nfailed++;
                    
                }
                
            }
            
        } catch (InterruptedException e) {

            throw new RuntimeException("Interrupted: "+e);

        }
        
        if (nfailed > 0) {
            
            throw new ClientException("Execution failed: ntasks="
                    + tasks.size() + ", nfailed=" + nfailed, causes);
            
        }

        if(INFO)
        log.info("Ran " + tasks.size() + " tasks in parallel: elapsed="
                + (System.currentTimeMillis() - begin));

    }

    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in strict sequence. The tasks are run on the
     * {@link #getThreadPool()} so that sequential tasks never increase the
     * total burden placed by the client above the size of that thread pool.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runSequence(ArrayList<Callable<Void>> tasks) {

        if (INFO)
            log.info("Running " + tasks.size() + " tasks in sequence (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + tasks.get(0).toString());

        final Iterator<Callable<Void>> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = (AbstractDataServiceProcedureTask) itr
                    .next();

            try {

                final Future<Void> f = getThreadPool().submit(task);
                
                // await completion of the task.
                f.get(taskTimeout, TimeUnit.MILLISECONDS);

            } catch (Exception e) {
        
                if(INFO) log.info("Execution failed: task=" + task, e);

                throw new ClientException("Execution failed: " + task, e, task.causes);

            }

        }

    }
    
    /**
     * Executes the tasks in the caller's thread.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runInCallersThread(ArrayList<Callable<Void>> tasks) {
        
        log.warn("Running " + tasks.size()
                + " tasks in caller's thread: recursionDepth="
                + getRecursionDepth().get() + "(#active="
                + getThreadPool().getActiveCount() + ", queueSize="
                + getThreadPool().getQueue().size() + ") : "
                + tasks.get(0).toString());

        final Iterator<Callable<Void>> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = (AbstractDataServiceProcedureTask) itr
                    .next();

            try {

                task.call();
                
            } catch (Exception e) {

                if(INFO) log.info("Execution failed: task=" + task, e);

                throw new ClientException("Execution failed: " + task, e, task.causes);
                
            }
            
        }

    }
    
    /**
     * Exposes a linked list of retry exceptions leading to the failure of an
     * {@link AbstractDataServiceProcedureTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ClientException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 7802533953100817726L;
        
        private List<Throwable> causes;

        /**
         * The list of causes, one per failed attempt.
         * 
         * @return
         * 
         * @see #getCause()
         */
        public List<Throwable> getCauses() {

            if (causes == null)
                return Collections.EMPTY_LIST;

            return causes;
            
        }
        
        /**
         * The final exception thrown which caused the task to fail. Normally
         * this will indicate that the retry count has been exceeded and
         * {@link #getCauses()} will report the underlying problem(s) which the
         * task encountered.
         * 
         * @see #getCauses()
         */
        public Throwable getCause() {
            
            return super.getCause();
            
        }
        
//        /**
//         *  
//         */
//        public ClientException() {
//            super();
//        }

        /**
         * @param msg
         * @param cause
         */
        public ClientException(String msg, Throwable cause,
                List<Throwable> causes) {

            super(msg+(causes==null?"":", causes="+causes), cause);

            this.causes = causes;

        }

        /**
         * @param msg
         */
        public ClientException(String msg, List<Throwable> causes) {

            super(msg+(causes==null?"":", causes="+causes));

            this.causes = causes;

        }

//        /**
//         * @param cause
//         */
//        public ClientException(Throwable cause, List<Throwable> causes) {
//
//            super((causes==null?"":", causes="+causes),cause);
//
//            this.causes = causes;
//            
//        }

    }
    
    /**
     * Helper class for submitting an {@link IIndexProcedure} to run on an
     * {@link IDataService}. The class traps {@link StaleLocatorException}s and
     * handles the redirection of requests to the appropriate
     * {@link IDataService}. When necessary, the data for an
     * {@link IKeyArrayIndexProcedure} will be re-split in order to distribute
     * the requests to the new index partitions following a split of the target
     * index partition.
     * <p>
     * Note: If an index partition is moved then the key range is unchanged.
     * <p>
     * Note: If an index partition is split, then the key range for each new
     * index partition is a sub-range of the original key range and the total
     * key range for the new index partitions is exactly the original key range.
     * <p>
     * Note: If an index partition is joined with another index partition then
     * the key range of the new index partition is increased. However, we will
     * wind up submitting one request to the new index partition for each index
     * partition that we knew about at the time that the procedure was first
     * mapped across the index partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractDataServiceProcedureTask implements Callable<Void> {

        protected final Split split;
        protected final IIndexProcedure proc;
        protected final IResultHandler resultHandler;
        
        /**
         * If the task fails then this will be populated with an ordered list of
         * the exceptions. There will be one exception per-retry of the task.
         */
        protected List<Throwable> causes = null;
        
        /**
         * A human friendly representation.
         */
        public String toString() {
            
            return "Procedure " + proc.getClass().getName() + " : " + split;
            
        }

        /**
         * Variant used for procedures that are NOT instances of
         * {@link IKeyArrayIndexProcedure}.
         * 
         * @param split
         * @param proc
         * @param resultHandler
         */
        public AbstractDataServiceProcedureTask(Split split,
                IIndexProcedure proc,
                IResultHandler resultHandler
        ) {
            
            if (split.pmd == null)
                throw new IllegalArgumentException();
            
            if(!(split.pmd instanceof PartitionLocator)) {
                
                throw new IllegalArgumentException("Split does not have a locator");
                
            }

            if (proc == null)
                throw new IllegalArgumentException();

            this.split = split;
            
            this.proc = proc;

            this.resultHandler = resultHandler;
            
        }
        
        final public Void call() throws Exception {

            // the index partition locator.
            final PartitionLocator locator = (PartitionLocator) split.pmd;

            submit(locator);
            
            return null;

        }

        /**
         * Submit the procedure to the {@link IDataService} identified by the
         * locator.
         * 
         * @param locator
         *            An index partition locator.
         *            
         * @throws Exception
         */
        final protected void submit(PartitionLocator locator) throws Exception {
            
            if (locator == null)
                throw new IllegalArgumentException();
            
            if(Thread.interrupted()) throw new InterruptedException();
            
            // resolve service UUID to data service.
            final IDataService dataService = getDataService(locator);

            if (dataService == null)
                throw new RuntimeException("DataService not found: " + locator);
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(//
                    ClientIndexView.this.name, // the name of the scale-out index.
                    split.pmd.getPartitionId() // the index partition identifier.
                    );

            if(INFO)
            log.info("Submitting task="+this+" on "+dataService);
            
            try {

                if(Thread.interrupted()) throw new InterruptedException();

                submit(dataService, name);
                
            } catch(Exception ex) {
                
                if (causes == null)
                    causes = new LinkedList<Throwable>();
                
                causes.add(ex);
                
                if(InnerCause.isInnerCause(ex, StaleLocatorException.class)) {
                    
                    if(Thread.interrupted()) throw new InterruptedException();
                    
                    if(INFO)
                    log.info("Locator stale (will retry) : name="+name+", stale locator="+locator);
                    
                    retry();
                    
                } else {
                    
                    throw ex;
                    
                }
                
            }

        }
        
        /**
         * Submit the procedure to the {@link IDataService} and aggregate the
         * result with the caller's {@link IResultHandler}.
         * 
         * @param dataService
         *            The data service on which the procedure will be executed.
         * @param name
         *            The name of the index partition on that data service.
         * 
         * @todo do not require aggregator for {@link ISimpleIndexProcedure} so
         *       make this abstract in the base class or just override for
         *       {@link ISimpleIndexProcedure}. This would also mean that
         *       {@link #call()} would return the value for
         *       {@link ISimpleIndexProcedure}.
         */
        @SuppressWarnings("unchecked")
        protected void submit(IDataService dataService, String name) throws Exception {

            final Object result = dataService.submit(timestamp, name, proc);

            if (resultHandler != null) {

                resultHandler.aggregate(result, split);

            }

        }
        
        /**
         * Invoked when {@link StaleLocatorException} was thrown. Since the
         * procedure was being run against an index partition of some scale-out
         * index this exception indicates that the index partition locator was
         * stale. We re-cache the locator(s) for the same key range as the index
         * partition which we thought we were addressing and then re-map the
         * operation against those updated locator(s). Note that a split will go
         * from one locator to N locators for a key range while a join will go
         * from N locators for a key range to 1. A move does not change the #of
         * locators for the key range, just where that index partition is living
         * at this time.
         * 
         * @throws Exception
         */
        abstract protected void retry() throws Exception;

    }

    /**
     * Class handles stale locators by finding the current locator for the
     * <i>key</i> and redirecting the request to execute the procedure on the
     * {@link IDataService} identified by that locator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SimpleDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        protected final byte[] key;
        
        private int ntries = 1;
        
        /**
         * @param key
         * @param split
         * @param proc
         * @param resultHandler
         */
        public SimpleDataServiceProcedureTask(byte[] key, Split split,
                ISimpleIndexProcedure proc, IResultHandler resultHandler) {

            super(split, proc, resultHandler);
            
            if (key == null)
                throw new IllegalArgumentException();
        
            this.key = key;
            
        }

        /**
         * The locator is stale. We locate the index partition that spans the
         * {@link #key} and re-submit the request.
         */
        @Override
        protected void retry() throws Exception {
            
            if (ntries++ > fed.getClient().getMaxStaleLocatorRetries()) {

                throw new RuntimeException("Retry count exceeded: ntries="
                        + ntries);

            }

            final PartitionLocator locator = getMetadataIndex().find(key);

            if(INFO)
            log.info("Retrying: proc=" + proc.getClass().getName()
                    + ", locator=" + locator + ", ntries=" + ntries);

            /*
             * Note: In this case we do not recursively submit to the outer
             * interface on the client since all we need to do is fetch the
             * current locator for the key and re-submit the request to the data
             * service identified by that locator.
             */

            submit(locator);
            
        }
        
    }

    /**
     * Handles stale locators for {@link IKeyRangeIndexProcedure}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class KeyRangeDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        final byte[] fromKey;
        final byte[] toKey;
        
        /**
         * @param fromKey
         * @param toKey
         * @param split
         * @param proc
         * @param resultHandler
         */
        public KeyRangeDataServiceProcedureTask(byte[] fromKey, byte[] toKey,
                Split split, IKeyRangeIndexProcedure proc,
                IResultHandler resultHandler) {

            super(split, proc, resultHandler);
            
            /*
             * Constrain the range to the index partition. This constraint will
             * be used if we discover that the locator data was stale in order
             * to discover the new locator(s).
             */
            
            this.fromKey = AbstractKeyRangeIndexProcedure.constrainFromKey(
                    fromKey, split.pmd);

            this.toKey = AbstractKeyRangeIndexProcedure.constrainFromKey(toKey,
                    split.pmd);
            
        }

        /**
         * The {@link IKeyRangeIndexProcedure} is re-mapped for the constrained
         * key range of the stale locator using
         * {@link ClientIndexView#submit(byte[], byte[], IKeyRangeIndexProcedure, IResultHandler)}.
         */
        @Override
        protected void retry() throws Exception {

            /*
             * Note: recursive retries MUST run in the same thread in order to
             * avoid deadlock of the client's thread pool. The recursive depth
             * is used to enforce this constrain.
             */

            final int depth = getRecursionDepth().incrementAndGet();

            try {
            
                if (depth > fed.getClient().getMaxStaleLocatorRetries()) {

                    throw new RuntimeException("Retry count exceeded: ntries="
                            + depth);
                    
                }
                
                ClientIndexView.this.submit(fromKey, toKey,
                    (IKeyRangeIndexProcedure) proc, resultHandler);
            
            } finally {
                
                final int tmp = getRecursionDepth().decrementAndGet();
                
                assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
                
            }
            
        }
        
    }

    /**
     * Handles stale locators for {@link IKeyArrayIndexProcedure}s. When
     * necessary the procedure will be re-split.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class KeyArrayDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        protected final byte[][] keys;
        protected final byte[][] vals;
        protected final AbstractIndexProcedureConstructor ctor;
        
        /**
         * Variant used for {@link IKeyArrayIndexProcedure}s.
         * 
         * @param keys
         *            The original keys[][].
         * @param vals
         *            The original vals[][].
         * @param split
         *            The split identifies the subset of keys and values to be
         *            applied by this procedure.
         * @param proc
         *            The procedure instance.
         * @param resultHandler
         *            The result aggregator.
         * @param ctor
         *            The object used to create instances of the <i>proc</i>.
         *            This is used to re-split the data if necessary in response
         *            to stale locator information.
         */
        public KeyArrayDataServiceProcedureTask(byte[][] keys, byte[][] vals,
                Split split, IKeyArrayIndexProcedure proc,
                IResultHandler resultHandler, AbstractIndexProcedureConstructor ctor
        ) {
            
            super( split, proc, resultHandler );
            
            if (ctor == null)
                throw new IllegalArgumentException();

            this.ctor = ctor;
            
            this.keys = keys;
            
            this.vals = vals;
            
        }

        /**
         * Submit using
         * {@link ClientIndexView#submit(int, int, byte[][], byte[][], AbstractIndexProcedureConstructor, IResultHandler)}.
         * This will recompute the split points and re-map the procedure across
         * the newly determined split points.
         */
        @Override
        protected void retry() throws Exception {

            /*
             * Note: recursive retries MUST run in the same thread in order to
             * avoid deadlock of the client's thread pool. The recursive depth
             * is used to enforce this constrain.
             */

            final int depth = getRecursionDepth().incrementAndGet();

            try {
            
                if (depth > fed.getClient().getMaxStaleLocatorRetries()) {

                    throw new RuntimeException("Retry count exceeded: ntries="
                            + depth);
                    
                }
                
                ClientIndexView.this.submit(split.fromIndex, split.toIndex, keys,
                        vals, ctor, resultHandler);
                
            } finally {
                
                final int tmp = getRecursionDepth().decrementAndGet();
                
                assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
                
            }
            
        }
        
    }
    
    /**
     * Utility method to split a set of ordered keys into partitions based the
     * index partitions defined for a scale-out index.
     * <p>
     * Find the partition for the first key. Check the last key, if it is in the
     * same partition then then this is the simplest case and we can just send
     * the data along.
     * <p>
     * Otherwise, perform a binary search on the remaining keys looking for the
     * index of the first key GTE the right separator key for that partition.
     * The batch for this partition is formed from all keys from the first key
     * for that partition up to but excluding the index position identified by
     * the binary search (if there is a match; if there is a miss, then the
     * binary search result needs to be converted into a key index and that will
     * be the last key for the current partition).
     * <p>
     * Examine the next key and repeat the process until all keys have been
     * allocated to index partitions.
     * <p>
     * Note: Split points MUST respect the "row" identity for a sparse row
     * store, but we get that constraint by maintaining the index partition
     * boundaries in agreement with the split point constraints for the index.
     * 
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     * 
     * @see Arrays#sort(Object[], int, int, java.util.Comparator)
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     * 
     * @todo Caching? This procedure performs the minimum #of lookups using
     *       {@link IMetadataIndex#find(byte[])} since that operation will be an
     *       RMI in a distributed federation. The find(byte[] key) operation is
     *       difficult to cache since it locates the index partition that would
     *       span the key and many, many different keys could fit into that same
     *       index partition. The only effective cache technique may be an LRU
     *       that scans ~10 caches locators to see if any of them is a match
     *       before reaching out to the remote {@link IMetadataService}. Or
     *       perhaps the locators can be cached in a local BTree and a miss
     *       there would result in a read through to the remote
     *       {@link IMetadataService} but then we have the problem of figuring
     *       out when to release locators if the client is long-lived.
     */
    public List<Split> splitKeys(final int fromIndex, final int toIndex,
            final byte[][] keys) {

        assert keys != null;
        
        assert fromIndex >= 0;
        assert fromIndex < toIndex;

        assert toIndex <= keys.length;
        
        final List<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int currentIndex = fromIndex;
        
        while (currentIndex < toIndex) {
                
            // partition spanning the current key (RMI)
            final PartitionLocator locator = getMetadataIndex().find(keys[currentIndex]);

            final byte[] rightSeparatorKey = locator.getRightSeparatorKey();

            if (rightSeparatorKey == null) {

                /*
                 * The last index partition does not have an upper bound and
                 * will absorb any keys that order GTE to its left separator
                 * key.
                 */

                assert validSplit( locator, currentIndex, toIndex, keys );
                
                splits.add(new Split(locator, currentIndex, toIndex));

                // done.
                currentIndex = toIndex;

            } else {

                /*
                 * Otherwise this partition has an upper bound, so figure out
                 * the index of the last key that would go into this partition.
                 * 
                 * We do this by searching for the rightSeparator of the index
                 * partition itself.
                 */
                
                int pos = BytesUtil.binarySearch(keys, currentIndex, toIndex
                        - currentIndex, rightSeparatorKey);

                if (pos >= 0) {

                    /*
                     * There is a hit on the rightSeparator key. The index
                     * returned by the binarySearch is the exclusive upper bound
                     * for the split. The key at that index is excluded from the
                     * split - it will be the first key in the next split.
                     * 
                     * Note: There is a special case when the keys[] includes
                     * duplicates of the key that corresponds to the
                     * rightSeparator. This causes a problem where the
                     * binarySearch returns the index of ONE of the keys that is
                     * equal to the rightSeparator key and we need to back up
                     * until we have found the FIRST ONE.
                     * 
                     * Note: The behavior of the binarySearch is effectively
                     * under-defined here and sometimes it will return the index
                     * of the first key EQ to the rightSeparator while at other
                     * times it will return the index of the second or greater
                     * key that is EQ to the rightSeparatoer.
                     */
                    
                    while (pos > currentIndex) {
                        
                        if (BytesUtil.bytesEqual(keys[pos - 1],
                                rightSeparatorKey)) {

                            // keep backing up.
                            pos--;

                            continue;

                        }
                        
                        break;
                        
                    }

                    log.debug("Exact match on rightSeparator: pos=" + pos
                            + ", key=" + BytesUtil.toString(keys[pos]));

                } else if (pos < 0) {

                    /*
                     * There is a miss on the rightSeparator key (it is not
                     * present in the keys that are being split). In this case
                     * the binary search returns the insertion point. We then
                     * compute the exclusive upper bound from the insertion
                     * point.
                     */
                    
                    pos = -pos - 1;

                    assert pos > currentIndex && pos <= toIndex : "Expected pos in ["
                        + currentIndex + ":" + toIndex + ") but pos=" + pos;

                }

                /*
                 * Note: this test can be enabled if you are having problems
                 * with KeyAfterPartition or KeyBeforePartition. It will go
                 * through more effort to validate the constraints on the split.
                 * However, due to the additional byte[] comparisons, this
                 * SHOULD be disabled except when tracking a bug.
                 */
//                assert validSplit( locator, currentIndex, pos, keys );

                splits.add(new Split(locator, currentIndex, pos));

                currentIndex = pos;

            }

        }

        return splits;

    }

    /**
     * Paranoia testing for generated splits.
     * 
     * @param locator
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @return
     */
    private boolean validSplit(PartitionLocator locator, int fromIndex,
            int toIndex, byte[][] keys) {

        assert fromIndex <= toIndex : "fromIndex=" + fromIndex + ", toIndex="
                + toIndex;

        assert fromIndex >= 0 : "fromIndex=" + fromIndex;

        assert toIndex <= keys.length : "toIndex=" + toIndex + ", keys.length="
                + keys.length;

        // begin with the left separator on the index partition.
        byte[] lastKey = locator.getLeftSeparatorKey();
        
        assert lastKey != null;

        for (int i = fromIndex; i < toIndex; i++) {

            final byte[] key = keys[i];

            assert key != null;

            if (lastKey != null) {

                final int ret = BytesUtil.compareBytes(lastKey, key);

                assert ret <= 0 : "keys out of order: i=" + i + ", lastKey="
                        + BytesUtil.toString(lastKey) + ", key="
                        + BytesUtil.toString(key)+", keys="+BytesUtil.toString(keys);
                
            }
            
            lastKey = key;
            
        }

        // Note: Must be strictly LT the rightSeparator key (when present).
        {

            final byte[] key = locator.getRightSeparatorKey();

            if (key != null) {

                int ret = BytesUtil.compareBytes(lastKey, key);

                assert ret < 0 : "keys out of order: lastKey="
                        + BytesUtil.toString(lastKey) + ", rightSeparator="
                        + BytesUtil.toString(key)+", keys="+BytesUtil.toString(keys);

            }
            
        }
        
        return true;
        
    }
    
    /**
     * Resolve the data service to which the index partition is mapped.
     * 
     * @param pmd
     *            The index partition locator.
     * 
     * @return The data service and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if none of the data services identified in the index
     *             partition locator record could be discovered.
     */
    public IDataService getDataService(PartitionLocator pmd) {

        final UUID [] dataServiceUUIDs = pmd.getDataServices();

        assert dataServiceUUIDs.length > 0: "No DataService UUIDs? : pmd="+pmd;

        final UUID serviceUUID = dataServiceUUIDs[0];

        assert serviceUUID != null : "DataService UUID is null? : pmd="+pmd;
        
        final IDataService dataService;

        try {

            dataService = fed.getDataService(serviceUUID);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return dataService;

    }

    /**
     * This operation is not supported - the resource description of a scale-out
     * index would include all "live" resources in the corresponding
     * {@link MetadataIndex}.
     */
    public IResourceMetadata[] getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }

}
