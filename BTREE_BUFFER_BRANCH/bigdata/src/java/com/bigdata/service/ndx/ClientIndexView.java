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

package com.bigdata.service.ndx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AsynchronousIndexWriteConfiguration;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.proc.RangeCountProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.BatchContains.BatchContainsConstructor;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IRunnableBuffer;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataServiceTupleIterator;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.Split;
import com.bigdata.service.IBigdataClient.Options;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.IndexAsyncWriteStats;
import com.bigdata.service.ndx.pipeline.IndexWriteTask;

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
 * {@link PartitionedTupleIterator}, and {@link DataServiceTupleIterator}.
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
public class ClientIndexView implements IScaleOutClientIndex {

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale-out efficiently.
     */
    protected static final transient Logger log = Logger
            .getLogger(ClientIndexView.class);
    
    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();
    
    /**
     * Error message used if we were unable to start a new transaction in order
     * to provide read-consistent semantics for an {@link ITx#READ_COMMITTED}
     * view or for a read-only operation on an {@link ITx#UNISOLATED} view.
     */
    static protected final transient String ERR_NEW_TX = "Could not start transaction";
    
    /**
     * Error message used if we were unable to abort a transaction that we
     * started in order to provide read-consistent semantics for an
     * {@link ITx#READ_COMMITTED} view or for a read-only operation on an
     * {@link ITx#UNISOLATED} view.
     */
    static protected final transient String ERR_ABORT_TX = "Could not abort transaction: tx=";
    
    private final AbstractScaleOutFederation fed;

    public AbstractScaleOutFederation getFederation() {
        
        return fed;
        
    }
    
    /**
     * The thread pool exposed by {@link IBigdataFederation#getExecutorService()}
     */
    protected ThreadPoolExecutor getThreadPool() {

        return (ThreadPoolExecutor) fed.getExecutorService();

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

    final public long getTimestamp() {
        
        return timestamp;
        
    }

    /**
     * The name of the scale-out index (from the ctor).
     */
    private final String name;
    
    final public String getName() {
        
        return name;
        
    }

    /**
     * The {@link IMetadataIndex} for this scale-out index.
     * 
     * @todo This is a bit dangerous since most of the time when you want the
     *       metadata index you may have a timestamp in effect which is
     *       different from the timestamp of the view (e.g., a read-consistent
     *       transaction).
     */
    private final IMetadataIndex metadataIndex;
    
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
     * Knows how to break down key[][]s into {@link Split}s. 
     */
    private final ISplitter splitter;
    
    /**
     * Return a view of the metadata index for the scale-out index as of the
     * timestamp associated with this index view.
     * 
     * @todo This is a bit dangerous since most of the time when you want the
     *       metadata index you may have a timestamp in effect which is
     *       different from the timestamp of the view (e.g., a read-consistent
     *       transaction).
     * 
     * @todo should be protected, but some unit tests in a different package
     *       access this.
     * 
     * @see IBigdataFederation#getMetadataIndex(String, long)
     */
    final public IMetadataIndex getMetadataIndex() {
        
        return metadataIndex;
        
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

    public AtomicInteger getRecursionDepth() {

        return recursionDepth.get();
        
    }
    
    /**
     * <code>true</code> iff globally consistent read operations are desired
     * for iterators or index procedures mapped across more than one index
     * partition. When <code>true</code> and the index is
     * {@link ITx#READ_COMMITTED} or (if the index is {@link ITx#UNISOLATED} and
     * the operation is read-only), {@link IIndexStore#getLastCommitTime()} is
     * queried at the start of the operation and used as the timestamp for all
     * requests made in support of that operation.
     * <p>
     * Note that {@link StaleLocatorException}s can not arise for
     * read-consistent operations. Such operations use a read-consistent view of
     * the {@link IMetadataIndex} and the locators therefore will not change
     * during the operation.
     * 
     * @todo make this a ctor argument or settable property?
     */
    final private boolean readConsistent = true;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());

        sb.append("{ ");

        sb.append("name=" + name);

        sb.append(", timestamp=" + timestamp);

        sb.append("}");

        return sb.toString();

    }

    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * @param metadataIndex
     *            The {@link IMetadataIndex} for the named scale-out index as of
     *            that timestamp. Note that the {@link IndexMetadata} on this
     *            object contains the template {@link IndexMetadata} for the
     *            scale-out index partitions.
     */
    public ClientIndexView(final AbstractScaleOutFederation fed,
            final String name, final long timestamp,
            final IMetadataIndex metadataIndex) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
        
        if (metadataIndex == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;

        this.name = name;

        this.timestamp = timestamp;
        
        this.metadataIndex = metadataIndex;
        
        this.metadataIndexMetadata = metadataIndex.getIndexMetadata();
        
        this.splitter = new AbstractSplitter() {

            @Override
            protected IMetadataIndex getMetadataIndex(final long ts) {
                
                return fed.getMetadataIndex(name, ts);
                
            }
            
        };
        
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

    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }

    private volatile ITupleSerializer tupleSer = null;

    protected ITupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            synchronized (this) {

                if (tupleSer == null) {

                    tupleSer = getIndexMetadata().getTupleSerializer();
                }

            }

        }

        return tupleSer;

    }
    
    public boolean contains(Object key) {

        key = getTupleSerializer().serializeKey(key);
        
        return contains((byte[])key);
        
    }
    
    public boolean contains(byte[] key) {
        
        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        
        final IResultHandler resultHandler = new IdentityHandler();

        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchContainsConstructor.INSTANCE, resultHandler);

        return ((ResultBitBuffer) resultHandler.getResult()).getResult()[0];
        
    }
    
    public Object insert(Object key,Object val) {
        
        final ITupleSerializer tupleSer = getTupleSerializer();
        
        key = tupleSer.serializeKey(key);
        
        val = tupleSer.serializeKey(val);
        
        final byte[] oldval = insert((byte[])key, (byte[])val);
        
        // FIXME decode tuple to old value.
        throw new UnsupportedOperationException();
        
    }
    
    public byte[] insert(byte[] key, byte[] value) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][] { key };
        final byte[][] vals = new byte[][] { value };
        
        final IResultHandler resultHandler = new IdentityHandler();

        submit(0/* fromIndex */, 1/* toIndex */, keys, vals,
                BatchInsertConstructor.RETURN_OLD_VALUES, resultHandler);

        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public Object lookup(Object key) {
        
        key = getTupleSerializer().serializeKey(key);

        final byte[] val = lookup((byte[])key);
        
        // FIXME decode tuple to old value.
        throw new UnsupportedOperationException();
        
    }

    public byte[] lookup(byte[] key) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
        final IResultHandler resultHandler = new IdentityHandler();

        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchLookupConstructor.INSTANCE, resultHandler);

        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    public Object remove(Object key) {
        
        key = getTupleSerializer().serializeKey(key);
        
        final byte[] oldval = remove((byte[])key);
        
        // FIXME decode tuple to old value.
        throw new UnsupportedOperationException();

    }
    
    public byte[] remove(byte[] key) {

        if (batchOnly)
            log.error(NON_BATCH_API,new RuntimeException());
        else
            if(WARN) log.warn(NON_BATCH_API);

        final byte[][] keys = new byte[][]{key};
        
        final IResultHandler resultHandler = new IdentityHandler();

        submit(0/* fromIndex */, 1/* toIndex */, keys, null/* vals */,
                BatchRemoveConstructor.RETURN_OLD_VALUES, resultHandler);

        return ((ResultBuffer) resultHandler.getResult()).getResult(0);

    }

    /*
     * All of these methods need to divide up the operation across index
     * partitions.
     */

    public long rangeCount() {
        
        return rangeCount(null, null);
        
    }
    
    /**
     * Returns the sum of the range count for each index partition spanned by
     * the key range.
     */
    public long rangeCount(final byte[] fromKey, final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(
                false/* exact */, false/* deleted */, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
        
    }

    /**
     * The exact range count is obtained by mapping a key-range scan over the
     * index partitions. The operation is parallelized.
     */
    final public long rangeCountExact(final byte[] fromKey, final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(
                true/* exact */, false/*deleted*/, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
    
    }
    
    /**
     * The exact range count of deleted and undeleted tuples is obtained by
     * mapping a key-range scan over the index partitions. The operation is
     * parallelized.
     */
    final public long rangeCountExactWithDeleted(final byte[] fromKey,
            final byte[] toKey) {

        final LongAggregator handler = new LongAggregator();

        final RangeCountProcedure proc = new RangeCountProcedure(
                true/* exact */, true/* deleted */, fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();

    }
    
    final public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);

    }
    
    /**
     * An {@link ITupleIterator} that kinds the use of a series of
     * {@link ResultSet}s to cover all index partitions spanned by the key
     * range.
     */
    public ITupleIterator rangeIterator(final byte[] fromKey, final byte[] toKey) {
        
        return rangeIterator(fromKey, toKey, capacity,
                IRangeQuery.DEFAULT /* flags */, null/* filter */);
        
    }

    /**
     * Identifies the index partition(s) that are spanned by the key range query
     * and maps an iterator across each index partition. The iterator buffers
     * responses up to the specified capacity and a follow up iterator request
     * is automatically issued if the iterator has not exhausted the key range
     * on a given index partition. Once the iterator is exhausted on a given
     * index partition it is then applied to the next index partition spanned by
     * the key range.
     * 
     * @todo If the return iterator implements {@link ITupleCursor} then this
     *       will need be modified to defer request of the initial result set
     *       until the caller uses first(), last(), seek(), hasNext(), or
     *       hasPrior().
     */
    public ITupleIterator rangeIterator(final byte[] fromKey,
            final byte[] toKey, int capacity, final int flags,
            final IFilterConstructor filter) {

        if (capacity == 0) {

            capacity = this.capacity;

        }

        // Parallel scan of each index partition?
        final boolean parallel = ((flags & PARALLEL) != 0);

        /*
         * Does the iterator declare that it will not write back on the index?
         */
        final boolean readOnly = ((flags & READONLY) != 0);

        if (readOnly && ((flags & REMOVEALL) != 0)) {

            throw new IllegalArgumentException();

        }

        final boolean isReadConsistentTx;
        final long ts;
        if ((timestamp == ITx.UNISOLATED && readOnly)
                || (timestamp == ITx.READ_COMMITTED && readConsistent)) {

            try {

                // run as globally consistent read.
                ts = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

            } catch (IOException ex) {

                throw new RuntimeException(ERR_NEW_TX, ex);

            }

            isReadConsistentTx = true;

        } else {

            ts = timestamp;

            isReadConsistentTx = false;

        }

        if (parallel) {

            /*
             * Parallel iterator scan. This breaks the total ordering guarantee
             * of the iterator in exchange for faster visitation of the tuples
             * in key range which spans multiple index partitions.
             * 
             * FIXME Implement the parallel iterator scan.
             */
            throw new UnsupportedOperationException();
            
        } else {

            /*
             * Process the index partitions in key order so the total order of
             * the keys is preserved by the iterator visitation ordering.
             */
            return new PartitionedTupleIterator(this, ts, isReadConsistentTx,
                    fromKey, toKey, capacity, flags, filter);

        }
        
    }

    public Object submit(final byte[] key, final ISimpleIndexProcedure proc) {

        if (readConsistent && proc.isReadOnly()
                && TimestampUtility.isReadCommittedOrUnisolated(getTimestamp())) {
            /*
             * Use globally consistent reads for the mapped procedure.
             */

            final long tx;
            try {

                tx = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

            } catch (IOException ex) {

                throw new RuntimeException(ERR_NEW_TX, ex);

            }

            try {

                return submit(tx, key, proc);

            } finally {

                try {

                    fed.getTransactionService().abort(tx);

                } catch (IOException ex) {

                    // log error and ignore since the operation is complete.
                    log.error(ERR_ABORT_TX + tx, ex);

                }

            }
            
        } else {

            /*
             * Timestamp is either a tx already or the caller is risking errors
             * with lightweight historical reads.
             */
            
            return submit(timestamp, key, proc);

        }
        
    }

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param key
     * @param proc
     * @return
     */
    private Object submit(final long ts, final byte[] key,
            final ISimpleIndexProcedure proc) {

        // Find the index partition spanning that key.
        final PartitionLocator locator = fed.getMetadataIndex(name, ts).find(
                key);

        /*
         * Submit procedure to that data service.
         */
        try {

            if (log.isInfoEnabled()) {

                log.info("Submitting " + proc.getClass() + " to partition"
                        + locator);

            }

            // required to get the result back from the procedure.
            final IResultHandler resultHandler = new IdentityHandler();

            final SimpleDataServiceProcedureTask task = new SimpleDataServiceProcedureTask(
                    this, key, ts, new Split(locator, 0, 0), proc, resultHandler);

            // submit procedure and await completion.
            getThreadPool().submit(task).get(taskTimeout, TimeUnit.MILLISECONDS);

            // the singleton result.
            Object result = resultHandler.getResult();

            return result;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    @SuppressWarnings("unchecked")
    public Iterator<PartitionLocator> locatorScan(final long ts,
            final byte[] fromKey, final byte[] toKey, final boolean reverseScan) {

        return fed.locatorScan(name, ts, fromKey, toKey, reverseScan);

    }
    
    /**
     * Maps an {@link IIndexProcedure} across a key range by breaking it down
     * into one task per index partition spanned by that key range.
     * <p>
     * Note: In order to avoid growing the task execution queue without bound,
     * an upper bound of {@link Options#CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST}
     * tasks will be placed onto the queue at a time. More tasks will be
     * submitted once those tasks finish until all tasks have been executed.
     * When the task is not parallelizable the tasks will be submitted to the
     * corresponding index partitions at a time and in key order.
     */
    public void submit(final byte[] fromKey, final byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler resultHandler) {

        if (proc == null)
            throw new IllegalArgumentException();

        if (readConsistent && proc.isReadOnly()
                && TimestampUtility.isReadCommittedOrUnisolated(getTimestamp())) {
            /*
             * Use globally consistent reads for the mapped procedure.
             */

            final long tx;
            try {

                tx = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

            } catch (IOException ex) {

                throw new RuntimeException(ERR_NEW_TX, ex);

            }

            try {

                submit(tx, fromKey, toKey, proc, resultHandler);

            } finally {

                try {

                    fed.getTransactionService().abort(tx);

                } catch (IOException ex) {

                    // log error and ignore since the operation is complete.
                    log.error(ERR_ABORT_TX + tx, ex);
                    
                }

            }
            
        } else {

            /*
             * Timestamp is either a tx already or the caller is risking errors
             * with lightweight historical reads.
             */
            
            submit(timestamp, fromKey, toKey, proc, resultHandler);
            
        }

    }

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param fromKey
     * @param toKey
     * @param proc
     * @param resultHandler
     */
    void submit(final long ts, final byte[] fromKey,
            final byte[] toKey, final IKeyRangeIndexProcedure proc,
            final IResultHandler resultHandler) {

        // true iff the procedure is known to be parallelizable.
        final boolean parallel = proc instanceof IParallelizableIndexProcedure;

        if (log.isInfoEnabled())
            log.info("Procedure " + proc.getClass().getName()
                    + " will be mapped across index partitions in "
                    + (parallel ? "parallel" : "sequence"));

        final int poolSize = ((ThreadPoolExecutor) getThreadPool())
                .getCorePoolSize();

        final int maxTasksPerRequest = fed.getClient()
                .getMaxParallelTasksPerRequest();

        // max #of tasks to queue at once.
        final int maxTasks = poolSize == 0 ? maxTasksPerRequest : Math.min(
                poolSize, maxTasksPerRequest);

        // verify positive or the loop below will fail to progress.
        assert maxTasks > 0 : "maxTasks=" + maxTasks + ", poolSize=" + poolSize
                + ", maxTasksPerRequest=" + maxTasksPerRequest;

        /*
         * Scan visits index partition locators in key order.
         * 
         * Note: We are using the caller's timestamp.
         */
        final Iterator<PartitionLocator> itr = locatorScan(ts, fromKey, toKey,
                false/* reverseScan */);

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

            final ArrayList<AbstractDataServiceProcedureTask> tasks = new ArrayList<AbstractDataServiceProcedureTask>(
                    maxTasks);

            for (int i = 0; i < maxTasks && itr.hasNext(); i++) {

                final PartitionLocator locator = itr.next();

                final Split split = new Split(locator, 0/* fromIndex */, 0/* toIndex */);

                tasks.add(new KeyRangeDataServiceProcedureTask(this, fromKey, toKey,
                        ts, split, proc, resultHandler));

                nparts++;

            }

            runTasks(parallel, tasks);

        } // next (chunk of) locators.

        if (log.isInfoEnabled())
            log.info("Procedure " + proc.getClass().getName()
                    + " mapped across " + nparts + " index partitions in "
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
    public void submit(final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals,
            final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        if (ctor == null) {

            throw new IllegalArgumentException();
        
        }
        
        // iff we created a read-historical tx in this method.
        final boolean isTx;
        // the timestamp that will be used for the operation.
        final long ts;
        {

            /*
             * Instantiate the procedure on all the data so we can figure out if
             * it is read-only and whether or not we need to create a read-only
             * transaction to run it.
             * 
             * @todo This assumes that people write procedures that are
             * flyweight in how they encode the data in their ctor. If the don't
             * then there will be an overhead for this.
             */
            final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                    fromIndex, toIndex, keys, vals);

            if (readConsistent
                    && proc.isReadOnly()
                    && TimestampUtility
                            .isReadCommittedOrUnisolated(getTimestamp())) {

                /*
                 * Create a read-historical transaction from the last commit
                 * point of the federation in order to provide consistent
                 * reads for the mapped procedure.
                 */

                isTx = true;

                try {

                    ts = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

                } catch (IOException e) {

                    throw new RuntimeException(ERR_NEW_TX, e);

                }

            } else {
            
                // might be a tx, but not one that we created here.
                isTx = false;
                
                ts = getTimestamp();
            
            }

        }

        try {

            submit(ts, fromIndex, toIndex, keys, vals, ctor, aggregator);

        } finally {

            if (isTx) {

                try {

                    fed.getTransactionService().abort(ts);

                } catch (IOException e) {
                    
                    /*
                     * log error but do not rethrow since operation is over
                     * anyway.
                     */
                    
                    log.error(ERR_ABORT_TX + ": " + ts, e);
                    
                }
        
            }
            
        }
        
    }

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @param ctor
     * @param aggregator
     */
    void submit(final long ts, final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals,
            final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        /*
         * Break down the data into a series of "splits", each of which will be
         * applied to a different index partition.
         * 
         * Note: We are using the caller's timestamp here so this will have
         * read-consistent semantics!
         */

        final LinkedList<Split> splits = splitKeys(ts, fromIndex, toIndex, keys);

        final int nsplits = splits.size();

        /*
         * Create the instances of the procedure for each split.
         */

        final ArrayList<AbstractDataServiceProcedureTask> tasks = new ArrayList<AbstractDataServiceProcedureTask>(
                nsplits);

        final Iterator<Split> itr = splits.iterator();

        boolean parallel = false;
        while (itr.hasNext()) {

            final Split split = itr.next();

            final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                    split.fromIndex, split.toIndex, keys, vals);

            if (proc instanceof IParallelizableIndexProcedure) {

                parallel = true;

            }

            tasks.add(new KeyArrayDataServiceProcedureTask(this, keys, vals, ts,
                    split, proc, aggregator, ctor));

        }

        if (log.isInfoEnabled())
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
            final ArrayList<AbstractDataServiceProcedureTask> tasks) {

        if (tasks.isEmpty()) {

            log.warn("No tasks to run?", new RuntimeException(
                    "No tasks to run?"));

            return;
            
        }

        if (getRecursionDepth().get() > 0) {

            /*
             * Force sequential execution of the tasks in the caller's thread.
             */

            runInCallersThread(tasks);

        } else if (tasks.size() == 1) {

            runOne(tasks.get(0));

        } else if (parallel) {

            /*
             * Map procedure across the index partitions in parallel.
             */

            runParallel(tasks);

        } else {

            /*
             * Sequential execution against of each split in turn.
             */

            runSequence(tasks);

        }

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
    protected void runOne(final Callable<Void> task) {

        if (log.isInfoEnabled())
            log.info("Running one task (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + task.toString());

        try {

            final Future<Void> f = getThreadPool().submit(task);

            // await completion of the task.
            f.get(taskTimeout, TimeUnit.MILLISECONDS);

        } catch (Exception e) {

            if (log.isInfoEnabled())
                log.info("Execution failed: task=" + task, e);

            throw new ClientException("Execution failed: " + task,e);

        }

    }
    
    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in parallel.
     * 
     * @param tasks
     *            The tasks.
     */
    protected void runParallel(
            final ArrayList<AbstractDataServiceProcedureTask> tasks) {

        final long begin = System.currentTimeMillis();
        
        if(log.isInfoEnabled())
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
                    
                    final AbstractDataServiceProcedureTask task = tasks.get(i);

                    // log w/ stack trace so that we can see where this came
                    // from.
                    log.error("Execution failed: task=" + task, e);

                    if (task.causes != null) {

                        causes.addAll(task.causes);

                    } else {

                        causes.add(e);

                    }

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

        if (log.isInfoEnabled())
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
    protected void runSequence(final ArrayList<AbstractDataServiceProcedureTask> tasks) {

        if (log.isInfoEnabled())
            log.info("Running " + tasks.size() + " tasks in sequence (#active="
                    + getThreadPool().getActiveCount() + ", queueSize="
                    + getThreadPool().getQueue().size() + ") : "
                    + tasks.get(0).toString());

        final Iterator<AbstractDataServiceProcedureTask> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = itr.next();

            try {

                final Future<Void> f = getThreadPool().submit(task);
                
                // await completion of the task.
                f.get(taskTimeout, TimeUnit.MILLISECONDS);

            } catch (Exception e) {
        
                if(log.isInfoEnabled()) log.info("Execution failed: task=" + task, e);

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
    protected void runInCallersThread(
            final ArrayList<AbstractDataServiceProcedureTask> tasks) {
        
        final int ntasks = tasks.size();
        
        if (WARN && ntasks > 1)
            log.warn("Running " + ntasks
                + " tasks in caller's thread: recursionDepth="
                + getRecursionDepth().get() + "(#active="
                + getThreadPool().getActiveCount() + ", queueSize="
                + getThreadPool().getQueue().size() + ") : "
                + tasks.get(0).toString());

        final Iterator<AbstractDataServiceProcedureTask> itr = tasks.iterator();

        while (itr.hasNext()) {

            final AbstractDataServiceProcedureTask task = itr.next();

            try {

                task.call();
                
            } catch (Exception e) {

//                if (log.isInfoEnabled())
//                    log.info("Execution failed: task=" + task, e);

                throw new ClientException("Execution failed: recursionDepth="
                        + getRecursionDepth() + ", task=" + task, e,
                        task.causes);

            }
            
        }

    }

    public LinkedList<Split> splitKeys(long ts, int fromIndex, int toIndex,
            byte[][] keys) {

        return splitter.splitKeys(ts, fromIndex, toIndex, keys);
        
    }

    public LinkedList<Split> splitKeys(long ts, int fromIndex, int toIndex,
            KVO[] a) {

        return splitter.splitKeys(ts, fromIndex, toIndex, a);
        
    }

//    /**
//     * {@inheritDoc}
//     * 
//     * Find the partition for the first key. Check the last key, if it is in the
//     * same partition then then this is the simplest case and we can just send
//     * the data along.
//     * <p>
//     * Otherwise, perform a binary search on the remaining keys looking for the
//     * index of the first key GTE the right separator key for that partition.
//     * The batch for this partition is formed from all keys from the first key
//     * for that partition up to but excluding the index position identified by
//     * the binary search (if there is a match; if there is a miss, then the
//     * binary search result needs to be converted into a key index and that will
//     * be the last key for the current partition).
//     * <p>
//     * Examine the next key and repeat the process until all keys have been
//     * allocated to index partitions.
//     * <p>
//     * Note: Split points MUST respect the "row" identity for a sparse row
//     * store, but we get that constraint by maintaining the index partition
//     * boundaries in agreement with the split point constraints for the index.
//     * 
//     * @see Arrays#sort(Object[], int, int, java.util.Comparator)
//     * 
//     * @see BytesUtil#compareBytes(byte[], byte[])
//     * 
//     * @todo Caching? This procedure performs the minimum #of lookups using
//     *       {@link IMetadataIndex#find(byte[])} since that operation will be an
//     *       RMI in a distributed federation. The find(byte[] key) operation is
//     *       difficult to cache since it locates the index partition that would
//     *       span the key and many, many different keys could fit into that same
//     *       index partition. The only effective cache technique may be an LRU
//     *       that scans ~10 caches locators to see if any of them is a match
//     *       before reaching out to the remote {@link IMetadataService}. Or
//     *       perhaps the locators can be cached in a local BTree and a miss
//     *       there would result in a read through to the remote
//     *       {@link IMetadataService} but then we have the problem of figuring
//     *       out when to release locators if the client is long-lived.
//     */
//    public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
//            final int toIndex, final byte[][] keys) {
//
//        assert keys != null;
//        
//        assert fromIndex >= 0;
//        assert fromIndex < toIndex;
//
//        assert toIndex <= keys.length;
//        
//        final LinkedList<Split> splits = new LinkedList<Split>();
//        
//        // start w/ the first key.
//        int currentIndex = fromIndex;
//        
//        while (currentIndex < toIndex) {
//                
//            /*
//             * This is partition spanning the current key (RMI)
//             * 
//             * Note: Using the caller's timestamp here!
//             */
//            final PartitionLocator locator = fed.getMetadataIndex(name, ts)
//                    .find(keys[currentIndex]);
//
//            if (locator == null)
//                throw new RuntimeException("No index partitions?: name=" + name);
//            
//            final byte[] rightSeparatorKey = locator.getRightSeparatorKey();
//
//            if (rightSeparatorKey == null) {
//
//                /*
//                 * The last index partition does not have an upper bound and
//                 * will absorb any keys that order GTE to its left separator
//                 * key.
//                 */
//
//                assert isValidSplit( locator, currentIndex, toIndex, keys );
//                
//                splits.add(new Split(locator, currentIndex, toIndex));
//
//                // done.
//                currentIndex = toIndex;
//
//            } else {
//
//                /*
//                 * Otherwise this partition has an upper bound, so figure out
//                 * the index of the last key that would go into this partition.
//                 * 
//                 * We do this by searching for the rightSeparator of the index
//                 * partition itself.
//                 */
//                
//                int pos = BytesUtil.binarySearch(keys, currentIndex, toIndex
//                        - currentIndex, rightSeparatorKey);
//
//                if (pos >= 0) {
//
//                    /*
//                     * There is a hit on the rightSeparator key. The index
//                     * returned by the binarySearch is the exclusive upper bound
//                     * for the split. The key at that index is excluded from the
//                     * split - it will be the first key in the next split.
//                     * 
//                     * Note: There is a special case when the keys[] includes
//                     * duplicates of the key that corresponds to the
//                     * rightSeparator. This causes a problem where the
//                     * binarySearch returns the index of ONE of the keys that is
//                     * equal to the rightSeparator key and we need to back up
//                     * until we have found the FIRST ONE.
//                     * 
//                     * Note: The behavior of the binarySearch is effectively
//                     * under-defined here and sometimes it will return the index
//                     * of the first key EQ to the rightSeparator while at other
//                     * times it will return the index of the second or greater
//                     * key that is EQ to the rightSeparatoer.
//                     */
//                    
//                    while (pos > currentIndex) {
//                        
//                        if (BytesUtil.bytesEqual(keys[pos - 1],
//                                rightSeparatorKey)) {
//
//                            // keep backing up.
//                            pos--;
//
//                            continue;
//
//                        }
//                        
//                        break;
//                        
//                    }
//
//                    if(log.isDebugEnabled()) log.debug("Exact match on rightSeparator: pos=" + pos
//                            + ", key=" + BytesUtil.toString(keys[pos]));
//
//                } else if (pos < 0) {
//
//                    /*
//                     * There is a miss on the rightSeparator key (it is not
//                     * present in the keys that are being split). In this case
//                     * the binary search returns the insertion point. We then
//                     * compute the exclusive upper bound from the insertion
//                     * point.
//                     */
//                    
//                    pos = -pos - 1;
//
//                    assert pos > currentIndex && pos <= toIndex : "Expected pos in ["
//                        + currentIndex + ":" + toIndex + ") but pos=" + pos;
//
//                }
//
//                /*
//                 * Note: this test can be enabled if you are having problems
//                 * with KeyAfterPartition or KeyBeforePartition. It will go
//                 * through more effort to validate the constraints on the split.
//                 * However, due to the additional byte[] comparisons, this
//                 * SHOULD be disabled except when tracking a bug.
//                 */
////                assert validSplit( locator, currentIndex, pos, keys );
//
//                splits.add(new Split(locator, currentIndex, pos));
//
//                currentIndex = pos;
//
//            }
//
//        }
//
//        return splits;
//
//    }
//
//    public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
//            final int toIndex, final KVO[] a) {
//
//        /*
//         * Change the shape of the data so that we can split it.
//         */
//
//        final byte[][] keys = new byte[a.length][];
//
//        for (int i = 0; i < a.length; i++) {
//
//            keys[i] = a[i].key;
//
//        }
//
//        return splitKeys(ts, fromIndex, toIndex, keys);
//
//    }
//
//    /**
//     * Paranoia testing for generated splits.
//     * 
//     * @param locator
//     * @param fromIndex
//     * @param toIndex
//     * @param keys
//     * @return
//     */
//    private boolean isValidSplit(final PartitionLocator locator,
//            final int fromIndex, final int toIndex, final byte[][] keys) {
//
//        assert fromIndex <= toIndex : "fromIndex=" + fromIndex + ", toIndex="
//                + toIndex;
//
//        assert fromIndex >= 0 : "fromIndex=" + fromIndex;
//
//        assert toIndex <= keys.length : "toIndex=" + toIndex + ", keys.length="
//                + keys.length;
//
//        // begin with the left separator on the index partition.
//        byte[] lastKey = locator.getLeftSeparatorKey();
//        
//        assert lastKey != null;
//
//        for (int i = fromIndex; i < toIndex; i++) {
//
//            final byte[] key = keys[i];
//
//            assert key != null;
//
//            if (lastKey != null) {
//
//                final int ret = BytesUtil.compareBytes(lastKey, key);
//
//                assert ret <= 0 : "keys out of order: i=" + i + ", lastKey="
//                        + BytesUtil.toString(lastKey) + ", key="
//                        + BytesUtil.toString(key)+", keys="+BytesUtil.toString(keys);
//                
//            }
//            
//            lastKey = key;
//            
//        }
//
//        // Note: Must be strictly LT the rightSeparator key (when present).
//        {
//
//            final byte[] key = locator.getRightSeparatorKey();
//
//            if (key != null) {
//
//                int ret = BytesUtil.compareBytes(lastKey, key);
//
//                assert ret < 0 : "keys out of order: lastKey="
//                        + BytesUtil.toString(lastKey) + ", rightSeparator="
//                        + BytesUtil.toString(key)+", keys="+BytesUtil.toString(keys);
//
//            }
//            
//        }
//        
//        return true;
//        
//    }
    
    public IDataService getDataService(final PartitionLocator pmd) {

        return fed.getDataService(pmd.getDataServiceUUID());

    }

    /**
     * This operation is not supported - the resource description of a scale-out
     * index would include all "live" resources in the corresponding
     * {@link MetadataIndex}.
     */
    public IResourceMetadata[] getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }

    public void staleLocator(final long ts, final PartitionLocator locator,
            final StaleLocatorException cause) {

        if (locator == null)
            throw new IllegalArgumentException();

        if (ts != ITx.UNISOLATED && ts != ITx.READ_COMMITTED) {

            /*
             * Stale locator exceptions should not be thrown for these views.
             */

            throw new RuntimeException(
                    "Stale locator, but views should be consistent? timestamp="
                            + TimestampUtility.toString(ts));

        }

        // notify the metadata index view that it has a stale locator.
        fed.getMetadataIndex(name, timestamp).staleLocator(locator);

    }

    public <T extends IKeyArrayIndexProcedure, O, R, A> IRunnableBuffer<KVO<O>[]> newWriteBuffer(
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor) {

        final AsynchronousIndexWriteConfiguration conf = getIndexMetadata()
                .getAsynchronousIndexWriteConfiguration();

        final BlockingBuffer<KVO<O>[]> writeBuffer = new BlockingBuffer<KVO<O>[]>(
                // @todo array vs linked w/ capacity and fair vs unfair.
                // @todo config deque vs queue (deque combines on add() as well)
                new LinkedBlockingDeque<KVO<O>[]>(conf.getMasterQueueCapacity()),
//                new ArrayBlockingQueue<KVO<O>[]>(conf.getMasterQueueCapacity()),
                conf.getMasterChunkSize(),//
                conf.getMasterChunkTimeoutNanos(),// 
                TimeUnit.NANOSECONDS,//
                true// ordered
        );
        
        final IndexWriteTask.M<T, O, R, A> task = new IndexWriteTask.M<T, O, R, A>(
                this, //
                conf.getSinkIdleTimeoutNanos(),//
                conf.getSinkPollTimeoutNanos(),//
                conf.getSinkQueueCapacity(), //
                conf.getSinkChunkSize(), //
                conf.getSinkChunkTimeoutNanos(),//
                duplicateRemover,//
                ctor,//
                resultHandler,//
                fed.getIndexCounters(name).asynchronousStats,
                writeBuffer//
                );

        final Future<? extends IndexAsyncWriteStats> future = fed
                .getExecutorService().submit(task);

        writeBuffer.setFuture(future);

        return task.getBuffer();

    }

    /**
     * Return a new {@link CounterSet} backed by the {@link ScaleOutIndexCounters}
     * for this scale-out index.
     */
    public ICounterSet getCounters() {

        return getFederation().getIndexCounters(name).getCounters();

    }

}
