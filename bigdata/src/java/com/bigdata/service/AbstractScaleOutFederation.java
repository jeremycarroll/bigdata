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
 * Created on Sep 13, 2008
 */

package com.bigdata.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StoreManager;
//BTM import com.bigdata.service.AbstractScaleOutClient.MetadataIndexCachePolicy;
import com.bigdata.service.AbstractScaleOutClient.Options;
import com.bigdata.service.ndx.ClientIndexView;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IScaleOutIndexManager;
import com.bigdata.journal.IScaleOutIndexStore;
import net.jini.admin.Administrable;

/**
 * Abstract base class for federation implementations using the scale-out index
 * architecture (federations that support key-range partitioned indices).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the client or service.
 */
//BTM public abstract class AbstractScaleOutFederation<T> extends AbstractFederation<T> {
public abstract class AbstractScaleOutFederation<T>
                          extends AbstractFederation<T>
                          implements IScaleOutIndexManager
{
    /**
     * @param client
     */
    public AbstractScaleOutFederation(final IBigdataClient<T> client) {
       
        super(client);
        
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        indexCache = new IndexCache(this, client.getIndexCacheCapacity(),
//BTM - PRE_CLIENT_SERVICE                client.getIndexCacheTimeout());
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        metadataIndexCache = new MetadataIndexCache(this, client
//BTM - PRE_CLIENT_SERVICE                .getIndexCacheCapacity(), client.getIndexCacheTimeout());
//BTM - PRE_CLIENT_SERVICE        
//BTM - PRE_CLIENT_SERVICE        final Properties properties = client.getProperties();
//BTM - PRE_CLIENT_SERVICE        
//BTM - PRE_CLIENT_SERVICE        metadataIndexCachePolicy = MetadataIndexCachePolicy.valueOf(properties
//BTM - PRE_CLIENT_SERVICE                .getProperty(Options.METADATA_INDEX_CACHE_POLICY,
//BTM - PRE_CLIENT_SERVICE                        Options.DEFAULT_METADATA_INDEX_CACHE_POLICY));
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE - NEED metadataIndexCachePolicy from the config BEFORE creating MetadataIndexCache so it can be input to the constructor
//BTM - PRE_CLIENT_SERVICE

        indexCache = 
            new IndexCache( (IBigdataDiscoveryManagement)this,
                            this,
                            (IScaleOutIndexStore)this,
                            client.getIndexCacheCapacity(),
                            client.getIndexCacheTimeout(),
                            client.getDefaultRangeQueryCapacity(),
                            client.getBatchApiOnly(),
                            client.getTaskTimeout(),
                            client.getMaxParallelTasksPerRequest(),
                            client.getMaxStaleLocatorRetries() );

        final Properties properties = client.getProperties();
        
        metadataIndexCachePolicy = MetadataIndexCachePolicy.valueOf(properties
                .getProperty(Options.METADATA_INDEX_CACHE_POLICY,
                        Options.DEFAULT_METADATA_INDEX_CACHE_POLICY));

        metadataIndexCache = 
            new MetadataIndexCache
                    ( (IBigdataDiscoveryManagement)this,
                      (IIndexManager)this,
                      metadataIndexCachePolicy,
                      client.getIndexCacheCapacity(),
                      client.getIndexCacheTimeout() );
//BTM - PRE_CLIENT_SERVICE - END

        if (log.isInfoEnabled())
            log.info(Options.METADATA_INDEX_CACHE_POLICY + "="
                    + metadataIndexCachePolicy);
               
    }
    
    protected final MetadataIndexCachePolicy metadataIndexCachePolicy;

    /**
     * Strengthens the return type.
     * 
     * {@inheritDoc}
     */
    public ClientIndexView getIndex(String name, long timestamp) {

        return (ClientIndexView) super.getIndex(name, timestamp);
        
    }
    
    public synchronized void shutdown() {
        
        super.shutdown();

        indexCache.shutdown();

        metadataIndexCache.shutdown();
        
    }

    public synchronized void shutdownNow() {
    
        super.shutdownNow();
        
        indexCache.shutdown();

        metadataIndexCache.shutdown();
            
    }

    /**
     * Return a read-only view onto an {@link IMetadataIndex}.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     *            The timestamp for the view.
     * 
     * @todo The easiest way to have the view be correct is for the operations
     *       to all run against the remote metadata index (no caching).
     *       <p>
     *       There are three kinds of queries that we do against the metadata
     *       index: (1) get(key); (2) find(key); and (3)
     *       locatorScan(fromKey,toKey). The first is only used by the unit
     *       tests. The second is used when we start a locator scan, when we
     *       split a batch operation against the index partitions, and when we
     *       map an index procedure over a key range or use a key range
     *       iterator. This is the most costly of the queries, but it is also
     *       the one that is the least easy to cache. The locator scan itself is
     *       heavily buffered - a cache would only help for frequently scanned
     *       and relatively small key ranges. For this purpose, it may be better
     *       to cache the iterator result itself locally to the client (for
     *       historical reads or transactional reads).
     *       <p>
     *       The difficulty with caching find(key) is that we need to use the
     *       {@link ILinearList} API to locate the appropriate index partition.
     *       However, since it is a cache, there can be cache misses. These
     *       would show up as a "gap" in the (leftSeparator, rightSeparator)
     *       coverage.
     *       <p>
     *       If we do not cache access to the remote metadata index then we will
     *       impose additional latency on clients, traffic on the network, and
     *       demands on the metadata service. However, with high client
     *       concurrency mitigates the increase in access latency to the
     *       metadata index.
     * 
     * @todo Use a weak-ref cache with an LRU (or hard reference cache) to evict
     *       cached {@link PartitionLocator}. The client needs access by {
     *       indexName, timestamp, key }. We need to eventually evict the cached
     *       locators to prevent the client from building up too much state
     *       locally. Also the cached locators can not be shared across
     *       different timestamps, so clients will build up a locator cache when
     *       working on a transaction but then never go back to that cache once
     *       the transaction completes.
     *       <p>
     *       While it may be possible to share cached locators between
     *       historical reads and transactions for the same point in history, we
     *       do not have enough information on hand to make those decisions.
     *       What we would need to know is the historical commit time
     *       corresponding to an assigned transaction startTime. This is not
     *       one-to-one since the start times for transactions must be unique
     *       (among those in play). See {@link ITransactionService#newTx(long)}
     *       for more on this.
     * 
     * @todo cache leased information about index partitions of interest to the
     *       client. The cache will be a little tricky since we need to know
     *       when the client does not possess a partition definition. Index
     *       partitions are defined by the separator key - the first key that
     *       lies beyond that partition. the danger then is that a client will
     *       presume that any key before the first leased partition is part of
     *       that first partition. To guard against that the client needs to
     *       know both the separator key that represents the upper and lower
     *       bounds of each partition. If a lookup in the cache falls outside of
     *       any known partitions upper and lower bounds then it is a cache miss
     *       and we have to ask the metadata service for a lease on the
     *       partition. the cache itself is just a btree data structure with the
     *       proviso that some cache entries represent missing partition
     *       definitions (aka the lower bounds for known partitions where the
     *       left sibling partition is not known to the client).
     *       <p>
     *       With even a modest #of partitions, a locator scan against the MDS
     *       will be cheaper than attempting to fill multiple "gaps" in a local
     *       locator cache, so such a cache might be reserved for point tests.
     *       Such point tests are used by the sparse row store for its row local
     *       operations (vs scans) but are less common for JOINs.
     * 
     * @todo Just create cache view when MDI is large and then cache on demand.
     * 
     * @todo If the {@link IMetadataIndex#get(byte[])} and
     *       {@link IMetadataIndex#find(byte[])} methods are to be invoked
     *       remotely then we should return the byte[] rather than the
     *       de-serialized {@link PartitionLocator} so that we don't
     *       de-serialize them from the index only to serialize them for RMI and
     *       then de-serialize them again on the client.
     * 
     * @todo the easiest way to handle a scale-out metadata index is to make it
     *       hash-partitioned (vs range-partitioned). We can just flood queries
     *       to the hash partitioned index. For the iterator, we have to buffer
     *       the results and place them back into order. A fused view style
     *       iterator could be used to merge the iterator results from each
     *       partition into a single totally ordered iterator.
     */
    public IMetadataIndex getMetadataIndex(String name, long timestamp) {

        if (log.isInfoEnabled())
            log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

        return getMetadataIndexCache().getIndex(name, timestamp);

//BTM - PRE_CLIENT_SERVICE MetadataIndexCache cache = getMetadataIndexCache();
//BTM - PRE_CLIENT_SERVICE IMetadataIndex index = cache.getIndex(name, timestamp);
//BTM - PRE_CLIENT_SERVICE log.warn("\n>>>>AbstractScaleOutFederation.getMetadataIndex: name="+name+", timestamp="+timestamp+", metadataIndexCache="+cache+", metadataIndex="+index+"\n");
//BTM - PRE_CLIENT_SERVICE return index;
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
     * Note: It is possible that a split, join or move could occur during the
     * process of mapping the procedure across the index partitions. When the
     * view is {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} this could
     * make the set of mapped index partitions inconsistent in the sense that it
     * might double count some parts of the key range or that it might skip some
     * parts of the key range. In order to avoid this problem the caller MUST
     * use <em>read-consistent</em> semantics. If the {@link ClientIndexView}
     * is not already isolated by a transaction, then the caller MUST create a
     * read-only transaction use the global last commit time of the federation.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     *            The timestamp of the view. It is the responsibility of the
     *            caller to choose <i>timestamp</i> so as to provide
     *            read-consistent semantics for the locator scan.
     * @param fromKey
     *            The scale-out index first key that will be visited
     *            (inclusive). When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first scale-out index key that will NOT be visited
     *            (exclusive). When <code>null</code> there is no upper bound.
     * @param reverseScan
     *            <code>true</code> if you need to visit the index partitions
     *            in reverse key order (this is done when the partitioned
     *            iterator is scanning backwards).
     * 
     * @return The iterator.
     */
    @SuppressWarnings("unchecked")
    public Iterator<PartitionLocator> locatorScan(final String name,
            final long timestamp, final byte[] fromKey, final byte[] toKey,
            final boolean reverseScan) {
        
        if (log.isInfoEnabled())
            log.info("Querying metadata index: name=" + name + ", timestamp="
                    + timestamp + ", reverseScan=" + reverseScan + ", fromKey="
                    + BytesUtil.toString(fromKey) + ", toKey="
                    + BytesUtil.toString(toKey));
        
        final IMetadataIndex mdi = getMetadataIndex(name, timestamp);
        
        final ITupleIterator<PartitionLocator> itr;

        // the values are the locators (keys are not required).
        final int flags = IRangeQuery.VALS;
        
        if (reverseScan) {
         
            /*
             * Reverse locator scan.
             * 
             * The first locator visited will be the first index partition whose
             * leftSeparator is LT the optional toKey. (If the toKey falls on an
             * index partition boundary then we use the prior index partition).
             */

            itr = mdi.rangeIterator(//
                    fromKey,//
                    toKey, //
                    0, // capacity
                    flags | IRangeQuery.REVERSE,
                    null // filter
                    );

        } else {
            
            /*
             * Forward locator scan.
             * 
             * Note: The scan on the metadata index needs to start at the index
             * partition in which the fromKey would be located. Therefore, when
             * the fromKey is specified, we replace it with the leftSeparator of
             * the index partition which would contain that fromKey.
             */

            final byte[] _fromKey = fromKey == null //
                ? null //
                : mdi.find(fromKey).getLeftSeparatorKey()//
                ;

            itr = mdi.rangeIterator(//
                    _fromKey,//
                    toKey, //
                    0, // capacity
                    flags,//
                    null // filter
                    );

        }

        return new Striterator(itr).addFilter(new Resolver(){

            private static final long serialVersionUID = 7874887729130530971L;

            @Override
            protected Object resolve(Object obj) {
             
                final ITuple<PartitionLocator> tuple = (ITuple<PartitionLocator>) obj;
                
                return tuple.getObject();
                
            }
        });
        
    }
        
    /**
     * Return <code>true</code>.
     */
    final public boolean isScaleOut() {
        
        return true;
        
    }

    private final IndexCache indexCache;
    private final MetadataIndexCache metadataIndexCache;
    
    protected IndexCache getIndexCache() {
        
        return indexCache;
        
    }
    
    /**
     * Return the cache for {@link IMetadataIndex} objects.
     */
    protected MetadataIndexCache getMetadataIndexCache() {
        
        return metadataIndexCache;
        
    }

    /**
     * Await the availability of a shard locator service and the specified
     * minimum #of {@link ShardService}s.
     * 
     * @param minDataServices
     *            The minimum #of data services.
     * @param timeout
     *            The timeout (ms).
     * 
     * @return An array #of the {@link UUID}s of the {@link ShardService}s
     *         that have been discovered by <em>this</em> client. Note that at
     *         least <i>minDataServices</i> elements will be present in this
     *         array but that ALL discovered data services MAY be reported.
     * 
     * @throws IllegalArgumentException
     *             if <i>minDataServices</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>timeout</i> is non-positive.
     * @throws IllegalStateException
     *             if the client is not connected to the federation.
     * @throws InterruptedException
     *             if this thread is interrupted while awaiting the availability
     *             of the shard locator service or the specified #of
     *             {@link ShardService}s.
     * @throws TimeoutException
     *             If a timeout occurs.
     * 
     * @todo We should await critical services during connect() {MDS, TS, LS}.
     *       The LBS is not critical, but we should either have it on hand to
     *       notice our service join or we should notice its JOIN and then
     *       notice it ourselves. That would leave this method with the
     *       responsibility for awaiting the join of at least N data services
     *       (and perhaps verifying that the other services are still joined).
     * 
     * FIXME This should be rewritten in the JiniFederation subclass to use the
     * ServiceDiscoveryListener interface implemented by that class.
     */
    public UUID[] awaitServices(final int minDataServices, final long timeout)
            throws InterruptedException, TimeoutException {

        assertOpen();

        if (minDataServices <= 0)
            throw new IllegalArgumentException();

        if (timeout <= 0)
            throw new IllegalArgumentException();
        
        final long begin = System.currentTimeMillis();

        // sleep interval if not ready (ms).
        final long interval = Math.min(100, timeout / 10);

        int ntries = 0;
        
        // updated each time through the loop.
        ShardLocator metadataService = null;
        
        // updated each time through the loop.
        UUID[] dataServiceUUIDs = null;
        
        while (true) {
            
            // set on entry each time through the loop.
            metadataService = getMetadataService();

            // set on entry each time through the loop.
            dataServiceUUIDs = getDataServiceUUIDs(0/* all */);

            if ((System.currentTimeMillis() - begin) >= timeout
                    || (metadataService != null && dataServiceUUIDs.length >= minDataServices)) {

                /*
                 * Either a timeout or we have the MDS and enough DS.
                 * 
                 * Either way, we are done so break out of the loop.
                 */
                
                break;
                
            }
            
            ntries++;

            if (log.isInfoEnabled())
                log.info("Waiting : ntries=" + ntries + ", metadataService="
                        + (metadataService == null ? "not " : "")
                        + " found; #dataServices=" + dataServiceUUIDs.length
                        + " out of " + minDataServices + " required : "
                        + Arrays.toString(dataServiceUUIDs));

            // @todo the way this is written can sleep longer than the remaining time 
            Thread.sleep(interval);

            continue;

        }

        if (log.isInfoEnabled())
            log.info("MDS=" + (metadataService != null) + ", #dataServices="
                    + dataServiceUUIDs.length);

        if (metadataService != null
                && dataServiceUUIDs.length >= minDataServices) {

            // success.
            return dataServiceUUIDs;

        }

        throw new TimeoutException("elapsed="
                + (System.currentTimeMillis() - begin) + "ms: metadataService="
                + (metadataService != null) + ", dataServices="
                + dataServiceUUIDs.length+" but require "+minDataServices);
        
    }

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
    public void forceOverflow(final boolean compactingMerge, final boolean truncateJournal) {
        
        // find UUID for each data service.
        final UUID[] dataServiceUUIDs = getDataServiceUUIDs(0/* maxCount */);

        final int ndataServices = dataServiceUUIDs.length;

        log.warn("Forcing overflow: #dataServices=" + ndataServices + ", now=" + new Date());

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(ndataServices);

        for (UUID serviceUUID : dataServiceUUIDs) {

            tasks.add(new ForceOverflowTask(getDataService(serviceUUID),
                    compactingMerge, truncateJournal));

        }

        if(truncateJournal) {

            /*
             * @todo The metadata service does not yet support overflow (it does
             * not support partitioned metadata indices) so it only has a live
             * journal. Therefore all we can do is truncate the live journal for
             * the metadata service.
             */

            tasks.add(new PurgeResourcesTask(getMetadataService(),
                    truncateJournal));
            
        }
        
        final List<Future<Void>> futures;
        try {

            futures = getExecutorService().invokeAll(tasks);
            
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

        log.warn("Did overflow: #ok=" + nok + ", #dataServices="
                + ndataServices + ", now=" + new Date());

        if (nok != tasks.size()) {

            throw new RuntimeException(
                    "Errors during overflow processing: #ok=" + nok
                            + ", #tasks=" + tasks.size());

        }

    }

//BTM - FOR_CLIENT_SERVICE - BEGIN -----------------------------------------------------------
//BTM - FOR_CLIENT_SERVICE - NOTE: this method was added to address trac issue #190
    public void dropIndex(String name) {
        super.dropIndex(name);
        try {
            getMetadataIndexCache().dropIndexFromCache(name);
            getIndexCache().dropIndexFromCache(name);
        } catch (Exception e) {//maintain same logic as super.dropIndex?
            throw new RuntimeException( e );
        }
    }
//BTM - FOR_CLIENT_SERVICE - END -------------------------------------------------------------
    

//BTM - PRE_CLIENT_SERVICE - BEGIN - moved to standalone classes -------------------------------------------------------------------
//BTM - PRE_CLIENT_SERVICE     /**
//BTM - PRE_CLIENT_SERVICE //BTM - PRE_CLIENT_SERVICE      * Task directs a {@link ShardService} to purge any unused resources and to
//BTM - PRE_CLIENT_SERVICE      * optionally truncate the extent of the live journal.
//BTM - PRE_CLIENT_SERVICE      * 
//BTM - PRE_CLIENT_SERVICE      */
//BTM - PRE_CLIENT_SERVICE     public static class PurgeResourcesTask implements Callable<Void> {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         protected static final Logger log = Logger
//BTM - PRE_CLIENT_SERVICE                 .getLogger(PurgeResourcesTask.class);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM        private final IDataService dataService;
//BTM - PRE_CLIENT_SERVICE private final ShardService dataService;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         private final boolean truncateJournal;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM - BEGIN
//BTM - PRE_CLIENT_SERVICE private final ShardLocator metadataService;
//BTM - PRE_CLIENT_SERVICE public PurgeResourcesTask(final ShardLocator metadataService, final boolean truncateJournal) {
//BTM - PRE_CLIENT_SERVICE     if (metadataService == null) {
//BTM - PRE_CLIENT_SERVICE         throw new IllegalArgumentException
//BTM - PRE_CLIENT_SERVICE                               ("null shard locator service");
//BTM - PRE_CLIENT_SERVICE     }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     this.dataService = null;
//BTM - PRE_CLIENT_SERVICE     this.metadataService = metadataService;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     this.truncateJournal = truncateJournal;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE }
//BTM - PRE_CLIENT_SERVICE //BTM - END
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM        public PurgeResourcesTask(final IDataService dataService, final boolean truncateJournal)
//BTM - PRE_CLIENT_SERVICE public PurgeResourcesTask(final ShardService dataService, final boolean truncateJournal)
//BTM - PRE_CLIENT_SERVICE         {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (dataService == null)
//BTM - PRE_CLIENT_SERVICE                 throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             this.dataService = dataService;
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE this.metadataService = null;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             this.truncateJournal = truncateJournal;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         public Void call() throws Exception {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM - BEGIN CHANGE FROM IDATA_SERVICE TO SHARD_SERVICE
//BTM - PRE_CLIENT_SERVICE if(dataService != null) {
//BTM - PRE_CLIENT_SERVICE //BTM            if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE //BTM                log.info("dataService: " + dataService.getServiceName());
//BTM - PRE_CLIENT_SERVICE //BTM            }
//BTM - PRE_CLIENT_SERVICE //BTM            if (!dataService.purgeOldResources(5000/* ms */, truncateJournal)) {
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                log.warn("Could not pause write service - resources will not be purged.");
//BTM - PRE_CLIENT_SERVICE //BTM            }
//BTM - PRE_CLIENT_SERVICE             String serviceName = null;
//BTM - PRE_CLIENT_SERVICE             if(dataService instanceof IService) {
//BTM - PRE_CLIENT_SERVICE                 serviceName = 
//BTM - PRE_CLIENT_SERVICE                     ((IService)dataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE             } else {
//BTM - PRE_CLIENT_SERVICE                 serviceName = 
//BTM - PRE_CLIENT_SERVICE                     ((Service)dataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             ShardManagement shardMgr = (ShardManagement)dataService;
//BTM - PRE_CLIENT_SERVICE             boolean resourcesPurged = shardMgr.purgeOldResources(5000/* ms */, truncateJournal);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE                 log.info("dataService: " + serviceName);
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             if ( !resourcesPurged ) {
//BTM - PRE_CLIENT_SERVICE                 log.warn("Could not pause shard service - resources will not be purged.");
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE }//endif (dataService != null)
//BTM - PRE_CLIENT_SERVICE //BTM - END CHANGE FROM IDATA_SERVICE TO SHARD_SERVICE
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE if(metadataService != null) {
//BTM - PRE_CLIENT_SERVICE //BTM - BEGIN CHANGE FROM IDATA_SERVICE TO SHARD_SERVICE
//BTM - PRE_CLIENT_SERVICE //BTM            String serviceName = null;
//BTM - PRE_CLIENT_SERVICE //BTM    IDataService remoteShardMgr = null;
//BTM - PRE_CLIENT_SERVICE //BTM    ShardManagement shardMgr = null;
//BTM - PRE_CLIENT_SERVICE //BTM            if(metadataService instanceof IMetadataService) {
//BTM - PRE_CLIENT_SERVICE //BTM                serviceName = 
//BTM - PRE_CLIENT_SERVICE //BTM                    ((IMetadataService)metadataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE //BTM                remoteShardMgr = (IDataService)metadataService;
//BTM - PRE_CLIENT_SERVICE //BTM            } else if(metadataService instanceof Service) {
//BTM - PRE_CLIENT_SERVICE //BTM                serviceName = 
//BTM - PRE_CLIENT_SERVICE //BTM                    ((Service)metadataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE //BTM                shardMgr = (ShardManagement)metadataService;
//BTM - PRE_CLIENT_SERVICE //BTM            } else {
//BTM - PRE_CLIENT_SERVICE //BTM                log.warn("wrong type for shard locator service "
//BTM - PRE_CLIENT_SERVICE //BTM                         +"["+metadataService.getClass()+"]");
//BTM - PRE_CLIENT_SERVICE //BTM            }
//BTM - PRE_CLIENT_SERVICE //BTM            if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE //BTM                log.info("metadataService: " + serviceName);
//BTM - PRE_CLIENT_SERVICE //BTM            }
//BTM - PRE_CLIENT_SERVICE //BTM    boolean resourcesPurged = false;
//BTM - PRE_CLIENT_SERVICE //BTM    if(remoteShardMgr != null) {
//BTM - PRE_CLIENT_SERVICE //BTM        resourcesPurged = remoteShardMgr.purgeOldResources(5000/* ms */, truncateJournal);
//BTM - PRE_CLIENT_SERVICE //BTM    } else if(shardMgr != null) {
//BTM - PRE_CLIENT_SERVICE //BTM        resourcesPurged = shardMgr.purgeOldResources(5000/* ms */, truncateJournal);
//BTM - PRE_CLIENT_SERVICE //BTM    }
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE             String serviceName = null;
//BTM - PRE_CLIENT_SERVICE             if(metadataService instanceof IService) {
//BTM - PRE_CLIENT_SERVICE                 serviceName = 
//BTM - PRE_CLIENT_SERVICE                     ((IService)metadataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE             } else {
//BTM - PRE_CLIENT_SERVICE                 serviceName = 
//BTM - PRE_CLIENT_SERVICE                     ((Service)metadataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             ShardManagement shardMgr = (ShardManagement)metadataService;
//BTM - PRE_CLIENT_SERVICE             boolean resourcesPurged = shardMgr.purgeOldResources(5000/* ms */, truncateJournal);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE                 log.info("metadataService: " + serviceName);
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             if ( !resourcesPurged ) {
//BTM - PRE_CLIENT_SERVICE                 log.warn("Could not pause shard locator service - resources will not be purged.");
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE //BTM - END CHANGE FROM IDATA_SERVICE TO SHARD_SERVICE
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM            if (!resourcesPurged) {
//BTM - PRE_CLIENT_SERVICE //BTM                log.warn("Could not pause write service - resources will not be purged.");
//BTM - PRE_CLIENT_SERVICE //BTM            }
//BTM - PRE_CLIENT_SERVICE }//endif (metadataService != null)
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             return null;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     }   
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     /**
//BTM - PRE_CLIENT_SERVICE      * Task forces immediate overflow of the specified data service, returning
//BTM - PRE_CLIENT_SERVICE      * once both synchronous AND asynchronous overflow are complete.
//BTM - PRE_CLIENT_SERVICE      * 
//BTM - PRE_CLIENT_SERVICE      * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//BTM - PRE_CLIENT_SERVICE      * @version $Id$
//BTM - PRE_CLIENT_SERVICE      */
//BTM - PRE_CLIENT_SERVICE     public static class ForceOverflowTask implements Callable<Void> {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         protected static final Logger log = Logger
//BTM - PRE_CLIENT_SERVICE                 .getLogger(ForceOverflowTask.class);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM        private final IDataService dataService;
//BTM - PRE_CLIENT_SERVICE private final ShardService dataService;
//BTM - PRE_CLIENT_SERVICE private String serviceName;
//BTM - PRE_CLIENT_SERVICE private ShardManagement shardMgr = null;
//BTM - PRE_CLIENT_SERVICE private OverflowAdmin overflowAdmin = null;
//BTM - PRE_CLIENT_SERVICE         private final boolean compactingMerge;
//BTM - PRE_CLIENT_SERVICE         private final boolean truncateJournal;
//BTM - PRE_CLIENT_SERVICE         
//BTM - PRE_CLIENT_SERVICE //BTM        public ForceOverflowTask(final IDataService dataService, final boolean compactingMerge, final boolean truncateJournal)
//BTM - PRE_CLIENT_SERVICE public ForceOverflowTask(final ShardService dataService, final boolean compactingMerge, final boolean truncateJournal)
//BTM - PRE_CLIENT_SERVICE         {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (dataService == null)
//BTM - PRE_CLIENT_SERVICE                 throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             this.dataService = dataService;
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             this.compactingMerge = compactingMerge;
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             this.truncateJournal = truncateJournal;
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE this.serviceName = "UNKNOWN";
//BTM - PRE_CLIENT_SERVICE if(this.dataService != null) {
//BTM - PRE_CLIENT_SERVICE     if(this.dataService instanceof IService) {
//BTM - PRE_CLIENT_SERVICE         try {
//BTM - PRE_CLIENT_SERVICE             this.serviceName = ((IService)this.dataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE             this.overflowAdmin = (OverflowAdmin)(this.dataService);
//BTM - PRE_CLIENT_SERVICE         } catch(java.io.IOException e) {
//BTM - PRE_CLIENT_SERVICE             log.warn("failed to retrieved serviceName from shard service", e);
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE     } else {
//BTM - PRE_CLIENT_SERVICE         this.serviceName = ((Service)this.dataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE         try {
//BTM - PRE_CLIENT_SERVICE             this.overflowAdmin =
//BTM - PRE_CLIENT_SERVICE             (OverflowAdmin)( ((Administrable)(this.dataService)).getAdmin() );
//BTM - PRE_CLIENT_SERVICE         } catch(java.rmi.RemoteException e) {
//BTM - PRE_CLIENT_SERVICE             log.warn("failure when retrieving overflow admin", e);
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE     }
//BTM - PRE_CLIENT_SERVICE     this.shardMgr = (ShardManagement)(this.dataService);
//BTM - PRE_CLIENT_SERVICE }
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE         
//BTM - PRE_CLIENT_SERVICE         public Void call() throws Exception {
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             if(log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE //BTM                log.info("dataService: " + dataService.getServiceName());
//BTM - PRE_CLIENT_SERVICE                 log.info("dataService: " + serviceName);
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             // returns once synchronous overflow is complete.
//BTM - PRE_CLIENT_SERVICE //BTM            dataService.forceOverflow(true/* immediate */, compactingMerge);
//BTM - PRE_CLIENT_SERVICE overflowAdmin.forceOverflow(true/* immediate */, compactingMerge);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE //BTM                log.info("Synchronous overflow is done: " + dataService.getServiceName());
//BTM - PRE_CLIENT_SERVICE                 log.info("Synchronous overflow is done: " + serviceName);
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             // wait until overflow processing is done.
//BTM - PRE_CLIENT_SERVICE //BTM            while (dataService.isOverflowActive()) {
//BTM - PRE_CLIENT_SERVICE while (overflowAdmin.isOverflowActive()) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 Thread.sleep(100/* ms */);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE //BTM                log.info("Asynchronous overflow is done: " + dataService.getServiceName());
//BTM - PRE_CLIENT_SERVICE                 log.info("Asynchronous overflow is done: " + serviceName);
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             /*
//BTM - PRE_CLIENT_SERVICE              * Note: Old resources are automatically released as the last step
//BTM - PRE_CLIENT_SERVICE              * of asynchronous overflow processing. Therefore all we are really
//BTM - PRE_CLIENT_SERVICE              * doing here is issuing a request to truncate the journal. However,
//BTM - PRE_CLIENT_SERVICE              * we use the same method to accomplish both ends.
//BTM - PRE_CLIENT_SERVICE              */
//BTM - PRE_CLIENT_SERVICE             if (truncateJournal) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM                if (!dataService.purgeOldResources(5000/* ms */, true/* truncateJournal */)) {
//BTM - PRE_CLIENT_SERVICE                 if (!shardMgr.purgeOldResources(5000/* ms */, true/* truncateJournal */)) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     log.warn("Could not pause shard service - resources will not be purged.");
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             return null;
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE         
//BTM - PRE_CLIENT_SERVICE     }
//BTM - PRE_CLIENT_SERVICE - END -------------------------------------------------------------------------------------------

}
