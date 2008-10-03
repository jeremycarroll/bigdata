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

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.bigdata.btree.ILinearList;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.AbstractScaleOutClient.MetadataIndexCachePolicy;
import com.bigdata.service.AbstractScaleOutClient.Options;

/**
 * Abstract base class for federation implementations using the scale-out index
 * architecture (federations that support key-range partitioned indices).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractScaleOutFederation extends AbstractFederation {

    /**
     * @param client
     */
    public AbstractScaleOutFederation(IBigdataClient client) {
       
        super(client);
        
        indexCache = new IndexCache(this, client.getIndexCacheCapacity());

        metadataIndexCache = new MetadataIndexCache(this, client
                .getIndexCacheCapacity());
        
        final Properties properties = client.getProperties();
        
        metadataIndexCachePolicy = MetadataIndexCachePolicy.valueOf(properties
                .getProperty(Options.METADATA_INDEX_CACHE_POLICY,
                        Options.DEFAULT_METADATA_INDEX_CACHE_POLICY));

        if (INFO)
            log.info(Options.METADATA_INDEX_CACHE_POLICY + "="
                    + metadataIndexCachePolicy);
       
        
    }
    
    protected final MetadataIndexCachePolicy metadataIndexCachePolicy;

    /**
     * Strengthens the return type.
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
     *       (among those in play). See
     *       {@link ITransactionManager#newTx(com.bigdata.journal.IsolationEnum)}
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

        if (INFO)
            log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

        return getMetadataIndexCache().getIndex(name, timestamp);
         
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
     * Await the availability of an {@link IMetadataService} and the specified
     * minimum #of {@link IDataService}s.
     * 
     * @param minDataServices
     *            The minimum #of data services.
     * @param timeout
     *            The timeout (ms).
     * 
     * @return An array #of the {@link UUID}s of the {@link IDataService}s
     *         that have been discovered by <em>this</em> client. Note that at
     *         least <i>minDataServices</i> elements will be present in this
     *         array but that ALL discovered data services may be reported.
     * 
     * @throws IllegalArgumentException
     *             if <i>minDataServices</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>timeout</i> is non-positive.
     * @throws IllegalStateException
     *             if the client is not connected to the federation.
     * @throws InterruptedException
     *             if this thread is interrupted while awaiting the availability
     *             of the {@link MetadataService} or the specified #of
     *             {@link DataService}s.
     * @throws TimeoutException
     *             If a timeout occurs.
     */
    public UUID[] awaitServices(final int minDataServices, long timeout)
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
        IMetadataService metadataService = null;
        
        // updated each time through the loop.
        UUID[] dataServiceUUIDs = new UUID[0];
        
        while ((System.currentTimeMillis() - begin) < timeout) {
            
            ntries++;

            // verify that the client has/can get the metadata service.
            metadataService = getMetadataService();

            // find all data services.
            dataServiceUUIDs = getDataServiceUUIDs(0/*all*/);
//            // find at most that many data services.
//            UUID[] dataServiceUUIDs = getDataServiceUUIDs(minDataServices);
        
            if (metadataService == null
                    || dataServiceUUIDs.length < minDataServices) {
                
                if(INFO)
                log.info("Waiting : ntries="+ntries+", metadataService="
                        + (metadataService == null ? "not " : "")
                        + " found; #dataServices=" + dataServiceUUIDs.length
                        + " out of " + minDataServices + " required : "
                        + Arrays.toString(dataServiceUUIDs));
                
                Thread.sleep(interval);
                
                continue;
                
            }
            
            if (INFO)
                log.info("Have metadata service and " + dataServiceUUIDs.length
                        + " data services");
            
            return dataServiceUUIDs;
            
        }
        
        throw new TimeoutException("elapsed="
                + (System.currentTimeMillis() - begin) + "ms: metadataService="
                + (metadataService != null) + ", dataServices="
                + dataServiceUUIDs.length);
        
    }

}
