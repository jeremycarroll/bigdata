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
 * Created on Sep 8, 2009
 */

package com.bigdata.cache;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import net.jini.config.Configuration;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BloomFilter;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.jini.JiniClient;
import com.sun.xml.internal.fastinfoset.sax.Properties;

/**
 * Factory for per-{@link IRawStore} caches backed by a global LRU allowing
 * competition for buffer space across all requests for decompressed records,
 * including B+Tree {@link INodeData} and {@link ILeafData} objects.
 * <p>
 * The vast majority of all data in the stores are B+Tree {@link INodeData} and
 * {@link ILeafData} records. This factory provides cache instances which can
 * dramatically reduce read IO for B+Trees, and thus for the entire database.
 * This also serves as the "leafCache" for the {@link IndexSegment}, which uses
 * linked-leaf navigation for {@link ITupleIterator} and {@link ITupleCursor}s
 * and does not otherwise buffer leaves when performing key range scans (versus
 * top-down navigation).
 * <p>
 * There is one cache per store. All cache instances are backed by a single
 * <em>global</em> LRU based on a {@link RingBuffer}. Cache entries will be
 * cleared by the garbage collector once they are no longer in use and have
 * fallen off of the global LRU. A global LRU so that all cache instances
 * compete for the same memory resources.
 * <p>
 * Each per-store cache provides a canonicalizing mapping from the {@link Long}
 * address of a record to a {@link WeakReference} value. The referent of the
 * {@link WeakReference} may be an {@link IDataRecord}, {@link INodeData},
 * {@link ILeafData}, or other object whose persistent state was coded by (or
 * serialized by) the record having that address.
 * <p>
 * Note: The individual cache instances for each {@link IRawStore} reclaim JVM
 * heap space as the {@link WeakReference} values are cleared. The set of such
 * instances is also a {@link ConcurrentWeakValueCache} with a backing LRU so we
 * can hold onto the canonicalizing mappings for closed stores which might be
 * reopened.
 * <p>
 * Note: The global LRU capacity is a somewhat tricky configuration parameter.
 * If the configured value is too small then the cache will not be able to force
 * retention of up to the configured {@link #maximumMemoryFootprint}. More data
 * may still be buffered, but only if retained by hard references in the higher
 * level data structures.
 * <p>
 * Note: The global LRU may have impact on GC of the old generation since there
 * will be a tendency of the backing byte[]s to be held long enough to become
 * tenured. Newer incremental garbage collections are designed to address this
 * problem.
 * <p>
 * Note: While caching compressed records would have a smaller memory footprint,
 * this class provides for caching decompressed records since high-level data
 * structures (such as the B+Trees) do not retain references to the compressed
 * data records. Therefore a compressed data record cache based on weak
 * reference semantics for the compressed {@link ByteBuffer}s would be of little
 * utility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Look into the memory pool threshold notification mechanism. See
 *       {@link ManagementFactory#getMemoryPoolMXBeans()} and
 *       {@link MemoryPoolMXBean}. TonyP suggests that tracking the old
 *       generation occupancy may be a better metric (more stable). The tricky
 *       part is to identify which pool(s?) correspond(s) to the old generation.
 *       Once that is done, the idea is to set a notification threshold using
 *       {@link MemoryPoolMXBean#setUsageThreshold(long)} and to only clear
 *       references from the tail of the global LRU when we have exceeded that
 *       threshold. Reading the javadoc, it seems that threshold notification
 *       would probably come after a (full) GC. The goal would have to be
 *       something like reducing the bytesInMemory to some percentage of its
 *       value at threshold notification (e.g., 80%). Since we can't directly
 *       control that and the feedback from the JVM is only at full GC
 *       intervals, we need to simply discard some percentage of the references
 *       from the tail of the global LRU. We could actually adjust the desired
 *       #of references on the LRU if that metric appears to be relatively
 *       stable. However, note that the average #of bytes per reference and the
 *       average #of instances of a reference on the LRU are not necessarily
 *       stable values. We could also examine the recordCount (total cache size
 *       across all caches). If weak references are cleared on an ongoing basis
 *       rather than during the full GC mark phase, then that will be very close
 *       to the real hard reference count.
 * 
 * @todo Configure from the {@link JiniClient} and {@link Journal} properties.
 *       Either pass in the {@link Configuration} object or the
 *       {@link Properties}? Alternatively, define a single static instance and
 *       reference from where ever we need it. E.g., INSTANCE or getInstance()
 *       using a singleton pattern.
 * 
 * @todo While we are using high concurrency maps for the caches, we should also
 *       pay attention to the cost of resizing the backing maps. E.g., give them
 *       a large initial size based on some expectation about the size of the
 *       data records. There should also be explicit profiling of throughput vs
 *       latency for the backing hash map implementation for the caches.
 * 
 * @todo does it make sense to both buffer the index segment nodes region and
 *       buffer the nodes and leaves? [buffering the nodes region is an option.]
 * 
 * @todo Note that a r/w store will require an approach in which addresses are
 *       PURGED from the store's cache during the commit protocol. That might be
 *       handled at the tx layer.
 */
public class LRUNexus {

    /**
     * The performance counters for the global LRUs.
     */
    private final LRUCounters counters = new LRUCounters();

    /**
     * Use up to this much of the RAM available to the JVM to buffer data.
     */
    private final long maximumMemoryFootprint;

    /**
     * The initial capacity of the per-store hash maps.
     */
    private final int initialCapacity;
    
    /**
     * The load factor for the per-store hash maps.
     */
    private final float loadFactor;

    /**
     * The load factor for the per-store hash maps.
     */
    private final int concurrencyLevel;

    /**
     * Global ring buffer used by the per-store caches. This enforces
     * competition for buffer space (RAM) across all higher-level data
     * structures (primarily {@link AbstractBTree}s) across all backing
     * {@link IRawStore} instances.
     */
    private final IHardReferenceQueue<Object> globalLRU;

    /**
     * A canonicalizing mapping for per-{@link IRawStore} caches. Cache
     * instances MAY be retained when the backing store is closed. However,
     * cache instances will be lost if their {@link WeakReference} is cleared
     * and this will typically happen once the {@link IRawStore} is no longer
     * strongly referenced.
     */
    private final ConcurrentWeakValueCache<UUID, CacheImpl<Object>> cacheSet;

    /**
     * Return the default maximum memory footprint, which is
     * {@link #percentMaximumMemory} of the maximum JVM heap. This limit is a
     * bit conservative. It is designed to leave some room for application data
     * objects and GC. You may be able to get away with significantly more on
     * machines with large RAM.
     */
    static long getMaximumMemoryFootprint() {
        
        return (long) (Runtime.getRuntime().maxMemory() * percentMaximumMemory);
        
    }

    /**
     * The percentage of the JVM heap to use for bigdata buffers as managed by
     * this class.
     */
    static final float percentMaximumMemory = .3f;

    /**
     * The average record size, which is an input to the nominal capacity of the
     * {@link #globalLRU}.
     * 
     * @todo Estimate the average record size based on real data (counters).
     *       Actually, 1024 is not bad for the RDF DB with a branching factor of
     *       32. This is a pretty common value for the byte length of the
     *       decompressed node and leaf data records.  It will be much larger
     *       for {@link IndexSegment}s of course so scale-out will need to
     *       plan for a mixture of records on journals and index segments in
     *       memory.
     */
    static final int baseAverageRecordSize = 1024;
    
    /**
     * Return the default {@link LRU} capacity based on the maximum
     * memory footprint and some heuristics.
     * 
     * @param maximumMemoryFootprint
     *            The maximum memory footprint.
     */
    static int getQueueCapacity(final long maximumMemoryFootprint) {

        final int averageRecordSize = (int) (baseAverageRecordSize * (Integer
                .valueOf(IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR) / 32.));

        // target capacity for that expected record size.
        final long maximumQueueCapacityEstimate = maximumMemoryFootprint / averageRecordSize;

        // -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC
        if(BigdataStatics.debug)
            System.err.println(//
                "defaultBranchingFactor="
                + IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR//
                + ", averageRecordSize=" + averageRecordSize//
                + "\nmaxMemory="+Runtime.getRuntime().maxMemory()//
                + ", percentMaximumMemory="+percentMaximumMemory//
                + ", maximumQueueCapacityEstimate=" + maximumQueueCapacityEstimate//
                );
        
        if (true)
            return (int) Math.min(Integer.MAX_VALUE, maximumQueueCapacityEstimate);

        if (maximumMemoryFootprint < Bytes.gigabyte * 2) {

            // capacity is no more than X
            return (int) Math.min(maximumQueueCapacityEstimate, 200000/* 200k */);

        } else {

            // capacity is no more than Y
            return (int) Math.min(maximumQueueCapacityEstimate, 1000000/* 1M */);

        }

    }

    /**
     * Extended to update the {@link LRUCounters}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <V>
     */
    private class CacheImpl<V> extends ConcurrentWeakValueCache<Long, V> {

        private final IAddressManager am;

        /**
         * Uses the specified values.
         * 
         * @param am
         *            The <em>delegate</em> {@link IAddressManager} associated
         *            with the {@link IRawStore} whose records are being cached.
         *            This is used to track the bytesOnDisk buffered by the
         *            cache using {@link IAddressManager#getByteCount(long)}. DO
         *            NOT provide a reference to an {@link IRawStore} here as
         *            that will cause the {@link IRawStore} to be retained by a
         *            hard reference!
         * @param queue
         *            The {@link IHardReferenceQueue} (optional).
         * @param initialCapacity
         *            The initial capacity of the backing hash map.
         * @param loadFactor
         *            The load factor.
         * @param concurrencyLevel
         *            The concurrency level.
         * @param removeClearedReferences
         *            When <code>true</code> the cache will remove entries for
         *            cleared references. When <code>false</code> those entries
         *            will remain in the cache.
         */
        public CacheImpl(final IAddressManager am, final IHardReferenceQueue<V> queue,
                final int initialCapacity, final float loadFactor,
                final int concurrencyLevel,
                final boolean removeClearedReferences) {

            super(queue, initialCapacity, loadFactor, concurrencyLevel,
                    removeClearedReferences);

            if(am instanceof IRawStore) {

                /*
                 * This would cause the IRawStore to be retained by a hard
                 * reference!
                 */

                throw new AssertionError(am.getClass().getName()
                        + " implements " + IRawStore.class.getName());

            }
            
            this.am = am;
            
        }
        
        /**
         * Overridden to update {@link LRUCounters#bytesInMemory} and
         * {@link LRUCounters#bytesOnDisk}.
         */
        @Override
        protected WeakReference<V> removeMapEntry(final Long k) {

            final WeakRef2 weakRef = (WeakRef2) super.removeMapEntry(k);

            counters.bytesInMemory.addAndGet(-weakRef.bytesInMemory);
            counters.bytesInMemory.addAndGet(-weakRef.bytesOnDisk);
            counters.recordCount.decrementAndGet();
            
            return weakRef;
            
        }

        @Override
        protected void didUpdate(final Long k, final WeakReference<V> newRef,
                final WeakReference<V> oldRef) {

//          super.didUpdate(k, newRef, oldRef);

            final Object newVal = newRef.get();
            
            if(!(newVal instanceof IDataRecordAccess)) {

                return;
                
            }

            // add in the byte length of the new data record.
            long delta = ((IDataRecordAccess)newVal).data().len();

            if (oldRef != null) {

                // subtract out the byte length of the old data record.
                delta -= ((WeakRef2) oldRef).bytesInMemory;

            }

            // adjust the counter.
            counters.bytesInMemory.addAndGet(delta);
            
            if (oldRef == null) {

                counters.recordCount.incrementAndGet();

            }
            
        }
        
        /**
         * Overridden to allocate {@link WeakRef2} instances which track the
         * byte length of the data record.
         */
        @Override
        protected WeakReference<V> newWeakRef(final Long k, final V v,
                final ReferenceQueue<V> referenceQueue) {

            return new WeakRef2(k, v, referenceQueue);

        }

        /**
         * Extended to note the byte count of the backing data record so that we
         * have that information on hand after the {@link WeakReference} has been
         * cleared.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         * @param <K>
         * @param <V>
         */
        private class WeakRef2 extends ConcurrentWeakValueCache.WeakRef<Long, V> {

            /**
             * The length of the compressed data record in bytes.
             */
            public final int bytesOnDisk;
            
            /**
             * The length of the backing data record in bytes. 
             */
            public final int bytesInMemory;
            
            /**
             * @param k
             * @param v
             * @param queue
             */
            public WeakRef2(final Long k, final V v, final ReferenceQueue<V> queue) {

                super(k, v, queue);

                if(v instanceof IDataRecordAccess) {

                    bytesInMemory = ((IDataRecordAccess)v).data().len();
                    
                } else {
                    
                    // Can not track w/o IDataRecord.
                    bytesInMemory = 0;
                    
                }

                if (am != null) {

                    bytesOnDisk = am.getByteCount((Long) k);
   
                } else {
                    
                    // Can not track w/o IAddressManager.
                    bytesOnDisk = 0;
                    
                }

            }

        }
        
    }

    /**
     * Thread-safe {@link IHardReferenceQueue} which clears the oldest
     * references from tail of the circular buffer when
     * {@link LRUCounters#bytesInMemory} exceeds the
     * {@link LRUNexus#maximumMemoryFootprint}.
     * <p>
     * There IS NO direct coupling between clearing references from the tail and
     * weak references being cleared. A reference can exist at multiple spots on
     * the queue, so clearing evicting the reference from the tail does not mean
     * that the reference is no longer on the queue. Likewise, nothing requires
     * the JVM to clear {@link WeakReference}s for the reference in a timely
     * manner even if there are no other occurrences of the reference on the
     * queue. This situation can be made much worse if the objects have been
     * tenured into the old generation since a full mark-and-sweep will be
     * required to reclaim the space allocated to those objects. For this
     * reason, an incremental garbage collector may significantly out-perform
     * even parallel mark-and-sweep for the old generation with large heaps.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <V>
     */
    private class LRU<V> extends SynchronizedHardReferenceQueue<V> {

        /**
         * @param capacity
         * @param nscan
         */
        public LRU(final int capacity, final int nscan) {

            super(null/* listener */, capacity, nscan);

        }

        /**
         * Evicts up to 10 elements from the tail if the bytesInMemory is over
         * the desired memory footprint.
         */
        @Override
        protected void beforeOffer(final V v) {

            if (counters.bytesInMemory.get() > maximumMemoryFootprint) {

                for (int i = 0; i < 10 && !isEmpty(); i++) {

                    // evict the tail.
                    evict();
                    
                }

            }

        }
      
    }

    /**
     * Global instance with default configuration.
     * 
     * @todo This is hardwired to {@link #INSTANCE} right now in
     *       {@link AbstractBTree}. That might be Ok since it imposes a JVM wide
     *       constraint on the memory used to buffer for stores and stores are
     *       identified by their UUIDs. Regardless, all caching for all stores
     *       MUST use the same instance. This includes the
     *       {@link TemporaryStore}, {@link IndexSegmentStore}, {@link Journal},
     *       {@link ManagedJournal}, etc.
     */
    public static final LRUNexus INSTANCE = new LRUNexus();

    /**
     * Constructor with reasonable defaults based on {@link Runtime#maxMemory()}.
     * 
     * @todo tune parameters.
     * 
     * @todo larger initialCapacity.
     */
    public LRUNexus() {

        this(//
                getMaximumMemoryFootprint(),// maximumMemoryFootprint
                5,    // minimumCacheSetCapacity
                getQueueCapacity(getMaximumMemoryFootprint()),// queueCapacity
                20,   // nscan
                16,   // initial capacity (the Java default).
                .75f, // loadFactor (the Java default)
                16    // concurrencyLevel (the Java default)
        );

    }

    /**
     * Constructor with caller specified parameters.
     * 
     * @param maximumMemoryFootprint
     *            The maximum in-memory footprint for the buffered
     *            {@link IDataRecordAccess} objects.
     * @param minimumCacheSetCapacity
     *            The #of caches for which we will force retention. E.g., a
     *            value of N implies that hard references will be retained to
     *            the LRU cache for N stores. In practice, stores will typically
     *            hold a hard reference to their LRU cache instance so many more
     *            LRU cache instances MAY be retained.
     * @param queueCapacity
     *            The {@link IHardReferenceQueue} capacity.
     * @param nscan
     *            The #of entries on the {@link IHardReferenceQueue} to scan for
     *            a match before adding a reference.
     * @param initialCapacity
     *            The initial capacity of the per-store hash maps.
     * @param loadFactor
     *            The load factor for the per store hash maps.
     * @param concurrencyLevel
     *            The concurrency level of the per-store hash maps.
     */
    public LRUNexus(final long maximumMemoryFootprint,
            final int minimumCacheSetCapacity, final int queueCapacity,
            final int nscan, final int initialCapacity, final float loadFactor,
            final int concurrencyLevel) {

        if (BigdataStatics.debug)
            System.err.println("maximumMemoryFootprint="
                    + maximumMemoryFootprint + ", queueCapacity="
                    + queueCapacity + ", initialCapacity=" + initialCapacity);
        
        this.maximumMemoryFootprint = maximumMemoryFootprint;

        this.initialCapacity = initialCapacity;
        
        this.loadFactor = loadFactor;

        this.concurrencyLevel = concurrencyLevel;

        globalLRU = new LRU<Object>(queueCapacity, nscan);

        cacheSet = new ConcurrentWeakValueCache<UUID, CacheImpl<Object>>(
                minimumCacheSetCapacity);

    }

    /**
     * Return the global LRU instance. This LRU enforces competition across all
     * {@link IRawStore}s for buffer space (RAM).
     * 
     * FIXME Simplify the integration pattern for use of a cache. You have to
     * follow a "get()" then if miss, read+wrap, then putIfAbsent(). You MUST
     * also "touch" the object on the global LRU on access to keep it "live".
     * Finally, for write through, you must insert the object into the cache.
     * You DO NOT need to "touch" the object on a cache hit or when inserting it
     * into the cache since the cache is backed by the global LRU and the
     * {@link ConcurrentWeakValueCache} will automatically "touch" the object on
     * the LRU. These semantics could be made more transparent if we define an
     * ICachedStore interface. However, the caller would need to pass in the
     * functor to create the appropriate object on get() and would need to
     * handle the "touch" protocol as well.
     */
    public IHardReferenceQueue<Object> getGlobalLRU() {

        return globalLRU;
        
    }
    
    /**
     * An canonicalizing factory for cache instances supporting random access to
     * decompressed {@link IDataRecord}s, higher-level data structures wrapping
     * those decompressed data records ({@link INodeData} and {@link ILeafData}
     * ), or objects deserialized from those {@link IDataRecord}s.
     * 
     * @todo Caches are NOT used transparently (but that might change). They
     *       MUST be integrated into the higher level access structures.
     *       Additional record types which SHOULD be cached include
     *       {@link Checkpoint}s, {@link IndexMetadata} , {@link BloomFilter}s,
     *       etc.
     * 
     * @todo This can not track bytesInMemory unless the weak referents
     *       implement {@link IDataRecordAccess} since it relies on
     *       {@link IDataRecordAccess#data()} to self-report the length of the
     *       decompressed {@link IDataRecord}.
     * 
     * @see AbstractBTree#readNodeOrLeaf(long)
     * @see IndexSegmentStore#reopen()
     */
    public ConcurrentWeakValueCache<Long, Object> getCache(final IRawStore store) {

        if (store == null)
            throw new IllegalArgumentException();
        
        final UUID storeUUID = store.getUUID();
        
        CacheImpl<Object> cache = cacheSet.get(storeUUID);

        if (cache == null) {

            final IAddressManager am = (store instanceof AbstractRawWormStore ? ((AbstractRawStore) store)
                    .getAddressManager()
                    : null);
            
            cache = new CacheImpl<Object>(am, globalLRU,
                    initialCapacity, loadFactor, concurrencyLevel, true/* removeClearedReferences */);

            CacheImpl<Object> oldVal = cacheSet.putIfAbsent(storeUUID, cache);

            if (oldVal != null) {

                // concurrent insert.
                cache = oldVal;

            }

        }

        return cache;

    }

    /**
     * Remove the cache for the {@link IRawStore} from the set of caches
     * maintained by this class and clear any entries in that cache. This method
     * SHOULD be used when the persistent resources for the store are deleted.
     * It SHOULD NOT be used if a store is simply closed in a context when the
     * store COULD be re-opened. In such cases, the cache for that store will be
     * automatically released after it has become only weakly reachable.
     * 
     * @param store
     *            The store.
     * 
     * @see IRawStore#destroy()
     * @see IRawStore#deleteResources()
     */
    public void deleteCache(final IRawStore store) {

        if (store == null)
            throw new IllegalArgumentException();
        
        // remove cache from the cacheSet.
        final CacheImpl<Object> cache = cacheSet.remove(store.getUUID());

        if(cache != null) {
            
            // if cache exists, the clear it.
            cache.clear();
            
        }
        
    }

    /**
     * Discard all hard reference in the {@link #getGlobalLRU()}. This may be
     * used if all bigdata instances in the JVM are closed, but SHOULD NOT be
     * invoked if you are just closing some {@link IRawStore}. The per-store
     * caches are not deleted, but they will empty as their weak references are
     * cleared by the JVM. Note that, depending on the garbage collector, the
     * JVM may delay clearing weak references for objects in the old generation
     * until the next full GC.
     */
    public void discardAllCaches() {

        globalLRU.clear(true/* clearRefs */);
        
    }
    
    /** The counters for the global LRU. */
    public LRUCounters getCounters() {

        return counters;

    }

    /**
     * Counters for the global {@link LRUNexus}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public class LRUCounters {

        /**
         * {@link #bytesOnDisk} is the sum of the compressed storage on the disk
         * for the buffered data records.
         */
        private final AtomicLong bytesOnDisk = new AtomicLong();

        /**
         * {@link #bytesInMemory} is the sum of the decompressed byte[] lengths.
         * In fact, the memory footprint is always larger than bytesInMemory.
         * The ratio of bytesOnDisk to bytesInMemory reflects the degree of
         * "active" compression.
         */
        private final AtomicLong bytesInMemory = new AtomicLong();

        /**
         * {@link #recordCount} is the #of distinct records retained by the
         * canonicalizing weak value cache for decompressed records.
         */
        private final AtomicLong recordCount = new AtomicLong();

        public CounterSet getCounters() {

            final CounterSet counters = new CounterSet();

            counters.addCounter("maximumMemoryFootprint",
                    new OneShotInstrument<Long>(
                            LRUNexus.this.maximumMemoryFootprint));

            counters.addCounter("bytesOnDisk", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(bytesOnDisk.get());
                }
            });

            counters.addCounter("bytesInMemory", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(bytesInMemory.get());
                }
            });

            counters.addCounter("recordCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(recordCount.get());
                }
            });
            
            /*
             * The #of stores whose nodes and leaves are being cached.
             */
            counters.addCounter("cacheCount", new Instrument<Integer>() {
                @Override
                protected void sample() {
                    setValue(cacheSet.size());
                }
            });

            return counters;

        }

    }

}
