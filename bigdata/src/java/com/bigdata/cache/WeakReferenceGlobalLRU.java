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

import java.io.File;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Implementation based on a shared {@link HardReferenceQueue} and
 * {@link WeakReference}s in per-store {@link ConcurrentWeakValueCache}
 * instances.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME First, there is no need for the use of {@link WeakReference}s
 *          to manage the cache. Second, the very large ring buffer allows lots
 *          of duplicates and therefore can not make efficient use of memory (it
 *          can under fill the allocated buffer space, which makes configuration
 *          and more haphazard, and the eviction of an entry from the cache is
 *          somewhat unpredictable because the cache contains duplicates).
 * 
 */
public class WeakReferenceGlobalLRU implements IGlobalLRU<Long,Object> {

    /**
     * The performance counters for the global LRUs.
     */
    private final LRUCounters counters = new LRUCounters();

    /**
     * Use up to this much of the RAM available to the JVM to buffer data.
     */
    private final long maximumBytesInMemory;

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
    public WeakReferenceGlobalLRU(final long maximumMemoryFootprint,
            final int minimumCacheSetCapacity, final int queueCapacity,
            final int nscan, final int initialCapacity, final float loadFactor,
            final int concurrencyLevel) {

        if (BigdataStatics.debug)
            System.err.println("maximumMemoryFootprint="
                    + maximumMemoryFootprint + ", queueCapacity="
                    + queueCapacity + ", initialCapacity=" + initialCapacity);
        
        this.maximumBytesInMemory = maximumMemoryFootprint;

        this.initialCapacity = initialCapacity;
        
        this.loadFactor = loadFactor;

        this.concurrencyLevel = concurrencyLevel;

        globalLRU = new LRU<Object>(queueCapacity, nscan);

        cacheSet = new ConcurrentWeakValueCache<UUID, CacheImpl<Object>>(
                minimumCacheSetCapacity);

    }

//    /**
//     * Return the global LRU instance. This LRU enforces competition across all
//     * {@link IRawStore}s for buffer space (RAM).
//     */
//    public IHardReferenceQueue<Object> getGlobalLRU() {
//
//        return globalLRU;
//        
//    }

    /**
     * An canonicalizing factory for cache instances supporting random access to
     * decompressed {@link IDataRecord}s, higher-level data structures wrapping
     * those decompressed data records ({@link INodeData} and {@link ILeafData}
     * ), or objects deserialized from those {@link IDataRecord}s.
     * <p>
     * Note: This can not track bytesInMemory unless the weak referents
     * implement {@link IDataRecordAccess} since it relies on
     * {@link IDataRecordAccess#data()} to self-report the length of the
     * decompressed {@link IDataRecord}.
     * 
     * @return A cache for an immutable view of records in the store.
     * 
     * @see AbstractBTree#readNodeOrLeaf(long)
     * @see IndexSegmentStore#reopen()
     */
    public ILRUCache<Long, Object> getCache(final IRawStore store) {

        if (store == null)
            throw new IllegalArgumentException();
        
        final UUID storeUUID = store.getUUID();
        
        CacheImpl<Object> cache = cacheSet.get(storeUUID);

        if (cache == null) {

            final Class<? extends IRawStore> cls = store.getClass();
            final IAddressManager am;
            final File file = store.getFile();
            
            if (store instanceof AbstractJournal) {

                am = ((AbstractBufferStrategy) ((AbstractJournal) store)
                        .getBufferStrategy()).getAddressManager();

            } else if (store instanceof TemporaryRawStore) {

                // Avoid hard reference to the temporary store (clone's the
                // address manager instead).
                am = new WormAddressManager(((TemporaryRawStore) store)
                        .getOffsetBits());
                
            } else if (store instanceof AbstractRawWormStore) {
                
                am = ((AbstractRawStore) store).getAddressManager();
                
            } else {

                // @todo which cases come though here? SimpleMemoryStore,
                // SimpleFileStore, ...
                am = null;
                
            }
            
            cache = new CacheImpl<Object>(storeUUID, cls, am, file, globalLRU,
                    initialCapacity, loadFactor, concurrencyLevel, true/* removeClearedReferences */);

            final CacheImpl<Object> oldVal = cacheSet.putIfAbsent(storeUUID,
                    cache);

            if (oldVal == null) {

//                if (BigdataStatics.debug)
//                    System.err.println("New store: " + store + " : file="
//                            + store.getFile());
                
            } else {

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
     * Discard all hard reference in the {@link #getGlobalLRU()}. The per-store
     * caches are not deleted, but they will empty as their weak references are
     * cleared by the JVM. Depending on the garbage collector, the JVM may delay
     * clearing weak references for objects in the old generation until the next
     * full GC.
     */
    public void discardAllCaches() {

        globalLRU.clear(true/* clearRefs */);
        
    }
    
    public CounterSet getCounterSet() {

        return counters.getCounterSet();

    }

    /**
     * Extended to update the {@link LRUCounters}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <V>
     */
    private class CacheImpl<V> implements ILRUCache<Long,V> {

        private final UUID storeUUID;
        private final Class<? extends IRawStore> cls;
        private final IAddressManager am;
        private final File file;
        private final ConcurrentWeakValueCache<Long,V> map;

        /**
         * Uses the specified values.
         * 
         * @param storeUUID
         *            The {@link UUID} of the associated {@link IRawStore}.
         * @param cls
         *            The {@link IRawStore} implementation class.
         * @param am
         *            The <em>delegate</em> {@link IAddressManager} associated
         *            with the {@link IRawStore} whose records are being cached.
         *            This is used to track the bytesOnDisk buffered by the
         *            cache using {@link IAddressManager#getByteCount(long)}. DO
         *            NOT provide a reference to an {@link IRawStore} here as
         *            that will cause the {@link IRawStore} to be retained by a
         *            hard reference!
         * @param file
         *            The backing file (may be <code>null</code>).
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
        public CacheImpl(final UUID storeUUID,
                final Class<? extends IRawStore> cls, final IAddressManager am,
                final File file, final IHardReferenceQueue<V> queue,
                final int initialCapacity, final float loadFactor,
                final int concurrencyLevel,
                final boolean removeClearedReferences) {

            if (storeUUID == null)
                throw new IllegalArgumentException();

            if (cls == null)
                throw new IllegalArgumentException();

            this.storeUUID = storeUUID;
            
            if(am instanceof IRawStore) {
 
                /*
                 * This would cause the IRawStore to be retained by a hard
                 * reference!
                 */

                throw new AssertionError(am.getClass().getName()
                        + " implements " + IRawStore.class.getName());

            }
            
            this.cls = cls;
            
            this.am = am;

            this.file = file;

            this.map = new InnerCacheImpl(queue,
                    initialCapacity, loadFactor, concurrencyLevel,
                    removeClearedReferences);

        }

        public IAddressManager getAddressManager() {
            return am;
        }

        public File getStoreFile() {
            return file;
        }

        public UUID getStoreUUID() {
            return storeUUID;
        }

        public int size() {
            return map.size();
        }
        
        public V get(Long k) {
            return map.get(k);
        }

        public V putIfAbsent(Long k, V v) {
            return map.putIfAbsent(k, v);
        }

        public V remove(Long k) {
            return map.remove(k);
        }

        public void clear() {
            map.clear();
        }

        /**
         * Adds logic to track bytesInMemory and bytesOnDisk.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private class InnerCacheImpl extends ConcurrentWeakValueCache<Long, V> {
            
            public InnerCacheImpl(final IHardReferenceQueue<V> queue,
                final int initialCapacity, final float loadFactor,
                final int concurrencyLevel,
                final boolean removeClearedReferences) {

                super(queue, initialCapacity, loadFactor, concurrencyLevel,
                        removeClearedReferences);
                
            }
            
            /**
             * Overridden to update {@link LRUCounters#bytesInMemory} and
             * {@link LRUCounters#bytesOnDisk}.
             */
            @Override
            protected WeakReference<V> removeMapEntry(final Long k) {

                final WeakRef2 weakRef = (WeakRef2) super.removeMapEntry(k);

                counters.bytesInMemory.addAndGet(-weakRef.bytesInMemory);
                
                counters.bytesOnDisk.addAndGet(-weakRef.bytesOnDisk);
                
                counters.lruDistinctCount.decrementAndGet();

                return weakRef;

            }

            @Override
            protected void didUpdate(final Long k, final WeakReference<V> newRef,
                    final WeakReference<V> oldRef) {

//              super.didUpdate(k, newRef, oldRef);

                final Object newVal = newRef.get();
                
                if(!(newVal instanceof IDataRecordAccess)) {

                    return;
                    
                }

                // add in the decompressed byte length of the new data record.
                long deltaBytesInMemory = ((WeakRef2) newRef).bytesInMemory;
                
                // add in the bytes on disk of the new data record.
                long deltaBytesOnDisk = ((WeakRef2) newRef).bytesOnDisk;

                if (oldRef != null) {

                    // subtract out the decompressed byte length of the old data record.
                    deltaBytesInMemory -= ((WeakRef2) oldRef).bytesInMemory;

                    // subtract out the bytes on disk of the old data record.
                    deltaBytesOnDisk -= ((WeakRef2) oldRef).bytesOnDisk;

                } else {
                    
                    counters.lruDistinctCount.incrementAndGet();

                }

                // adjust counters.
                
                if (deltaBytesInMemory != 0)
                    counters.bytesInMemory.addAndGet(deltaBytesInMemory);
                
                if (deltaBytesOnDisk != 0)
                    counters.bytesOnDisk.addAndGet(deltaBytesOnDisk);

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
        
    }

    /**
     * Thread-safe {@link IHardReferenceQueue} which clears the oldest
     * references from tail of the circular buffer when
     * {@link LRUCounters#bytesInMemory} exceeds the
     * {@link WeakReferenceGlobalLRU#maximumBytesInMemory}.
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
         * Evicts up to N=10 elements from the tail if the bytesInMemory is over
         * the desired memory footprint.
         * 
         * @todo configuration parameter for N and tune.  probably N=2 is Ok.
         */
        @Override
        protected void beforeOffer(final V v) {

            if (counters.bytesInMemory.get() > maximumBytesInMemory) {

                for (int i = 0; i < 10 && !isEmpty(); i++) {

                    // evict the tail.
                    evict();
                    
                }

            }

        }
      
    }

    /**
     * Counters for the global {@link WeakReferenceGlobalLRU}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class LRUCounters {

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
         * {@link #lruDistinctCount} is the #of distinct records retained by the
         * canonicalizing weak value cache for decompressed records.
         */
        private final AtomicLong lruDistinctCount = new AtomicLong();

        public CounterSet getCounterSet() {

            final CounterSet counters = new CounterSet();

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

            counters.addCounter("bytesInMemory Percent Used", new Instrument<Double>() {
                @Override
                protected void sample() {
                    setValue(((int) (10000 * bytesInMemory.get() / (double) WeakReferenceGlobalLRU.this.maximumBytesInMemory)) / 100d);
                }
            });

            counters.addCounter("bytesInMemory Maximum Allowed",
                    new OneShotInstrument<Long>(
                            WeakReferenceGlobalLRU.this.maximumBytesInMemory));

            counters.addCounter("LRU Capacity", new Instrument<Integer>() {
                @Override
                protected void sample() {
                    setValue(WeakReferenceGlobalLRU.this.globalLRU.capacity());
                }
            });

            counters.addCounter("LRU Size", new Instrument<Integer>() {
                @Override
                protected void sample() {
                    setValue(WeakReferenceGlobalLRU.this.globalLRU.size());
                }
            });

            counters.addCounter("LRU Distinct", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(lruDistinctCount.get());
                }
            });

            counters.addCounter("LRU Percent Used", new Instrument<Double>() {
                @Override
                protected void sample() {
//                    setValue(((int) (10000 * WeakReferenceGlobalLRU.this.globalLRU
//                            .size() / (double) WeakReferenceGlobalLRU.this.globalLRU
//                            .capacity())) / 100d);
                    setValue(((int) (10000L * WeakReferenceGlobalLRU.this.globalLRU
                            .size() / (double) WeakReferenceGlobalLRU.this.globalLRU
                            .capacity())) / 100d);
                }
            });

            counters.addCounter("LRU Percent Distinct",
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(((int) (10000 * lruDistinctCount.get() / (double) WeakReferenceGlobalLRU.this.globalLRU
                                    .capacity())) / 100d);
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

            /*
             * The average bytes in memory per buffered record.
             */
            counters.addCounter("averageRecordSizeInMemory",
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            final long tmp = lruDistinctCount.get();
                            if (tmp == 0) {
                                setValue(0);
                                return;
                            }
                            setValue((int) (bytesInMemory.get() / tmp));
                        }
                    });

            /*
             * The average bytes on disk per buffered record.
             */
            counters.addCounter("averageRecordSizeOnDisk",
                    new Instrument<Integer>() {
                        @Override
                        protected void sample() {
                            final long tmp = lruDistinctCount.get();
                            if (tmp == 0) {
                                setValue(0);
                                return;
                            }
                            setValue((int) (bytesOnDisk.get() / tmp));
                        }
            });

            return counters;

        }
        
        public String toString() {
            
            return getCounterSet().toString();
            
        }

    }

    public String toString() {
        
        final String t = getCounterSet().toString();
        
        if(!BigdataStatics.debug) {
            
            return t;
            
        }
        
        final StringBuilder sb = new StringBuilder();

        sb.append(t);

        final Iterator<WeakReference<CacheImpl<Object>>> itr = cacheSet
                .iterator();

        while (itr.hasNext()) {

            final CacheImpl<Object> cache = itr.next().get();

            if (cache == null) {
                // weak reference was cleared.
                continue;
            }

            sb.append("\ncache: storeClass=" + cache.cls.getName() + ", size="
                    + cache.size() + ", file=" + cache.file);

        }
            
        return sb.toString();
        
    }

}
