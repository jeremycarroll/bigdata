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
 * Created on Sep 13, 2009
 */

package com.bigdata.cache;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.BigdataStatics;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
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
 * A collection of hard reference hash maps backed by a single Least Recently
 * Used (LRU) ordering over entries. This is used to impose a shared LRU policy
 * on the cache for a set of {@link IRawStore}s.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * @param <K>
 *            The generic type of the key.
 * @param <V>
 *            The generic type of the value.
 * 
 *            FIXME This is a candidate implementation which HAS NOT been
 *            tested!!!! Write a concurrent stress test and put it under a
 *            profiler.
 * 
 * @todo ... Disallow anything where <V> does not extend
 *       {@link IDataRecordAccess} since we can not measure the bytesInMemory
 *       for those objects and hence the LRU eviction policy will not account
 *       for their memory footprint?
 */
public class HardReferenceGlobalLRU<K, V> implements IGlobalLRU<K,V> {

    /**
     * A canonicalizing mapping for per-{@link IRawStore} caches. Cache
     * instances MAY be retained when the backing store is closed. However,
     * cache instances will be lost if their {@link WeakReference} is cleared
     * and this will typically happen once the {@link IRawStore} is no longer
     * strongly referenced.
     */
    private final ConcurrentWeakValueCache<UUID, LRUCacheImpl<K,V>> cacheSet;

    /**
     * The counters for the shared LRU.
     */
    private final LRUCounters counters = new LRUCounters();
    
    /**
     * Lock used to gate access to changes in the LRU ordering. A fair policy is
     * NOT selected in the hopes that the cache will have higher throughput.
     */
    private final ReentrantLock lock = new ReentrantLock(false/* fair */);

    /**
     * The maximum bytes in memory for the LRU across all cache instances.
     */
    private final long maximumBytesInMemory;

    /**
     * The initial capacity for each cache instance.
     */
    private final int initialCacheCapacity;
    
    /**
     * The load factor for each cache instance.
     */
    private final float loadFactor;

    /**
     * The current LRU linked list size (the entry count) across all cache
     * instances.
     */
    private volatile int size = 0;
    
    /**
     * The entry which is first in the ordering (the
     * <em>least recently used</em>) and <code>null</code> iff the cache is
     * empty.
     */
    private volatile Entry<K, V> first = null;

    /**
     * The entry which is last in the ordering (the <em>most recently used</em>)
     * and <code>null</code> iff the cache is empty.
     */
    private volatile Entry<K, V> last = null;

    /**
     * 
     * @param maximumBytesInMemory
     *            The maximum bytes in memory for the cached records across all
     *            cache instances.
     * @param minimumCacheSetCapacity
     *            The #of per-{@link IRawStore} {@link ILRUCache} instances that
     *            will be maintained by hard references unless their cache is
     *            explicitly discarded.
     * @param initialCacheCapacity
     *            The initial capacity of each new cache instance.
     * @param loadFactor
     *            The load factor for the cache instances.
     */
    public HardReferenceGlobalLRU(final long maximumBytesInMemory,
            final int minimumCacheSetCapacity, final int initialCacheCapacity,
            final float loadFactor) {

        if (maximumBytesInMemory <= 0)
            throw new IllegalArgumentException();

        this.maximumBytesInMemory = maximumBytesInMemory;

        this.initialCacheCapacity = initialCacheCapacity;

        this.loadFactor = loadFactor;

        cacheSet = new ConcurrentWeakValueCache<UUID, LRUCacheImpl<K, V>>(
                minimumCacheSetCapacity);

    }

    /**
     * Canonicalizing mapping and factory for a per-{@link IRawStore} cache
     * instance.
     */
    public ILRUCache<K, V> getCache(final IRawStore store) {

        final UUID storeUUID = store.getUUID();

        LRUCacheImpl<K,V> cache = cacheSet.get(storeUUID);

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
                // SimpleFileStore,
                am = null;
                
            }

            cache = new LRUCacheImpl<K, V>(storeUUID, cls, am, file, this,
                    initialCacheCapacity, loadFactor);

            final LRUCacheImpl<K, V> oldVal = cacheSet.putIfAbsent(storeUUID,
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
    
    public void deleteCache(final IRawStore store) {

        if (store == null)
            throw new IllegalArgumentException();

        // remove cache from the cacheSet.
        final ILRUCache<K, V> cache = cacheSet.remove(store.getUUID());

        if (cache != null) {

            // if cache exists, the clear it.
            cache.clear();

        }
        
    }

    public void discardAllCaches() {
        
        lock.lock();
        try {
            
            cacheSet.clear();

            size = 0;

            first = last = null;
            
            counters.clear();
            
        } finally {

            lock.unlock();
            
        }
        
    }

    public CounterSet getCounterSet() {

        return counters.getCounterSet();
        
    }
    
    /**
     * Counters for the {@link HardReferenceGlobalLRU}.
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
         * The #of cache entries that have been evicted.
         */
        private final AtomicLong evictionCount = new AtomicLong();
        
        public void clear() {
            
            bytesOnDisk.set(0L);
            
            bytesInMemory.set(0L);
            
            evictionCount.set(0L);
                
        }
        
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

            counters.addCounter("bytesInMemory Percent Used",
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(((int) (10000 * bytesInMemory.get() / (double) HardReferenceGlobalLRU.this.maximumBytesInMemory)) / 100d);
                        }
                    });

            counters.addCounter("bytesInMemory Maximum Allowed",
                    new OneShotInstrument<Long>(
                            HardReferenceGlobalLRU.this.maximumBytesInMemory));

            counters.addCounter("LRU Size", new Instrument<Integer>() {
                @Override
                protected void sample() {
                    setValue(HardReferenceGlobalLRU.this.size);
                }
            });

            counters.addCounter("LRU Eviction Count", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(evictionCount.get());
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
                            final long tmp = HardReferenceGlobalLRU.this.size;
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
                            final long tmp = HardReferenceGlobalLRU.this.size;
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

        final Iterator<WeakReference<LRUCacheImpl<K, V>>> itr = cacheSet
                .iterator();

        while (itr.hasNext()) {

            final LRUCacheImpl<K, V> cache = itr.next().get();

            if (cache == null) {
                // weak reference was cleared.
                continue;
            }

            sb.append("\ncache: storeClass=" + cache.cls.getName() + ", size="
                    + cache.size() + ", file=" + cache.file);

            sb.append(cache.counters.getCounters());

        }

        return sb.toString();

    }

    /**
     * A (key,value) pair for insertion into an {@link LRUCacheImpl} with a
     * (prior,next) reference used to maintain a double-linked list across all
     * {@link LRUCacheImpl}s for a given {@link HardReferenceGlobalLRU}.
     * 
     * @version $Id$
     * @author thompsonbry
     */
    final private static class Entry<K, V> {

        private K k;

        private V v;

        private Entry<K, V> prior;

        private Entry<K, V> next;

        /** The owning cache for this entry. */
        private LRUCacheImpl<K,V> cache;
        
        /** The bytes in memory for this entry. */
        int bytesInMemory;
        
        /** The bytes on disk for this entry. */
        int bytesOnDisk;
        
        Entry() {
        }
        
        private void set(final LRUCacheImpl<K,V> cache, final K k, final V v) {

            this.k = k;
            
            this.v = v;

            this.cache = cache;
            
            if(v instanceof IDataRecordAccess) {

                bytesInMemory = ((IDataRecordAccess)v).data().len();
                
            } else {
                
                // Can not track w/o IDataRecord.
                bytesInMemory = 0;
                
            }

            if (cache.am != null) {

                bytesOnDisk = cache.am.getByteCount((Long) k);

            } else {
                
                // Can not track w/o IAddressManager.
                bytesOnDisk = 0;
                
            }

        }

        /**
         * Human readable representation used for debugging in test cases.
         */
        public String toString() {
            return "Entry{key=" + k + ",val=" + v + ",prior="
                    + (prior == null ? "N/A" : "" + prior.k) + ",next="
                    + (next == null ? "N/A" : "" + next.k) + "}";
        }

    } // class Entry

    /**
     * Add an Entry to the tail of the linked list (the MRU position).
     */
    private void addEntry(final Entry<K, V> e) {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (first == null) {
            first = e;
            last = e;
        } else {
            last.next = e;
            e.prior = last;
            last = e;
        }
        size++;
        counters.bytesInMemory.addAndGet(e.bytesInMemory);
        counters.bytesOnDisk.addAndGet(e.bytesOnDisk);
    }

    /**
     * Remove an {@link Entry} from linked list that maintains the LRU ordering.
     * The {@link Entry} is modified but not deleted. The key and value fields
     * on the {@link Entry} are not modified. The {@link #first} and
     * {@link #last} fields are updated as necessary. This method is used when
     * the LRU entry is being evicted and the {@link Entry} will be recycled
     * into the MRU position in the ordering. You must also remove the entry
     * under that key from the hash map.
     */
    private void removeEntry(final Entry<K, V> e) {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        final Entry<K, V> prior = e.prior;
        final Entry<K, V> next = e.next;
        if (e == first) {
            first = next;
        }
        if (last == e) {
            last = prior;
        }
        if (prior != null) {
            prior.next = next;
        }
        if (next != null) {
            next.prior = prior;
        }
        e.prior = null;
        e.next = null;
        size--;
        counters.bytesInMemory.addAndGet(-e.bytesInMemory);
        counters.bytesOnDisk.addAndGet(-e.bytesOnDisk);
    }

    /**
     * Move the entry to the end of the linked list (the MRU position).
     */
    private void touchEntry(final Entry<K, V> e) {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (last == e) {

            return;

        }

        removeEntry(e);

        addEntry(e);

    }

    /**
     * A hard reference hash map backed by a shared Least Recently Used (LRU)
     * ordering over entries.
     * <p>
     * Note: The synchronization policy is carried out at two levels. Each
     * instance of the {@link LRUCacheImpl} uses a backing {@link #map} which is
     * not thread-safe. Therefore, all methods which access the {@link #map}
     * MUST be synchronized on <i>this</i>. In addition, all methods which
     * update the access order MUST own the {@link HardReferenceGlobalLRU#lock} .
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     * @param <K>
     *            The generic type of the key.
     * @param <V>
     *            The generic type of the value.
     * 
     * @todo Synchronization review, especially if hotspots indicated by
     *       profiler. The individual maps could be concurrent (for concurrent
     *       readers per store). The global LRU updates MUST be consistent
     *       (maintain a well-structured double-linked list) but need not be
     *       synchronized (assuming they is any other way to make them
     *       consistent).
     */
    private static class LRUCacheImpl<K, V> implements ILRUCache<K, V> {

        /**
         * Counters for a {@link LRUCacheImpl} instance.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        private static class LRUCacheCounters {

            /**
             * The largest #of entries in the cache to date.
             */
            private int highTide = 0;

            /** The #of inserts into the cache. */
            private long ninserts = 0;

            /** The #of cache tests (get())). */
            private long ntests = 0;

            /**
             * The #of cache hits (get() returns non-<code>null</code>).
             */
            private long nsuccess = 0;

            /**
             * Reset the counters.
             */
            public void clear() {

                highTide = 0;
                
                ninserts = ntests = nsuccess = 0;
                
            }

            /**
             * A new {@link CounterSet} instance for this cache instance.
             */
            public CounterSet getCounters() {

                final CounterSet c = new CounterSet();

                c.addCounter("highTide", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(highTide);
                    }
                });

                c.addCounter("ninserts", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ninserts);
                    }
                });

                c.addCounter("ntests", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntests);
                    }
                });

                c.addCounter("nsuccess", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nsuccess);
                    }
                });

                c.addCounter("hitRatio", new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        final long tmp = ntests;
                        setValue(tmp == 0 ? 0 : (double) nsuccess / tmp);
                    }
                });

                return c;

            }

        }

        /**
         * Counters for this cache instance.
         */
        private final LRUCacheCounters counters = new LRUCacheCounters();

        /**
         * The {@link UUID} of the associated {@link IRawStore}.
         */
        private final UUID storeUUID;
        
        /**
         * The {@link IRawStore} implementation class.
         */
        private final Class<? extends IRawStore> cls;

        /**
         * An {@link IAddressManager} that can decode the record byte count from
         * the record address without causing the {@link IRawStore} reference to
         * be retained.
         */
        private final IAddressManager am;

        /**
         * The backing file for the {@link IRawStore} (if any).
         */
        private final File file;
        
        /**
         * The shared LRU.
         */
        private final HardReferenceGlobalLRU<K, V> globalLRU;

        /**
         * The hash map from keys to entries wrapping cached object references.
         */
        private final Map<K, HardReferenceGlobalLRU.Entry<K, V>> map;

        /**
         * Create an LRU cache with the specific initial capacity and load
         * factor.
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
         * @param initialCapacity
         *            The capacity of the cache (must be positive).
         * @param loadFactor
         *            The load factor for the internal hash table.
         */
        public LRUCacheImpl(final UUID storeUUID,
                final Class<? extends IRawStore> cls, final IAddressManager am,
                final File file, final HardReferenceGlobalLRU<K, V> lru,
                final int initialCapacity, final float loadFactor) {

            if (storeUUID == null)
                throw new IllegalArgumentException();
            
            if (cls == null)
                throw new IllegalArgumentException();
            
            // [am] MAY be null.
            
            if(am instanceof IRawStore) {
                
                /*
                 * This would cause the IRawStore to be retained by a hard
                 * reference!
                 */

                throw new AssertionError(am.getClass().getName()
                        + " implements " + IRawStore.class.getName());

            }

            // [file] MAY be null.
            
            if (lru == null)
                throw new IllegalArgumentException();

            this.storeUUID = storeUUID;
            
            this.cls = cls;

            this.am = am;
            
            this.file = file;
            
            this.globalLRU = lru;

            this.map = new HashMap<K, HardReferenceGlobalLRU.Entry<K, V>>(initialCapacity,
                    loadFactor);

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

        /**
         * Discards each entry in this cache and resets the statistics for this
         * cache.
         */
        public void clear() {

            globalLRU.lock.lock();
            
            try {
            
                final Iterator<Map.Entry<K, HardReferenceGlobalLRU.Entry<K, V>>> itr = map
                        .entrySet().iterator();
                
                while (itr.hasNext()) {
                
                    itr.next();
                    
                    itr.remove();
                    
                }

                counters.clear();
                
            } finally {
                
                globalLRU.lock.unlock();
                
            }
            
        }

        /**
         * The #of entries in the cache.
         */
        synchronized public int size() {

            return map.size();

        }

        synchronized public V putIfAbsent(final K k, final V v) {

            assert k != null;

            globalLRU.lock.lock();
            try {

                HardReferenceGlobalLRU.Entry<K, V> entry = map.get(k);

                if (entry != null) {

                    /*
                     * There is an existing entry under the key.
                     */

                    // Update entry ordering.
                    globalLRU.touchEntry(entry);

                    // Return the old value.
                    return entry.v;

                }

                /*
                 * There is no entry under that key.
                 */
                
                if (globalLRU.counters.bytesInMemory.get() >= globalLRU.maximumBytesInMemory) {

                    /*
                     * The global LRU is over capacity.
                     * 
                     * Purge the the LRU position until the cache is just under
                     * capacity.
                     * 
                     * Note: MUST use the same test (GTE) or [entry] can be null
                     * afterwards!
                     */

                    while (globalLRU.counters.bytesInMemory.get() >= globalLRU.maximumBytesInMemory) {

                        // entry in the LRU position.
                        entry = globalLRU.first;

                        // remove LRU entry from ordering.
                        globalLRU.removeEntry(entry);

                        // remove entry under that key from hash map for that
                        // store.
                        entry.cache.remove(entry.k);

                        globalLRU.counters.evictionCount.incrementAndGet();

                    }

                    /*
                     * Recycle the last cache entry that we purged.
                     */

                    assert entry != null;
                    
                    // set key and object on LRU entry.
                    entry.set(this, k, v);

                    // add entry into the hash map.
                    map.put(k, entry);

                    // add entry into MRU position in ordering.
                    globalLRU.addEntry(entry);

                    counters.ninserts++;

                    // return [null] since there was no entry under the key.
                    return null;

                }

                /*
                 * The map was not over capacity.
                 * 
                 * Create a new entry and link into the MRU position.
                 */

                entry = new HardReferenceGlobalLRU.Entry<K, V>();

                entry.set(this, k, v);

                map.put(k, entry);

                globalLRU.addEntry(entry);

                final int count = map.size();

                if (count > counters.highTide) {

                    counters.highTide = count;

                }

                counters.ninserts++;

                // return [null] since there was no entry under the key.
                return null;

            } finally {

                globalLRU.lock.unlock();

            }

        }

        synchronized public V get(final K key) {

            assert key != null;

            counters.ntests++;

            final HardReferenceGlobalLRU.Entry<K, V> entry = map.get(key);

            if (entry == null) {

                return null;

            }

            globalLRU.lock.lock();
            try {

                globalLRU.touchEntry(entry);
                
            } finally {
                
                globalLRU.lock.unlock();
                
            }

            counters.nsuccess++;

            return entry.v;

        }

        synchronized public V remove(final K key) {

            assert key != null;

            final HardReferenceGlobalLRU.Entry<K, V> entry = map.remove(key);

            if (entry == null)
                return null;

            globalLRU.lock.lock();
            try {
                globalLRU.removeEntry(entry);
            } finally {
                globalLRU.lock.unlock();
            }

            return entry.v;

        }

    }

}
