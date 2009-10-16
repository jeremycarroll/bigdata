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

package com.bigdata;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.cache.HardReferenceGlobalLRU;
import com.bigdata.cache.HardReferenceGlobalLRURecycler;
import com.bigdata.cache.IGlobalLRU;
import com.bigdata.cache.StoreAndAddressLRUCache;
import com.bigdata.cache.WeakReferenceGlobalLRU;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * Static singleton factory.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME LRUNexus : writes MUST must be "isolated" until the commit.
 *          Isolated indices MUST have their own cache backed by the shared LRU
 *          (actually, they are on the shared temporary store so that helps).
 *          Unisolated indices SHOULD have their own cache backed by the shared
 *          LRU. At commit, any records in the "isolated" cache for a B+Tree
 *          should be putAll() onto the unisolated cache for the backing store.
 *          This way, we do not need to do anything if there is an abort().
 *          <p>
 *          There are two quick fixes: (1) Disable the Global LRU; and (2)
 *          discard the cache if there is an abort on a store. The latter is
 *          pretty easy since we only have one store with abort semantics, which
 *          is the {@link AbstractJournal}, so that is how this is being handled
 *          right now by {@link AbstractJournal#abort()}.
 *          <p>
 *          An optimization would essentially isolate the writes on the cache
 *          per BTree or between commits. At the commit point, the written
 *          records would be migrated into the "committed" cache for the store.
 *          The caller would read on the uncommitted cache, which would read
 *          through to the "committed" cache. This would prevent incorrect reads
 *          without requiring us to throw away valid records in the cache. This
 *          could be a significant performance gain if aborts are common on a
 *          machine with a lot of RAM.
 * 
 * 
 * @todo Test w/ G1 <code>-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC</code>
 *       <p>
 *       G1 appears faster for query, but somewhat slower for load. This is
 *       probably related to the increased memory demand during load (more of
 *       the data winds up buffered). G1 might work for both use cases with a
 *       smaller portion of the heap given over to buffers.
 *       <p>
 *       G1 can also trip a crash, at least during load. There is a Sun incident
 *       ID# 1609804 for this.
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
 * @todo Does it make sense to both buffer the index segment nodes region and
 *       buffer the nodes and leaves? [buffering the nodes region is an option.]
 * 
 * @todo Note that a r/w store will require an approach in which addresses are
 *       PURGED from the store's cache during the commit protocol. That might be
 *       handled at the tx layer.
 * 
 * @todo Better ergonomics! Perhaps keep some minimum amount for the JVM and
 *       then set a trigger on the GC time and if it crosses 5-10% of the CPU
 *       time for the application, then reduce the maximum bytes allowed for the
 *       global LRU buffer.
 * 
 * @see Options
 */
public class LRUNexus {

    protected static final transient Logger log = Logger
            .getLogger(LRUNexus.class);
    
    /**
     * Global instance.
     * <p>
     * Note: A <a href="http://bugs.sun.com/view_bug.do?bug_id=6880903">Sun G1
     * bug in JDK 1.6.0_16</a> provides a false estimate of the available
     * memory.
     *
     * @see Options
     */
    public static final IGlobalLRU<Long, Object> INSTANCE;

    /**
     * These options are MUST BE specified as <em>ENVIRONMENT</em> variables on
     * the command line when you start the JVM. The options control the
     * existence of and behavior of the {@link LRUNexus#INSTANCE}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /**
         * Option may be used to disable the {@link LRUNexus#INSTANCE}.
         */
        String ENABLED = LRUNexus.class.getName() + ".enabled";

        String DEFAULT_ENABLED = "true";

        /**
         * The maximum heap capacity as a percentage of the JVM heap expressed
         * as a value in <code>[0.0:1.0]</code>. This is used IFF
         * {@link #MAX_HEAP} is not specified or is ZERO (0), which is its
         * default value. If both options are zero, then the maximum heap is
         * understood to be zero and the {@link LRUNexus#INSTANCE} will be
         * disabled.
         */
        String PERCENT_HEAP = LRUNexus.class.getName() + ".percentHeap";

        /**
         * The default ({@value #DEFAULT_PERCENT_HEAP}) is a bit conservative.
         * It is designed to leave some room for application data objects and
         * GC. You may be able to get away with significantly more on machines
         * with large RAM, or just specify the buffer heap size directly using
         * {@link #MAX_HEAP}.
         */ 
        String DEFAULT_PERCENT_HEAP = ".1";

        /**
         * This option overrides {@link #PERCENT_HEAP} and directly specifies
         * the maximum capacity of the {@link LRUNexus#INSTANCE} in bytes. If
         * both options are zero, then the maximum heap is understood to be zero
         * and the {@link LRUNexus#INSTANCE} will be disabled. Legal examples
         * include:
         * 
         * <pre>
         * 30000000
         * 400m
         * 2Gb
         * </pre>
         * 
         * @see BytesUtil#getByteCount(String)
         */
        String MAX_HEAP = LRUNexus.class.getName() + ".maxHeap";

        String DEFAULT_MAX_HEAP = "0";

        /**
         * The {@link IGlobalLRU} implementation class.  Only implementations
         * which are hard wired into the code can be specified.
         */
        String CLASS = LRUNexus.class.getName() + ".class";

        /**
         * FIXME The {@link HardReferenceGlobalLRURecycler} has less throughput
         * than the {@link HardReferenceGlobalLRU} but I want to test the
         * {@link HardReferenceGlobalLRU} more throughly on high throughput
         * cluster data loads to make sure that it is performing correctly.
         */
        String DEFAULT_CLASS = HardReferenceGlobalLRURecycler.class.getName();

        /**
         * The load factor for the cache instances.
         */
        String LOAD_FACTOR = LRUNexus.class.getName() + ".loadFactor";

        String DEFAULT_LOAD_FACTOR = ".75";

        /**
         * The initial capacity for the cache instances.
         */
        String INITIAL_CAPACITY = LRUNexus.class.getName() + ".initialCapacity";

        String DEFAULT_INITIAL_CAPACITY = "16";

        /**
         * The minimum #of per-{@link IRawStore} cache instances that will be
         * retained by hard references. This controls the size of a hard
         * reference ring buffer backing a weak value hash map. The actual
         * number of cache instances will be less if fewer stores have been
         * opened or if open stores have been
         * {@link IRawStore#deleteResources() destroyed}. More cache instances
         * will exist if there are hard references to more {@link IRawStore}
         * instances.
         */
        String MIN_CACHE_SET_SIZE = LRUNexus.class.getName()+".minCacheSetSize";
        
        String DEFAULT_MIN_CACHE_SET_SIZE = "5";
        
    }

    static {

        IGlobalLRU<Long, Object> tmp = null;

        try {

            final boolean enabled = Boolean.valueOf(System.getProperty(
                    Options.ENABLED, Options.DEFAULT_ENABLED));

            if (enabled) {

                /*
                 * Which implementation to use.
                 * 
                 * Note: All three strategies are pretty comparable, at least on
                 * small data sets. The WeakReferenceGlobalLRU can use less
                 * memory, which is really its weakness -- the backing ring
                 * buffer can have lots of duplicates so it winds up retaining
                 * fewer records and hence is not able to exploit as much RAM.
                 */
                final Class<? extends IGlobalLRU> cls;
                // final Class<? extends IGlobalLRU> cls =
                // WeakReferenceGlobalLRU.class;
                // final Class<? extends IGlobalLRU> cls =
                // HardReferenceGlobalLRU.class;
                // final Class<? extends IGlobalLRU> cls =
                // StoreAndAddressLRUCache.class;
                cls = (Class<? extends IGlobalLRU>) LRUNexus.class
                        .forName(System.getProperty(Options.CLASS,
                                Options.DEFAULT_CLASS));

                if (cls.isAssignableFrom(IGlobalLRU.class)) {

                    throw new RuntimeException("Class does not implement: "
                            + IGlobalLRU.class.getName());

                }

                // The load factor for the backing hash map(s).
                final float loadFactor = Float.valueOf(System.getProperty(
                        Options.LOAD_FACTOR, Options.DEFAULT_LOAD_FACTOR));

                // The initial capacity for the backing hash map(s).
                final int initialCacheCapacity = Integer.valueOf(System
                        .getProperty(Options.INITIAL_CAPACITY,
                                Options.DEFAULT_INITIAL_CAPACITY));

                // The percentage of the JVM heap to use for bigdata buffers.
                final float percentHeap = Float.valueOf(System.getProperty(
                        Options.PERCENT_HEAP, Options.DEFAULT_PERCENT_HEAP));

                if (percentHeap < 0f || percentHeap > 1f) {

                    throw new IllegalArgumentException(Options.PERCENT_HEAP
                            + " : must be in [0:1].");

                }

                // The maximum heap size in bytes (optional).
                final long maxHeap = BytesUtil.getByteCount(System.getProperty(
                        Options.MAX_HEAP, Options.DEFAULT_MAX_HEAP));
                
                if (maxHeap < 0)
                    throw new IllegalArgumentException(Options.MAX_HEAP
                            + "="
                            + System.getProperty(Options.MAX_HEAP,
                                    Options.DEFAULT_MAX_HEAP));

                // The maximum bytesInMemory to retain across the caches.
                final long maximumBytesInMemory;
                if (maxHeap == 0 && percentHeap != 0f) {
                    // compute based on the percentage of the heap.
                    maximumBytesInMemory = (long) (Runtime.getRuntime()
                            .maxMemory() * percentHeap);
                } else if (maxHeap != 0) {
                    // directly given.
                    maximumBytesInMemory = maxHeap;
                } else {
                    // disabled.
                    maximumBytesInMemory = 0L;
                }

                // The minimum #of caches to keep open.
                final int minCacheSetSize = Integer.valueOf(System.getProperty(
                        Options.MIN_CACHE_SET_SIZE,
                        Options.DEFAULT_MIN_CACHE_SET_SIZE));

                if (BigdataStatics.debug)
                    System.err.println(//
                            "m="
                                    + IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR//
                                    + ", maxPercent="
                                    + percentHeap//
                                    + ", maxHeap="
                                    + maxHeap
                                    + ", bufferSize="
                                    + maximumBytesInMemory
                                    + ", maxMemory="
                                    + Runtime.getRuntime().maxMemory()//
                                    + ", loadFactor=" + loadFactor
                                    + ", initialCacheCapacity="
                                    + initialCacheCapacity
                                    + ", minCacheSetSize=" + minCacheSetSize
                                    + ", cls=" + cls);

                if (maximumBytesInMemory > 0) {

                    if (cls == WeakReferenceGlobalLRU.class) {

                        final int queueCapacity;

                        /*
                         * Estimate of the average record size.
                         * 
                         * Note: 1024 is not bad value.
                         */
                        // The average record size.
                        final int baseAverageRecordSize = 1024;

                        final int averageRecordSize = (int) (baseAverageRecordSize * (Integer
                                .valueOf(IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR) / 32.));

                        /*
                         * The target capacity for that expected record size.
                         * 
                         * Note: This parameter can get you into trouble with
                         * too much GC if too much gets buffered on the queue
                         * (this is the reasons this LRU implementation is not
                         * recommended!)
                         * 
                         * 4x may be a bit aggressive. Try 3x.
                         * 
                         * TestTripleStoreLoadRateLocal: 4x yields 38s GC time
                         * with 1G heap.
                         * 
                         * TestTripleStoreLoadRateLocal: 3x yields 36s GC time
                         * with 1G heap.
                         */
                        final long maximumQueueCapacityEstimate = maximumBytesInMemory
                                / averageRecordSize * 2;

                        if (BigdataStatics.debug)
                            System.err.println(//
                                    "averageRecordSize="
                                            + averageRecordSize//
                                            + ", maximumQueueCapacityEstimate="
                                            + maximumQueueCapacityEstimate//
                                    );

                        if (true) {

                            queueCapacity = (int) Math.min(Integer.MAX_VALUE,
                                    maximumQueueCapacityEstimate);

                        } else if (maximumBytesInMemory < Bytes.gigabyte * 2) {

                            // capacity is no more than X
                            queueCapacity = (int) Math
                                    .min(maximumQueueCapacityEstimate, 200000/*
                                                                              * 200k
                                                                              */);

                        } else {

                            // capacity is no more than Y
                            queueCapacity = (int) Math
                                    .min(maximumQueueCapacityEstimate, 1000000/*
                                                                               * 1M
                                                                               */);

                        }

                        tmp = new WeakReferenceGlobalLRU(//
                                maximumBytesInMemory,//
                                minCacheSetSize,//
                                queueCapacity,//
                                20, // nscan
                                initialCacheCapacity,//
                                loadFactor,//
                                16 // concurrencyLevel (the Java default)
                        );

                    } else if (cls == HardReferenceGlobalLRU.class) {

                        tmp = new HardReferenceGlobalLRU<Long, Object>(
                                maximumBytesInMemory, minCacheSetSize,
                                initialCacheCapacity, loadFactor);

                    } else if (cls == HardReferenceGlobalLRURecycler.class) {

                        tmp = new HardReferenceGlobalLRURecycler<Long, Object>(
                                maximumBytesInMemory, minCacheSetSize,
                                initialCacheCapacity, loadFactor);

                    } else if (cls == StoreAndAddressLRUCache.class) {

                        tmp = new StoreAndAddressLRUCache<Object>(
                                maximumBytesInMemory, minCacheSetSize,
                                initialCacheCapacity, loadFactor);

                    } else {

                        throw new UnsupportedOperationException(
                                "Can not create global cache: cls="
                                        + cls.getName());

                    }

                }

            }

        } catch (Throwable t) {

            log.error("LRUNexus disabled", t);

        } finally {

            INSTANCE = tmp;

        }

    }

}
