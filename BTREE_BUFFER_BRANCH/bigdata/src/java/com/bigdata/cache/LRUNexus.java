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

import com.bigdata.BigdataStatics;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.jini.JiniClient;

/**
 * Static singleton factory.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Test in low memory scenarios to look for fence posts.
 * 
 * FIXME Test sensitivity to the percentage of the JVM memory available which
 * is allowed for the cache
 * 
 * @todo Simplify the integration pattern for use of a cache. You have to follow
 *       a "get()" then if miss, read+wrap, then putIfAbsent(). You MUST also
 *       "touch" the object on the global LRU on access to keep it "live".
 *       Finally, for write through, you must insert the object into the cache.
 *       You DO NOT need to "touch" the object on a cache hit or when inserting
 *       it into the cache since the cache is backed by the global LRU and the
 *       {@link ConcurrentWeakValueCache} will automatically "touch" the object
 *       on the LRU. These semantics could be made more transparent if we define
 *       an ICachedStore interface. However, the caller would need to pass in
 *       the functor to create the appropriate object on get() and would need to
 *       handle the "touch" protocol as well.
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
 * @todo Pay attention to both concurrency and the cost of resizing the backing
 *       maps.
 * 
 * @todo does it make sense to both buffer the index segment nodes region and
 *       buffer the nodes and leaves? [buffering the nodes region is an option.]
 * 
 * @todo Note that a r/w store will require an approach in which addresses are
 *       PURGED from the store's cache during the commit protocol. That might be
 *       handled at the tx layer.
 * 
 * @todo test with the G1 policy
 * 
 *       <pre>
 * -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC
 * </pre>
 * 
 * @todo Estimate the average record size based on real data (counters).
 *       Actually, 1024 is not bad for the RDF DB with a branching factor of 32.
 *       This is a pretty common value for the byte length of the decompressed
 *       node and leaf data records. It will be much larger for
 *       {@link IndexSegment}s of course so scale-out will need to plan for a
 *       mixture of records on journals and index segments in memory.
 */
public class LRUNexus {

    /**
     * Return the default maximum memory footprint, which is
     * {@link #percentMaximumMemory} of the maximum JVM heap. This limit is a
     * bit conservative. It is designed to leave some room for application data
     * objects and GC. You may be able to get away with significantly more on
     * machines with large RAM.
     */
    static long getMaximumMemoryFootprint(final float percentMaximumMemory) {
        
        return (long) (Runtime.getRuntime().maxMemory() * percentMaximumMemory);
        
    }

    /**
     * Global instance.
     * <p>
     * Note: A <a href="http://bugs.sun.com/view_bug.do?bug_id=6880903">Sun G1
     * bug in JDK 1.6.0_16</a> provides a false estimate of the available
     * memory.
     * 
     * @todo Define configure properties. Document for {@link JiniClient} and
     *       {@link Journal} users.
     */
    public static final IGlobalLRU<Long, Object> INSTANCE;

    static {

        /*
         * Which implementation to use.
         * 
         * Note: All three strategies are pretty comparable, at least on small
         * data sets. The WeakReferenceGlobalLRU can use less memory, which is
         * really its weakness -- the backing ring buffer can have lots of
         * duplicates so it winds up retaining fewer records and hence is not
         * able to exploit as much RAM.
         */
//        final Class<? extends IGlobalLRU> cls = WeakReferenceGlobalLRU.class;
        final Class<? extends IGlobalLRU> cls = HardReferenceGlobalLRU.class;
//        final Class<? extends IGlobalLRU> cls = StoreAndAddressLRUCache.class;
        
        // The load factor for the backing hash map(s).
        final float loadFactor = .75f;
        
        // The initial capacity for the backing hash map(s).
        final int initialCacheCapacity = 16;
        
        // The percentage of the JVM heap to use for bigdata buffers.
        final float percentMaximumMemory = .3f;

        // The maximum bytesInMemory to retain across the caches.
        final long maximumBytesInMemory = getMaximumMemoryFootprint(percentMaximumMemory);
        
        // The minimum #of caches to keep open.
        final int minimumCacheSetCapacity = 5;
        
        // The average record size.
        final int baseAverageRecordSize = 1024;
        
        if(cls == WeakReferenceGlobalLRU.class) {

            final int queueCapacity;
            
            final int averageRecordSize = (int) (baseAverageRecordSize * (Integer
                    .valueOf(IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR) / 32.));

            /*
             * The target capacity for that expected record size.
             * 
             * FIXME This parameter can get you into trouble with too much GC if too
             * much gets buffered on the queue.
             * 
             * 4x may be a bit aggressive. Try 3x.
             * 
             * TestTripleStoreLoadRateLocal: 4x yields 38s GC time with 1G heap.
             * 
             * TestTripleStoreLoadRateLocal: 3x yields 36s GC time with 1G heap.
             */
            final long maximumQueueCapacityEstimate = maximumBytesInMemory
                    / averageRecordSize * 2;

            if(BigdataStatics.debug)
                System.err.println(//
                    "defaultBranchingFactor="
                    + IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR//
                    + ", averageRecordSize=" + averageRecordSize//
                    + "\nmaxMemory="+Runtime.getRuntime().maxMemory()//
                    + ", percentMaximumMemory="+percentMaximumMemory//
                    + ", maximumQueueCapacityEstimate=" + maximumQueueCapacityEstimate//
                    );

            if (true) {
                
                queueCapacity = (int) Math.min(Integer.MAX_VALUE,
                        maximumQueueCapacityEstimate);
                
            } else if (maximumBytesInMemory < Bytes.gigabyte * 2) {

                // capacity is no more than X
                queueCapacity = (int) Math.min(maximumQueueCapacityEstimate,
                        200000/* 200k */);

            } else {

                // capacity is no more than Y
                queueCapacity = (int) Math.min(maximumQueueCapacityEstimate,
                        1000000/* 1M */);

            }

            INSTANCE = new WeakReferenceGlobalLRU(//
                    maximumBytesInMemory,//
                    minimumCacheSetCapacity,//
                    queueCapacity,//
                    20,   // nscan
                    initialCacheCapacity,//
                    loadFactor,//
                    16    // concurrencyLevel (the Java default)
            );
            
        } else if(cls == HardReferenceGlobalLRU.class) {

            INSTANCE = new HardReferenceGlobalLRU<Long, Object>(
                    maximumBytesInMemory, minimumCacheSetCapacity,
                    initialCacheCapacity, loadFactor);
            
        } else if(cls == StoreAndAddressLRUCache.class) {

            INSTANCE = new StoreAndAddressLRUCache<Object>(
                    maximumBytesInMemory, minimumCacheSetCapacity,
                    initialCacheCapacity, loadFactor);

        } else {

            throw new UnsupportedOperationException(
                    "Can not create global cache: cls=" + cls.getName());
            
        }

    }

}
