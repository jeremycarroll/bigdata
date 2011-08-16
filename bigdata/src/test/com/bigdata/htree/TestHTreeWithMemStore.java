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
 * Created on Jul 16, 2011
 */
package com.bigdata.htree;

import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rwstore.sector.MemStore;

/**
 * Integration test with a persistence store.
 * 
 * @author bryan
 */
public class TestHTreeWithMemStore extends TestCase {

    private final static Logger log = Logger.getLogger(TestHTreeWithMemStore.class);

    public TestHTreeWithMemStore() {
    }

    public TestHTreeWithMemStore(String name) {
        super(name);
    }
    
    public void test_stressInsert_addressBits1() {

        /*
         * Note: If the retention queue is too small here then the test will
         * fail once the maximum depth of the tree exceeds the capacity of the
         * retention queue and a parent is evicted while a child is being
         * mutated.
         */
        doStressTest(1/* addressBits */, 50);
        
    }

   public void test_stressInsert_addressBits2() {

        doStressTest(2/* addressBits */, s_retentionQueueCapacity);
        
    }

    public void test_stressInsert_addressBits3() {

        doStressTest(3/* addressBits */,s_retentionQueueCapacity);

    }

    public void test_stressInsert_addressBits4() {

        doStressTest(4/* addressBits */,s_retentionQueueCapacity);

    }

    public void test_stressInsert_addressBits5() {

        doStressTest(5/* addressBits */,s_retentionQueueCapacity);

    }

    public void test_stressInsert_addressBits6() {

        doStressTest(6/* addressBits */,s_retentionQueueCapacity);
        
    }

    public void test_stressInsert_addressBits8() {

        doStressTest(8/* addressBits */,s_retentionQueueCapacity);
        
    }

    public void test_stressInsert_addressBits10() {

        doStressTest(10/* addressBits */,s_retentionQueueCapacity);

    }
    
    /**
     * Stress test for handling of overflow pages.
     */
    public void test_overflowPage_addressBits3() {

        final int addressBits = 3; 

        final int writeRetentionQueueCapacity = 20;
        
        final int numOverflowPages = 5000;

        doOverflowStressTest(addressBits, writeRetentionQueueCapacity, numOverflowPages);
        
    }

    public void test_overflowPage_addressBits10() {
    
        final int addressBits = 10; 

        final int writeRetentionQueueCapacity = 20;
        
        final int numOverflowPages = 5000;

        doOverflowStressTest(addressBits, writeRetentionQueueCapacity, numOverflowPages);
        
    }

    /**
     * 
     * @param addressBits
     * @param writeRetentionQueueCapacity
     * @param numOverflowPages
     *            The #of overflow bucket pages for the largest overflow bucket
     *            in the tree.
     */
    private void doOverflowStressTest(final int addressBits,
            final int writeRetentionQueueCapacity, final int numOverflowPages) {
        
        final long start = System.currentTimeMillis();

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE,
                Integer.MAX_VALUE);

        try {

            final HTree htree = getHTree(store, addressBits,
                    false/* rawRecords */, writeRetentionQueueCapacity);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final byte[] key = new byte[]{1};
            final byte[] keyAlt = new byte[]{4};
            final byte[] keyAlt2 = new byte[]{5};
            final byte[] val = new byte[]{2};
            
            final int inserts = (1 << addressBits) * numOverflowPages;
            
//            long altInserts = 0;
            long altInserts1 = 0;
            long altInserts2 = 0;
            
            // insert enough tuples to fill the page twice over.
            for (int i = 0; i < inserts; i++) {

                htree.insert(key, val);
                
                if (i % 5 == 0) {
                    htree.insert(keyAlt, val);
                    altInserts1++;
                }
                if (i % 7 == 0) {
                    htree.insert(keyAlt2, val);
                    altInserts2++;
                }
                
            }
//            altInserts = altInserts1 + altInserts2;
                       
            final long load = System.currentTimeMillis();
            
            // now iterate over all the values
            {
                //  first using lookupAll
                final ITupleIterator tups = htree.lookupAll(key);
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    visits++;
                }
                assertEquals(inserts,visits);
            }
            {
                //  first using lookupAll
                final ITupleIterator tups = htree.lookupAll(keyAlt);
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    visits++;
                }
                assertEquals(altInserts1,visits);
            }
            {
                //  first using lookupAll
                final ITupleIterator tups = htree.lookupAll(keyAlt2);
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    visits++;
                }
                assertEquals(altInserts2,visits);
            }
            {
                // then using the rangeIterator
                final ITupleIterator tups = htree.rangeIterator();
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    visits++;
                }
                final long total =(inserts + altInserts1 + altInserts2);
                assertEquals(total,visits);
            }
            
            final long end = System.currentTimeMillis();
            
            if (log.isInfoEnabled())
                log.info("Load took " + (load - start) + "ms, loops for "
                        + (inserts + altInserts1 + altInserts2) + " "
                        + (end - load) + "ms");

            htree.flush();
            
        } finally {

            store.destroy();

        }

    }


//    public void test_stressInsert_addressBitsMAX() {
//
//        doStressTest(16/* addressBits */);
//
//    }

    /**
     * 
     * Note: If the retention queue is less than the maximum depth of the HTree
     * then we can encounter a copy-on-write problem where the parent directory
     * becomes immutable during a mutation on the child.
     * 
     * @param store
     * @param addressBits
     * @param rawRecords
     * @param writeRetentionQueueCapacity
     * 
     * @return
     */
    private HTree getHTree(final IRawStore store, final int addressBits,
            final boolean rawRecords, final int writeRetentionQueueCapacity) {

        /*
         * TODO This sets up a tuple serializer for a presumed case of 4 byte
         * keys (the buffer will be resized if necessary) and explicitly chooses
         * the SimpleRabaCoder as a workaround since the keys IRaba for the
         * HTree does not report true for isKeys(). Once we work through an
         * optimized bucket page design we can revisit this as the
         * FrontCodedRabaCoder should be a good choice, but it currently
         * requires isKeys() to return true.
         */
        final ITupleSerializer<?,?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                //new FrontCodedRabaCoder(),// Note: reports true for isKeys()!
                new SimpleRabaCoder(),// keys
                new SimpleRabaCoder() // vals
                );
        
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        if (rawRecords) {
            metadata.setRawRecords(true);
            metadata.setMaxRecLen(0);
        }

        metadata.setAddressBits(addressBits);

        metadata.setTupleSerializer(tupleSer);

        /*
         * Note: A low retention queue capacity will drive evictions, which is
         * good from the perspective of stressing the persistence store
         * integration.
         */
        metadata.setWriteRetentionQueueCapacity(writeRetentionQueueCapacity);
        metadata.setWriteRetentionQueueScan(4); // Must be LTE capacity.

        return HTree.create(store, metadata);

    }
    
    private static final int s_limit = 100000;
    private static final int s_retentionQueueCapacity = 20;

    /**
     * Note: If the retention queue is less than the maximum depth of the HTree
     * then we can encounter a copy-on-write problem where the parent directory
     * becomes immutable during a mutation on the child.
     * 
     * @param addressBits
     * @param writeRetentionQueueCapacity
     */
    private void doStressTest(final int addressBits,
            final int writeRetentionQueueCapacity) {

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE,
                Integer.MAX_VALUE);

        try {

            final HTree htree = getHTree(store, addressBits,
                    false/* rawRecords */, writeRetentionQueueCapacity);

            try {

                // Verify initial conditions.
                assertTrue("store", store == htree.getStore());
                assertEquals("addressBits", addressBits, htree.getAddressBits());

                final IKeyBuilder keyBuilder = new KeyBuilder();

                final byte[][] keys = new byte[s_limit][];
                for (int i = 0; i < s_limit; i++) {
                    keys[i] = keyBuilder.reset().append(new Integer(i).hashCode()).getKey();
                }
                final long begin = System.currentTimeMillis();
                for (int i = 0; i < s_limit; i++) {
                    final byte[] key = keys[i];
                    htree.insert(key, key);
                    if (log.isTraceEnabled())
                        log.trace("after key=" + i + "\n" + htree.PP());

                }

                final long elapsedInsertMillis = System.currentTimeMillis() - begin;
                
                assertEquals(s_limit, htree.getEntryCount());
                
                final long beginLookupFirst = System.currentTimeMillis();
                // Verify all tuples are found.
                for (int i = 0; i < s_limit; i++) {

                    final byte[] key = keys[i];

                    final byte[] firstVal = htree.lookupFirst(key);

                    if (!BytesUtil.bytesEqual(key, firstVal))
                        fail("Expected: " + BytesUtil.toString(key)
                                + ", actual="
                                + Arrays.toString(htree.lookupFirst(key)));

                }

                final long elapsedLookupFirstTime = System.currentTimeMillis()
                        - beginLookupFirst;

                final long beginValueIterator = System.currentTimeMillis();
                
                // Verify the iterator visits all of the tuples.
                AbstractHTreeTestCase.assertSameIteratorAnyOrder(keys,
                        htree.values());

                final long elapsedValueIteratorTime = System
                        .currentTimeMillis() - beginValueIterator;
                
                if (log.isInfoEnabled()) {
                    log.info("Inserted: " + s_limit + " tuples in "
                            + elapsedInsertMillis + "ms, lookupFirst(all)="
                            + elapsedLookupFirstTime+ ", valueScan(all)="
                            + elapsedValueIteratorTime + ", addressBits="
                            + htree.getAddressBits() + ", nnodes="
                            + htree.getNodeCount() + ", nleaves="
                            + htree.getLeafCount());
                }

            } catch (Throwable t) {

                log.error(t);

//              try {
//                  log.error("Pretty Print of error state:\n" + htree.PP(), t);
//              } catch (Throwable t2) {
//                  log.error("Problem in pretty print: t2", t2);
//              }

                // rethrow the original exception.
                throw new RuntimeException(t);

            }

//          log.error("Pretty Print of final state:\n" + htree.PP());

        } finally {

            store.destroy();

        }

    }

    
}
