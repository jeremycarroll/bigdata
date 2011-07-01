/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 19, 2011
 */

package com.bigdata.htree;

import junit.framework.TestCase2;

import org.apache.log4j.Level;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.htree.HTree.BucketPage;
import com.bigdata.htree.HTree.DirectoryPage;
import com.bigdata.htree.data.MockBucketData;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for {@link HTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Unit test for the code path where the sole buddy bucket on a
 *          page consists solely of duplicate keys thus forcing the size of the
 *          bucket page to double rather than splitting the page.
 * 
 *          TODO Work through a detailed example in which we have an elided
 *          bucket page or directory page because nothing has been inserted into
 *          that part of the address space.
 */
public class TestHTree extends TestCase2 {

    /**
     * 
     */
    public TestHTree() {
    }

    /**
     * @param name
     */
    public TestHTree(String name) {
        super(name);
    }
    
    public void test_ctor_correctRejection() {
        
		try {
			new HTree(null/* store */, 2/* addressBits */);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				new HTree(store, 0/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				new HTree(store, -1/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				new HTree(store, 33/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

		/*
		 * and spot check some valid ctor forms to verify that no exceptions are
		 * thrown.
		 */
		{
			final IRawStore store = new SimpleMemoryRawStore();
			new HTree(store, 1/* addressBits */); // min addressBits
			new HTree(store, 32/* addressBits */); // max addressBits
		}

    }
    
    /**
     * Basic test for correct construction of the initial state of an
     * {@link HTree}.
     */
    public void test_ctor() {

        final int addressBits = 10; // implies ~ 4k page size.
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final HTree htree = new HTree(store, addressBits);
            
			assertTrue("store", store == htree.getStore());
			assertEquals("addressBits", addressBits, htree.getAddressBits());
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 1, htree.getLeafCount());
			assertEquals("nentries", 0, htree.getEntryCount());
            
        } finally {

            store.destroy();
            
        }

    }

    /**
     * Test of basic operations using a TWO (2) bit address space inserting the
     * key sequence {1,2,3,4}.
     * <p>
     * Note: This test stops after the 4th tuple insert. At that point the
     * {@link HTree} is in a state where inserting the next tuple in the
     * sequence (5) will force a new directory page to be introduced. The next
     * two unit tests examine in depth the logic which introduces that directory
     * page. They are followed by another unit test which picks up on this same
     * insert sequences and goes beyond the introduction of the new directory
     * page.
     * 
     * @see bigdata/src/architecture/htree.xls#example1
     * @see #test_example1_splitDir_addressBits2_splitBits1()
     * @see #test_example1_splitDir_addressBits2_splitBits2()
     */
    public void test_example1_addressBits2_insert_1_2_3_4() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            
            // a key which we never insert and which should never be found by lookup.
            final byte[] unused = new byte[]{-127};
            
            final HTree htree = new HTree(store, addressBits, false/* rawRecords */);
            
            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final DirectoryPage root = htree.getRoot();
            assertEquals(4, root.childRefs.length);
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            assertTrue(a == (BucketPage) root.childRefs[1].get());
            assertTrue(a == (BucketPage) root.childRefs[2].get());
            assertTrue(a == (BucketPage) root.childRefs[3].get());
            assertEquals(2, root.getGlobalDepth());// starts at max.
            assertEquals(0, a.getGlobalDepth());// starts at min.

            /**
             * verify preconditions.
             * 
             * <pre>
             * root := [2] (a,a,a,a)   //
             * a    := [0]   (-;-;-;-) //
             * </pre>
             */
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 0, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());
            assertEquals(0, a.getKeyCount());
            assertEquals(0, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    a);
            assertFalse(htree.contains(k1));
            assertFalse(htree.contains(unused));
            assertEquals(null, htree.lookupFirst(k1));
            assertEquals(null, htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] {}, htree.lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] {}, htree.lookupAll(unused));

            /**
             * 1. Insert a tuple (0x01) and verify post-conditions. The tuple
             * goes into an empty buddy bucket with a capacity of one.
             * 
             * <pre>
             * root := [2] (a,a,a,a)   //
             * a    := [0]   (1;-;-;-) //
             * </pre>
             */
            htree.insert(k1, v1);
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 1, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());
            assertEquals(1, a.getKeyCount());
            assertEquals(1, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(1, new byte[][] { // keys
                            k1, null, null, null }),//
                    new ReadOnlyValuesRaba(1, new byte[][] { // vals
                            v1, null, null, null})),//
                    a);
            assertTrue(htree.contains(k1));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(null,htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] { v1 }, htree.lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] {}, htree.lookupAll(unused));

            /**
             * 2. Insert a tuple (0x02). Since the root directory is only paying
             * attention to the 2 MSB bits, this will be hashed into the same
             * buddy hash bucket as the first key. Since the localDepth of the
             * bucket page is zero, each buddy bucket on the page can only
             * accept one entry and this will force a split of the buddy bucket.
             * That means that a new bucket page will be allocated, the pointers
             * in the parent will be updated to link in the new bucket page, and
             * the buddy buckets will be redistributed among the old and new
             * bucket page.
             * <pre> 
             * root := [2] (a,a,b,b)   //
             * a    := [1]   (1,2;-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k2, v2);
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 2, htree.getLeafCount());
            assertEquals("nentries", 2, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertTrue(root == htree.getRoot());
            assertEquals(4, root.childRefs.length);
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == (BucketPage) root.childRefs[0].get());
            assertTrue(a == (BucketPage) root.childRefs[1].get());
            assertTrue(b == (BucketPage) root.childRefs[2].get());
            assertTrue(b == (BucketPage) root.childRefs[3].get());
            assertEquals(2, root.getGlobalDepth());
            assertEquals(1, a.getGlobalDepth());// localDepth has increased.
            assertEquals(1, b.getGlobalDepth());// localDepth is same as [a].
            assertEquals(2, a.getKeyCount());
            assertEquals(2, a.getValueCount());
            assertEquals(0, b.getKeyCount());
            assertEquals(0, b.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, k2, null, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);
            assertTrue(htree.contains(k1));
            assertTrue(htree.contains(k2));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertNull(htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v1 }, htree
                    .lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v2 }, htree
                    .lookupAll(k2));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] {}, htree
                    .lookupAll(unused));

            /**
             * 3. Insert 0x03. This forces another split.
             * <pre> 
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (1,2,3,-) //
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k3, v3);
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount());
            assertEquals("nentries", 3, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertTrue(root == htree.getRoot());
            assertEquals(4, root.childRefs.length);
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            assertTrue(a == (BucketPage) root.childRefs[0].get());
            assertTrue(c == (BucketPage) root.childRefs[1].get());
            assertTrue(b == (BucketPage) root.childRefs[2].get());
            assertTrue(b == (BucketPage) root.childRefs[3].get());
            assertEquals(2, root.getGlobalDepth());
            assertEquals(2, a.getGlobalDepth());// localDepth has increased.
            assertEquals(1, b.getGlobalDepth());// 
            assertEquals(2, c.getGlobalDepth());// localDepth is same as [a].
            assertEquals(3, a.getKeyCount());
            assertEquals(3, a.getValueCount());
            assertEquals(0, b.getKeyCount());
            assertEquals(0, b.getValueCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, c.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(3, new byte[][] { // keys
                            k1, k2, k3, null }),//
                    new ReadOnlyValuesRaba(3, new byte[][] { // vals
                            v1, k2, k3, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertTrue(htree.contains(k1));
            assertTrue(htree.contains(k2));
            assertTrue(htree.contains(k3));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertNull(htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v1 }, htree
                    .lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v2 }, htree
                    .lookupAll(k2));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v3 }, htree
                    .lookupAll(k3));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] {}, htree
                    .lookupAll(unused));

            /**
             * 4. Insert 0x04. This goes into the same buddy bucket. The buddy
             * bucket is now full again. It is only the only buddy bucket on the
             * page, e.g., global depth == local depth.
             * 
             * Note: Inserting another tuple into this buddy bucket will not
             * only cause it to split but it will also introduce a new directory
             * page into the hash tree.
             * 
             * <pre> 
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (1,2,3,4) //
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k4, v4);
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount());
            assertEquals("nentries", 4, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertTrue(root == htree.getRoot());
            assertEquals(4, root.childRefs.length);
            assertTrue(a == (BucketPage) root.childRefs[0].get());
            assertTrue(c == (BucketPage) root.childRefs[1].get());
            assertTrue(b == (BucketPage) root.childRefs[2].get());
            assertTrue(b == (BucketPage) root.childRefs[3].get());
            assertEquals(2, root.getGlobalDepth());
            assertEquals(2, a.getGlobalDepth());// localDepth has increased.
            assertEquals(1, b.getGlobalDepth());// 
            assertEquals(2, c.getGlobalDepth());// localDepth is same as [a].
            assertEquals(4, a.getKeyCount());
            assertEquals(4, a.getValueCount());
            assertEquals(0, b.getKeyCount());
            assertEquals(0, b.getValueCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, c.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, k2, k3, k4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertTrue(htree.contains(k1));
            assertTrue(htree.contains(k2));
            assertTrue(htree.contains(k3));
            assertTrue(htree.contains(k4));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));
            assertNull(htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v1 }, htree
                    .lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v2 }, htree
                    .lookupAll(k2));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v3 }, htree
                    .lookupAll(k3));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v4 }, htree
                    .lookupAll(k4));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] {}, htree
                    .lookupAll(unused));

        } finally {

            store.destroy();

        }

    }

    /**
     * Unit test for
     * {@link HTree#addLevel(DirectoryPage, int, int, com.bigdata.htree.HTree.AbstractPage)}
     * .
     */
    public void test_example1_splitDir_addressBits2_splitBits1() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k20 = new byte[]{0x20};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v20 = new byte[]{0x20};
            
            final HTree htree = new HTree(store, addressBits);

            final DirectoryPage root = htree.getRoot();
            
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);
            
            /*
             * Verify preconditions for the unit test.
             */
            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);

            /*
             * Force a split of the root directory page. Note that (a) has the
             * same depth as the directory page we want to split, which is a
             * precondition for being able to split the directory.
             */

            // verify that [a] will not accept an insert.
            assertFalse(a
                    .insert(k20, v20, root/* parent */, 0/* buddyOffset */));
            
            // split the root directory page.
            htree.addLevel(root/* oldParent */, 0/* buddyOffset */,
                    1/* splitBits */, a/* child */);
            
            /*
             * Verify post-conditions.
             */
            
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(d == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(c == d.childRefs[2].get());
            assertTrue(c == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(1, d.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);

        } finally {

            store.destroy();
            
        }

    }
    
    /**
     * Unit test for
     * {@link HTree#addLevel(DirectoryPage, int, int, com.bigdata.htree.HTree.AbstractPage)}
     * .
     */
    public void test_example1_splitDir_addressBits2_splitBits2() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k20 = new byte[]{0x20};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v20 = new byte[]{0x20};
            
            final HTree htree = new HTree(store, addressBits);

            final DirectoryPage root = htree.getRoot();
            
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);
            
            /*
             * Verify preconditions for the unit test.
             */
            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);

            /*
             * Force a split of the root directory page. Note that (a) has the
             * same depth as the directory page we want to split, which is a
             * precondition for being able to split the directory.
             */

            // verify that [a] will not accept an insert.
            assertFalse(a
                    .insert(k20, v20, root/* parent */, 0/* buddyOffset */));
            
            // split the root directory page.
            htree.addLevel(root/* oldParent */, 0/* buddyOffset */,
                    2/* splitBits */, a/* child */);
            
            /*
             * Verify post-conditions.
             */
            
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(a == d.childRefs[2].get());
            assertTrue(a == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);

        } finally {

            store.destroy();
            
        }

    }

    /**
     * A unit test which continues the scenario begun above (insert 1,2,3,4),
     * working through the structural changes required until we can finally
     * insert the key (5) into the {@link HTree}.  This test uses high level
     * insert operations when they do not cause structural changes and low 
     * level operations when structural changes are required. This allows us
     * to view all of the intermediate states of the {@link HTree} by taking
     * the control logic "out of the loop" as it were.
     * <p>
     * In order to insert the key (5) we will need to split (a). However, the
     * prefix length for (a) is only THREE (3) bits and we need a prefix length
     * of 5 bits before we can split (a). This prefix length requirement arises
     * because we can not split (a) until the prefix bits considered are
     * sufficient to redistribute the tuples between (a) and its siblings
     * (children of the same parent directory page). The first such split will
     * occur when we have a prefix length of 5 bits and therefore can make a
     * distinction among the keys in (a), which are:
     * 
     * <pre>
     * key|bits
     * ---+--------
     * 1   00000001
     * 2   00000010
     * 3   00000011
     * 4   00000100
     * ---+--------
     *    |01234567
     * </pre>
     * 
     * Consulting the table immediately above, the first non-zero bit is bit 5
     * of the key whose value is FOUR (4).
     */
    public void test_example1_splitDir_addressBits2_splitBits1_increasePrefixBits() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k5 = new byte[]{0x05};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v5 = new byte[]{0x05};
            
            final HTree htree = new HTree(store, addressBits, false/*rawRecords*/);

            // Note: The test assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            /**
             * Insert a series of keys to bring us to the starting point for
             * this test. The post-condition of this insert sequence is:
             * 
             * <pre>
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (1,2,3,4) // Note: depth(root) == depth(a) !
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);

            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);

            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /**
             * Add a directory level.
             * 
             * Note: Since depth(root) EQ depth(a) we can not split (a).
             * Instead, we have to add a level which decreases depth(d) and
             * makes it possible to split (a).  There will be two references
             * to (a) in the post-condition, at which point it will be possible
             * to split (a).
             * 
             * <pre>
             * root := [2] (d,d,b,b)     //
             * d    := [1]   (a,a;c,c)   // added new level below the root.  
             * a    := [0]     (1;2;3;4) // local depth now 0. NB: inconsistent intermediate state!!!
             * c    := [0]     (-;-;-;-) // local depth now 0.
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             */

            // add a level below the root.
            htree.addLevel(root/* oldParent */, 0/* buddyOffset */,
                    1/* splitBits */, a/* child */);
            
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(d == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(c == d.childRefs[2].get());
            assertTrue(c == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(1, d.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);

            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));
            
            /**
             * Split (a), creating a new bucket page (e) and re-index the tuples
             * from (a) into (a,e). In this case, they all wind up back in (a)
             * so (a) remains full (no free slots). The local depth of (a) and
             * (e) are now (1).
             * 
             * <pre>
             * root := [2] (d,d,b,b)     //
             * d    := [1]   (a,e;c,c)   // 
             * a    := [1]     (1,2;3,4) // local depth now 1. 
             * e    := [1]     (-,-;-,-) // local depth now 1 (new sibling)
             * c    := [0]     (-;-;-;-) // 
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             */

            // split (a) into (a,e).
            htree.splitAndReindexFullBucketPage(d/* parent */,
                    0/* buddyOffset */, 4 /* prefixLength */, a/* oldBucket */);

            assertEquals("nnodes", 2, htree.getNodeCount()); // unchanged.
            assertEquals("nleaves", 4, htree.getLeafCount());
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final BucketPage e = (BucketPage) d.childRefs[1].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(d == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(e == d.childRefs[1].get());
            assertTrue(c == d.childRefs[2].get());
            assertTrue(c == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(1, d.globalDepth);
            assertEquals(1, a.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);

            // verify that [a] will not accept an insert (still full).
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /*
             * FIXME We really need to verify the tuples in (a) and in (e). The
             * current splitBucketOnPage logic does NOT reindex the tuples and
             * will have moved some to (e). We are not noticing this because of
             * the broken semantics for MutableKeysBuffer!
             * 
             * FIXME Also, the keys and values buffers are dimensions to one to
             * large (the overflow slot used for the B+Tree is not required
             * here).
             */

//            /**
//             * Split the new directory page (d), increasing its depth from (1)
//             * to (2).
//             * 
//             * Note: The depth(root) GT depth(d), therefore this split is
//             * similar to the split of a bucket page. It increases the size of
//             * each buddy bucket on (d) and reduces the number of references to
//             * (d) in the parent directory (the root).
//             * 
//             * A directory page always has all slots filled. We we split (d) we
//             * wind up with (4) references in (d) to (a) and (4) references in
//             * (e) to (c).
//             * 
//             * The prefix length of (a) has increased by ONE (1) to FOUR (4),
//             * which is closer to our goal of FIVE (5).
//             * 
//             * <pre>
//             * root := [2] (d,e,b,b)     //
//             * d    := [2]   (a,a,a,a)   // localDepth now (2).
//             * e    := [2]   (c,c,c,c)   // localDepth same as (d).
//             * a    := [0]     (1,2;3,4) // 
//             * c    := [0]     (-,-;-,-) // 
//             * b    := [1]   (-,-;-,-)   //
//             * </pre>
//             */
//            
//            // verify that [a] will not accept an insert.
//            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));
//
//            htree.splitDirectoryPage(root, 0/* buddyOffset */, d/* oldChild */);
//            
//            /*
//             * Verify post-conditions.
//             */
//
//            assertEquals("nnodes", 3, htree.getNodeCount());
//            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
//            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged
//
//            assertTrue(root == htree.getRoot());
//            final DirectoryPage e = (DirectoryPage) root.childRefs[1].get();
//            assertTrue(d == root.childRefs[0].get());
//            assertTrue(e == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertTrue(a == d.childRefs[0].get());
//            assertTrue(a == d.childRefs[1].get());
//            assertTrue(a == d.childRefs[2].get());
//            assertTrue(a == d.childRefs[3].get());
//            assertTrue(c == e.childRefs[0].get());
//            assertTrue(c == e.childRefs[1].get());
//            assertTrue(c == e.childRefs[2].get());
//            assertTrue(c == e.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(2, d.globalDepth);
//            assertEquals(2, e.globalDepth);
//            assertEquals(0, a.globalDepth);
//            assertEquals(0, c.globalDepth); 
//            assertEquals(1, b.globalDepth);
//
//            /**
//             * We are not there yet. The prefix length is only (4). We need to
//             * go through one more add level before we can differentiate the
//             * keys which are already in (a). Here, we add the new directory
//             * page as the parent of (a). This increases the bit length of the
//             * prefix along the path to (a) without causing global reordering of
//             * the hash tree (all tuples remain on the child page (a)).
//             * 
//             * <pre>
//             * root := [2] (d,e,b,b)     //
//             * d    := [2]   (a,a,a,a)   // localDepth now (2).
//             * e    := [2]   (c,c,c,c)   // localDepth same as (d).
//             * a    := [0]     (1;2;3;4) // 
//             * c    := [0]     (-;-;-;-) // 
//             * b    := [1]   (-,-;-,-)   //
//             * 
//             * root := [2] (f,f,b,b)     //
//             * f    := [1]   (d,d;e,e)   // localDepth is (1).
//             * d    := [0]     (a;a;a;a) // localDepth now (0).
//             * e    := [0]     (c;c;c;c) // localDepth now (0).
//             * a    := [0]       (1;2;3;4) // 
//             * c    := [0]       (-;-;-;-) // 
//             * b    := [1]   (-,-;-,-)   //
//             * </pre>
//             * 
//             * FIXME This test might need to be redone depending on how the
//             * logic proceeds. If we in fact split (a) only to discover that it
//             * is still full, then we will have a lot of empty siblings of (a)
//             * which are not present in the pre-/post- condition states above.
//             * If that turns out to be true, it would be best to expose the
//             * other structure modification methods on HTree to the test suite
//             * so we can directly invoke each of the operations required to make
//             * the necessary structural changes in turn while validating their
//             * pre-/post- conditions in depth. We can then write another test
//             * which uses the same insert sequence, but allows the htree logic
//             * to drive the structural changes.
//             * 
//             * FIXME Make sure that we validate the tuple redistribution logic
//             * in depth. The case of splitting (a) is a good one in the sense
//             * that we will get the wrong answer unless we recognizes a split of
//             * a full buddy bucket as distinct from doubling the size of an
//             * existing buddy bucket. [Both split a page, but in the latter #of
//             * buddy buckets does not change while in the former it does and we
//             * have to rehash to figure out which bucket gets which key.]
//             */
//            htree.addLevel(d/* oldParent */, 0/* buddyOffset */,
//                    1/* splitBits */, a/* oldChild */);
//
//
//            /*
//             * Verify post-conditions.
//             */
//
//            assertEquals("nnodes", 3, htree.getNodeCount());
//            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
//            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged
//
//            assertTrue(root == htree.getRoot());
//            final DirectoryPage f = (DirectoryPage) root.childRefs[1].get();
//            assertTrue(d == root.childRefs[0].get());
//            assertTrue(e == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertTrue(a == d.childRefs[0].get());
//            assertTrue(a == d.childRefs[1].get());
//            assertTrue(a == d.childRefs[2].get());
//            assertTrue(a == d.childRefs[3].get());
//            assertTrue(c == e.childRefs[0].get());
//            assertTrue(c == e.childRefs[1].get());
//            assertTrue(c == e.childRefs[2].get());
//            assertTrue(c == e.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(2, d.globalDepth);
//            assertEquals(2, e.globalDepth);
//            assertEquals(0, a.globalDepth);
//            assertEquals(0, c.globalDepth); 
//            assertEquals(1, b.globalDepth);

        } finally {

            store.destroy();
            
        }

    }

    /**
     * Unit test inserts a sequences of keys into an {@link HTree} having an
     * address space of TWO (2) bits.
     * <p>
     * Note: This test relies on the tests above to validate the preconditions
     * for the insert of the key sequences {1,2,3,4} and the introduction of a
     * new direction page in the {@link HTree} formed by inserting that key
     * sequence when another insert would be directed into a bucket page where
     * global depth == local depth.
     * 
     * @see bigdata/src/architecture/htree.xls#example1
     */
    public void test_example_addressBits2_insert_1_2_3_4_5_6_7_8_9_10() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k5 = new byte[]{0x05};
            final byte[] k6 = new byte[]{0x06};
            final byte[] k7 = new byte[]{0x07};
            final byte[] k8 = new byte[]{0x08};
            final byte[] k9 = new byte[]{0x09};
            final byte[] k10 = new byte[]{0x0a};
            final byte[] k20 = new byte[]{0x20};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v5 = new byte[]{0x05};
            final byte[] v6 = new byte[]{0x06};
            final byte[] v7 = new byte[]{0x07};
            final byte[] v8 = new byte[]{0x08};
            final byte[] v9 = new byte[]{0x09};
            final byte[] v10 = new byte[]{0x0a};
            final byte[] v20 = new byte[]{0x20};
            
            // a key which we never insert and which should never be found by lookup.
            final byte[] unused = new byte[]{-127};
            
            final HTree htree = new HTree(store, addressBits);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final DirectoryPage root = htree.getRoot();

            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);
            
            /*
             * Verify preconditions for the unit test.
             */
            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);
            // TODO Verify that the tuples we've inserted are all in (a).

            /**
             * 5. Insert 0x05 (5). This goes into the same buddy bucket (a). The
             * buddy bucket is full from the previous insert. It is the only
             * buddy bucket on the page (global depth == local depth). Therefore
             * we must introduce a new directory page which is a parent of that
             * buddy bucket in order to split it. The introduction of that
             * directory page is treated in depth by two other unit tests
             * (above).
             * 
             * The pre-condition state of the htree is:
             * 
             * <pre>
             * root := [2] (a,c,b,b)
             * a    := [2]   (1,2,3,4)
             * c    := [2]   (-,-,-,-)
             * b    := [1]   (-,-;-,-)
             * </pre>
             * 
             * The sequence of events when we attempt this insert is:
             * 
             * 1. The key would go into (a), but (a) is full.
             * 
             * 2. globalDepth(root) == localDepth(a), so we can not split (a).
             * 
             * 3. Introduce new directory page (d) as parent of (a).
             * 
             * The post-condition state is:
             * 
             * <pre>
             * root := [2] (d,d,b,b)     //
             * d    := [1]   (a,a;c,c)   // 
             * a    := [0]     (1,2;3,4) // 
             * c    := [0]     (-,-;-,-) // 
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             * 
             * 4. Retry the insert. The key is still directed into (a) (the
             * three bit prefix of the new key is all are zeros). (a) is still
             * full. However, depth(d):=1 GT depth(a):=0 so we split (a) into
             * (a,e) and rehash the tuples in (a), distributing them between (a)
             * and (e). However, the path prefix is not long enough so all
             * tuples are rehashed into (a).
             * 
             * FIXME PICK IT UP HERE. We need to incrementally split (d) to
             * increase its depth and then introduce a new directory page (above
             * (a) or below the root?) and keep going until we have enough
             * prefix bits to make a distinction when we rehash (a). This
             * can be worked out as more unit tests of splitting directories
             * until we get to the point where we can actually split (a).
             * 
             * The post-condition state is:
             * 
             * <pre>
             * root := [2] (d,d,b,b)     //
             * d    := [1]   (a,e;c,c)   // depth unchanged
             * a    := [1]     (1,_;2,_) // depth changes since now 1 ptr to (a)
             * e    := [1]     (3,_;4,_) // depth changes since now 1 ptr to (e)
             * c    := [0]     (-,-;-,-) // depth unchanged
             * b    := [1]   (-,-;-,-)   // depth unchanged
             * </pre>
             * 
             * 5. Retry the insert. The key goes into (a) and fills one of its
             * buddy buckets.
             * 
             * <pre>
             * root := [2] (d,d,b,b)     //
             * d    := [1]   (a,e;c,c)   // 
             * a    := [1]     (1,_;2,_) // 
             * e    := [1]     (3,_;4,_) // 
             * c    := [0]     (-,-;-,-) // 
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             */
            // Note: The test is assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);
            htree.insert(k4, v4);

//            assertEquals("nnodes", 2, htree.getNodeCount());
//            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
//            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged
//            htree.dump(Level.ALL, System.err, true/* materialize */);
//
//            assertTrue(root == htree.getRoot());
//            assertEquals(4, root.childRefs.length);
//            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
//            assertTrue(d == root.childRefs[0].get());
//            assertTrue(d == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertTrue(a == d.childRefs[0].get());
//            assertTrue(a == d.childRefs[1].get());
//            assertTrue(c == d.childRefs[2].get());
//            assertTrue(c == d.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(1, d.globalDepth);
//            assertEquals(0, a.globalDepth);
//            assertEquals(0, c.globalDepth);
//            assertEquals(1, b.globalDepth);
            
        } finally {
            
            store.destroy();
            
        }
        
    }

//  /**
//	 * Test of basic insert, lookup, and split page operations (including when a
//	 * new directory page must be introduced) using an address space with only
//	 * TWO (2) bits.
//	 * 
//	 * @see bigdata/src/architecture/htree.xls
//	 * 
//	 *      TODO Verify that we can store keys having more than 2 bits (or 4
//	 *      bits in a 4-bit address space) through a deeper hash tree.
//	 */
//	public void test_example_addressBits2_01x() {
//
//		final int addressBits = 2;
//
//		final IRawStore store = new SimpleMemoryRawStore();
//
//		try {
//
//			final HTree htree = new HTree(store, addressBits);
//
//			// Verify initial conditions.
//			assertTrue("store", store == htree.getStore());
//			assertEquals("addressBits", addressBits, htree.getAddressBits());
//
//			final DirectoryPage root = htree.getRoot();
//			assertEquals(4, root.childRefs.length);
//			final BucketPage a = (BucketPage) root.childRefs[0].get();
//			assertTrue(a == (BucketPage) root.childRefs[1].get());
//			assertTrue(a == (BucketPage) root.childRefs[2].get());
//			assertTrue(a == (BucketPage) root.childRefs[3].get());
//			assertEquals(2, root.getGlobalDepth());// starts at max.
//			assertEquals(0, a.getGlobalDepth());// starts at min.
//			
//			// verify preconditions.
//			assertEquals("nnodes", 1, htree.getNodeCount());
//			assertEquals("nleaves", 1, htree.getLeafCount());
//			assertEquals("nentries", 0, htree.getEntryCount());
//			htree.dump(Level.ALL, System.err, true/* materialize */);
//			assertEquals(2, root.getGlobalDepth());
//			assertEquals(0, a.getGlobalDepth());
//			assertFalse(htree.contains(new byte[] { 0x01 }));
//			assertFalse(htree.contains(new byte[] { 0x02 }));
//			assertEquals(null,htree.lookupFirst(new byte[] { 0x01 }));
//			assertEquals(null,htree.lookupFirst(new byte[] { 0x02 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x01 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
//
//			/*
//			 * 1. Insert a tuple and verify post-conditions. The tuple goes into
//			 * an empty buddy bucket with a capacity of one.
//			 */
//			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
//			assertEquals("nnodes", 1, htree.getNodeCount());
//			assertEquals("nleaves", 1, htree.getLeafCount());
//			assertEquals("nentries", 1, htree.getEntryCount());
//			htree.dump(Level.ALL, System.err, true/* materialize */);
//			assertEquals(2, root.getGlobalDepth());
//			assertEquals(0, a.getGlobalDepth());
//			assertTrue(htree.contains(new byte[] { 0x01 }));
//			assertFalse(htree.contains(new byte[] { 0x02 }));
//			assertEquals(new byte[] { 0x01 }, htree
//					.lookupFirst(new byte[] { 0x01 }));
//			assertEquals(null,htree.lookupFirst(new byte[] { 0x02 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] { new byte[] { 0x01 } }, htree
//							.lookupAll(new byte[] { 0x01 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
//
//			/*
//			 * 2. Insert a duplicate key. Since the localDepth of the bucket
//			 * page is zero, each buddy bucket on the page can only accept one
//			 * entry and this will force a split of the buddy bucket. That means
//			 * that a new bucket page will be allocated, the pointers in the
//			 * parent will be updated to link in the new bucket page, and the
//			 * buddy buckets will be redistributed among the old and new bucket
//			 * page.
//			 * 
//			 * Note: We do not know the order of the tuples in the bucket so
//			 * lookupFirst() is difficult to test when there are tuples for the
//			 * same key with different values.
//			 */
//			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
//			assertEquals("nnodes", 1, htree.getNodeCount());
//			assertEquals("nleaves", 2, htree.getLeafCount());
//			assertEquals("nentries", 2, htree.getEntryCount());
//			htree.dump(Level.ALL, System.err, true/* materialize */);
//			assertTrue(root == htree.getRoot());
//			assertEquals(4, root.childRefs.length);
//			final BucketPage b = (BucketPage) root.childRefs[2].get();
//			assertTrue(a == (BucketPage) root.childRefs[0].get());
//			assertTrue(a == (BucketPage) root.childRefs[1].get());
//			assertTrue(b == (BucketPage) root.childRefs[2].get());
//			assertTrue(b == (BucketPage) root.childRefs[3].get());
//			assertEquals(2, root.getGlobalDepth());
//			assertEquals(1, a.getGlobalDepth());// localDepth has increased.
//			assertEquals(1, b.getGlobalDepth());// localDepth is same as [a].
//			assertTrue(htree.contains(new byte[] { 0x01 }));
//			assertFalse(htree.contains(new byte[] { 0x02 }));
//			assertEquals(new byte[] { 0x01 }, htree
//					.lookupFirst(new byte[] { 0x01 }));
//			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
//			AbstractBTreeTestCase.assertSameIterator(
//					//
//					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 } },
//					htree.lookupAll(new byte[] { 0x01 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
//
//			/*
//			 * 3. Insert another duplicate key. This forces another split.
//			 * 
//			 * TODO Could check the #of entries on a bucket page and even in
//			 * each bucket of the bucket pages.
//			 */
//			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
//			assertEquals("nnodes", 1, htree.getNodeCount());
//			assertEquals("nleaves", 3, htree.getLeafCount());
//			assertEquals("nentries", 3, htree.getEntryCount());
//			htree.dump(Level.ALL, System.err, true/* materialize */);
//			assertTrue(root == htree.getRoot());
//			assertEquals(4, root.childRefs.length);
//			final BucketPage c = (BucketPage) root.childRefs[1].get();
//			assertTrue(a == (BucketPage) root.childRefs[0].get());
//			assertTrue(c == (BucketPage) root.childRefs[1].get());
//			assertTrue(b == (BucketPage) root.childRefs[2].get());
//			assertTrue(b == (BucketPage) root.childRefs[3].get());
//			assertEquals(2, root.getGlobalDepth());
//			assertEquals(2, a.getGlobalDepth());// localDepth has increased.
//			assertEquals(1, b.getGlobalDepth());// 
//			assertEquals(2, c.getGlobalDepth());// localDepth is same as [a].
//			assertTrue(htree.contains(new byte[] { 0x01 }));
//			assertFalse(htree.contains(new byte[] { 0x02 }));
//			assertEquals(new byte[] { 0x01 }, htree
//					.lookupFirst(new byte[] { 0x01 }));
//			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
//			AbstractBTreeTestCase.assertSameIterator(
//			//
//					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
//							new byte[] { 0x01 } }, htree
//							.lookupAll(new byte[] { 0x01 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
//
//			/*
//			 * 4. Insert another duplicate key. The buddy bucket is now full
//			 * again. It is only the only buddy bucket on the page.
//			 * 
//			 * Note: At this point if we insert another duplicate key the page
//			 * size will be doubled because all keys on the page are duplicates
//			 * and there is only one buddy bucket on the page.
//			 * 
//			 * However, rather than test the doubling of the page size, this
//			 * example is written to test a split where global depth == local
//			 * depth, which will cause the introduction of a new directory page
//			 * in the htree.
//			 */
//			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
//			assertEquals("nnodes", 1, htree.getNodeCount());
//			assertEquals("nleaves", 3, htree.getLeafCount());
//			assertEquals("nentries", 4, htree.getEntryCount());
//			htree.dump(Level.ALL, System.err, true/* materialize */);
//			assertTrue(root == htree.getRoot());
//			assertEquals(4, root.childRefs.length);
//			assertTrue(a == (BucketPage) root.childRefs[0].get());
//			assertTrue(c == (BucketPage) root.childRefs[1].get());
//			assertTrue(b == (BucketPage) root.childRefs[2].get());
//			assertTrue(b == (BucketPage) root.childRefs[3].get());
//			assertEquals(2, root.getGlobalDepth());
//			assertEquals(2, a.getGlobalDepth());// localDepth has increased.
//			assertEquals(1, b.getGlobalDepth());// 
//			assertEquals(2, c.getGlobalDepth());// localDepth is same as [a].
//			assertTrue(htree.contains(new byte[] { 0x01 }));
//			assertFalse(htree.contains(new byte[] { 0x02 }));
//			assertEquals(new byte[] { 0x01 }, htree
//					.lookupFirst(new byte[] { 0x01 }));
//			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
//			AbstractBTreeTestCase.assertSameIterator(
//			//
//					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
//							new byte[] { 0x01 }, new byte[] { 0x01 } }, htree
//							.lookupAll(new byte[] { 0x01 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
//
//			/*
//			 * FIXME We MUST NOT decrease the localDepth of (a) since that would
//			 * place duplicate keys into different buckets (lost retrival).
//			 * Since (a) can not have its localDepth decreased, we need to have
//			 * localDepth(d=2) with one pointer to (a) to get localDepth(a:=2).
//			 * That makes this a very complicated example. It would be a lot
//			 * easier if we started with distinct keys such that the keys in (a)
//			 * could be redistributed. This case should be reserved for later
//			 * once the structural mutations are better understood.
//			 * 
//			 * 5. Insert 0x20. This key is directed into the same buddy bucket
//			 * as the 0x01 keys that we have been inserting. That buddy bucket
//			 * is already full and, further, it is the only buddy bucket on the
//			 * page. Since we are not inserting a duplicate key we can split the
//			 * buddy bucket (rather than doubling the size of the bucket).
//			 * However, since global depth == local depth (i.e., only one buddy
//			 * bucket on the page), this split will introduce a new directory
//			 * page. The new directory page basically codes for the additional
//			 * prefix bits required to differentiate the two distinct keys such
//			 * that they are directed into the appropriate buckets.
//			 * 
//			 * The new directory page (d) is inserted one level below the root
//			 * on the path leading to (a). The new directory must have a local
//			 * depth of ONE (1), since it will add a one bit distinction. Since
//			 * we know the global depth of the root and the local depth of the
//			 * new directory page, we solve for npointers := 1 << (globalDepth -
//			 * localDepth). This tells us that we will have 2 pointers in the
//			 * root to the new directory page.
//			 * 
//			 * The precondition state of root is {a,c,b,b}. Since we know that
//			 * we need npointers:=2 pointers to (d), this means that we will
//			 * copy the {a,c} references into (d) and replace those references
//			 * in the root with references to (d). The root is now {d,d,b,b}. d
//			 * is now {a,a;c,c}. Since d has a local depth of 1 and address bits
//			 * of 2, it is comprised of two buddy hash tables {a,a} and {c,c}.
//			 * 
//			 * Linking in (d) has also changes the local depths of (a) and (c).
//			 * Since they each now have npointers:=2, their localDepth is has
//			 * been reduced from TWO (2) to ONE (1) (and their transient cached
//			 * depth values must be either invalidated or recomputed). Note that
//			 * the local depth of (d) and its children (a,c)) are ONE after this
//			 * operation so if we force another split in (a) that will force a
//			 * split in (d).
//			 * 
//			 * Having introduced a new directory page into the hash tree, we now
//			 * retry the insert. Once again, the insert is directed into (a).
//			 * Since (a) is still full it is split. (d) is now the parent of
//			 * (a). Once again, we have globalDepth(d=1)==localDepth(a=1) so we
//			 * need to split (d). However, since localDepth(d=1) is less than
//			 * globalDepth(root=2) we can split the buddy hash tables in (d).
//			 * This will require us to allocate a new page (f) which will be the
//			 * right sibling of (d). There are TWO (2) buddy hash tables in (d).
//			 * They are now redistributed between (d) and (f). We also have to
//			 * update the pointers to (d) in the parent such that 1/2 of them
//			 * point to the new right sibling of (d). Since we have changed the
//			 * #of pointers to (d) (from 2 to 1) the local depth of (d) (and of
//			 * f) is now TWO (2). Since globalDepth(d=2) is greater than
//			 * localDepth(a=1) we can now split (a) into (a,e), redistribute the
//			 * tuples in the sole buddy page (a) between (a,e) and update the
//			 * pointers in (d) to (a,a,e,e).
//			 * 
//			 * Having split (d) into (d,f), we now retry the insert. This time
//			 * the insert is directed into (e). There is room in (e) (it is
//			 * empty) and the tuple is inserted without further mutation to the
//			 * structure of the hash tree.
//			 * 
//			 * TODO At this point we should also prune the buckets [b] and [c]
//			 * since they are empty and replace them with null references.
//			 * 
//			 * TODO The #of new directory levels which have to be introduced
//			 * here is a function of the #of prefix bits which have to be
//			 * consumed before a distinction can be made between the existing
//			 * key (0x01) and the new key (0x20). With an address space of 2
//			 * bits, each directory level examines the next 2-bits of the key.
//			 * The key (x20) was chosen since the distinction can be made by
//			 * adding only one directory level (the keys differ in the first 4
//			 * bits). [Do an alternative example which requires recursive splits
//			 * in order to verify that we reenter the logic correctly each time.
//			 * E.g., by inserting 0x02 rather than 0x20.]
//			 * 
//			 * TODO Do an example in which we explore an insert which introduces
//			 * a new directory level in a 3-level tree. This should be a driven
//			 * by a single insert so we can examine in depth how the new
//			 * directory is introduced and verify whether it is introduced below
//			 * the root or above the bucket. [I believe that it is introduced
//			 * immediately below below the root. Note that a balanced tree,
//			 * e.g., a B-Tree, introduces the new level above the root. However,
//			 * the HTree is intended to be unbalanced in order to optimize
//			 * storage and access times to the parts of the index which
//			 * correspond to unequal distributions in the hash codes.]
//			 */
////			htree.insert(new byte[] { 0x20 }, new byte[] { 0x20 });
////			assertEquals("nnodes", 2, htree.getNodeCount());
////			assertEquals("nleaves", 4, htree.getLeafCount());
////			assertEquals("nentries", 5, htree.getEntryCount());
////			htree.dump(Level.ALL, System.err, true/* materialize */);
////			assertTrue(root == htree.getRoot());
////			assertEquals(4, root.childRefs.length);
////			final DirectoryPage d = (DirectoryPage)root.childRefs[0].get();
////			assertTrue(d == (DirectoryPage) root.childRefs[0].get());
////			assertTrue(d == (DirectoryPage) root.childRefs[1].get());
////			assertTrue(b == (BucketPage) root.childRefs[2].get());
////			assertTrue(b == (BucketPage) root.childRefs[3].get());
////			assertEquals(4, d.childRefs.length);
////			final BucketPage e = (BucketPage)d.childRefs[1].get();
////			assertTrue(a == (BucketPage) d.childRefs[0].get());
////			assertTrue(e == (BucketPage) d.childRefs[1].get());
////			assertTrue(c == (BucketPage) d.childRefs[2].get());
////			assertTrue(c == (BucketPage) d.childRefs[3].get());
////			assertEquals(2, root.getGlobalDepth());
////			assertEquals(2, d.getGlobalDepth());
////			assertEquals(2, a.getGlobalDepth());// unchanged
////			assertEquals(2, e.getGlobalDepth());// same as [a].
////			assertEquals(1, b.getGlobalDepth());// unchanged.
////			assertEquals(2, c.getGlobalDepth());// unchanged.
////			assertTrue(htree.contains(new byte[] { 0x01 }));
////			assertFalse(htree.contains(new byte[] { 0x02 }));
////			assertEquals(new byte[] { 0x01 }, htree
////					.lookupFirst(new byte[] { 0x01 }));
////			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
////			AbstractBTreeTestCase.assertSameIterator(
////			//
////					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
////							new byte[] { 0x01 }, new byte[] { 0x01 } }, htree
////							.lookupAll(new byte[] { 0x01 }));
////			AbstractBTreeTestCase.assertSameIterator(//
////					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
//
//			// TODO REMOVE (or test suite for remove).
//			
//			// TODO Continue progression here? 
//
//		} finally {
//
//			store.destroy();
//
//		}
//
//	}

    /*
     * TODO This might need to be modified to verify the sets of tuples in each
     * buddy bucket without reference to their ordering within the buddy bucket.
     * If so, then we will need to pass in the global depth of the bucket page
     * and clone and write new logic for the comparison of the leaf data state.
     */
    static void assertSameBucketData(ILeafData expected, ILeafData actual) {
        
        AbstractBTreeTestCase.assertSameLeafData(expected, actual);
        
    }
    
}
