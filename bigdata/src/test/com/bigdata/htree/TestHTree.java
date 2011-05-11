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
import com.bigdata.htree.HTree.BucketPage;
import com.bigdata.htree.HTree.DirectoryPage;
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
	 * Test of basic insert, lookup, and split page operations (including when a
	 * new directory page must be introduced) using an address space with only
	 * TWO (2) bits.
	 * 
	 * @see bigdata/src/architecture/htree.xls
	 * 
	 *      TODO Verify that we can store keys having more than 2 bits (or 4
	 *      bits in a 4-bit address space) through a deeper hash tree.
	 */
	public void test_example_addressBits2_01() {

		final int addressBits = 2;

		final IRawStore store = new SimpleMemoryRawStore();

		try {

			final HTree htree = new HTree(store, addressBits);

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
			
			// verify preconditions.
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 1, htree.getLeafCount());
			assertEquals("nentries", 0, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertEquals(2, root.getGlobalDepth());
			assertEquals(0, a.getGlobalDepth());
			assertFalse(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(null,htree.lookupFirst(new byte[] { 0x01 }));
			assertEquals(null,htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 1. Insert a tuple and verify post-conditions. The tuple goes into
			 * an empty buddy bucket with a capacity of one.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 1, htree.getLeafCount());
			assertEquals("nentries", 1, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertEquals(2, root.getGlobalDepth());
			assertEquals(0, a.getGlobalDepth());
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertEquals(null,htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] { new byte[] { 0x01 } }, htree
							.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 2. Insert a duplicate key. Since the localDepth of the bucket
			 * page is zero, each buddy bucket on the page can only accept one
			 * entry and this will force a split of the buddy bucket. That means
			 * that a new bucket page will be allocated, the pointers in the
			 * parent will be updated to link in the new buck page, and the
			 * buddy buckets will be redistributed among the old and new bucket
			 * page.
			 * 
			 * Note: We do not know the order of the tuples in the bucket so
			 * lookupFirst() is difficult to test when there are tuples for the
			 * same key with different values.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
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
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(
					//
					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 } },
					htree.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 3. Insert another duplicate key. This forces another split.
			 * 
			 * TODO Could check the #of entries on a bucket page and even in
			 * each bucket of the bucket pages.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
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
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(
			//
					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
							new byte[] { 0x01 } }, htree
							.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));
			
			// TODO REMOVE (or test suite for remove).
			
			// TODO Continue progression here? 

		} finally {

			store.destroy();

		}

	}

	/**
	 * Work through a detailed example.
	 * 
	 * @see bigdata/src/architecture/htree.xls
	 */
	public void test_example_addressBits2_02() {

		fail("write test");

	}

	/**
	 * Work through a detailed example in which we have an elided bucket page
	 * or directory page because nothing has been inserted into that part of
	 * the address space.
	 */
	public void test_example_addressBits2_elidedPages() {

		fail("write test");

	}

}
