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
 * Created on Nov 17, 2006
 */

package com.bigdata.btree;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Abstract test case for {@link BTree} tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeTestCase extends TestCase2 {

    protected Random r = new Random();

    protected IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);
    
    /**
     * Encodes an integer as a unsigned byte[] key.
     * 
     * @param v
     *            An integer.
     *            
     * @return The sort key for that integer.
     */
    protected byte[] i2k(int v) {
        
        return keyBuilder.reset().append(v).getKey();
        
    }

    /**
     * Logger for the test suites in this package.
     */
    protected static final Logger log = Logger.getLogger
    ( AbstractBTreeTestCase.class
      );

    /**
     * 
     */
    public AbstractBTreeTestCase() {
    }

    /**
     * @param name
     */
    public AbstractBTreeTestCase(String name) {
        super(name);
    }

    /**
     * Test helper verifies the #of keys and their ordered values.
     * 
     * @param expected
     *            A ground truth node.
     * @param actual
     *            The actual node.
     */
    public void assertKeys(AbstractNode expected,AbstractNode actual) {

        // verify the #of defined keys.
        assertEquals("nkeys", expected.keys.getKeyCount(), actual.keys.getKeyCount());
        assertEquals("nkeys", expected.nkeys, actual.nkeys);
        
        // verify ordered values for the defined keys.
        for( int i=0; i<expected.nkeys; i++ ) {

            assertEquals(0, BytesUtil.compareBytes(expected.keys.getKey(i),
                    actual.keys.getKey(i)));
            
        }
        
    }
    
    /**
     * Test helper provides backwards compatibility for a large #of tests that
     * were written with <code>int</code> keys. Each key is encoded by the
     * {@link KeyBuilder} before comparison with the key at the corresponding
     * index in the node.
     * 
     * @param keys
     *            An array of the defined <code>int</code> keys.
     * @param node
     *            The node whose keys will be tested.
     */
    public void assertKeys(int[] keys, AbstractNode node) {
        
//        // verify the capacity of the keys[] on the node.
//        assertEquals("keys[] capacity", (node.maxKeys + 1) * stride,
//                actualKeys.length);
        
        final int nkeys = keys.length;
        
        // verify the #of defined keys.
        assertEquals("nkeys", nkeys, node.nkeys);
        
        // verify ordered values for the defined keys.
        for( int i=0; i<nkeys; i++ ) {

            byte[] expectedKey = keyBuilder.reset().append(keys[i]).getKey();
            
            byte[] actualKey = node.keys.getKey(i);
            
            if(BytesUtil.compareBytes(expectedKey, actualKey)!=0) {

                fail("keys[" + i + "]: expected="
                        + BytesUtil.toString(expectedKey) + ", actual="
                        + BytesUtil.toString(actualKey));
                
            }
            
        }
        
//        // verify the undefined keys are all NEGINF.
//        for (int i = nkeys * stride; i < actualKeys.length; i++) {
//
//            assertEquals("keys[" + i + "]", (byte) 0, actualKeys[i]);
//
//        }
        
    }

    /**
     * Test helper verifies the #of values, their ordered values, and that all
     * values beyond the last defined value are <code>null</code>.
     * 
     * @param msg
     *            A label, typically the node name.
     * @param values
     *            An array containing the expected defined values. The #of
     *            values in this array should be exactly the #of defined values
     *            (that is, do not include trailing nulls or attempt to size the
     *            array to the branching factor of the tree).
     */
    public void assertValues(String msg, Object[] values, Leaf leaf ) {
        
        assert values != null;
        
        int nvalues = values.length;
        
        if( msg == null ) {
            
            msg = "";
            
        }

        // verify the capacity of the values[] on the node.
        assertEquals(msg+"values[] capacity", leaf.maxKeys+1, leaf.values.length );
        
        // verify the #of defined values (same as the #of defined keys).
        assertEquals(msg+"nvalues", nvalues, leaf.nkeys);
        
        // verify ordered values for the defined values.
        for( int i=0; i<nvalues; i++ ) {

            assertEquals(msg+"values["+i+"]", values[i], leaf.values[i]);
            
        }
        
        // verify the undefined values are all null.
        for( int i=nvalues; i<leaf.values.length; i++ ) {
            
            assertEquals(msg+"values["+i+"]", null, leaf.values[i]);
            
        }
        
    }

    public void assertValues(Object[] values, Leaf leaf ) {
        
        assertValues("",values,leaf);
        
    }
    
    public void assertSameNodeOrLeaf(AbstractNode n1, AbstractNode n2 ) {
        
        if( n1 == n2 ) return;
        
        if( n1.isLeaf() && n2.isLeaf() ) {
            
            assertSameLeaf((Leaf)n1,(Leaf)n2);
            
        } else if( !n1.isLeaf() && !n2.isLeaf() ) {
            
            assertSameNode((Node)n1,(Node)n2);

        } else {
            
            fail("Expecting two nodes or two leaves, but not a node and a leaf");
            
        }
        
    }

    /**
     * Compares two nodes (or leaves) for the same data.
     * 
     * @param n1
     *            The expected node state.
     * @param n2
     *            The actual node state.
     */
    public void assertSameNode(Node n1, Node n2 ) {

        if( n1 == n2 ) return;
        
        assertEquals("index",n1.btree,n2.btree);
        
        assertEquals("dirty", n1.isDirty(), n2.isDirty());

        assertEquals("persistent", n1.isPersistent(), n2.isPersistent());
        
        if (n1.isPersistent()) {
            
            assertEquals("id", n1.getIdentity(), n2.getIdentity());
            
        }
        
        assertEquals("minKeys",n1.minKeys,n2.minKeys);
        
        assertEquals("maxKeys",n1.maxKeys,n2.maxKeys);
        
        assertEquals("branchingFactor",n1.branchingFactor,n2.branchingFactor);

//        assertEquals("nnodes",n1.nnodes,n2.nnodes);
//        
//        assertEquals("nleaves",n1.nleaves,n2.nleaves);
        
        assertEquals("nentries",n1.nentries,n2.nentries);

        assertEquals("nkeys",n1.nkeys,n2.nkeys);

        assertKeys(n1,n2);
        
        assertEquals("childAddr",n1.childAddr,n2.childAddr);

        assertEquals("childEntryCounts",n1.childEntryCounts,n2.childEntryCounts);

    }

    /**
     * Compares leaves for the same data.
     * 
     * @param n1
     *            The expected leaf state.
     * @param n2
     *            The actual leaf state.
     */
    public void assertSameLeaf(Leaf n1, Leaf n2 ) {

        if( n1 == n2 ) return;
        
        assertEquals("index",n1.btree,n2.btree);
        
        assertEquals("dirty", n1.isDirty(), n2.isDirty());

        assertEquals("persistent", n1.isPersistent(), n2.isPersistent());
        
        if (n1.isPersistent()) {
            
            assertEquals("id", n1.getIdentity(), n2.getIdentity());
            
        }

        assertEquals("minKeys",n1.minKeys,n2.minKeys);
        
        assertEquals("maxKeys",n1.maxKeys,n2.maxKeys);
        
        assertEquals("branchingFactor",n1.branchingFactor,n2.branchingFactor);
        
        assertEquals("first", n1.nkeys, n2.nkeys);

        assertKeys(n1, n2);

        assertEquals("hasDeleteMarkers", n1.hasDeleteMarkers(), n2
                .hasDeleteMarkers());

        if (n1.hasDeleteMarkers()) {

            for (int i = 0; i < n1.nkeys; i++) {

                assertEquals("deleteMarkers[" + i + "]", n1.getDeleteMarker(i),
                        n2.getDeleteMarker(i));

            }

        }

        assertEquals("hasVersionTimestamps", n1.hasVersionTimestamps(), n2
                .hasVersionTimestamps());

        if (n1.hasVersionTimestamps()) {

            for (int i = 0; i < n1.nkeys; i++) {

                assertEquals("versionTimestamps[" + i + "]", n1
                        .getVersionTimestamp(i), n2.getVersionTimestamp(i));
                
            }
            
        }
        
        for (int i = 0; i < n1.nkeys; i++) {

            assertEquals("values[" + i + "]", n1.values[i], n2.values[i]);
            
        }
        
    }

    /**
     * Special purpose helper used to vet {@link Node#childAddr}.
     * 
     * @param childAddr
     *            An array all of whose values will be tested against the
     *            corresponding child identities in the node.
     * @param node
     *            The node.
     */
    public void assertChildKeys(long[] childKeys, Node node ) {
        
        int nChildKeys = childKeys.length;
        
        long[] actualKeys = node.childAddr;
        
        // verify the capacity of the childAddr[] on the node.
        assertEquals("childAddr[] capacity", node.branchingFactor+1, actualKeys.length );
        
        // verify the #of defined keys.
        assertEquals("nChildKeys", nChildKeys, node.nkeys+1);
        
        // verify ordered values for the defined keys.
        for( int i=0; i<nChildKeys; i++ ) {

            assertEquals("childAddr["+i+"]", childKeys[i], actualKeys[i]);
            
        }
        
        // verify the undefined keys are all NULL.
        for( int i=nChildKeys; i<actualKeys.length; i++ ) {
            
            assertEquals("childAddr[" + i + "]", IIdentityAccess.NULL, actualKeys[i]);
            
        }
        
    }

    /**
     * Validate the keys in the node.
     * 
     * @param keys
     *            An array all of whose entries will be tested against the
     *            corresponding keys in the node.
     * @param node
     *            The node.
     */
    public void assertKeys(byte[][] keys, AbstractNode node ) {
        
//        // verify the capacity of the keys[] on the node.
//        assertEquals("keys[] capacity", (node.maxKeys + 1) * stride,
//                actualKeys.length);
        
        // verify the #of defined keys.
        assertEquals("nkeys", keys.length, node.nkeys);
        assertEquals("nkeys", keys.length, node.keys.getKeyCount());
        
        // verify ordered values for the defined keys.
        for( int i=0; i<keys.length; i++ ) {

            if( BytesUtil.compareBytes(keys[i], node.keys.getKey(i)) != 0) {
                
                fail("expected=" + BytesUtil.toString(keys[i]) + ", actual="
                        + BytesUtil.toString(node.keys.getKey(i)));
                
            }
            
        }
        
//        // verify the undefined keys are all NEGINF.
//        for (int i = node.nkeys * stride; i < actualKeys.length; i++) {
//
//            assertEquals("keys[" + i + "]", (int) 0, actualKeys[i]);
//
//        }
        
    }

    /**
     * Special purpose helper used to vet the per-child entry counts for an
     * {@link INodeData}.
     * 
     * @param expected
     *            An array all of whose values will be tested against the
     *            corresponding elements in the node as returned by
     *            {@link INodeData#getChildEntryCounts()}. The sum of the
     *            expected array is also tested against the value returned by
     *            {@link IAbstractNodeData#getEntryCount()}.
     * @param node
     *            The node.
     */
    public void assertEntryCounts(int[] expected, INodeData node ) {
        
        int len = expected.length;
        
        int[] actual = (int[]) node.getChildEntryCounts();
        
        // verify the capacity of the keys[] on the node.
        assertEquals("childEntryCounts[] capacity", node.getBranchingFactor()+1, actual.length );
        
        // verify the #of defined elements.
        assertEquals("nchildren", len, node.getChildCount());
        
        // verify defined elements.
        int nentries = 0;
        for( int i=0; i<len; i++ ) {

            assertEquals("childEntryCounts["+i+"]", expected[i], actual[i]);
            
            nentries += expected[i];
            
        }
        
        // verify total #of spanned entries.
        assertEquals("nentries",nentries,node.getEntryCount());
        
        // verify the undefined keys are all ZERO(0).
        for( int i=len; i<actual.length; i++ ) {
            
            assertEquals("keys[" + i + "]", 0, actual[i]);
            
        }
        
    }

    /**
     * Return a new btree backed by a simple transient store that will NOT evict
     * leaves or nodes onto the store. The leaf cache will be large and cache
     * evictions will cause exceptions if they occur. This provides an
     * indication if cache evictions are occurring so that the tests of basic
     * tree operations in this test suite are known to be conducted in an
     * environment without incremental writes of leaves onto the store. This
     * avoids copy-on-write scenarios and let's us test with the knowledge that
     * there should always be a hard reference to a child or parent.
     * 
     * The {@link SimpleLeafSplitPolicy} is used.
     * 
     * @param branchingFactor
     *            The branching factor.
     */
    public BTree getBTree(int branchingFactor) {
        
        IRawStore store = new SimpleMemoryRawStore();

        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBranchingFactor(branchingFactor);

        // override the BTree class.
        metadata.setClassName(NoEvictionBTree.class.getName());
        
        return (NoEvictionBTree) BTree.create(store,metadata);
        
//        BTree btree = new BTree(store, //
//                branchingFactor, //
//                UUID.randomUUID(),//
//                false, //isolatable
//                null,//conflictResolver
//                new HardReferenceQueue<PO>(new NoEvictionListener(),
//                        leafQueueCapacity, nscan),
//                KeyBufferSerializer.INSTANCE,
//                ByteArrayValueSerializer.INSTANCE,
//                null // no record compressor
//        );
//
//        return btree;
        
    }
    
    /**
     * Specifies a {@link NoEvictionListener}.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class NoEvictionBTree extends BTree {

        /**
         * @param store
         * @param checkpoint
         * @param metadata
         */
        public NoEvictionBTree(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {
         
            super(store, checkpoint, metadata);
            
        }
        
        protected HardReferenceQueue<PO> newWriteRetentionQueue() {

            return new HardReferenceQueue<PO>(//
                    new NoEvictionListener(),//
                    10000,//
                    10//
            );

        }
        
    }
    
//    /**
//     * <p>
//     * Unit test for the {@link #getRandomKeys(int, int, int)} test helper. The
//     * test verifies fence posts by requiring the randomly generated keys to be
//     * dense in the target array. The test checks for several different kinds of
//     * fence post errors.
//     * </p>
//     * 
//     * <pre>
//     *   nkeys = 6;
//     *   min   = 1; (inclusive)
//     *   max   = min + nkeys = 7; (exclusive)
//     *   indices : [ 0 1 2 3 4 5 ]
//     *   keys    : [ 1 2 3 4 5 6 ]
//     * </pre>
//     */
//    public void test_randomKeys() {
//        
//        /*
//         * Note: You can raise or lower this value to increase the probability
//         * of triggering a fence post error. In practice, I have found that a
//         * fence post was reliably triggered at keys = 6.  After debugging, I
//         * then raised the #of keys to be generated to increase the likelyhood
//         * of triggering a fence post error.
//         */
//        final int nkeys = 20;
//        
//        final int min = 1;
//        
//        final int max = min + nkeys;
//        
//        final int[] keys = getRandomKeys(nkeys,min,max);
//        
//        assertNotNull( keys );
//        
//        assertEquals(nkeys,keys.length);
//        
//        System.err.println("keys  : "+Arrays.toString(keys));
//
//        Arrays.sort(keys);
//        
//        System.err.println("sorted: "+Arrays.toString(keys));
//        
//        // first key is the minimum value (the min is inclusive).
//        assertEquals(min,keys[0]);
//
//        // last key is the maximum minus one (since the max is exclusive).
//        assertEquals(max-1,keys[nkeys-1]);
//        
//        for( int i=0; i<nkeys; i++ ) {
//
//            // verify keys in range.
//            assertTrue( keys[i] >= min );
//            assertTrue( keys[i] < max );
//            
//            if( i > 0 ) {
//                
//                // verify monotonically increasing.
//                assertTrue( keys[i] > keys[i-1]);
//
//                // verify dense.
//                assertEquals( keys[i], keys[i-1]+1);
//                
//            }
//            
//        }
//        
//    }
//    
//    /**
//     * Test helper produces a set of distinct randomly selected external keys.
//     */
//    public int[] getRandomKeys(int nkeys) {
//        
//        return getRandomKeys(nkeys,Node.NEGINF+1,Node.POSINF);
//        
//    }
//    
//    /**
//     * <p>
//     * Test helper produces a set of distinct randomly selected external keys in
//     * the half-open range [fromKey:toKey).
//     * </p>
//     * <p>
//     * Note: An alternative to generating random keys is to generate known keys
//     * and then generate a random permutation of the key order.  This technique
//     * works well when you need to permutate the presentation of keys and values.
//     * See {@link TestCase2#getRandomOrder(int)}.
//     * </p>
//     * 
//     * @param nkeys
//     *            The #of keys to generate.
//     * @param fromKey
//     *            The smallest key value that may be generated (inclusive).
//     * @param toKey
//     *            The largest key value that may be generated (exclusive).
//     */
//    public int[] getRandomKeys(int nkeys,int fromKey,int toKey) {
//    
//        assert nkeys >= 1;
//        
//        assert fromKey > Node.NEGINF;
//        
//        assert toKey <= Node.POSINF;
//        
//        // Must be enough distinct values to populate the key range.
//        if( toKey - fromKey < nkeys ) {
//            
//            throw new IllegalArgumentException(
//                    "Key range too small to populate array" + ": nkeys="
//                            + nkeys + ", fromKey(inclusive)=" + fromKey
//                            + ", toKey(exclusive)=" + toKey);
//            
//        }
//        
//        final int[] keys = new int[nkeys];
//        
//        int n = 0;
//        
//        int tries = 0;
//        
//        while( n<nkeys ) {
//            
//            if( ++tries >= 100000 ) {
//                
//                throw new AssertionError(
//                        "Possible fence post : fromKey(inclusive)=" + fromKey
//                                + ", toKey(exclusive)=" + toKey + ", tries="
//                                + tries + ", n=" + n + ", "
//                                + Arrays.toString(keys));
//                
//            }
//            
//            final int key = r.nextInt(toKey - 1) + fromKey;
//
//            assert key >= fromKey;
//
//            assert key < toKey;
//
//            /*
//             * Note: This does a linear scan of the existing keys. We do NOT use
//             * a binary search since the keys are NOT sorted.
//             */
//            boolean exists = false;
//            for (int i = 0; i < n; i++) {
//                if (keys[i] == key) {
//                    exists = true;
//                    break;
//                }
//            }
//            if (exists) continue;
//
//            // add the key.
//            keys[n++] = key;
//            
//        }
//        
//        return keys;
//        
//    }

    /**
     * Test helper for {@link #test_splitRootLeaf_increasingKeySequence()}.
     * creates a sequence of keys in increasing order and inserts them into the
     * tree. Note that you do not know, in general, how many inserts it will
     * take to split the root node since the split decisions are path dependent.
     * They depend on the manner in which the leaves get filled, whether or not
     * holes are created in the leaves, etc.
     * 
     * Once all keys have been inserted into the tree the keys are removed in
     * forward order (from the smallest to the largest). This stresses a
     * specific pattern of joining leaves and nodes together with their right
     * sibling.
     * 
     * @param m
     *            The branching factor.
     * @param ninserts
     *            The #of keys to insert.
     */
    public void doSplitWithIncreasingKeySequence(BTree btree, int m,int ninserts) {
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a series of external keys in increasing order. When we
         * insert these keys in sequence, the result is that all inserts go into
         * the right-most leaf (this is the original leaf until the first split,
         * and is thereafter always a new leaf).
         */
        
        int[] keys = new int[ninserts];

        SimpleEntry[] entries = new SimpleEntry[ninserts];
        
        int lastKey = 1;
        
        for (int i = 0; i < ninserts; i++) {
        
            keys[i] = lastKey;
            
            entries[i] = new SimpleEntry();
            
            lastKey += 1;
        
        }

        /*
         * Do inserts.
         */
        
        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            final int ikey = keys[i];
            
            SimpleEntry entry = entries[i];
            
            if( i>0 && i % 10000 == 0 ) {
                
                System.err.println("i="+i+", key="+ikey);
                
            }

            assertEquals("#entries",i,btree.nentries);
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

//                System.err.println("Split: i=" + i + ", key=" + key
//                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(/*Level.DEBUG,*/System.err));

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(entries, btree.entryIterator());

        // remove keys in forward order.
        {
            
            for( int i=0; i<keys.length; i++ ) {
                
                byte[] key = KeyBuilder.asSortKey(keys[i]);
                
                assertEquals(entries[i],btree.lookup(key));
                assertEquals(entries[i],btree.remove(key));
                assertEquals(null,btree.lookup(key));
                
            }
            
        }
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

    }
    
    /**
     * Creates a sequence of keys in decreasing order and inserts them into the
     * tree. Note that you do not know, in general, how many inserts it will
     * take to split the root node since the split decisions are path dependent.
     * They depend on the manner in which the leaves get filled, whether or not
     * holes are created in the leaves, etc.
     * 
     * Once all keys have been inserted into the tree the keys are removed in
     * reverse order (from the largest to the smallest). This stresses a
     * specific pattern of joining leaves and nodes together with their left
     * sibling.
     * 
     * @param m
     *            The branching factor.
     * @param ninserts
     *            The #of keys to insert.
     */
    public void doSplitWithDecreasingKeySequence(BTree btree,int m, int ninserts) {

        log.info("m="+m+", ninserts="+ninserts);
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a series of external keys in decreasing order. When we
         * insert these keys in sequence, the result is that all inserts go into
         * the left-most leaf (the original leaf).
         */
        
        int[] keys = new int[ninserts];
        SimpleEntry[] entries = new SimpleEntry[ninserts];
        SimpleEntry[] reverseEntries = new SimpleEntry[ninserts];
        {
            int lastKey = ninserts;
            int reverseIndex = ninserts - 1;
            for (int i = 0; i < ninserts; i++) {
                keys[i] = lastKey;
                SimpleEntry entry = new SimpleEntry();
                entries[i] = entry;
                reverseEntries[reverseIndex--] = entry;
                lastKey -= 1;
            }
        }

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            final int ikey = keys[i];
            
            SimpleEntry entry = entries[i];
            
            if( i>0 && i%10000 == 0 ) {
            
                System.err.println("i="+i+", key="+ikey);
                
            }

            assertEquals("#entries",i,btree.nentries);
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

//                System.err.println("Split: i=" + i + ", key=" + key
//                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(reverseEntries, btree.entryIterator());

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(System.err));
        
        // remove keys in reverse order.
        {
        
            for( int i=0; i<keys.length; i++ ) {
                
                final byte[] key = KeyBuilder.asSortKey(keys[i]);
                
                assertEquals(entries[i],btree.lookup(key));
                assertEquals(entries[i],btree.remove(key));
                assertEquals(null,btree.lookup(key));

            }
            
        }
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

    }
    
    /**
     * Stress test helper inserts random permutations of keys into btrees of
     * order m for several different btrees, #of keys to be inserted, and
     * permutations of keys. Several random permutations of dense and sparse
     * keys are inserted. The #of keys to be inserted is also varied.
     */
    public void doSplitTest(int m, int trace) {
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m, trace);
            
            doInsertRandomSparseKeySequenceTest(m, m, trace);
            
        }
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m*m, trace);
            
            doInsertRandomSparseKeySequenceTest(m, m*m, trace);
            
        }
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m*m*m, trace);
            
            doInsertRandomSparseKeySequenceTest(m, m*m*m, trace);
            
        }
        
//        /*
//         * Try several permutations of the key-value presentation order.
//         */
//        for( int i=0; i<20; i++ ) {
//         
//            doInsertRandomKeySequenceTest(m, m*m*m*m, trace).getStore().close();
//
//            doInsertRandomSparseKeySequenceTest(m, m*m*m*m, trace).getStore().close();
//            
//        }

    }

    /**
     * Insert dense key-value pairs into the tree in a random order and verify
     * the expected entry traversal afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param ninserts
     *            The #of distinct key-value pairs to insert.
     * @param trace
     *            The trace level (zero disables most tracing).
     */
    public BTree doInsertRandomKeySequenceTest(int m, int ninserts, int trace) {

        /*
         * generate keys.  the keys are a dense monotonic sequence.
         */

        int keys[] = new int[ninserts];

        SimpleEntry entries[] = new SimpleEntry[ninserts];
        
        for( int i=0; i<ninserts; i++ ) {
        
            keys[i] = i+1; // Note: origin one.
            
            entries[i] = new SimpleEntry();
            
        }

        return doInsertRandomKeySequenceTest(m, keys, entries, trace);
        
    }

    /**
     * Insert a sequence of monotonically increase keys with random spacing into
     * a tree in a random order and verify the expected entry traversal
     * afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param ninserts
     *            The #of distinct key-value pairs to insert.
     * @param trace
     *            The trace level (zero disables most tracing).
     * 
     * @return The populated {@link BTree}.
     */
    public BTree doInsertRandomSparseKeySequenceTest(int m, int ninserts, int trace) {
        
        /*
         * generate random keys.  the keys are a sparse monotonic sequence.
         */
        int keys[] = new int[ninserts];

        SimpleEntry entries[] = new SimpleEntry[ninserts];
        
        int lastKey = 0;

        for( int i=0; i<ninserts; i++ ) {
        
            int key = r.nextInt(100)+lastKey+1;
            
            keys[i] = key;
            
            entries[i] = new SimpleEntry();
            
            lastKey = key;
            
        }

        return doInsertRandomKeySequenceTest(m, keys, entries, trace);
        
    }

    /**
     * Insert key value pairs into the tree in a random order and verify the
     * expected entry traversal afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param keys
     *            The keys.
     * @param entries
     *            The entries.
     * @param trace
     *            The trace level (zero disables most tracing).
     * 
     * @return The populated {@link BTree}.
     */
    public BTree doInsertRandomKeySequenceTest(int m, int[] keys,
            SimpleEntry[] entries, int trace) {

        return doInsertKeySequenceTest(m, keys, entries,
                getRandomOrder(keys.length), trace);

    }

    /**
     * Present a known sequence.
     * 
     * @param m
     *            The branching factor.
     * @param order
     *            The key presentation sequence.
     * @param trace
     *            The trace level.
     */
    public void doKnownKeySequenceTest(int m, int[] order, int trace) {

        int ninserts = order.length;
        
        int keys[] = new int[ninserts];

        SimpleEntry entries[] = new SimpleEntry[ninserts];
        
        for( int i=0; i<ninserts; i++ ) {
        
            keys[i] = i+1; // Note: origin one.
            
            entries[i] = new SimpleEntry();
            
        }

        doInsertKeySequenceTest(m, keys, entries, order, trace);

    }
    
    /**
     * Insert key value pairs into the tree in the specified order and verify
     * the expected entry traversal afterwards. If the test fails, then the
     * details necessary to recreate the test (m, ninserts, and the order[]) are
     * printed out.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param keys
     *            The keys.
     * @param entries
     *            The entries.
     * @param order
     *            The order in which the key-entry pairs will be inserted.
     * @param trace
     *            The trace level (zero disables most tracing).
     * 
     * @return The populated {@link BTree}.
     */
    protected BTree doInsertKeySequenceTest(int m, int[] keys, SimpleEntry[] entries, int[] order, int trace){

        BTree btree = getBTree(m);

        try {
            
            int lastLeafCount = btree.nleaves;

            for (int i = 0; i < keys.length; i++) {

                final int ikey = keys[order[i]];

                SimpleEntry entry = entries[order[i]];

                if( i>0 && i%10000 == 0 ) {
                    
                    log.info("index=" + i + ", key=" + ikey + ", entry="
                            + entry);
                    
                }

                assertEquals("#entries", i, btree.nentries);

                final byte[] key = KeyBuilder.asSortKey(ikey);
                
                assertNull(btree.lookup(key));

                if (trace >= 2) {

                    System.err.println("Before insert: index=" + i + ", key="
                            + key);
                    assertTrue(btree.dump(System.err));

                }

                btree.insert(key, entry);

                if (trace >= 2) {

                    System.err.println("After insert: index=" + i + ", key="
                            + key);
                    
                    assertTrue(btree.dump(System.err));

                }

                assertEquals(entry, btree.lookup(key));

                assertEquals("#entries", i + 1, btree.nentries);

                if (btree.nleaves > lastLeafCount) {

                    if (trace >= 1) {

                        System.err.println("Split: i=" + i + ", key=" + key
                                + ", nleaves=" + btree.nleaves);
                        
                    }

                    if (trace >= 1) {

                        System.err.println("After split: ");

                        assertTrue(btree.dump(System.err));

                    }

                    lastLeafCount = btree.nleaves;

                }

            }

            // Note: The height, #of nodes, and #of leaves is path dependent.
            assertEquals("#entries", keys.length, btree.nentries);

            assertTrue(btree.dump(System.err));

            /*
             * Verify entries in the expected order.
             */
            assertSameIterator(entries, btree.entryIterator());

            return btree;
            
        } catch (AssertionFailedError ex) {
            System.err.println("int m=" + m+";");
            System.err.println("int ninserts="+keys.length+";");
            System.err.print("int[] keys   = new   int[]{");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    System.err.print(", ");
                System.err.print(keys[order[i]]);
            }
            System.err.println("};");
            System.err.print("SimpleEntry[] vals = new SimpleEntry[]{");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    System.err.print(", ");
                System.err.print(entries[order[i]]);
            }
            System.err.println("};");
            System.err.print("int[] order  = new   int[]{");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    System.err.print(", ");
                System.err.print(order[i]);
            }
            System.err.println("};");
            throw ex;
        }
    }


    /**
     * Creates a sequence of dense keys in random order and inserts them into
     * the tree. Note that the split decision points are path dependent and can
     * not be predicated given random inserts.
     * 
     * @param m
     *            The branching factor.
     * 
     * @param ninserts
     *            The #of keys to insert.
     */
    public BTree doSplitWithRandomDenseKeySequence(BTree btree,int m, int ninserts) {

        log.info("m="+m+", ninserts="+ninserts);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a sequence of keys in increasing order and a sequence of
         * random indices into the keys (and values) that is used to present
         * the key-value pairs in random order to insert(key,value).
         */
        
        int[] keys = new int[ninserts];
        SimpleEntry[] entries = new SimpleEntry[ninserts];
        
        int lastKey = 1;
        for( int i=0; i<ninserts; i++) {
            keys[i] = lastKey;
            entries[i] = new SimpleEntry();
            lastKey+=1;
        }
        
        // Random indexing into the generated keys and values.
        int[] order = getRandomOrder(ninserts);

        try {
            doRandomKeyInsertTest(btree,keys,entries, order);
        }
        catch( AssertionError ex ) {
            System.err.println("m="+m);
            System.err.print("keys=[");
            for(int i=0; i<keys.length; i++ ) {
                if( i>0 ) System.err.print(", ");
                System.err.print(keys[order[i]]);
            }
            System.err.println("]");
            throw ex;
        }
        catch( AssertionFailedError ex ) {
            System.err.println("m="+m);
            System.err.print("keys=[");
            for(int i=0; i<keys.length; i++ ) {
                if( i>0 ) System.err.print(", ");
                System.err.print(keys[order[i]]);
            }
            System.err.println("]");
            throw ex;
        }

        log.info(btree.counters.toString());

        return btree;
        
    }

    protected void doRandomKeyInsertTest(BTree btree, int[] keys, SimpleEntry[] entries, int[] order ) {
        
        log.info("m="+btree.getBranchingFactor()+", nkeys="+keys.length);
        
        /*
         * Insert keys into the tree.
         */

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            final int ikey = keys[order[i]];
            
            SimpleEntry entry = entries[order[i]];
            
            if( i >0 && i%10000 == 0 ) {
            
                log.info("i="+i+", key="+ikey);
                
            }

            assertEquals("#entries",i,btree.nentries);

            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

//                System.err.println("Split: i=" + i + ", key=" + key
//                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        /*
         * Verify entries in the expected order. While we used a random
         * presentation order, the entries MUST now be in the original generated
         * order.
         */
        assertSameIterator(entries,btree.entryIterator());

        // Note: The height, #of nodes, and #of leaves are path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(Level.ERROR,System.err));
        
        log.info(btree.counters.toString());

    }


    /**
     * Stress test helper performs random inserts, removal and lookup operations
     * and compares the behavior of the {@link BTree} against ground truth as
     * tracked by a {@link TreeMap}.
     * 
     * Note: This test uses dense keys, but that is not a requirement.
     * 
     * @param m
     *            The branching factor
     * @param nkeys
     *            The #of distinct keys.
     * @param ntrials
     *            The #of trials.
     */
    public void doInsertLookupRemoveStressTest(int m,int nkeys,int ntrials) {
        
        log.info("m="+m+", nkeys="+nkeys+", ntrials="+ntrials);
        
        Integer[] keys = new Integer[nkeys];
        
        SimpleEntry[] vals = new SimpleEntry[nkeys];

        for( int i=0; i<nkeys; i++ ) {
            
            keys[i] = i+1; // Note: this produces dense keys with origin ONE(1).
            
            vals[i] = new SimpleEntry();
            
        }
        
        final BTree btree = getBTree(m);

        /*
         * Run test.
         */
        Map<Integer,SimpleEntry> expected = new TreeMap<Integer,SimpleEntry>();
        
        for( int i=0; i<ntrials; i++ ) {
            
            boolean insert = r.nextBoolean();
            
            int index = r.nextInt(nkeys);
            
            final Integer ikey = keys[index];
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            SimpleEntry val = vals[index];
            
            if( insert ) {
                
//                System.err.println("insert("+key+", "+val+")");
                SimpleEntry old = expected.put(ikey, val);
                
                SimpleEntry old2 = (SimpleEntry) SerializerUtil.deserialize(btree.insert(key, val));
                
                assertTrue(btree.dump(Level.ERROR,System.err));
                
                assertEquals(old, old2);

            } else {
                
//                System.err.println("remove("+key+")");
                SimpleEntry old = expected.remove(ikey);
                
                SimpleEntry old2 = (SimpleEntry) SerializerUtil.deserialize(btree.remove(key));
                
                assertTrue(btree.dump(Level.ERROR,System.err));
                
                assertEquals(old, old2);
                
            }

            if( i % 100 == 0 ) {

                /*
                 * Validate the keys and entries.
                 */
                
                assertEquals("#entries", expected.size(), btree.getEntryCount());
                
                Iterator<Map.Entry<Integer,SimpleEntry>> itr = expected.entrySet().iterator();
                
                while( itr.hasNext()) { 
                    
                    Map.Entry<Integer,SimpleEntry> entry = itr.next();
                    
                    final byte[] tmp = KeyBuilder.asSortKey(entry.getKey()); 
                    
                    assertEquals("lookup(" + entry.getKey() + ")", entry
                            .getValue(), btree.lookup(tmp));
                    
                }
                
            }
            
        }
        
        assertTrue( btree.dump(System.err) );
        
        log.info(btree.counters.toString());
    
    }

    /**
     * Stress test for building up a tree and then removing all keys in a random
     * order. The test populates a btree with enough keys to split the root leaf
     * at least once then verifies that delete correctly removes each keys and
     * any unused leaves and finally replaces the root node with a root leaf.
     * All inserted keys are eventually deleted by this test and the end state
     * is an empty btree of height zero(0) having a single root leaf.
     */
    public void doRemoveStructureStressTest(int m, int nkeys) {
        
        log.info("m="+m+", nkeys="+nkeys);
        
        BTree btree = getBTree(m);
        
        byte[][] keys = new byte[nkeys][];
        
        SimpleEntry[] vals = new SimpleEntry[nkeys];

        for( int i=0; i<nkeys; i++ ) {
            
            keys[i] = KeyBuilder.asSortKey(i+1); // Note: this produces dense keys with origin ONE(1).
            
            vals[i] = new SimpleEntry();
            
        }
        
        /*
         * populate the btree.
         */
        for( int i=0; i<nkeys; i++) {
            
            // lookup does not find key.
            assertNull(btree.insert(keys[i], vals[i]));
            
            // insert key and val.
            assertEquals(vals[i],btree.lookup(keys[i]));

            // reinsert finds key and returns existing value.
            assertEquals(vals[i],btree.insert(keys[i], vals[i]));

            assertEquals("size", i + 1, btree.getEntryCount());
            
        }
        
        /*
         * verify the total order.
         */
        assertSameIterator(vals, btree.entryIterator());
        
        assertTrue(btree.dump(Level.ERROR,System.out));
        
        /*
         * Remove the keys one by one, verifying that leafs are deallocated
         */
        
        int[] order = getRandomOrder(nkeys);
        
        for( int i=0; i<nkeys; i++ ) {

            final byte[] key = keys[order[i]];
            
            SimpleEntry val = vals[order[i]];
            
            //System.err.println("i="+i+", key="+key+", val="+val);
            
            // lookup finds the key, return the correct value.
            assertEquals("lookup("+key+")", val,btree.lookup(key));
            
            // remove returns the existing key.
            assertEquals("remove(" + key+")", val, btree.remove(key));
            
            // verify structure.
            assertTrue(btree.dump(Level.ERROR,System.out));

            // lookup no longer finds the key.
            assertNull("lookup("+key+")",btree.lookup(key));

        }
        
        /*
         * Verify the post-condition for the tree, which is an empty root leaf.
         * If the height, #of nodes, or #of leaves are reported incorrectly then
         * empty leaves probably were not removed from the tree or the root node
         * was not converted into a root leaf when the tree reached m entries.
         */
        assertTrue(btree.dump(Level.ERROR,System.out));
        assertEquals("#entries", 0, btree.nentries);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("height", 0, btree.height);
        log.info(btree.counters.toString());
        
    }
    
    /**
     * A suite of tests designed to verify that one btree correctly represents
     * the information present in a ground truth btree. The test verifies the
     * #of entries, key type, the {@link AbstractBTree#entryIterator()}, and
     * also both lookup by key and lookup by entry index. The height, branching
     * factor, #of nodes and #of leaves may differ (the test does not presume
     * that the btrees were built with the same branching factor, but merely
     * with the same data and key type).
     * 
     * @param expected
     *            The ground truth btree.
     * @param actual
     *            The btree that is being validated.
     */
    static public void assertSameBTree(AbstractBTree expected, IIndex actual) {

        assert expected != null;
        
        assert actual != null;
        
        // Must be the same "index".
        assertEquals("indexUUID", expected.getIndexMetadata().getIndexUUID(),
                actual.getIndexMetadata().getIndexUUID());
        
        // The #of entries must agree.
        assertEquals("entryCount", expected.getEntryCount(), actual
                .rangeCount(null,null));
        
        // verify the entry iterator.
//        doEntryIteratorTest(actual,expected);
        doEntryIteratorTest(expected,actual);
        
        /*
         * Extract the ground truth mapping from the input btree.
         */
        byte[][] keys = new byte[expected.getEntryCount()][];
        
        byte[][] vals = new byte[expected.getEntryCount()][];
        
        getKeysAndValues(expected, keys, vals);
        
        /*
         * Verify lookup against the segment with random keys choosen from
         * the input btree. This vets the separatorKeys. If the separator
         * keys are incorrect then lookup against the index segment will
         * fail in various interesting ways.
         */
        doRandomLookupTest("actual", actual, keys, vals);

        /*
         * Verify lookup by entry index with random keys. This vets the
         * childEntryCounts[] on the nodes of the generated index segment.
         * If the are wrong then this test will fail in various interesting
         * ways.
         */
        if(actual instanceof AbstractBTree) {

            doRandomIndexOfTest("actual", ((AbstractBTree)actual), keys, vals);
            
        }

        /*
         * Examine the btree for inconsistencies (we also examine the ground
         * truth btree for inconsistencies to be paranoid).
         */

        System.err.println("Examining expected tree for inconsistencies");
        assert expected.dump(System.err);

        if(actual instanceof AbstractBTree) {
            System.err.println("Examining actual tree for inconsistencies");
            assert ((AbstractBTree)actual).dump(System.err);
        }

    }
    
    /**
     * Compares the total ordering of a btree against the total ordering of a
     * ground truth btree.
     * <p>
     * Note: This uses the {@link AbstractBTree#entryIterator()} method. Due to
     * the manner in which that iterator is implemented, the iterator does NOT
     * rely on the separator keys. Therefore while this validates the total
     * order it does NOT validate that the index may be searched by key (or by
     * entry index).
     * 
     * @param expected
     *            The ground truth btree.
     * 
     * @param actual
     *            The btree to be tested.
     * 
     * @see #doRandomLookupTest(String, AbstractBTree, byte[][], Object[])
     * @see #doRandomIndexOfTest(String, AbstractBTree, byte[][], Object[])
     */
    static public void doEntryIteratorTest(AbstractBTree expected, IIndex actual ) {

        ITupleIterator expectedItr = expected.entryIterator();
        
        ITupleIterator actualItr = actual.rangeIterator(null,null);
        
        int index = 0;
        
        while( expectedItr.hasNext() ) {
            
            if( ! actualItr.hasNext() ) {
                
                fail("The iterator is not willing to visit enough entries");
                
            }
            
            ITuple expectedTuple = expectedItr.next();
            
            ITuple actualTuple = actualItr.next();
            
            byte[] expectedKey = expectedTuple.getKey();
            
            byte[] actualKey = actualTuple.getKey();

//            System.err.println("index="+index+", key="+actualKey+", val="+actualVal);
            
            try {
                
                assertEquals(expectedKey, actualKey);
                
            } catch (AssertionFailedError ex) {
                
                /*
                 * Lazily generate message.
                 */
                fail("Keys differ: index=" + index + ", expected="
                        + BytesUtil.toString(expectedKey) + ", actual="
                        + BytesUtil.toString(actualKey), ex);
                
            }


            if (expectedTuple.isDeletedVersion()) {

                assert actualTuple.isDeletedVersion();

            } else {

                byte[] expectedVal = expectedTuple.getValue();

                byte[] actualVal = actualTuple.getValue();

                try {

                    assertSameValue(expectedVal, actualVal);

                } catch (AssertionFailedError ex) {
                    /*
                     * Lazily generate message.
                     */
                    fail("Values differ: index="
                            + index
                            + ", key="
                            + BytesUtil.toString(expectedKey)
                            + ", expected="
                            + (expectedVal instanceof byte[] ? BytesUtil
                                    .toString((byte[]) expectedVal)
                                    : expectedVal)
                            + ", actual="
                            + (actualVal instanceof byte[] ? BytesUtil
                                    .toString((byte[]) actualVal) : actualVal),
                            ex);

                }

            }
            
            assertEquals("timestamps differ: index=" + index + ", key="
                    + BytesUtil.toString(expectedKey), expectedTuple
                    .getVersionTimestamp(), actualTuple.getVersionTimestamp());
            
            index++;
            
        }
        
        if( actualItr.hasNext() ) {
            
            fail("The iterator is willing to visit too many entries");
            
        }


    }

    /**
     * Extract all keys and values from the btree in key order.  The caller must
     * correctly dimension the arrays before calling this method.
     * 
     * @param btree
     *            The btree.
     * @param keys
     *            The keys in key order (out).
     * @param vals
     *            The values in key order (out).
     */
    static public void getKeysAndValues(AbstractBTree btree, byte[][] keys,
            byte[][] vals) {
        
        ITupleIterator itr = btree.entryIterator();

        int i = 0;
        
        while( itr.hasNext() ) {

            ITuple tuple= itr.next();
            
            byte[] val = tuple.getValue();
            
            byte[] key = tuple.getKey();

            assert val != null;
            
            assert key != null;
            
            keys[i] = key;
            
            vals[i] = val;
            
            i++;
            
        }
        
    }
    
    /**
     * Tests the performance of random {@link IIndex#lookup(Object)}s on the
     * btree. This vets the separator keys and the childAddr and/or childRef
     * arrays since those are responsible for lookup.
     * 
     * @param label
     *            A descriptive label for error messages.
     * 
     * @param btree
     *            The btree.
     * 
     * @param keys
     *            the keys in key order.
     * 
     * @param vals
     *            the values in key order.
     */
    static public void doRandomLookupTest(String label, IIndex btree, byte[][] keys, byte[][] vals) {
        
        int nentries = (int)btree.rangeCount(null,null);
        
        System.err.println("\ncondition: "+label+", nentries="+nentries);
        
        int[] order = getRandomOrder(nentries);

        long begin = System.currentTimeMillis();

        boolean randomOrder = true;
        
        for(int i=0; i<nentries; i++) {

            int entryIndex = randomOrder ? order[i] : i;
            
            byte[] key = keys[entryIndex];
        
            byte[] val = btree.lookup(key);
            
            if(val==null && true) {
                
                // Note: This exists only as a debug point.
                
                btree.lookup(key);
                
            }
            
            byte[] expectedVal = vals[entryIndex];

            assertEquals(expectedVal,val);
            
        }
 
        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println(label + " : tested " + nentries
                + " keys order in " + elapsed + "ms");
        
        System.err.println(label + " : " + btree.getStatistics());
         
    }

    /**
     * Tests the performance of random lookups of keys and values by entry
     * index. This vets the separator keys and childRef/childAddr arrays, which
     * are used to lookup the entry index for a key, and also vets the
     * childEntryCount[] array, since that is responsible for lookup by entry
     * index.
     * 
     * @param label
     *            A descriptive label for error messages.
     * 
     * @param btree
     *            The btree.
     * 
     * @param keys
     *            the keys in key order.
     * 
     * @param vals
     *            the values in key order.
     */
    static public void doRandomIndexOfTest(String label, AbstractBTree btree, byte[][] keys, byte[][] vals) {
        
        int nentries = btree.getEntryCount();
        
        System.err.println("\ncondition: "+label+", nentries="+nentries);
        
        int[] order = getRandomOrder(nentries);

        long begin = System.currentTimeMillis();

        boolean randomOrder = true;
        
        for(int i=0; i<nentries; i++) {
            
            int entryIndex = randomOrder ? order[i] : i;
            
            byte[] key = keys[entryIndex];
        
            assertEquals("indexOf",entryIndex,btree.indexOf(key));
            
            byte[] expectedVal = vals[entryIndex];

            assertEquals("keyAt",key,btree.keyAt(entryIndex));

            assertEquals("valueAt",expectedVal,btree.valueAt(entryIndex));
            
        }
 
        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println(label + " : tested " + nentries
                + " keys in " + elapsed + "ms");
        
        System.err.println(label + " : " + btree.counters);
         
    }
    
    /**
     * Method verifies that the <i>actual</i> {@link ITupleIterator} produces the
     * expected values in the expected order. Errors are reported if too few or
     * too many values are produced, etc.
     */
    static public void assertSameIterator(byte[][] expected, ITupleIterator actual) {

        assertSameIterator("", expected, actual);

    }

    /**
     * Method verifies that the <i>actual</i> {@link ITupleIterator} produces
     * the expected values in the expected order. Errors are reported if too few
     * or too many values are produced, etc.
     */
    static public void assertSameIterator(String msg, byte[][] expected,
            ITupleIterator actual) {

        int i = 0;

        while (actual.hasNext()) {

            if (i >= expected.length) {

                fail(msg + ": The iterator is willing to visit more than "
                        + expected.length + " values.");

            }

            ITuple tuple = actual.next();

            final byte[] val = tuple.getValue();

            if (expected[i] == null) {

                if (val != null) {

                    /*
                     * Only do message construction if we know that the assert
                     * will fail.
                     */
                    fail(msg + ": Different values at index=" + i
                            + ": expected=null" + ", actual="
                            + Arrays.toString(val));

                }

            } else {

                if (val == null) {

                    /*
                     * Only do message construction if we know that the assert
                     * will fail.
                     */
                    fail(msg + ": Different values at index=" + i
                            + ": expected=" + Arrays.toString(expected[i])
                            + ", actual=null");

                }
                
                if (BytesUtil.compareBytes(expected[i], val) != 0) {
                    
                    /*
                     * Only do message construction if we know that the assert
                     * will fail.
                     */
                    fail(msg + ": Different values at index=" + i
                            + ": expected=" + Arrays.toString(expected[i])
                            + ", actual=" + Arrays.toString(val));
                    
                }

            }
            
            i++;

        }

        if (i < expected.length) {

            fail(msg + ": The iterator SHOULD have visited " + expected.length
                    + " values, but only visited " + i + " values.");

        }

    }

}
