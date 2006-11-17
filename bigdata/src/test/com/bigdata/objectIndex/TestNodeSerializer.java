/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 30, 2006
 */

package com.bigdata.objectIndex;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.bigdata.journal.IRawStore;
import com.bigdata.journal.SlotMath;

/**
 * Test case for {@link NodeSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNodeSerializer extends AbstractObjectIndexTestCase {

    /**
     * 
     */
    public TestNodeSerializer() {
    }

    /**
     * @param arg0
     */
    public TestNodeSerializer(String arg0) {
        super(arg0);
    }

    /**
     * Prints out the offsets and various other sizing information for nodes and
     * leaves given the slotSize for the journal and the branching factor for
     * the object index.
     * 
     * @param slotSize
     *            The size of a slot on the journal.
     * @param branchingFactor
     *            The #of keys in a node of the object index (aka the branching
     *            factor).
     */
    public void showInfo(int slotSize,int branchingFactor) {

        final SlotMath slotMath = new SlotMath(slotSize);
        
        final NodeSerializer nodeSer = new NodeSerializer(slotMath);

        System.err.println("Shared record format:");

        System.err.println("slotSize : " + slotSize);
        
        System.err
                .println("pageSize : " + branchingFactor + " (aka branching factor)");
        
        System.err.println(" checksum: offset="
                + NodeSerializer.OFFSET_CHECKSUM + ", size="
                + NodeSerializer.SIZEOF_ADLER32);
        
        System.err.println(" nbytes  : offset=" + NodeSerializer.OFFSET_NBYTES
                + ", size=" + NodeSerializer.SIZEOF_NBYTES);

        System.err.println(" isLeaf  : offset=" + NodeSerializer.OFFSET_IS_LEAF
                + ", size=" + NodeSerializer.SIZEOF_IS_LEAF);
        
        System.err.println(" order   : offset=" + NodeSerializer.OFFSET_ORDER
                + ", size=" + NodeSerializer.SIZEOF_ORDER);

        System.err.println(" nkeys   : offset=" + NodeSerializer.OFFSET_NKEYS
                + ", size=" + NodeSerializer.SIZEOF_NKEYS);

        /*
         * a node
         */
        {

            int nkeys = branchingFactor - 1;
            int nchildren = branchingFactor;
            int keysSize = (nkeys * NodeSerializer.SIZEOF_KEY);
            int valuesSize = (nchildren * NodeSerializer.SIZEOF_NODE_VALUE);
            int offsetValues = NodeSerializer.OFFSET_KEYS + keysSize;
            
            System.err.println("Node specific record format:");
            
            System.err.println(" key[]   : offset="
                    + NodeSerializer.OFFSET_KEYS + ", size="
                    + NodeSerializer.SIZEOF_KEY + ", #keys=" + nkeys
                    + ", #bytes=" + keysSize);
            
            System.err.println(" value   : child node ref         ("
                    + NodeSerializer.SIZEOF_REF + ")");
            
            System.err.println(" value   : total node value       ("
                    + NodeSerializer.SIZEOF_NODE_VALUE + ")");
            
            System.err.println(" value[] : offset="
                    + offsetValues + ", size="
                    + NodeSerializer.SIZEOF_NODE_VALUE + ", #values="
                    + nchildren + ", #bytes="
                    + valuesSize );

            final int nodeSize = nodeSer.getSize(false, branchingFactor);

            final int slotsPerNode = slotMath.getSlotCount(nodeSize);

            System.err.println(" totals  : nodeSize=" + nodeSize
                    + ", slotsPerNode=" + slotsPerNode
                    + ", #bytesInThoseSlots=" + (slotsPerNode * slotSize)
                    + ", wastePerNode=" + (slotsPerNode * slotSize - nodeSize));

        }

        /*
         * a leaf
         */
        {
            // assume #of keys == branching factor.
            int nkeys = branchingFactor;
            int keysSize = (nkeys * NodeSerializer.SIZEOF_KEY);
            int valuesSize = (nkeys * NodeSerializer.SIZEOF_LEAF_VALUE);
            int offsetValues = NodeSerializer.OFFSET_KEYS + keysSize;

            System.err.println("Leaf specific record format:");
            
            System.err.println(" key[]   : offset="
                    + NodeSerializer.OFFSET_KEYS + ", size="
                    + NodeSerializer.SIZEOF_KEY + ", #keys=" + branchingFactor
                    + ", #bytes=" + (nkeys * NodeSerializer.SIZEOF_KEY));
            
            System.err.println(" value   : versionCounter         ("
                    + NodeSerializer.SIZEOF_VERSION_COUNTER + ")");
            
            System.err.println(" value   : currentVersion ref     ("
                    + NodeSerializer.SIZEOF_SLOTS + ")");
            
            System.err.println(" value   : preExistingVersion ref ("
                    + NodeSerializer.SIZEOF_SLOTS + ")");
            
            System.err.println(" value   : total leaf value       ("
                    + NodeSerializer.SIZEOF_LEAF_VALUE + ")");

            System.err.println(" value[] : offset=" + offsetValues + ", size="
                    + NodeSerializer.SIZEOF_LEAF_VALUE + ", #values=" + nkeys
                    + ", #bytes=" + valuesSize);

            final int leafSize = nodeSer.getSize(true, branchingFactor - 1);

            final int slotsPerLeaf = slotMath.getSlotCount(leafSize);

            System.err.println(" totals  : leafSize=" + leafSize
                    + ", slotsPerLeaf=" + slotsPerLeaf
                    + ", #bytesInThoseSlots=" + (slotsPerLeaf * slotSize)
                    + ", wastePerLeaf=" + (slotsPerLeaf * slotSize - leafSize));
        }
        
        /*
         * @todo Compute the #of nodes, leaves, and depth for an index for N
         * objects and the percentage of the store required by the index as
         * opposed to the objects. We need to make an assumption about the
         * object size to make this computation.
         */

//        // limits for purposes of computations.
//        final int MAX_HEIGHT = 5; // maximum tree height/depth.
//        final int MAX_ENTRIES = 100000; // maximum #of entries in the tree.
//        
//        int nodes_nm1 = 0;
//        
//        for( int height=0; height<MAX_HEIGHT; height++ ) {
//            
//            long nentries = (long) Math.pow(pageSize, height + 1);
//            
//            if( nentries > MAX_ENTRIES ) break;
//            
//            long nnodes = (long) Math.pow(pageSize,height);
//            
//            long nleaves = nnodes * pageSize; 
//            
//        }

    }

    /**
     * Show size info.
     */
    public void test_sizeInfo_slotSize64_pageSize512() {
        
        showInfo(64, 512);
        
    }
    
//    /**
//     * Show size info.
//     */
//    public void test_sizeInfo_slotSize256_pageSize1024() {
//        
//        showInfo(256,1024);
//        
//    }

    /**
     * Test of leaf serialization.
     */
    public void test_leaf_serialization01() {
        
        final int slotSize = 64;
        
        final SlotMath slotMath = new SlotMath(slotSize);
        
        final NodeSerializer nodeSer = new NodeSerializer(slotMath);
        
        final int branchingFactor = 8;
        IRawStore store = new SimpleStore();
        ObjectIndex ndx = new ObjectIndex(store, branchingFactor);
        
        // Create test node.
        final Leaf expected = getRandomLeaf(ndx,nodeSer);
        
        expected.dump(System.err);
        
        final Leaf actual = (Leaf)doRoundTripTest( true, ndx, nodeSer, expected );

        actual.dump(System.err);
        
    }

    /**
     * Test that an attempt to deserialize a leaf as a node will fail.
     */
    public void test_leaf_as_node_serialization_correct_rejection() {
        
        final int slotSize = 64;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        final NodeSerializer nodeSer = new NodeSerializer(slotMath);
        
        final int branchingFactor = 8;
        IRawStore store = new SimpleStore();
        ObjectIndex ndx = new ObjectIndex(store, branchingFactor);

        // Create test node.
        final Leaf expected = getRandomLeaf(ndx,nodeSer);

        /*
         * Serialize onto a buffer.
         */
        ByteBuffer buf = ByteBuffer.allocate(nodeSer.getSize(expected));
        
        nodeSer.putLeaf(buf, expected);
        
        /*
         * Attempt to read the buffer back as a node (vs a leaf).
         */
        
        buf.clear(); // prepare for reading.

        try {
            
            nodeSer.getNode(ndx, expected.getIdentity(), buf);
            
            fail("Expecting exception: "+RuntimeException.class);
            
        } catch( RuntimeException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * Test that an attempt to deserialize a node as a leaf will fail.
     */
    public void test_node_as_leaf_serialization_correct_rejection() {
        
        final int slotSize = 64;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        final NodeSerializer nodeSer = new NodeSerializer(slotMath);
        
        final int branchingFactor = 8;
        IRawStore store = new SimpleStore();
        ObjectIndex ndx = new ObjectIndex(store, branchingFactor);

        // Create test node.
        final Node expected = getRandomNode(ndx,nodeSer);

        /*
         * Serialize onto a buffer.
         */
        ByteBuffer buf = ByteBuffer.allocate(nodeSer.getSize(expected));
        
        nodeSer.putNode(buf, expected);
        
        /*
         * Attempt to read the buffer back as a leaf (vs a node).
         */
        
        buf.clear(); // prepare for reading.

        try {
            
            nodeSer.getLeaf(ndx, expected.getIdentity(), buf);
            
            fail("Expecting exception: "+RuntimeException.class);
            
        } catch( RuntimeException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * Test of node serialization.
     */
    public void test_node_serialization01() {
        
        final int slotSize = 64;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        final NodeSerializer nodeSer = new NodeSerializer(slotMath);

        final int branchingFactor = 8;
        IRawStore store = new SimpleStore();
        ObjectIndex ndx = new ObjectIndex(store, branchingFactor);

        // Create test node.
        final Node expected = getRandomNode(ndx, nodeSer);

        expected.dump(System.err);

        final Node actual = (Node)doRoundTripTest( true, ndx, nodeSer, expected);

        actual.dump(System.err);

    }
    
    /**
     * Round trip serialization test.
     * 
     * @param ndx
     *            The object index.
     * @param nodeSer
     *            The node serialization helper.
     * @param expected
     *            The node or leaf to be serialized.
     *            
     * @return The de-serialized node or leaf.
     */
    public AbstractNode doRoundTripTest(boolean verbose, ObjectIndex ndx,
            NodeSerializer nodeSer, AbstractNode expected) {

        final boolean isLeaf = expected.isLeaf();
        
        final int BUF_SIZE = nodeSer.getSize(expected);
        
        ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);
        
        if (isLeaf) {

            nodeSer.putLeaf(buf, (Leaf)expected);
            
        } else {
            
            nodeSer.putNode(buf, (Node)expected);
            
        }
        
        if (verbose)
            System.err.println("buf: " + Arrays.toString(buf.array()));
        
        buf.flip(); // prepare for reading.
        
        AbstractNode actual = nodeSer.getNodeOrLeaf(ndx,
                expected.getIdentity(), buf);

        if (verbose)
            actual.dump(System.err);

        // write on buf2.
        ByteBuffer buf2 = ByteBuffer.allocate(BUF_SIZE);

        if (isLeaf) {

            nodeSer.putLeaf(buf2, (Leaf)expected);
            
        } else {
            
            nodeSer.putNode(buf2, (Node)expected);
            
        }
        
        if (verbose)
            System.err.println("buf2: " + Arrays.toString(buf2.array()));
        
        // compare buffers - they should have images of the same node.
        assertEquals(buf.array(),buf2.array());

        /*
         * Overwrite the checksum to test for correct reporting of checksum
         * failure.
         */

        final int checksum = buf.getInt(NodeSerializer.OFFSET_CHECKSUM);
        
        int randomChecksum;
        
        do {
            
            randomChecksum = r.nextInt();
            
        } while( randomChecksum == checksum );
        
        buf.putInt(NodeSerializer.OFFSET_CHECKSUM, randomChecksum);
        
        buf.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx, expected.getIdentity(),buf);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose) 
                System.err.println("Ignoring expected exception: "+ex);
            
        }

        /*
         * Override the index of the first valid key in the node to test for a
         * checksum error.
         */

        final int nkeys = buf2.getShort(NodeSerializer.OFFSET_NKEYS);
        
        int randomNKeys;
        
        do {
            
            randomNKeys = r.nextInt(ndx.branchingFactor-1);
            
        } while( randomNKeys == nkeys );
        
        buf2.putShort(NodeSerializer.OFFSET_NKEYS,  (short) randomNKeys );
        
        buf2.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx,expected.getIdentity(),buf2);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose )
                System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        assertSameNodeOrLeaf(expected,actual);

        return actual;
        
    }
    
    /**
     * Small (de-)serialization stress test conducted for a variety of slot
     * sizes and branching factors.
     * 
     * @see #main(String[] args) for a large stress test.
     */
    public void testStress() {
     
        int ntrials = 10;
        int nnodes = 100;
        
        doStressTest( ntrials,nnodes);
        
    }

    /**
     * Run a stress test.
     * 
     * @param ntrials
     *            The #of trials. Each trial has a random slotSize and
     *            branchingFactor.
     * @param nnodes
     *            The #of random nodes per trial.
     */
    public void doStressTest(int ntrials,int nnodes) {

        // Some slot sizes to choose from.
        int[] slotSizes = new int[]{32,48,64,96,112,128,256,512,1024};
        
        // Some branching factors to choose from.
        int[] pageSizes = new int[]{8,16,32,48,64,96,112,128,256,512,1024};
        
        for (int trial = 0; trial < ntrials; trial++) {

            // Choose the slot size randomly.
            final int slotSize = slotSizes[r.nextInt(slotSizes.length)];
            
            final SlotMath slotMath = new SlotMath(slotSize);
            
            final NodeSerializer nodeSer = new NodeSerializer(slotMath);

            // Choose the branching factor randomly.
            final int pageSize = pageSizes[r.nextInt(pageSizes.length)];

            IRawStore store = new SimpleStore();

            ObjectIndex ndx = new ObjectIndex(store, pageSize);

            System.err.println("Trial " + trial + " of " + ntrials
                    + " : testing " + nnodes + " random nodes: slotSize="
                    + slotSize + ", branchingFactor=" + pageSize);
            
            for( int i=0; i<nnodes; i++ ) {
                
                AbstractNode expected = getRandomNodeOrLeaf(ndx, nodeSer);
                
                doRoundTripTest( false, ndx, nodeSer, expected);
                
            }
            
        }
        
    }
    
    /**
     * Run a large stress test.
     * 
     * @param args
     *            unused.
     */
    public static void main(String[] args) {

        final int NTRIALS = 100;
        final int NNODES = 1000;

        new TestNodeSerializer().doStressTest(NTRIALS, NNODES);
        
    }
    
}
