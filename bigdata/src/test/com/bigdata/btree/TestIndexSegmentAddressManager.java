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
 * Created on Dec 26, 2006
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Tests logic to encode and decode the offsets within regions in an
 * {@link IndexSegmentStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IndexSegment
 * @see IndexSegmentBuilder
 * @see IndexSegmentStore
 */
public class TestIndexSegmentAddressManager extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIndexSegmentAddressManager() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentAddressManager(String name) {
        super(name);
    }

    /**
     * Test works through the basic operations required to encode and decode
     * an address for a node and a leaf.
     * 
     * @see IndexSegmentRegion
     */
    public void test_bitMath() {

        // addr:=1, shift left by one, result is (2).
        assertEquals(2,1<<1);
        
        // set low bit to true to indicate a node (vs a leaf).
        assertEquals(3,2|1);

        // test low bit - indicates a node.
        assertTrue((3&1)==1);

        // test low bit - indicates a leaf.
        assertTrue((2&1)==0);
        
        // shift right one to recover the address (low bit discarded).
        assertEquals(1,2>>1);
        
        // shift right one to recover the address (low bit discarded).
        assertEquals(1,3>>1);
        
    }
    
    /**
     * Test encoding and decoding of the region code and the offset.
     */
    public void test_regionEnum_encodeDecode() {
        
        {
            
            final long expectedOffset = 12L;
        
            final long encodedOffset = IndexSegmentRegion.BASE.encodeOffset(expectedOffset);
            
            assertEquals(IndexSegmentRegion.BASE,IndexSegmentRegion.decodeRegion(encodedOffset));

            assertEquals(expectedOffset,IndexSegmentRegion.decodeOffset(encodedOffset));
            
        }

        {
            
            final long expectedOffset = 12L;
        
            final long encodedOffset = IndexSegmentRegion.NODE.encodeOffset(expectedOffset);
            
            assertEquals(IndexSegmentRegion.NODE,IndexSegmentRegion.decodeRegion(encodedOffset));

            assertEquals(expectedOffset,IndexSegmentRegion.decodeOffset(encodedOffset));
            
        }

        {
            
            final long expectedOffset = 12L;
        
            final long encodedOffset = IndexSegmentRegion.BLOB.encodeOffset(expectedOffset);
            
            assertEquals(IndexSegmentRegion.BLOB,IndexSegmentRegion.decodeRegion(encodedOffset));

            assertEquals(expectedOffset,IndexSegmentRegion.decodeOffset(encodedOffset));
            
        }

    }

    /**
     * Unit test verifies that an offset of <code>0L</code> (not a full
     * address, just an offset) is correctly encoded and decoded.
     */
    public void test_encodeDecode_offsetZero() {

        final long expectedOffset = 0L;

        final long encodedOffset = IndexSegmentRegion.BLOB
                .encodeOffset(expectedOffset);

        assertEquals(IndexSegmentRegion.BLOB, IndexSegmentRegion.decodeRegion(encodedOffset));

        assertEquals(expectedOffset, IndexSegmentRegion.decodeOffset(encodedOffset));

    }

    /**
     * Test of correct decoding of addresses by the
     * {@link IndexSegmentAddressManager}.
     */
    public void test_addressManager_decode() {

        /*
         * Note: allows records up to 64M in length.
         */
        final int offsetBits = WormAddressManager.SCALE_OUT_OFFSET_BITS;

        /*
         * Fake a checkpoint record. The only parts of this that we need are the
         * addresses of the nodes and blobs. Those addresses MUST be formed as
         * relative to the BASE region of the file. The offsets encoded within
         * those addresses will be used to decode addresses in the non-BASE
         * regions of the file.
         * 
         * Note: the checkpoint ctor has a variety of assertions so that
         * constrains how we can generate this fake checkpoint record.
         */
        final IndexSegmentCheckpoint checkpoint;
        final long offsetBase = 0L;
        final long offsetNodes;
        final long offsetBlobs;
        final int sizeNodes;
        final int sizeBlobs;
        {

            // Used to encode the addresses.
            WormAddressManager am = new WormAddressManager(offsetBits);

            final int sizeLeaves = 216;
            
            final long offsetLeaves = IndexSegmentCheckpoint.SIZE;
            
            final long addrLeaves = am.toAddr(sizeLeaves, IndexSegmentRegion.BASE
                    .encodeOffset(offsetLeaves));

            sizeNodes = 123;
            
            offsetNodes = offsetLeaves + sizeLeaves;
            
            final long addrNodes = am.toAddr(sizeNodes, IndexSegmentRegion.BASE
                    .encodeOffset(offsetNodes));

            // Note: only one node and it is the root, so addrRoot==addrNodes
            final long addrRoot = addrNodes;
            
            sizeBlobs = Bytes.megabyte32 * 20;
            
            offsetBlobs = offsetNodes + sizeNodes;
            
            final long addrBlobs = am.toAddr(sizeBlobs, IndexSegmentRegion.BASE
                    .encodeOffset(offsetBlobs));

            final long addrBloom = 0L;
            
            final int sizeMetadata = 712;
            
            final long offsetMetadata = offsetBlobs + sizeBlobs;

            final long addrMetadata = am.toAddr(sizeMetadata, IndexSegmentRegion.BASE
                    .encodeOffset(offsetMetadata));

            final long length = offsetMetadata + sizeMetadata; 
            
            checkpoint = new IndexSegmentCheckpoint(
                offsetBits,//
                1, // height
                2, // nleaves
                1, // nnodes
                29, // nentries
                128,// maxNodeOrLeafLength`
                addrLeaves,//
                addrNodes,//
                addrRoot,//
                addrMetadata,//
                addrBloom, //
                addrBlobs,//
                length,//
                UUID.randomUUID(),// segmentUUID,
                System.currentTimeMillis()//commitTime
                );
        
            System.err.println("Checkpoint: "+checkpoint);
            
        }

        /*
         * Used to decode the addresses.
         */
        IndexSegmentAddressManager am = new IndexSegmentAddressManager(checkpoint);
        
        final int nbytes = 12;
        final long offset = 44L;
        
        doRoundTripTest(IndexSegmentRegion.BASE, nbytes, offset, offsetBase + offset,
                am);

        doRoundTripTest(IndexSegmentRegion.NODE, nbytes, offset, offsetNodes + offset,
                am);

        doRoundTripTest(IndexSegmentRegion.BLOB, nbytes, offset, offsetBlobs + offset,
                am);

        /*
         * Now verify that range checks work within each region.
         */
        
        // legal.
        am.getOffset(am.toAddr(1, offsetNodes));
        am.getOffset(am.toAddr(sizeNodes-1, offsetNodes));
        am.getOffset(am.toAddr(sizeNodes, offsetNodes));
        // illegal.
        try {
            am.getOffset(am.toAddr(sizeNodes+1, offsetNodes));
        } catch(AssertionError ex) {
            log.info("Ignoring expected exception: "+ex);
        }

        // legal.
        am.getOffset(am.toAddr(1, offsetBlobs));
        am.getOffset(am.toAddr(sizeBlobs-1, offsetBlobs));
        am.getOffset(am.toAddr(sizeBlobs, offsetBlobs));
        // illegal.
        try {
            am.getOffset(am.toAddr(sizeBlobs+1, offsetBlobs));
        } catch(AssertionError ex) {
            log.info("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * Test helper forms an encoded address whose offset is relative to the
     * specified region and then attempts to decode that address.
     * 
     * @param region
     *            The region.
     * @param nbytes
     *            The #of bytes in the addressed record.
     * @param offset
     *            The offset of the addressed record (relative to the start of
     *            the region).
     * @param am
     *            The object used to decode the address.
     */
    protected static void doRoundTripTest(IndexSegmentRegion region, int nbytes,
            long offset, long expectedOffset, IndexSegmentAddressManager am) {
        
        final long addr = am.toAddr(nbytes, region.encodeOffset(offset));
        
        assertEquals("nbytes in " + region + " region", nbytes, am
                .getByteCount(addr));

        assertEquals("offset in " + region + " region", expectedOffset, am
                .getOffset(addr));
        
    }
    
}
