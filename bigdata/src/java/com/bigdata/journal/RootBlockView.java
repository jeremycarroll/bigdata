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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;

import com.bigdata.rawstore.Bytes;
import com.bigdata.util.TimestampFactory;

/**
 * A view onto a root block of the {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add checksum field to the root blocks and maintain it. we don't really
 *       need a magic for the root blocks if we use a checksum. or maybe it is
 *       [magic,checksum,[data]] with the timestamps inside of the checksumed
 *       data region.
 */
public class RootBlockView implements IRootBlockView {

    /**
     * The #of root ids. Their indices are [0:N-1].
     */
    static public transient final short MAX_ROOT_ADDRS       = 50;
    /**
     * The first root address that may be used for a user-defined object. User
     * defined root addresses begin at index 10. The first 10 root addresses are
     * reserved for use by the bigdata architecture.
     */
    static public transient final short FIRST_USER_ROOT      = 10;

    static final transient short SIZEOF_TIMESTAMP  = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_MAGIC      = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_SEGMENT_ID = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_ADDR       = Bytes.SIZEOF_LONG;
    static final transient short SIZEOF_OFFSET     = Bytes.SIZEOF_INT;
    static final transient short SIZEOF_UNUSED     = 1024; // Note: a chunk of reserved bytes.
    static final transient short SIZEOF_ROOT_ADDRS = SIZEOF_ADDR * MAX_ROOT_ADDRS;
    
//  static final transient short OFFSET_CHECKSUM   =  
    static final transient short OFFSET_TIMESTAMP0 = 0;
    static final transient short OFFSET_MAGIC      = OFFSET_TIMESTAMP0 + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_SEGMENT_ID = OFFSET_MAGIC      + SIZEOF_MAGIC;
    static final transient short OFFSET_NEXT_OFFSET= OFFSET_SEGMENT_ID  + SIZEOF_SEGMENT_ID;
    static final transient short OFFSET_FIRST_TX   = OFFSET_NEXT_OFFSET + SIZEOF_OFFSET;
    static final transient short OFFSET_LAST_TX    = OFFSET_FIRST_TX    + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_COMMIT_CTR = OFFSET_LAST_TX     + SIZEOF_TIMESTAMP;
    static final transient short OFFSET_UNUSED     = OFFSET_COMMIT_CTR  + Bytes.SIZEOF_LONG;
    static final transient short OFFSET_ROOT_ADDRS = OFFSET_UNUSED      + SIZEOF_UNUSED;
    static final transient short OFFSET_TIMESTAMP1 = OFFSET_ROOT_ADDRS  + SIZEOF_ROOT_ADDRS;
    static final transient short SIZEOF_ROOT_BLOCK = OFFSET_TIMESTAMP1  + SIZEOF_TIMESTAMP;

    /**
     * Magic value for root blocks.
     */
    final int MAGIC = 0x65fe21bc;
    
    private final ByteBuffer buf;
    
    private final boolean rootBlock0;
    
    public boolean isRootBlock0() {
        
        return rootBlock0;
        
    }
    
    /**
     * Create a new read-only root block image with a unique timestamp. The
     * other fields are populated from the supplied parameters.
     * 
     * @param segmentId
     *            The segment identifier for the journal.
     * @param nextOffset
     *            The next offset at which a record will be written on the
     *            store.
     * @param firstTxId
     *            The timestamp of the earliest transaction committed on the
     *            store or zero (0L) iff no transactions have committed on the
     *            store.
     * @param lastTxId
     *            The timestamp of the most recent transaction committed on the
     *            store or zero (0L) iff no transactions have committed on the
     *            store.
     * @param commitCounter
     *            The commit counter. This should be ZERO (0L) for a new
     *            journal. For an existing journal, the value should be
     *            incremented by ONE (1) each time the root block is written (as
     *            part of a commit naturally).
     * @param rootAddrs
     *            An array of root addresses.
     */
    RootBlockView(boolean rootBlock0, int segmentId, int nextOffset,
            long firstTxId, long lastTxId, long commitCounter, long[] rootAddrs) {

        if (nextOffset < 0)
            throw new IllegalArgumentException("nextOffset is negative.");
        if( firstTxId == 0L && lastTxId != 0L)
            throw new IllegalArgumentException("first transaction identifier is zero, but last transaction identifier is not.");
        if (firstTxId != 0 && lastTxId <= firstTxId)
            throw new IllegalArgumentException("last transaction identifier is less than first transaction identifier.");
        if (commitCounter < 0)
            throw new IllegalArgumentException("commit counter is zero.");
        if (commitCounter == Long.MAX_VALUE )
            throw new IllegalArgumentException("commit counter would overflow.");
        if( rootAddrs == null) 
            throw new IllegalArgumentException("roots are null.");
        if( rootAddrs.length != MAX_ROOT_ADDRS ) 
            throw new IllegalArgumentException("#of roots is wrong.");
        
        buf = ByteBuffer.allocate(SIZEOF_ROOT_BLOCK);
        
        this.rootBlock0 = rootBlock0;
        
        final long timestamp = TimestampFactory.nextNanoTime();

        buf.putLong(timestamp);
        buf.putInt(MAGIC);
        buf.putInt(segmentId);
        buf.putInt(nextOffset);
        buf.putLong(firstTxId);
        buf.putLong(lastTxId);
        buf.putLong(commitCounter);
        buf.position(buf.position()+SIZEOF_UNUSED); // skip unused region.
        for (int i = 0; i < MAX_ROOT_ADDRS; i++) { // root addrs.
            buf.putLong(rootAddrs[i]);
        }
        buf.putLong(timestamp);

        assert buf.limit() == SIZEOF_ROOT_BLOCK;

        buf.position(0);
        
    }

    /**
     * A read-only buffer whose contents are the root block.
     */
    public ByteBuffer asReadOnlyBuffer() {

        return buf;
        
    }
    
    /**
     * Create a new read-only view of the region of the supplied buffer from its
     * current position to its current limit.
     * 
     * @param rootBlock0
     *            There are two root blocks and they are written in an
     *            alternating order. For the sake of distinction, the first one
     *            is referred to as "rootBlock0" while the 2nd one is referred
     *            to as "rootBlock1". This parameter allows the caller to store
     *            a transient field on the view that indicates which root block
     *            it represents.
     * @param buf
     *            The buffer. If the buffer is modified in this region, those
     *            changes will be immediately reflected in the methods on the
     *            created {@link RootBlockView} object.
     * 
     * @exception IllegalArgumentException
     *                if the buffer is null or if the #of bytes remaining in the
     *                buffer is not exactly {@link #SIZEOF_ROOT_BLOCK}.
     * @exception RootBlockException
     *                if the root block is not valid (bad magic, timestamps do
     *                not agree, etc).
     */
    RootBlockView(boolean rootBlock0, ByteBuffer buf) throws RootBlockException {
        
        if( buf == null ) throw new IllegalArgumentException();
        
        if( buf.remaining() != SIZEOF_ROOT_BLOCK ) {
            
            throw new IllegalArgumentException("Expecting " + SIZEOF_ROOT_BLOCK
                    + " remaining, acutal=" + buf.remaining());
            
        }
        
        this.buf = buf.slice().asReadOnlyBuffer();
        
        this.rootBlock0 = rootBlock0;
        
        valid();
        
    }
    
    public int getNextOffset() {
        
        return buf.getInt(OFFSET_NEXT_OFFSET);
        
    }

    public long getFirstTxId() {
        
        return buf.getLong(OFFSET_FIRST_TX);
        
    }
    
    public long getLastTxId() {
        
        return buf.getLong(OFFSET_LAST_TX);
        
    }
    
    public long getTimestamp() throws RootBlockException {
        
        long timestamp0 = buf.getLong(OFFSET_TIMESTAMP0);
        
        long timestamp1 = buf.getLong(OFFSET_TIMESTAMP1);
        
        if( timestamp0 != timestamp1 ) {
            
            throw new RootBlockException("Timestamps differ: "+timestamp0 +" vs "+ timestamp1);
            
        }
        
        return timestamp0;
        
    }

    public long getRootAddr(int index) {
        
        if( index < 0 || index > MAX_ROOT_ADDRS ) {
            
            throw new IndexOutOfBoundsException("index=" + index
                    + " must be in [0:" + (MAX_ROOT_ADDRS - 1) + "]");
            
        }
        
        return buf.getLong(OFFSET_ROOT_ADDRS + index * SIZEOF_ADDR);
        
    }

    public long[] getRootAddrs() {
        
       long[] rootIds = new long[MAX_ROOT_ADDRS]; 
       
       for( int i=0; i<MAX_ROOT_ADDRS; i++ ) {
           
           rootIds[ i ] = buf.getLong(OFFSET_ROOT_ADDRS + i * SIZEOF_ADDR);
           
       }
       
       return rootIds;
       
    }
    
    public long getCommitCounter() {
        
        return buf.getLong(OFFSET_COMMIT_CTR);
        
    }
    
    public void valid() {
        
        int magic = buf.getInt(OFFSET_MAGIC);
        
        if( magic != MAGIC ) {

            throw new RuntimeException("MAGIC: expected="+MAGIC+", actual="+magic);
            
        }

        // test timestamps.
        getTimestamp();
        
    }

    public int getSegmentId() {

        return buf.getInt(OFFSET_SEGMENT_ID);
        
    }

}
