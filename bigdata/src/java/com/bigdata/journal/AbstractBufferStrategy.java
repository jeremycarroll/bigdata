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
package com.bigdata.journal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.io.FileChannelUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.resources.ResourceManager;

/**
 * Abstract base class for {@link IBufferStrategy} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractBufferStrategy extends AbstractRawWormStore implements IBufferStrategy {
    
    /**
     * Log for btree operations.
     */
    protected static final Logger log = Logger.getLogger(AbstractBufferStrategy.class);
    
    protected static final boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();
        
    /**
     * Text of the error message used when a {@link ByteBuffer} with zero bytes
     * {@link ByteBuffer#remaining()} is passed to {@link #write(ByteBuffer)}.
     */
    protected static final String ERR_BUFFER_EMPTY = "Zero bytes remaining in buffer";
    
    /**
     * Text of the error message used when a <code>null</code> reference is
     * provided for a {@link ByteBuffer}.
     */
    protected static final String ERR_BUFFER_NULL = "Buffer is null";

    /**
     * Text of the error message used when an address is given has never been
     * written. Since the journal is an append-only store, an address whose
     * offset plus record length exceeds the {@link #nextOffset} on which data
     * would be written may be easily detected.
     */
    protected static final String ERR_ADDRESS_NOT_WRITTEN = "Address never written.";
    
    /**
     * Text of the error message used when a ZERO (0L) is passed as an address
     * to {@link IRawStore#read(long)} or similar methods. This value 0L is
     * reserved to indicate a persistent null reference and may never be read.
     */
    protected static final String ERR_ADDRESS_IS_NULL = "Address is 0L";
    
    /**
     * Text of the error message used when an address provided to
     * {@link IRawStore#read(long)} or a similar method encodes a record length
     * of zero (0). Empty records are not permitted on write and addresses with
     * a zero length are rejected on read.
     */
    protected static final String ERR_RECORD_LENGTH_ZERO = "Record length is zero";
    
    /**
     * Text of the error message used when a write operation would exceed the
     * #of bytes that can be addressed by a {@link ByteBuffer} backed by an
     * array or native memory (both are limited to int32 bytes since they
     * are addressed by a Java <code>int</code>).
     */
    protected static final String ERR_INT32 = "Would exceed int32 bytes (not allowed unless backed by disk).";
    
    /**
     * Text of the error message used when
     * {@link IBufferStrategy#truncate(long)} would truncate data that has
     * already been written.
     */
    protected static final String ERR_TRUNCATE = "Would truncate written data.";
    
    /**
     * Error message used when the writes are not allowed.
     */
    protected static final String ERR_READ_ONLY = "Read only";

    /**
     * Error message used when the record size is invalid (e.g., negative).
     * 
     * @todo There is some overlap with {@link #ERR_RECORD_LENGTH_ZERO} and
     *       {@link #ERR_BUFFER_EMPTY}.
     */
    protected static final String ERR_BAD_RECORD_SIZE = "Bad record size";
    
    /**
     * Error message used when the store is closed. 
     */
    protected static final String ERR_NOT_OPEN = "Not open";
    
    /**
     * <code>true</code> iff the {@link IBufferStrategy} is open.
     */
    private volatile boolean open = false;

    /**
     * <code>true</code> iff the {@link IBufferStrategy} is read-only.
     */
    private boolean readOnly;

//    private final UUID storeUUID;
    
    protected final long initialExtent;
    protected final long maximumExtent;
    
    /**
     * The buffer strategy implemented by this class.
     */
    protected final BufferMode bufferMode;
    
    /**
     * The next offset at which a data item would be written on the store as an
     * offset into the <em>user extent</em> (offset zero(0) addresses the
     * first byte after the root blocks). This is updated each time a new record
     * is written on the store. On restart, the value is initialized from the
     * current root block. The current value is written as part of the new root
     * block during each commit.
     * <p>
     * Note: It is NOT safe to reload the current root block and therefore reset
     * this to an earlier offset unless all transactions are discarded. The
     * reason is that transactions may use objects (btrees) to provide
     * isolation. Those objects write on the store but do not register as
     * {@link ICommitter}s and therefore never make themselves restart safe.
     * However, you can not discard the writes of those objects unless the
     * entire store is being restarted, e.g., after a shutdown or a crash.
     */
    protected long nextOffset;

    static final NumberFormat cf;
    
    static {
        
        cf = NumberFormat.getIntegerInstance();
        
        cf.setGroupingUsed(true);
        
    }
    
//    final public UUID getUUID() {
//        
//        return storeUUID;
//        
//    }
    
    final public long getInitialExtent() {
        
        return initialExtent;
        
    }
    
    final public long getMaximumExtent() {
        
        return maximumExtent;
        
    }
    
    /**
     * The minimum amount to extend the backing storage when it overflows.
     */
    protected long getMinimumExtension() {
        
        return Bytes.megabyte * 32;
        
    }
    
    final public BufferMode getBufferMode() {

        return bufferMode;
        
    }

    final public long getNextOffset() {

        return nextOffset;
        
    }

    /**
     * (Re-)open a buffer.
     * 
     * @param storeUUID
     *            The UUID that identifies the owning {@link IRawStore}.
     * @param initialExtent
     *            - as defined by {@link #getInitialExtent()}
     * @param maximumExtent
     *            - as defined by {@link #getMaximumExtent()}.
     * @param offsetBits
     *            The #of bits that will be used to represent the byte offset in
     *            the 64-bit long integer addresses for the store. See
     *            {@link WormAddressManager}.
     * @param nextOffset
     *            The next offset within the buffer on which a record will be
     *            written. Note that the buffer begins _after_ the root blocks
     *            and offset zero is always the first byte in the buffer.
     * @param bufferMode
     *            The {@link BufferMode}.
     */
    AbstractBufferStrategy(
            // UUID storeUUID,
            long initialExtent, long maximumExtent, int offsetBits,
            long nextOffset, BufferMode bufferMode, boolean readOnly) {

        super(offsetBits);
        
        assert nextOffset >= 0;

        if (bufferMode == null)
            throw new IllegalArgumentException();

//        this.storeUUID = storeUUID;
        
        this.initialExtent = initialExtent;
        
        this.maximumExtent = maximumExtent; // MAY be zero!
        
        this.nextOffset = nextOffset;
        
        this.bufferMode = bufferMode;
        
        this.open = true;
        
        this.readOnly = readOnly;
        
    }
    
    public final long size() {
        
        return nextOffset;
        
    }

    protected final void assertOpen() {
        
        if (!open)
            throw new IllegalStateException(ERR_NOT_OPEN);
        
    }
    
    public boolean isOpen() {
        
        return open;
        
    }

    public boolean isReadOnly() {

        assertOpen();

        return readOnly;
        
    }
    
    /**
     * Manages the {@link #open} flag state.
     */
    public void close() {
        
        if (!open)
            throw new IllegalStateException();
        
        open = false;

    }

    final public void destroy() {
        
        if (open)
            close();

        deleteResources();
        
    }

    /**
     * Invoked if the store would exceed its current extent by
     * {@link #write(ByteBuffer)}. The default behavior extends the capacity of
     * the buffer by the at least the requested amount and a maximum of 32M or
     * the {@link Options#INITIAL_EXTENT}.
     * <p>
     * If the data are fully buffered, then the maximum store size is limited to
     * int32 bytes which is the maximum #of bytes that can be addressed in RAM
     * (the pragmatic maximum is slightly less than 2G due to the limits of the
     * JVM to address system memory).
     * 
     * @return true if the capacity of the store was extended and the write
     *         operation should be retried.
     */
    final public boolean overflow(final long needed) {

        final long userExtent = getUserExtent();
        
        final long required = userExtent + needed;
        
        if (required > Integer.MAX_VALUE && bufferMode.isFullyBuffered()) {
            
            /*
             * Would overflow int32 bytes and data are buffered in RAM.
             */

            log.error(ERR_INT32);
            
            return false;
            
        }

        if (maximumExtent != 0L && required > maximumExtent) {

            /*
             * Would exceed the maximum extent (iff a hard limit).
             * 
             * Note: this will show up for transactions that whose write set
             * overflows the in-memory buffer onto the disk.
             */

            if (WARN)
                log.warn("Would exceed maximumExtent=" + maximumExtent);

            return false;

        }

        /*
         * Increase by the initial extent or by 32M, whichever is greater, but
         * by no less that the requested amount.
         */
        long newExtent = userExtent
                + Math.max(needed, Math.max(initialExtent,
                                getMinimumExtension()));
        
        if (newExtent > Integer.MAX_VALUE && bufferMode.isFullyBuffered()) {

            /*
             * Do not allocate more than int32 bytes when using a buffered mode
             * (anything not backed by the disk).
             */

            newExtent = Integer.MAX_VALUE;

            if (newExtent - userExtent < needed) {

                /*
                 * Not enough room in memory for the requested extension.
                 */

                log.error(ERR_INT32);
                
                return false;
                
            }
            
        }

        /*
         * Extend the capacity.
         */
        truncate(newExtent);

        // report event.
        ResourceManager.extendJournal(getFile() == null ? null : getFile()
                .toString(), newExtent);
        
        // Retry the write operation.
        return true;
        
    }
    
    /**
     * Throws an exception if the extent is too large for an in-memory buffer.
     * 
     * @param extent
     *            The extent.
     * 
     * @return The extent.
     */
    static long assertNonDiskExtent(long extent) {

        if( extent > Integer.MAX_VALUE ) {
            
            /*
             * The file image is too large to address with an int32. This
             * rules out both the use of a direct buffer image and the use
             * of a memory-mapped file. Therefore, the journal must use a
             * disk-based strategy.
             */
           
            throw new RuntimeException("The extent requires the "
                    + BufferMode.Disk + " mode: extent=" + extent);
            
        }

        return extent;
        
    }

    /**
     * Helper method used by {@link DiskBackedBufferStrategy} and
     * {@link DiskOnlyStrategy} to implement
     * {@link IBufferStrategy#transferTo(RandomAccessFile)} using a
     * {@link FileChannel} to {@link FileChannel} transfer.
     * 
     * @param src
     *            The source.
     * @param out
     *            The output file.
     * 
     * @return The #of bytes transferred.
     * 
     * @throws IOException
     */
    static protected long transferFromDiskTo(final IDiskBasedStrategy src,
            final RandomAccessFile out) throws IOException {

        // We want everything after the file header.
        final long fromPosition = src.getHeaderSize();

        // #of bytes to transfer (everything in the user extent).
        final long count = src.getNextOffset();

        // the source channel.
        final FileChannel srcChannel = src.getRandomAccessFile().getChannel();
        
        // the output channel.
        final FileChannel outChannel = out.getChannel();
        
        // the current file position on the output channel.
        final long outPosition = outChannel.position();
        
//        final long outSize = outChannel.size();
//        
//        final long outRemaining = outSize - outPosition;

        /*
         * Transfer the user extent from the source channel onto the output
         * channel starting at its current file position. The output channel
         * will be transparently extended if necessary.
         * 
         * Note: this has a side-effect on the position for both the source and
         * output channels.
         * 
         * Note: If the last record on the source was allocated but not written
         * then the data transfer operation will fail since the source channel
         * will not have enough data.
         */
        FileChannelUtility.transferAll(srcChannel, fromPosition, count, out,
                outPosition);
        
        return count;

//        final long begin = System.currentTimeMillis();
//        
//        // the output channel.
//        final FileChannel outChannel = out.getChannel();
//        
//        // current position on the output channel.
//        final long toPosition = outChannel.position();
//
//        /* 
//         * Transfer data from channel to channel.
//         */
//
//        /*
//         * Extend the output file. This is required at least for some
//         * circumstances.
//         */
//        out.setLength(toPosition+count);
//                    
//        /*
//         * Transfer the data. It is possible that this will take multiple
//         * writes for at least some implementations.
//         */
//
//        if (log.isInfoEnabled())
//            log.info("fromPosition="+tmpChannel.position()+", toPosition="+toPosition+", count="+count);
//
//        int nwrites = 0; // #of write operations.
//
//        {
//            
//            long n = count;
//            
//            long to = toPosition;
//            
//            while (n > 0) {
//
//                if (log.isInfoEnabled())
//                    log.info("to=" + toPosition+", remaining="+n+", nwrites="+nwrites);
//                
//                long nxfer = outChannel.transferFrom(tmpChannel, to, n);
//                
//                to += nxfer;
//                
//                n -= nxfer;
//                
//                nwrites++;
//        
////        // Verify transfer is complete.
////            if (nxfer != count) {
////
////                throw new IOException("Expected to transfer " + count
////                        + ", but transferred " + nxfer);
////
////            }
//
//            }
//            
//        }
//        
//        /*
//         * Update the position on the output channel since transferFrom does
//         * NOT do this itself.
//         */
//        outChannel.position(toPosition+count);
//        
//        final long elapsed = System.currentTimeMillis() - begin;
//        
//        log.warn("Transferred " + count
//                + " bytes from disk channel to disk channel (offset="
//                + toPosition + ") in " + nwrites + " writes and " + elapsed
//                + "ms");
//
//        return count;
        
    }

    /**
     * Not supported - this is available on the {@link AbstractJournal}.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public UUID getUUID() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Not supported - this is available on the {@link AbstractJournal}.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public IResourceMetadata getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Sets the <code>readOnly</code> flag.
     * <p>
     * Note: This method SHOULD be extended to release write caches, etc.
     */
    public void closeForWrites() {

        if(isReadOnly()) {
            
            throw new IllegalStateException();
            
        }
        
        readOnly = true;
        
    }

	@Override
	public void delete(long addr) {
		// TODO Auto-generated method stub
		
	}

	public void commit() {
		// TODO Auto-generated method stub
		
	}

	public long getMetaBitsAddr() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMetaStartAddr() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean requiresCommit(IRootBlockView block) {
		return getNextOffset() > block.getNextOffset();
	}
	
    public int getMaxRecordSize() {
        return getAddressManager().getMaxByteCount();

    }
}
