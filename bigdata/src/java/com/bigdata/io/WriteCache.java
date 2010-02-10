/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Feb 10, 2010
 */

package com.bigdata.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.AbstractMRMWTestCase;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.concurrent.Latch;

/**
 * This class provides a write cache with read-through for NIO writes on a
 * {@link FileChannel} (and potentially on a remote service). This class is
 * designed to maximize the opportunity for efficient NIO by combining many
 * writes onto a single direct {@link ByteBuffer} and then efficiently
 * transferring those writes onto the backing channel in a channel dependent
 * manner. In general, there are three use cases for a {@link WriteCache}:
 * <ol>
 * <li>Gathered writes. This case is used by the {@link RWStore}.</li>
 * <li>Pure append of sequentially allocated records. This case is used by the
 * {@link DiskOnlyStrategy} (WORM) and by the {@link IndexSegmentBuilder}.</li>
 * <li>Write of a single large buffer owned by the caller. This case may be used
 * when the caller wants to manage the buffers or when the caller's buffer is
 * larger than the write cache.</li>
 * </ol>
 * The caller is responsible for managing which buffers are being written on and
 * read on, when they are flushed, and when they are reset.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class WriteCache<C extends Channel> {

    protected static final Logger log = Logger.getLogger(WriteCache.class);

    /**
     * The buffer used to absorb writes that are destined for the disk. Writes
     * are simply appended into this buffer until it would overflow. On
     * overflow, {@link #flushWriteCache()} is invoked to flush the data to the
     * disk (without synchronizing the disk). If a record is too large to fit
     * into this buffer, then the write cache is flushed and the record is
     * written directly on the disk.
     * <p>
     * Note: We must clone the data since the
     * {@link IRawStore#write(ByteBuffer)} contract says that the caller can
     * reuse the buffer once we return. In order minimize heap churn we simply
     * copy the data into {@link #buf}, a {@link ByteBuffer} that
     * buffers recently written records. Writes are deferred until the buffer is
     * would overflow and then all buffered are written at once onto the disk.
     * <p>
     * In order to ensure consistency we read through the {@link #buf} in
     * {@link #read(long)}. Otherwise a {@link #write(ByteBuffer)} could return
     * and a subsequent read on the record while it is in the
     * {@link #buf} would "miss" causing us to read through to the disk
     * (which would not have the correct data).
     * <p>
     * Note: The write cache design assumes an "append only" store. In
     * particular, it assumes that the application data records are written in
     * are purely sequential manner on the end of the file (the root blocks are
     * outside of the application data). Either the write cache must be disabled
     * or a different design must be used if you are using a store where records
     * may be deleted and recycled.
     * <p>
     * The write cache offers a 27% performance gain when compared to the same
     * condition without the write cache as measured by
     * {@link AbstractMRMWTestCase}.
     */
    final private AtomicReference<ByteBuffer> buf;

//    /**
//     * This field is used to assign the next offset in the buffer to which the
//     * caller's data will be copied.
//     */
//    final private AtomicInteger nextPosition = new AtomicInteger();

    /**
     * This latch tracks the number of read-locks on the buffer. It is
     * incremented by {@link #acquire()} and decremented by {@link #release()}.
     */
    final private Latch latch = new Latch();

    /**
     * The read lock allows concurrent {@link #acquire()}s while the write lock
     * prevents {@link #acquire()} during critical sections such as
     * {@link #flush(boolean, long, TimeUnit)}, {@link #reset()}, and
     * {@link #close()}.
     * <p>
     * Note: To avoid lock ordering problems, acquire the read lock before you
     * increment the latch and acquire the write lock before you await the
     * latch.
     */
    final private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Return the backing {@link ByteBuffer}. The caller may read or write on
     * the buffer. Once they are done, the caller MUST call {@link #release()}.
     * This uses the read lock to allow concurrent read/write operations on the
     * backing buffer. Note that at most one write operation may execute
     * concurrently in order to avoid side effects on the buffers position when
     * copying data onto the buffer.
     * 
     * @return The {@link ByteBuffer}.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the {@link WriteCache} is closed.
     */
    private ByteBuffer acquire() throws InterruptedException,
            IllegalStateException {

        final Lock readLock = lock.readLock();

        readLock.lockInterruptibly();
        
        try {

            latch.inc();

            final ByteBuffer tmp = buf.get();

            if (tmp == null) {

                latch.dec();

                throw new IllegalStateException();

            }

            return tmp;

        } finally {

            readLock.unlock();
            
        }

    }

    /**
     * Release the latch on an acquired {@link ByteBuffer}.
     */
    private void release() {
        
        latch.dec();

    }

//    /**
//     * Return the buffer. No other thread will have access to the buffer. No
//     * latch is established and there is no protocol for releasing the buffer
//     * back. Instead, the buffer will become available again if the caller
//     * releases the write lock.
//     * 
//     * @throws IllegalMonitorStateException
//     *             unless the caller is holding the write lock.
//     * @throws IllegalStateException
//     *             if the buffer reference has been cleared.
//     */
//    protected ByteBuffer getExclusiveBuffer() {
//
//        if (!lock.writeLock().isHeldByCurrentThread())
//            throw new IllegalMonitorStateException();
//
//        final ByteBuffer tmp = buf.get();
//
//        if (tmp == null)
//            throw new IllegalStateException();
//
//        return tmp;
//        
//    }
    
    /**
     * The metadata associated with a record in the {@link WriteCache}.
     */
    protected static class RecordMetadata {

        /**
         * The address of the record on the {@link IRawStore}.
         */
        public final long addr;
        
        /**
         * The offset within the backing {@link ByteBuffer} of the start of
         * the record.
         */
        public final int bufferOffset;

        /**
         * The length of the record in bytes.
         */
        public final int recordLength;

        public RecordMetadata(final long addr, final int bufferOffset,
                final int recordLength) {

            this.addr = addr;
            
            this.bufferOffset = bufferOffset;
            
            this.recordLength = recordLength;
            
        }

        public String toString() {

            return getClass().getSimpleName() + "{addr=" + addr + ",off="
                    + bufferOffset + ",len=" + recordLength + "}";

        }
        
    }

    /**
     * An index into the write cache used for read through on the cache. The
     * keys are the addresses that would be used to read the corresponding
     * record. The values describe the position in buffer where that record is
     * found and the length of the record.
     */
    final private ConcurrentMap<Long, RecordMetadata> addrMap;

    /**
     * The address of the first record written onto the {@link WriteCache}. This
     * information is used when {@link #appendOnly} is <code>true</code> as it
     * gives the starting address at which the entire {@link ByteBuffer} may be
     * written in a single IO. When {@link #appendOnly} is <code>false</code>
     * this is basically meaningless.  This is initialized to <code>-1L</code>
     * as a clear indicator that there is no valid address written yet. 
     */
    final private AtomicLong firstAddr = new AtomicLong(-1L);

    /**
     * The capacity of the backing buffer.
     */
    final private int capacity;
    
    /**
     * When <code>true</code> {@link #close()} will release the
     * {@link ByteBuffer} back to the {@link DirectBufferPool}.
     */
    final private boolean releaseBuffer;

    /**
     * Create a {@link WriteCache} from either a caller supplied buffer or a
     * direct {@link ByteBuffer} allocated from the {@link DirectBufferPool}.
     * <p>
     * Note: The application MUST ensure that it {@link #close()}s the
     * {@link WriteCache} or it can leak direct {@link ByteBuffer}s!
     * <p>
     * Note: NIO operations are performed using a direct {@link ByteBuffer}
     * (that is, one use backing bytes are allocated on the C heap). When the
     * caller supplies a {@link ByteBuffer} that is allocated on the Java heap
     * as opposed to in native memory, a temporary direct {@link ByteBuffer}
     * will be allocated for the IO operation by Java. The JVM can fail to
     * release this temporary direct {@link ByteBuffer}, resulting in a memory
     * leak. For this reason, the {@link WriteCache} SHOULD use a direct
     * {@link ByteBuffer}.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8f
     *      ab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541
     * 
     * @param buf
     *            A {@link ByteBuffer} to be used as the write cache (optional).
     *            When <code>null</code> a buffer will be allocated for you from
     *            the {@link DirectBufferPool}. Buffers allocated on your behalf
     *            will be automatically released by {@link #close()}.
     * 
     * @throws InterruptedException
     */
    public WriteCache(ByteBuffer buf) throws InterruptedException {

        if (buf == null) {
            
            buf = DirectBufferPool.INSTANCE.acquire();
            
            this.releaseBuffer = true;

        } else {
            
            this.releaseBuffer = false;
            
        }

        // save reference to the write cache.
        this.buf = new AtomicReference<ByteBuffer>(buf);
        
        // the capacity of the buffer in bytes.
        this.capacity = buf.capacity();
        
        /*
         * Discard anything in the buffer, resetting the position to zero,
         * the mark to zero, and the limit to the capacity.
         */
        buf.clear();

        /*
         * An estimate of the #of records that might fit within the write cache.
         * This is based on an assumption that the "average" record is 1k. This
         * is used solely to assign the initial capacity of this map.
         */
        final int indexDefaultCapacity = capacity / (1 * Bytes.kilobyte32);
        
        // allocate and initialize the write cache index.
        addrMap = new ConcurrentHashMap<Long, RecordMetadata>(
                indexDefaultCapacity);

    }

    /**
     * The address of the first record written onto the {@link WriteCache}. This
     * information is used when {@link #appendOnly} is <code>true</code> as it
     * gives the starting address at which the entire {@link ByteBuffer} may be
     * written in a single IO. When {@link #appendOnly} is <code>false</code>
     * this is basically meaningless.
     * 
     * @return The first address written into the {@link WriteCache} since it
     *         was last {@link #reset()} and <code>-1L</code> if nothing has
     *         been written since the {@link WriteCache} was created or was last
     *         {@link #reset()}.
     */
    final public long getFirstAddr() {

        return firstAddr.get();
        
    }
    
    /**
     * The capacity of the buffer.
     */
    final public int capacity() {
        
        return capacity;
        
    }

    /**
     * Write the record on the cache.
     * <p>
     * Note: It is perfectly reasonable to have more than one {@link WriteCache}
     * and to read through on any {@link WriteCache} until it has been recycled.
     * A {@link WriteCache} must be reset before it is put into play again for
     * new writes.
     * <p>
     * Note: For an append-only model (WORM), the caller MUST serialize writes
     * onto the {@link IRawStore} and the {@link WriteCache}. This is required
     * in order to ensure that the records are laid out in a dense linear
     * fashion on the {@link WriteCache} and permits the backing buffer to be
     * transferred in a single IO to the backing file.
     * <p>
     * Note: For a {@link RWStore}, the caller must take more responsibility for
     * managing the {@link WriteCache}(s) which are in play and scheduling their
     * eviction onto the backing store. The caller can track the space remaining
     * in each {@link WriteCache} and decide when to flush a {@link WriteCache}
     * based on that information.
     * 
     * @param addr
     *            The address assigned to that record.
     * @param data
     *            The record. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()} . The caller may subsequently
     *            modify the contents of the buffer without changing the state
     *            of the cache (i.e., the data are copied into the cache).
     * 
     * @return <code>true</code> iff the caller's record was transferred to the
     *         cache. When <code>false</code>, there is not enough room left in
     *         the write cache for this record.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             If the buffer is closed.
     * @throws IllegalArgumentException
     *             If the caller's record is larger than the maximum capacity of
     *             cache (the record could not fit within the cache). The caller
     *             should check for this and provide special handling for such
     *             large records. For example, they can be written directly onto
     *             the backing channel.
     */
    public boolean write(final long addr, final ByteBuffer data)
            throws InterruptedException, IllegalStateException {

        // Note: The address MAY be zero. This allows for stores without any
        // header block.
        
        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);
        
        final ByteBuffer tmp = acquire();

        try {

            // The #of bytes to transfer into the write cache.
            final int nbytes = data.remaining();

            if (nbytes > tmp.capacity()) {

                throw new IllegalArgumentException(
                        "Record is too large for cache.");
                
            }

            if (nbytes == 0)
                throw new IllegalArgumentException(
                        AbstractBufferStrategy.ERR_BUFFER_EMPTY);

            /*
             * This does not work because it can extend the new position without
             * actually having room for the record at the adjusted position. We
             * have to serialize operations here in order to meet the necessary
             * criteria (sufficient room for the record in the cache).
             */
//            /*
//             * Note: This provides high concurrency by creating a view with its
//             * own limit and position. This makes it possible to transfer data
//             * into the write cache from concurrent threads without corruption.
//             * However, the application may be required to serialize writes for
//             * a variety of reasons. See the javadoc above.
//             */
//            final int pos = nextPosition.addAndGet(nbytes) - nbytes;
//
//            if (pos + nbytes > tmp.capacity()) {
//                /*
//                 * There is not enough room left in the write cache for this
//                 * record.
//                 */
//                return false;
//                
//            }
//            
//            // create a view with same offset, limit and position.
//            final ByteBuffer view = tmp.duplicate();
//
//            // adjust the view to just the record of interest.
//            view.limit(pos + nbytes);
//            view.position(pos);
//
//            // copy the record into the cache.
//            view.put(data);

            /*
             * Note: We need to be synchronized on the ByteBuffer here since
             * this operation relies on the position() being stable.
             * 
             * Note: No other code adjust touch the buffer's position() !!!
             */
            final int pos;
            synchronized (tmp) {

                // the position() at which the record is cached.
                pos = tmp.position();

                // copy the record into the cache, updating position() as we go.
                tmp.put(data);

                // set while synchronized since no contention.
                firstAddr.compareAndSet(-1L/* expect */, addr/* update */);

            }

            // Add metadata for the record so it can be read back from the
            // cache.
            addrMap.put(Long.valueOf(addr), new RecordMetadata(addr, pos,
                    nbytes));

            return true;
            
        } finally {

            release();

        }

    }

    /**
     * Read a record from the write cache.
     * 
     * @param addr
     *            The address assigned to that record in the journal.
     * @param nbytes
     *            The length of the record (decoded from the address by the
     *            caller).
     * 
     * @return The data read -or- <code>null</code> iff the record does not lie
     *         within this {@link WriteCache}. When non-null, this will be a
     *         newly allocated exact fit mutable {@link ByteBuffer} backed by a
     *         Java <code>byte[]</code>. The buffer will be flipped to prepare
     *         for reading (the position will be zero and the limit will be the
     *         #of bytes read).
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the buffer is closed.
     */
    public ByteBuffer read(final long addr) throws InterruptedException,
            IllegalStateException {

        final ByteBuffer tmp = acquire();

        try {

            // Look up the metadata for that record in the cache.
            final RecordMetadata md;
            if ((md = addrMap.get(addr)) == null) {
                // The record is not in this write cache.
                return null;
            }

            // the start of the record in writeCache.
            final int pos = md.bufferOffset;

            // create a view with same offset, limit and position.
            final ByteBuffer view = tmp.duplicate();

            // adjust the view to just the record of interest.
            view.limit(pos + md.recordLength);
            view.position(pos);

            /*
             * Copy the data into a newly allocated buffer. This is necessary
             * because our hold on the backing ByteBuffer for the WriteCache is
             * only momentary. As soon as we release() the buffer the data in
             * the buffer could be changed.
             */
            final ByteBuffer dst = ByteBuffer.allocate(md.recordLength);

            // copy the data into [dst].
            dst.put(view);

            // flip buffer for reading.
            dst.flip();

            return dst;

        } finally {

            release();
            
        }

    }

    /**
     * Update a record in the {@link WriteCache}.
     * 
     * @param addr
     *            The address of the record.
     * @param off
     *            The byte offset of the update.
     * @param data
     *            The data to be written onto the record in the cache starting
     *            at that byte offset. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()}. The caller may subsequently modify
     *            the contents of the buffer without changing the state of the
     *            cache (i.e., the data are copied into the cache).
     * 
     * @return <code>true</code> iff the record was updated and
     *         <code>false</code> if no record for that address was found in the
     *         cache.
     *         
     * @throws InterruptedException
     * @throws IllegalStateException
     * 
     * @throws IllegalStateException
     *             if the buffer is closed.
     */
    public boolean update(final long addr, final int off, final ByteBuffer data)
            throws IllegalStateException, InterruptedException {

        if (addr == 0L)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_ADDRESS_IS_NULL);

        if (off < 0)
            throw new IllegalArgumentException("Offset is negative");

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        // #of bytes to be updated on the pre-existing record.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        /*
         * Check the writeCache. If the record is found in the write cache then
         * we just update the slice of the record corresponding to the caller's
         * request. This is a common use case and results in no IO.
         */
        final ByteBuffer tmp = acquire();

        try {

            // Look up the metadata for that record in the cache.
            final RecordMetadata md;
            if ((md = addrMap.get(addr)) == null) {

                // The record is not in this write cache.
               return false;

            }

            if (off + nbytes > md.recordLength) {

                // The update would overrun the record's extent in the cache.
                throw new IllegalArgumentException(
                        AbstractBufferStrategy.ERR_BUFFER_OVERRUN);

            }

            // the start of the record in writeCache.
            final int pos = md.bufferOffset;

            // create a view with same offset, limit and position.
            final ByteBuffer view = tmp.duplicate();

            // adjust the limit on the record in the write cache.
            view.limit(pos + off + nbytes);

            // adjust the position on the record in the write cache.
            view.position(pos + off);

            // copy the caller's data onto the record in write cache.
            view.put(data);

            // Done.
            return true;

        } finally {

            release();

        }
                
    }

    /**
     * Flush the writes to the backing channel.
     * 
     * @param force
     *            When <code>true</code>, the data will be forced to stable
     *            media.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void flush(final boolean force) throws IOException,
            InterruptedException, AssertionError {

        try {
            
            if (!flush(force, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            
                throw new RuntimeException();
                
            }
            
        } catch (TimeoutException e) {
         
            throw new RuntimeException(e);
            
        }

    }

    /**
     * Flush the writes to the backing channel (does not sync the channel).
     * 
     * @param force
     *            When <code>true</code>, the data will be forced to stable
     *            media.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit unit) throws IOException, TimeoutException,
            InterruptedException {

        // start time
        final long begin = System.nanoTime();

        // total nanoseconds to wait.
        final long nanos = unit.toNanos(timeout);
        
        // remaining nanoseconds to wait.
        long remaining = nanos;
        
        final Lock writeLock = lock.writeLock();

        if (!writeLock.tryLock(remaining, TimeUnit.NANOSECONDS)) {

            return false;
            
        }

        try {

            // remaining := (total - elapsed).
            remaining = nanos - (System.nanoTime() - begin);

            if (!latch.await(remaining, TimeUnit.NANOSECONDS)) {

                throw new TimeoutException();

            }

            final ByteBuffer tmp = this.buf.get();

            if (tmp == null)
                throw new IllegalStateException();
            
            // #of bytes to write on the disk.
            final int nbytes = tmp.position();

            if (nbytes == 0) {

                // NOP.
                return true;
                
            }

            /*
             * Create a view with same offset, limit and position.
             * 
             * Note: The writeOnChannel method is given the view. This prevents
             * it from adjusting the position() on the backing buffer.
             */
            {
             
                final ByteBuffer view = tmp.duplicate();

                // adjust the view to just the dirty record.
                view.limit(nbytes);
                view.position(0);

                // write the data on the disk file.
                return writeOnChannel(view, Collections
                        .unmodifiableMap(addrMap), remaining);

            }
            
        } finally {
            
            writeLock.unlock();
            
        }

    }

    /**
     * Write the data from the buffer onto the channel. This method provides a
     * uniform means to request that the buffer write itself onto the backing
     * channel, regardless of whether the channel is backed by a file, a socket,
     * etc.
     * <p>
     * Implementations of this method MAY support gathered writes, depending on
     * the channel. The necessary information to perform a gathered write is
     * present in the address map. On the other hand, the implementation MAY
     * require that the records in the cache are laid out for a WORM, in which
     * case {@link #getFirstAddr()} provides the starting address for the data
     * to be written. The application MUST coordinate the requirements for a R/W
     * or WORM store with the use of the {@link WriteCache} and the means to
     * write on the backing channel.
     * 
     * @param buf
     *            The data to be written. Only the dirty bytes are visible in
     *            this view. The implementation should write all bytes from the
     *            current position to the limit.
     * @param addrMap
     *            The mapping of record addresses onto byte offsets within the
     *            give {@link ByteBuffer}.
     * @param nanos
     *            The timeout for the operation in nanoseconds.
     * 
     * @return <code>true</code> if the operation was completed successfully
     *         within the time alloted.
     * 
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws IOException
     */
    abstract protected boolean writeOnChannel(final ByteBuffer buf,
            final Map<Long, RecordMetadata> addrMap, final long nanos)
            throws InterruptedException, TimeoutException, IOException;

    /**
     * Clear the buffer, the address map, and other internal metadata such that
     * the {@link WriteCache} is prepared to receive new writes.
     * 
     * @throws InterruptedException
     */
    public void reset() throws InterruptedException {
        
        final Lock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {

            // wait until there are no readers using the buffer.
            latch.await();

            final ByteBuffer tmp = buf.get();

            if (tmp == null) {

                // Already closed.
                return;
                
            }

            // reset all state.
            _resetState(tmp);

        } finally {
            
            writeLock.unlock();
        
        }
        
    }

    /**
     * Permanently take the {@link WriteCache} instance out of service. If the
     * buffer was allocated by the {@link WriteCache} then it is released back
     * to the {@link DirectBufferPool}. After this method is called, records can
     * no longer be read from nor written onto the {@link WriteCache}.
     * <p>
     * Concurrent {@link #read(long, int)} requests will be serviced if the
     * already hold the the read lock but requests will fail once the 
     * 
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {

        final Lock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {

            // wait until there are no readers using the buffer.
            latch.await();
            
            /*
             * Note: This method is thread safe. Only one thread will manage to
             * clear the AtomicReference and it will do the rest of the work as
             * well.
             */

            // position := 0; limit := capacity.
            final ByteBuffer tmp = buf.get();

            if (tmp == null) {
             
                // Already closed.
                return;

            }

            if (buf.compareAndSet(tmp/* expected */, null/* update */)) {

                try {

                    _resetState(tmp);

                } finally {

                    if (releaseBuffer) {

                        DirectBufferPool.INSTANCE.release(tmp);

                    }

                }

            }

        } finally {

            writeLock.unlock();

        }

    }

    /**
     * Reset the internal state of the {@link WriteCache} in preparation to
     * reuse it to receive more writes.
     * <p>
     * Note: Keep private unless strong need for override since you can not call
     * this method without holding the write lock and having the {@link #latch}
     * at zero.
     * 
     * @param tmp
     */
    private void _resetState(final ByteBuffer tmp) {

        if (tmp == null)
            throw new IllegalArgumentException();
        
        // clear the index since all records were flushed to disk.
        addrMap.clear();

        // clear to well known invalid address.
        firstAddr.set(-1L);
        
//        nextPosition.set(0);

        // position := 0; limit := capacity.
        tmp.clear();

    }

    /**
     * A {@link WriteCache} implementation suitable for an append-only file such
     * as the {@link DiskOnlyStrategy} or the output file of the
     * {@link IndexSegmentBuilder}.
     * 
     * FIXME Write unit tests.
     * <p>
     * Put it to use in the {@link IndexSegmentBuilder}.
     * <p>
     * Then backport this to the {@link DiskOnlyStrategy} (this will require
     * working out an agreement for writing large records directly rather than
     * buffering them, which could be done using a constructor variant to wrap
     * the record and make the caller block).
     * <p>
     * Next, work out the {@link WriteCacheService} and backport that to the
     * {@link DiskOnlyStrategy}.
     * <p>
     * Finally, work out a replication and resynchronization service with error
     * handling for bad reads.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class FileChannelWriteCache extends WriteCache<FileChannel> {

        /**
         * An offset which will be applied to each record written onto the
         * backing {@link FileChannel}. The offset is generally the size of the
         * root blocks for a journal or the checkpoint record for an index
         * segment. It can be zero if you do not have anything at the head of
         * the file.
         * 
         * @todo This implies that writing the root blocks is done separately in
         *       the protocol since you can't write below this offset otherwise.
         */
        final private long baseOffset;
        
        /**
         * Used to re-open the {@link FileChannel} in this class.
         */
        private final IReopenChannel<FileChannel> opener;

        /**
         * @param baseOffset
         *            An offset
         * @param buf
         * @param opener
         * 
         * @throws InterruptedException
         */
        public FileChannelWriteCache(final long baseOffset,
                final ByteBuffer buf, final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

            super(buf);

            if (baseOffset < 0)
                throw new IllegalArgumentException();

            if (opener == null)
                throw new IllegalArgumentException();
            
            this.baseOffset = baseOffset;

            this.opener = opener;
            
        }

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final Map<Long, RecordMetadata> addrMap, final long nanos)
                throws InterruptedException, TimeoutException, IOException {

            final long begin = System.nanoTime();
            
            final int nbytes = data.remaining();

            /*
             * The position in the file at which the record will be written.
             */
            final long pos = getFirstAddr() + baseOffset;

            /*
             * Write bytes in [data] from position to limit onto the channel.
             * 
             * @todo This ignores the timeout.
             */
            FileChannelUtility.writeAll(opener, data, pos);

            final long elapsed = System.nanoTime() - begin;
            
            if (log.isInfoEnabled()) {

                log.info("wrote on disk: bytes=" + nbytes + ", elapsed="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
            
            }
            
            return true;

        }

    }
  
}
