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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.ha.QuorumManager;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;

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
 * read on, when they are flushed, and when they are reset. It is perfectly
 * reasonable to have more than one {@link WriteCache} and to read through on
 * any {@link WriteCache} until it has been recycled. A {@link WriteCache} must
 * be reset before it is put into play again for new writes.
 * <p>
 * Note: For an append-only model (WORM), the caller MUST serialize writes onto
 * the {@link IRawStore} and the {@link WriteCache}. This is required in order
 * to ensure that the records are laid out in a dense linear fashion on the
 * {@link WriteCache} and permits the backing buffer to be transferred in a
 * single IO to the backing file.
 * <p>
 * Note: For a {@link RWStore}, the caller must take more responsibility for
 * managing the {@link WriteCache}(s) which are in play and scheduling their
 * eviction onto the backing store. The caller can track the space remaining in
 * each {@link WriteCache} and decide when to flush a {@link WriteCache} based
 * on that information.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class WriteCache implements IWriteCache {

	protected static final Logger log = Logger.getLogger(WriteCache.class);

	private final boolean useChecksum;
	
	/**
	 * The buffer used to absorb writes that are destined for some channel.
	 */
	final private AtomicReference<ByteBuffer> buf;

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

            // latch.inc();

            final ByteBuffer tmp = buf.get();

            if (tmp == null) {

                // latch.dec();

                throw new IllegalStateException();

            }

            // Note: The ReadLock is still held!
            return tmp;

        } catch (Throwable t) {

            // Release the lock only on the error path.
            readLock.unlock();

            if (t instanceof InterruptedException)
                throw (InterruptedException) t;

            if (t instanceof IllegalStateException)
                throw (IllegalStateException) t;

            throw new RuntimeException(t);

		}

	}

	/**
	 * Release the read lock on an acquired {@link ByteBuffer}.
	 */
	private void release() {

	    lock.readLock().unlock();
	    
//		latch.dec();

	}

	// /**
	// * Return the buffer. No other thread will have access to the buffer. No
	// * latch is established and there is no protocol for releasing the buffer
	// * back. Instead, the buffer will become available again if the caller
	// * releases the write lock.
	// *
	// * @throws IllegalMonitorStateException
	// * unless the caller is holding the write lock.
	// * @throws IllegalStateException
	// * if the buffer reference has been cleared.
	// */
	// protected ByteBuffer getExclusiveBuffer() {
	//
	// if (!lock.writeLock().isHeldByCurrentThread())
	// throw new IllegalMonitorStateException();
	//
	// final ByteBuffer tmp = buf.get();
	//
	// if (tmp == null)
	// throw new IllegalStateException();
	//
	// return tmp;
	//        
	// }

	/**
	 * The metadata associated with a record in the {@link WriteCache}.
	 */
	public static class RecordMetadata {

        /**
         * The offset of the record in the file. The offset may be relative to a
         * base offset known to the writeOnChannel() implementation.
         */
		public final long fileOffset;

        /**
         * The offset within the {@link WriteCache}'s backing {@link ByteBuffer}
         * of the start of the record.
         */
		public final int bufferOffset;

        /**
         * The length of the record in bytes as it will be written on the
         * channel. If checksums are being written, then the length of the
         * record has already been incorporated into this value.
         */
		public final int recordLength;

        public RecordMetadata(final long fileOffset, final int bufferOffset,
                final int recordLength) {

			this.fileOffset = fileOffset;

			this.bufferOffset = bufferOffset;

			this.recordLength = recordLength;

		}

		public String toString() {

            return getClass().getSimpleName() + "{fileOffset=" + fileOffset
                    + ",off=" + bufferOffset + ",len=" + recordLength + "}";

		}

	}

    /**
     * An index into the write cache used for read through on the cache. The
     * keys are the file offsets that would be used to read the corresponding
     * record. The values describe the position in buffer where that record is
     * found and the length of the record.
     */
	final private ConcurrentMap<Long, RecordMetadata> recordMap;

    /**
     * The offset of the first record written onto the {@link WriteCache}. This
     * information is used when {@link #appendOnly} is <code>true</code> as it
     * gives the starting offset at which the entire {@link ByteBuffer} may be
     * written in a single IO. When {@link #appendOnly} is <code>false</code>
     * this is basically meaningless. This is initialized to <code>-1L</code> as
     * a clear indicator that there is no valid record written yet onto the
     * cache.
     */
	final private AtomicLong firstOffset = new AtomicLong(-1L);

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
     * @param useChecksum
     *            <code>true</code> iff the write cache will store the caller's
     *            checksum for a record and validate it on read.
     * 
     * @throws InterruptedException
     */
    public WriteCache(final ByteBuffer buf, final boolean useChecksum)
            throws InterruptedException {

        this(buf, false/* scatteredWrites */, useChecksum);

    }

    public WriteCache(ByteBuffer buf, final boolean scatteredWrites,
            final boolean useChecksum) throws InterruptedException {

        if (buf == null) {

            buf = DirectBufferPool.INSTANCE.acquire();

            this.releaseBuffer = true;

        } else {

            this.releaseBuffer = false;

        }

        this.useChecksum = useChecksum;
        
		// save reference to the write cache.
		this.buf = new AtomicReference<ByteBuffer>(buf);

		// the capacity of the buffer in bytes.
		this.capacity = buf.capacity();

		/*
		 * Discard anything in the buffer, resetting the position to zero, the
		 * mark to zero, and the limit to the capacity.
		 */
		buf.clear();

		/*
		 * An estimate of the #of records that might fit within the write cache.
		 * This is based on an assumption that the "average" record is 1k. This
		 * is used solely to assign the initial capacity of this map.
		 */
		final int indexDefaultCapacity = capacity / (1 * Bytes.kilobyte32);

		/*
		 * allocate and initialize the write cache index.
		 * 
		 * For scattered writes we choose to use a sorted map so that we can
		 * easily flush writes to the file channel in order. This may not be
		 * important depending on the caching strategy of the underlying system
		 * but it cannot be a bad thing.
		 * 
		 * If we do not need to support scattered writes then we have the option
		 * to use the ConcurrentHashMap which has the advantage of constant
		 * access time for read through support.
		 * 
		 * TODO: some literature indicates the ConcurrentSkipListMap scales
		 * better with concurrency, so we should benchmark this option for
		 * non-scattered writes as well.
		 */
		if (scatteredWrites) {
			recordMap = new ConcurrentSkipListMap<Long, RecordMetadata>();
		} else {
			recordMap = new ConcurrentHashMap<Long, RecordMetadata>(indexDefaultCapacity);
		}

	}

    /**
     * Adds some debugging information.
     */
    public String toString() { 

        return super.toString()//
        +"{size="+recordMap.size()//
        +",firstOffset="+firstOffset//
        +",releaseBuffer="+releaseBuffer//
        +"}"
        ;
        
    }

    /**
     * The offset of the first record written onto the {@link WriteCache}. This
     * information is used when {@link #appendOnly} is <code>true</code> as it
     * gives the starting offset at which the entire {@link ByteBuffer} may be
     * written in a single IO. When {@link #appendOnly} is <code>false</code>
     * this is basically meaningless.
     * <p>
     * Note: This has been raised into the
     * {@link #writeOnChannel(ByteBuffer, long, Map, long)} method signature. It
     * has been reduced to a package private method so it will remain visible to
     * the unit tests, otherwise it could become private.
     * 
     * @return The first offset written into the {@link WriteCache} since it was
     *         last {@link #reset()} and <code>-1L</code> if nothing has been
     *         written since the {@link WriteCache} was created or was last
     *         {@link #reset()}.
     */
	final long getFirstOffset() {

		return firstOffset.get();

	}

    /**
     * The maximum length of a record which could be inserted into the buffer.
     * <p>
     * Note: When checksums are enabled, this is 4 bytes less than the actual
     * capacity of the underlying buffer since each record requires an
     * additional four bytes for the checksum field.
     */
	final public int capacity() {

        return capacity - (useChecksum ? 4 : 0);

	}

    /**
     * Return the #of bytes remaining in the buffer.
     * <p>
     * Note: in order to rely on this value the caller MUST have exclusive
     * access to the buffer. This API does not provide the means for acquiring
     * that exclusive access. This is something that the caller has to arrange
     * for themselves, which is why this is a package private method.
     */
	final int remaining() {

	    final int remaining = capacity - buf.get().position();
        
        return remaining;
	    
	}

	/**
	 * The capacity of the buffer.
	 */
	final public boolean isEmpty() {

		return recordMap.isEmpty();

	}

    /**
     * {@inheritDoc}
     * 
     * @throws IllegalStateException
     *             If the buffer is closed.
     * @throws IllegalArgumentException
     *             If the caller's record is larger than the maximum capacity of
     *             cache (the record could not fit within the cache). The caller
     *             should check for this and provide special handling for such
     *             large records. For example, they can be written directly onto
     *             the backing channel.
     */
    public boolean write(final long offset, final ByteBuffer data, final int chk)
            throws InterruptedException {

        return write(offset, data, chk, true/* writeChecksum */);

    }

    /**
     * 
     * @param offset
     * @param data
     * @param chk
     * @param writeChecksum
     *            The checksum is appended to the record IFF this argument is
     *            <code>true</code> and checksums are in use.
     * @return
     * @throws InterruptedException
     */
    boolean write(final long offset, final ByteBuffer data, final int chk,
            boolean writeChecksum) throws InterruptedException {

        // Note: The offset MAY be zero. This allows for stores without any
		// header block.

    	if (m_written) { // should be clean, NO WAY should this be written to!
    		log.warn("Writing to CLEAN cache: " + hashCode());
    	}

    	if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

		final WriteCacheCounters counters = this.counters.get();
		
		final ByteBuffer tmp = acquire();

		try {

		    final int remaining = data.remaining();
		    
			// The #of bytes to transfer into the write cache.
            final int nwrite = remaining + (writeChecksum && useChecksum ? 4 : 0);

			if (nwrite > capacity) {
			    // This is more bytes than the total capacity of the buffer.
                throw new IllegalArgumentException(
                        AbstractBufferStrategy.ERR_BUFFER_OVERRUN);

			}

            if (remaining == 0)
                throw new IllegalArgumentException(
                        AbstractBufferStrategy.ERR_BUFFER_EMPTY);

			/*
			 * Note: We need to be synchronized on the ByteBuffer here since
			 * this operation relies on the position() being stable.
			 * 
			 * Note: No other code adjust touch the buffer's position() !!!
			 */
			final int pos;
			synchronized (tmp) {

				// the position() at which the record is cached in the buffer.
				pos = tmp.position();

				if (pos + nwrite > capacity) {

					/*
					 * There is not enough room left in the write cache for this
					 * record.
					 */

					return false;

				}

				// copy the record into the cache, updating position() as we go.
				tmp.put(data);
				// write checksum - if any
                if (writeChecksum && useChecksum) {
                    tmp.putInt(chk);
                }

				// set while synchronized since no contention.
				firstOffset.compareAndSet(-1L/* expect */, offset/* update */);

				// update counters while holding the lock.
                counters.naccept++;
                counters.bytesAccepted += nwrite;

			}

            /*
             * Add metadata for the record so it can be read back from the
             * cache.
             */
            if (recordMap.put(Long.valueOf(offset), new RecordMetadata(offset,
                    pos, nwrite)) != null) {
                /*
                 * Note: This exception indicates that the abort protocol did
                 * not reset() the current write cache before new writes were
                 * laid down onto the buffer.
                 */
                throw new AssertionError(
                        "Record exists for offset in cache: offset=" + offset);
			}

            if (log.isTraceEnabled()) { // @todo rather than hashCode() set a buffer# on each WriteCache instance.
                log.trace("offset=" + offset + ", pos=" + pos + ", nwrite="
                        + nwrite + ", writeChecksum=" + writeChecksum
                        + ", useChecksum=" + useChecksum + ", nrecords="
                        + recordMap.size() + ", hashCode=" + hashCode());
            }
            
			return true;

		} finally {

			release();

		}

    }

    /**
     * {@inheritDoc}
     * 
     * @throws IllegalStateException
     *             If the buffer is closed.
     */
    public ByteBuffer read(final long offset) throws InterruptedException,
            ChecksumError {

        final WriteCacheCounters counters = this.counters.get(); 
            
		final ByteBuffer tmp = acquire();

		try {

			// Look up the metadata for that record in the cache.
			final RecordMetadata md;
			if ((md = recordMap.get(offset)) == null) {
			    
				// The record is not in this write cache.
			    counters.nmiss.incrementAndGet();
			    
				return null;
			}

			// length of the record w/o checksum field.
			final int reclen = md.recordLength - (useChecksum ? 4 : 0);
			
			// the start of the record in writeCache.
			final int pos = md.bufferOffset;

			// create a view with same offset, limit and position.
			final ByteBuffer view = tmp.duplicate();

			// adjust the view to just the record of interest.
            view.limit(pos + reclen);
			view.position(pos);

			// System.out.println("WriteCache, addr: " + offset + ", from: " + pos + ", " + md.recordLength + ", thread: " + Thread.currentThread().getId());
			/*
			 * Copy the data into a newly allocated buffer. This is necessary
			 * because our hold on the backing ByteBuffer for the WriteCache is
			 * only momentary. As soon as we release() the buffer the data in
			 * the buffer could be changed.
			 */

			final byte[] b = new byte[reclen];
			
			final ByteBuffer dst = ByteBuffer.wrap(b);

			// copy the data into [dst] (and the backing byte[]).
			dst.put(view);

			// flip buffer for reading.
			dst.flip();

            if (useChecksum) {

                final int chk = tmp.getInt(pos + md.recordLength - 4);

                if (chk != ChecksumUtility.threadChk.get().checksum(b,
                        0/* offset */, b.length)) {

                    throw new ChecksumError();

                }

            }

            counters.nhit.incrementAndGet();

            return dst;

		} finally {

			release();

		}

	}

	/**
     * Flush the writes to the backing channel but DOES NOT sync the channel and
     * DOES NOT {@link #reset()} the {@link WriteCache}. {@link #reset()} is a
     * separate operation because a common use is to retain recently flushed
     * instances for read-back.
	 * 
	 * @param force
	 *            When <code>true</code>, the data will be forced to stable
	 *            media.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void flush(final boolean force) throws IOException, InterruptedException {

		try {

			if (!flush(force, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {

				throw new RuntimeException();

			}

		} catch (TimeoutException e) {

			throw new RuntimeException(e);

		}

	}

    /**
     * Variant which resets the cache if it was successfully flushed.
     */
    public void flushAndReset(final boolean force) throws IOException, InterruptedException {

        try {

            if (!flushAndReset(force, true/* reset */, Long.MAX_VALUE,
                    TimeUnit.NANOSECONDS)) {

                throw new RuntimeException();

            }

        } catch (TimeoutException e) {

            throw new RuntimeException(e);

        }

    }
    
    /**
     * Flush the writes to the backing channel but DOES NOT sync the channel and
     * DOES NOT {@link #reset()} the {@link WriteCache}. {@link #reset()} is a
     * separate operation because a common use is to retain recently flushed
     * instances for read-back.
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

        return flushAndReset(force, false/* reset */, timeout, unit);
        
    }

    /**
     * Core impl.
     * 
     * @param forceIsIgnored
     *            ignored (deprecated).
     * @param reset
     *            When <code>true</code>, does atomic reset IFF the flush was
     *            successful in the allowed time while holding the lock to
     *            prevent new records from being written onto the buffer
     *            concurrently.
     * @param timeout
     * @param unit
     * @return
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    private boolean flushAndReset(final boolean forceIsIgnored,
            final boolean reset, final long timeout, final TimeUnit unit)
            throws IOException, TimeoutException, InterruptedException {

		// start time
		final long begin = System.nanoTime();

		// total nanoseconds to wait.
		final long nanos = unit.toNanos(timeout);

		// remaining nanoseconds to wait.
		long remaining = nanos;

        final WriteCacheCounters counters = this.counters.get();
        
		final Lock writeLock = lock.writeLock();

		if (!writeLock.tryLock(remaining, TimeUnit.NANOSECONDS)) {

			return false;

		}

		try {

			final ByteBuffer tmp = this.buf.get();

			if (tmp == null)
				throw new IllegalStateException();

			// #of bytes to write on the disk.
			final int nbytes = tmp.position();

            if (log.isTraceEnabled()) {
                log.trace("nbytes=" + nbytes + ", firstOffset="
                        + getFirstOffset() + ", nflush=" + counters.nflush);
            }
               
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

                // remaining := (total - elapsed).
                remaining = nanos - (System.nanoTime() - begin);
                
                // write the data on the disk file.
                final boolean ret = writeOnChannel(view, getFirstOffset(),
                        Collections.unmodifiableMap(recordMap), remaining);

                counters.nflush++;

                if (ret && reset) {

                    /*
                     * Atomic reset while holding the lock to prevent new
                     * records from being written onto the buffer concurrently.
                     */
                    
                    reset();

                }

				return ret;
				
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
     * present in the <i>recordMap</i>. On the other hand, the implementation
     * MAY require that the records in the cache are laid out for a WORM, in
     * which case {@link #getFirstOffset()} provides the starting offset for the
     * data to be written. The application MUST coordinate the requirements for
     * a R/W or WORM store with the use of the {@link WriteCache} and the means
     * to write on the backing channel.
     * 
     * @param buf
     *            The data to be written. Only the dirty bytes are visible in
     *            this view. The implementation should write all bytes from the
     *            current position to the limit.
     * @param firstOffset
     *            The offset of the first record in the recordMap into the file
     *            (may be relative to a base offset within the file). This is
     *            provided as an optimization for the WORM which writes its
     *            records contiguously on the backing store.
     * @param recordMap
     *            The mapping of record offsets onto metadata about those
     *            records.
     * @param nanos
     *            The timeout for the operation in nanoseconds.
     * 
     * @return <code>true</code> if the operation was completed successfully
     *         within the time alloted.
     * 
     * @throws InterruptedException
     *             if the thread was interrupted.
     * @throws IOException
     *             if there was an IO problem.
     */
    abstract protected boolean writeOnChannel(final ByteBuffer buf,
            final long firstOffset, final Map<Long, RecordMetadata> recordMap,
            final long nanos) throws InterruptedException, TimeoutException,
            IOException;

    /**
     * Return a {@link Runnable} which will write the cache contents onto the
     * downstream service in the quorum. This method is not used unless the
     * journal is highly available.
     */
    abstract protected Runnable getDownstreamWriteRunnable(
            QuorumManager quorumManager);
    
    /**
     * {@inheritDoc}.
     * <p>
     * This implementation clears the buffer, the record map, and other internal
     * metadata such that the {@link WriteCache} is prepared to receive new
     * writes.
     * 
     * @throws IllegalStateException
     *             if the write cache is closed.
     */
	public void reset() throws InterruptedException {

		final Lock writeLock = lock.writeLock();

		writeLock.lockInterruptibly();

		try {

//			// wait until there are no readers using the buffer.
//			latch.await();

			final ByteBuffer tmp = buf.get();

			if (tmp == null) {

				// Already closed.
				throw new IllegalStateException();

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
	 * no longer be read from nor written onto the {@link WriteCache}. It is
	 * safe to invoke this method more than once.
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

//			// wait until there are no readers using the buffer.
//			latch.await();

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
		recordMap.clear();

		// clear to well known invalid offset.
		firstOffset.set(-1L);

		// position := 0; limit := capacity.
		tmp.clear();

		// Martyn: I moved your debug flag here so it is always cleared by reset().
        m_written = false;
        
	}

    /**
     * The current performance counters.
     */
    protected final AtomicReference<WriteCacheCounters> counters = new AtomicReference<WriteCacheCounters>(
            new WriteCacheCounters());

    /**
     * Sets the performance counters to be used by the write cache. A service
     * should do this if you want to aggregate the performance counters across
     * multiple {@link WriteCache} instances.
     * 
     * @param newVal
     *            The shared performance counters.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    void setCounters(final WriteCacheCounters newVal) {
        
        if (newVal == null)
            return;

        this.counters.set(newVal);
        
    }

    /**
     * Return the performance counters for the write cacher.
     */
    public CounterSet getCounters() {

        return counters.get().getCounters();

    }

    /**
     * Performance counters for the {@link WriteCache}.
     * <p>
     * Note: thread-safety is required for: {@link #nhit} and {@link #nmiss}.
     * The rest should be Ok without additional synchronization, CAS operators,
     * etc (mainly because they are updated while holding a lock).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class WriteCacheCounters {

        /*
         * read on the cache.
         */
        
        /**
         * #of read requests that are satisfied by the write cache.
         */
        public final AtomicLong nhit = new AtomicLong();

        /**
         * The #of read requests that are not satisfied by the write cache.
         */
        public final AtomicLong nmiss = new AtomicLong();

        /*
         * write on the cache.
         */
        
        /**
         * #of records accepted for eventual write onto the backing channel.
         */
        public long naccept;

        /**
         * #of bytes accepted for eventual write onto the backing channel. 
         */
        public long bytesAccepted;

        /*
         * write on the channel.
         */
        
        /**
         * #of times {@link IWriteCache#flush(boolean)} was called.
         */
        public long nflush;

        /**
         * #of writes on the backing channel. Note that some write cache
         * implementations do ordered writes and will therefore do one write per
         * record while others do append only and therefore do one write per
         * write cache flush. Note that in both cases we may have to redo a
         * write if the backing channel was concurrently closed, so the value
         * here can diverge from the #of accepted records and the #of requested
         * flushes.
         */
        public long nwrite;
        
        /**
         * #of bytes written onto the backing channel.
         */
        public long bytesWritten;
        
        /**
         * Total elapsed time writing onto the backing channel.
         */
        public long elapsedWriteNanos;

        public CounterSet getCounters() {

            final CounterSet root = new CounterSet();

            /*
             * read on the cache.
             */
            
            root.addCounter("nhit", new Instrument<Long>() {
                public void sample() {
                    setValue(nhit.get());
                }
            });

            root.addCounter("nmiss", new Instrument<Long>() {
                public void sample() {
                    setValue(nmiss.get());
                }
            });

            root.addCounter("hitRate", new Instrument<Double>() {
                public void sample() {
                    final long nhit = WriteCacheCounters.this.nhit.get();
                    final long ntests = nhit + WriteCacheCounters.this.nmiss.get();
                    setValue(ntests == 0L ? 0d : (double) nhit / ntests);
                }
            });

            /*
             * write on the cache.
             */

            // #of records accepted by the write cache.
            root.addCounter("naccept", new Instrument<Long>() {
                public void sample() {
                    setValue(naccept);
                }
            });
            
            // #of bytes in records accepted by the write cache.
            root.addCounter("bytesAccepted", new Instrument<Long>() {
                public void sample() {
                    setValue(bytesAccepted);
                }
            });

            /*
             * write on the channel.
             */

            // #of times the write cache was flushed to the backing channel.
            root.addCounter("nflush", new Instrument<Long>() {
                public void sample() {
                    setValue(nflush);
                }
            });

            // #of writes onto the backing channel.
            root.addCounter("nwrite", new Instrument<Long>() {
                public void sample() {
                    setValue(nwrite);
                }
            });

            // #of bytes written onto the backing channel.
            root.addCounter("bytesWritten", new Instrument<Long>() {
                public void sample() {
                    setValue(bytesWritten);
                }
            });

            // average bytes per write (will under report if we must retry writes).
            root.addCounter("bytesPerWrite", new Instrument<Double>() {
                public void sample() {
                    final double bytesPerWrite = (nwrite == 0 ? 0d
                            : (bytesWritten / (double)nwrite));
                    setValue(bytesPerWrite);
                }
            });
            
            // elapsed time writing on the backing channel.
            root.addCounter("writeSecs", new Instrument<Double>() {
                public void sample() {
                    setValue(elapsedWriteNanos / 1000000000.);
                }
            });

            return root;

        } // getCounters()

        public String toString() {
            
            return getCounters().toString();
            
        }
        
    } // class WriteCacheCounters
	
	/**
	 * A {@link WriteCache} implementation suitable for an append-only file such
	 * as the {@link DiskOnlyStrategy} or the output file of the
	 * {@link IndexSegmentBuilder}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	public static class FileChannelWriteCache extends WriteCache {

        /**
         * An offset which will be applied to each record written onto the
         * backing {@link FileChannel}. The offset is generally the size of the
         * root blocks for a journal or the checkpoint record for an index
         * segment. It can be zero if you do not have anything at the head of
         * the file.
         * <p>
         * Note: This implies that writing the root blocks is done separately in
         * the protocol since you can't write below this offset otherwise.
         */
		final protected long baseOffset;

		/**
		 * Used to re-open the {@link FileChannel} in this class.
		 */
		public final IReopenChannel<FileChannel> opener;

		/**
		 * @param baseOffset
		 *            An offset
		 * @param buf
		 * @param opener
		 * 
		 * @throws InterruptedException
		 */
        public FileChannelWriteCache(final long baseOffset,
                final ByteBuffer buf, final boolean useChecksum,
                final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

			super(buf, useChecksum);

			if (baseOffset < 0)
				throw new IllegalArgumentException();

			if (opener == null)
				throw new IllegalArgumentException();

			this.baseOffset = baseOffset;

			this.opener = opener;

		}

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffset,
                final Map<Long, RecordMetadata> recordMap, final long nanos)
                throws InterruptedException, IOException {

            final long begin = System.nanoTime();
            
            final int nbytes = data.remaining();

            /*
             * The position in the file at which the record will be written.
             */
            final long pos = baseOffset + firstOffset;

            /*
             * Write bytes in [data] from position to limit onto the channel.
             * 
             * @todo This ignores the timeout.
             */
            final int nwrites = FileChannelUtility.writeAll(opener, data, pos);

            final WriteCacheCounters counters = this.counters.get();
            counters.nwrite += nwrites;
            counters.bytesWritten += nbytes;
            counters.elapsedWriteNanos += (System.nanoTime() - begin);

            return true;

		}

		@Override
		protected Runnable getDownstreamWriteRunnable(QuorumManager quorumManager) {
			// TODO Auto-generated method stub
			return null;
		}

	}

    /**
     * The scattered write cache is used by the {@link RWStore} since the writes
     * can be made to any part of the file assigned for data allocation.
     * 
     * The writeonChannel must therefore utilize the {@link RecordMetadata} to
     * write each update separately.
     */
	public static class FileChannelScatteredWriteCache extends WriteCache {

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
        public FileChannelScatteredWriteCache(final ByteBuffer buf,
                final boolean useChecksum,
                final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

            super(buf, useChecksum);

			if (opener == null)
				throw new IllegalArgumentException();

			this.opener = opener;

		}

		/**
		 * Called by WriteCacheService to process a direct write for large blocks and
		 * also to flush data from dirty caches.
		 */
		@Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffsetIgnored,
                final Map<Long, RecordMetadata> recordMap, final long nanos)
                throws InterruptedException, IOException {

			final long begin = System.nanoTime();

			final int nbytes = data.remaining();

			if (m_written) {
				log.warn("DUPLICATE writeOnChannel for : " + this.hashCode());
			} else {
				assert !this.isEmpty();
				
				m_written = true;
			}
			
			/*
			 * Retrieve the sorted write iterator and write each block to the
			 * file
			 */
			int nwrites = 0;
			final Iterator<Entry<Long, RecordMetadata>> entries = recordMap.entrySet().iterator();
			while (entries.hasNext()) {
				
			    final Entry<Long, RecordMetadata> entry = entries.next();

				final RecordMetadata md = entry.getValue();

				// create a view on record of interest.
				final ByteBuffer view = data.duplicate();
				final int pos = md.bufferOffset;
				view.limit(pos + md.recordLength);
				view.position(pos);

				final long offset = entry.getKey(); // offset in file to update
				nwrites += FileChannelUtility.writeAll(opener, view, offset);
//				if (log.isInfoEnabled())
//					log.info("writing to: " + offset);
			}

			final WriteCacheCounters counters = this.counters.get();
            counters.nwrite += nwrites;
            counters.bytesWritten += nbytes;
            counters.elapsedWriteNanos += (System.nanoTime() - begin);
			
			return true;

		}

		@Override
		protected Runnable getDownstreamWriteRunnable(QuorumManager quorumManager) {
			// TODO Auto-generated method stub
			return null;
		}

	}

    /**
     * To support deletion we will remove any entries for the provided address
     * 
     * @param addr
     * 
     * @todo This seems a bit odd to me. How does this propagate over a write
     *       replication chain? Why not leave the old record in the cache? Or is
     *       this just to yank something out of the cache which was created and
     *       then immediately deleted on the RW store before it could be written
     *       through to the disk? BT 2/26/2010
     */
	public void clearAddrMap(long addr) {
		recordMap.remove(addr);
	}

	boolean m_written = false;

	/**
	 * Called to clear the WriteCacheService map of references to this WriteCache.
	 * 
	 * @param recordMap the map of the WriteCacheService that associates an address with a WriteCache
	 * @throws InterruptedException 
	 */
	public void resetWith(final ConcurrentMap<Long, WriteCache> serviceRecordMap) throws InterruptedException {
	    final Iterator<Long> entries = recordMap.keySet().iterator();
		if (entries.hasNext()) {
            if (log.isInfoEnabled())
                log.info("resetting existing WriteCache: nrecords="
                        + recordMap.size() + ", hashCode=" + hashCode());

			while (entries.hasNext()) {
				final Long addr = entries.next();
				// System.out.println("Removing address: " + addr + " from " + hashCode());
				serviceRecordMap.remove(addr);
			}
			
		} else {
			if (log.isInfoEnabled())
				log.info("clean WriteCache: hashCode=" + hashCode()); // debug to see recycling
			if (m_written) {
				log.warn("Written WriteCache but with no records");
			}
		}
		
		reset(); // must ensure reset state even if cache already empty
	}

	public void setRecordMap(Collection<RecordMetadata> map) {
		throw new RuntimeException("setRecordMap NotImplemented");
	}

	/**
	 * Method called by HA to send to socket.
	 * 
	 * WriteCache serializes the recordMap and then the buffer.  Enabling the use of DirectBuffers if
	 * available.
	 * 
	 * @param ostr
	 * @throws InterruptedException 
	 * @throws IllegalStateException 
	 */
	public void sendTo(ObjectSocketChannelStream out) throws IllegalStateException, InterruptedException {
		ObjectOutputStream outstr = out.getOutputStream();
		sendRecordMap(outstr);
		
		ByteBuffer tmp = acquire();
		try {
			outstr.writeInt(tmp.position());
			out.getChannel().write(tmp);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			release();
		}
	}

	/**
	 * This is the HA chaining method for the writeCache.  Setting this writeCache to the
	 * state defined by the inputStream, and parsing data on to the output stream if not null
	 * 
	 * FIXME: The initial implementation populates the ByteBuffer from the input channel and
	 * then writes to the output.  We would like to be able to write concurrently with the
	 * input using non-blocking IO.  This isn't necessarily a huge win if the network IO is significantly
	 * faster than the disk IO, since we need a full buffer in order to write to start the write task.
	 * 
	 * @param in
	 * @param out
	 * @throws InterruptedException 
	 * @throws IllegalStateException 
	 */
	public void receiveAndForward(ObjectSocketChannelStream in, ObjectSocketChannelStream out) throws IllegalStateException, InterruptedException {
		receiveRecordMap(in.getInputStream());
		
		ByteBuffer tmp = acquire();
		try {
			int sze = in.getInputStream().readInt();
			tmp.position(0);
			tmp.limit(sze);
			in.getChannel().read(tmp);
			
			if (out != null) {
				sendRecordMap(out.getOutputStream());
				
				out.getChannel().write(tmp);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			release();
		}
		
	}

	/**
	 * If scattered writes then send whole map... 
	 * TODO: optimise for non-scattered! 
	 */
	private void sendRecordMap(ObjectOutputStream out) {
		Collection<RecordMetadata> data = recordMap.values();
		try {
			out.writeInt(data.size());
			Iterator<RecordMetadata> values = data.iterator();
			while (values.hasNext()) {
				RecordMetadata md = values.next();
				out.writeLong(md.fileOffset);
				out.writeInt(md.bufferOffset);
				out.writeInt(md.recordLength);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void receiveRecordMap(ObjectInputStream in) {
		try {
			int mapsize = in.readInt();
			while (mapsize-- > 0) {
				long fileOffset = in.readLong();
				int bufferOffset = in.readInt();
				int recordLength = in.readInt();
				
				recordMap.put(fileOffset, new RecordMetadata(fileOffset, bufferOffset, recordLength));
				if (recordMap.size() == 1) {
					firstOffset.set(fileOffset);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}
}
