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

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.journal.ha.Quorum;
import com.bigdata.journal.ha.QuorumManager;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.RWWriteCacheService;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A {@link WriteCacheService} is provisioned with some number of
 * {@link WriteCache} buffers and a writer thread. Caller's populate
 * {@link WriteCache} instances. When they are full, they are transferred to a
 * queue which is drained by the thread writing on the local disk. Hooks are
 * provided to wait until the current write set has been written (e.g., at a
 * commit point when the cached writes must be written through to the backing
 * channel). This implementation supports high availability using a write
 * replication pipeline.
 * <p>
 * A pool of {@link WriteCache} instances is used. Readers test all of the
 * {@link WriteCache} using a shared {@link ConcurrentMap} and will return
 * immediately the desired record or <code>null</code> if the record is not in
 * any of the {@link WriteCache} instances. Write caches remain available to
 * readers until they need to be recycled as the current write cache (the one
 * servicing new writes).
 * <p>
 * The {@link WriteCacheService} maintains a dirty list of {@link WriteCache}
 * instances. A single thread handle writes onto the disk and onto the write
 * replication pipeline (for HA). When the caller calls flush() on the write
 * cache service it flush() the current write cache is transferred to the dirty
 * list and then wait until the write cache instances now on the dirty list have
 * been serviced. In order to simplify the design and the provide boundary
 * conditions for HA decision making, writers block during
 * {@link #flush(boolean, long, TimeUnit)}.
 * <p>
 * Instances of this class are used by both the {@link RWStrategy} and the
 * {@link WORMStrategy}. These classes differ in how they allocate space on the
 * backing file and in the concurrency which they permit for writers.
 * <dl>
 * <dt>{@link WORMStrategy}</dt>
 * <dd>The {@link WORMStrategy} serializes all calls to
 * {@link #writeChk(long, ByteBuffer, int)} since it must guarantee the precise
 * offset at which each record is written onto the backing file. As a
 * consequence of its design, each {@link WriteCache} is a single contiguous
 * chunk of data and is transferred directly to a known offset on the disk. This
 * append only strategy makes for excellent transfer rates to the disk.</dd>
 * <dt>{@link RWStrategy}</dt>
 * <dd>The {@link RWStrategy} only needs to serialize the decision making about
 * the offset at which the records are allocated. Since the records may be
 * allocated at any location in the backing file, each {@link WriteCache}
 * results in a scattered write on the disk.</dd>
 * </dl>
 * Both the {@link WORMStrategy} and the {@link RWStrategy} implementations need
 * to also establish a read-write lock to prevent changes in the file extent
 * from causing corrupt data for concurrent read or write operations on the
 * file. See {@link #writeChk(long, ByteBuffer, int)} for more information on
 * this issue (it is a workaround for a JVM bug).
 * 
 * <h2>Checksums</h2>
 * 
 * The WORM and RW buffer strategy implementations, the WriteCacheService, and
 * the WriteCache all know whether or not checksums are in use. When they are,
 * the buffer strategy computes the checksum and passes it down (otherwise it
 * passes down a 0, which will be ignored since checksums are not enabled). The
 * WriteCache adjusts its capacity by -4 when checksums are enabled and adds the
 * checksum when transferring the caller's data into the WriteCache. On read,
 * the WriteCache will verify the checksum if it exists and returns a new
 * allocation backed by a byte[] showing only the caller's record.
 * <p>
 * {@link IAddressManager#getByteCount(long)} must be the actual on the disk
 * record length, not the size of the record when it reaches the application
 * layer. This on the disk length is the adjusted size after optional
 * compression and with the optional checksum. Applications which assume that
 * lengthOf(addr) == byte[].length will break, but that's life.
 * 
 * @see WriteCache
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There needs to be a unit test which verifies overwrite of a record in
 *       the {@link WriteCache}. It is possible for this to occur with the
 *       {@link RWStore} (highly unlikely, but possible).
 * 
 * @todo When compression is enabled, it is applied above the level of the
 *       {@link WriteCache} and {@link WriteCacheService} (which after all
 *       require the caller to pass in the checksum of the compressed record).
 *       It is an open question as to whether the caller or the store handles
 *       record compression. Note that the B+Tree leaf and node records may
 *       require an uncompressed header to allow fixup of the priorAddr and
 *       nextAddr fields.
 */
abstract public class WriteCacheService implements IWriteCache {

    protected static final Logger log = Logger
            .getLogger(WriteCacheService.class);

    /**
     * <code>true</code> until the service is {@link #close() closed}.
     */
    private volatile boolean open = true;

    /**
     * <code>true</code> iff record level checksums are enabled.
     */
    final private boolean useChecksum;
    
    /**
     * A single threaded service which writes dirty {@link WriteCache}s onto the
     * backing store.
     */
    final private ExecutorService localWriteService;

    /**
     * The {@link Future} of the task running on the {@link #localWriteService}.
     * 
     * @see WriteTask
     * @see #reset()
     */
    private Future<Void> localWriteFuture;

    /**
     * A single threaded service which writes dirty {@link WriteCache}s onto the
     * downstream service in the quorum.
     */
    final private ExecutorService remoteWriteService;

    /**
     * The {@link Future} of the task running on the {@link #remoteWriteService}.
     * <p>
     * Note: Since this is <em>volatile</em> you MUST guard against concurrent
     * clear to <code>null</code> by {@link #reset()}.
     * 
     * @see WriteTask
     * @see #reset()
     */
    private volatile Future<?> remoteWriteFuture = null;
    
    /**
     * A list of clean buffers. By clean, we mean not needing to be written.
     * Once a dirty write cache has been flushed, it is placed onto the
     * {@link #cleanList}. Clean buffers can be taken at any time for us as the
     * current buffer.
     */
    final private BlockingQueue<WriteCache> cleanList;

    /**
     * Lock for the {@link #cleanList} allows us to notice when it becomes empty
     * and not-empty.
     */
    final private ReentrantLock cleanListLock = new ReentrantLock();

    /**
     * Condition <code>!cleanList.isEmpty()</code>
     * <p>
     * Note: If you wake up from this condition you MUST also test {@link #halt}.
     */
    final private Condition cleanListNotEmpty = cleanListLock.newCondition();
    
    /**
     * The read lock allows concurrent {@link #acquireForWriter()}s while the write lock
     * prevents {@link #acquireForWriter()} when we must either reset the
     * {@link #current} cache buffer or change the {@link #current} reference.
     * E.g., {@link #flush(boolean, long, TimeUnit)}.
     */
    final private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * A list of dirty buffers. Writes from these may be combined, but not
     * across {@link #flush(boolean)}.
     */
    final private BlockingQueue<WriteCache> dirtyList;

    /**
     * Lock for the {@link #dirtyList} allows us to notice when it becomes empty
     * and not-empty.
     */
    final private ReentrantLock dirtyListLock = new ReentrantLock();

    /**
     * Lock used to put cache buffers onto the {@link #dirtyList}. This lock is
     * required in order for {@link #flush(boolean, long, TimeUnit)} to have
     * atomic semantics, otherwise new cache buffers could be added to the dirty
     * list. This lock is distinct from the {@link #lock} because we do not want
     * to yield that lock when awaiting the {@link #dirtyListEmpty} condition.
     * <p>
     * Note: If you wake up from this condition you MUST also test {@link #halt}.
     * 
     * @see #dirtyListLock.
     */
    final private Condition dirtyListEmpty = dirtyListLock.newCondition();

    /**
     * Condition signaled whenever the dirty list becomes non-empty.
     * <p>
     * Note: If you wake up from this condition you MUST also test {@link #halt}.
     */
    final private Condition dirtyListNotEmpty = dirtyListLock.newCondition();

    /**
     * The current buffer. Modification of this value and reset of the current
     * {@link WriteCache} are protected by the write lock of {@link #lock()}.
     */
    final private AtomicReference<WriteCache> current = new AtomicReference<WriteCache>();

    /**
     * Flag set if {@link WriteTask} encounters an error. The cause is set on
     * {@link #firstCause} as well.
     * 
     * FIXME Error handling for this must cause the write cache service buffers
     * to be {@link #reset()} and make sure the HA write pipeline is correctly
     * configured.
     * <p>
     * A high-level abort() is necessary. It is NOT Ok to simply re-try writes
     * of partly filled buffers since they may already have been partly written
     * to the disk. A high-level abort() is necessary to ensure that we discard
     * any bad writes. abort() will have to reconfigure the WriteCacheService,
     * perhaps using a new instance. The abort() will need to propagate to all
     * members of the {@link Quorum} so they are all reset to the last commit
     * point and have reconfigured write cache services and write pipelines.
     */
    private volatile boolean halt = false;
    
    /**
     * The first cause of an error within the asynchronous {@link WriteTask}.
     */
    private final AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>();
        
    /**
     * The capacity of the cache buffers. This is assumed to be the same for
     * each buffer.
     */
    final private int capacity;

    /**
     * Object knows how to (re-)open the backing channel.
     */
    final private IReopenChannel<? extends Channel> opener;

    /**
     * The object which notices the quorum change events.
     */
    final protected QuorumManager quorumManager;

    /**
     * A map from the offset of the record on the backing file to the cache
     * buffer on which that record was written.
     * 
     * FIXME This field probably should be private.
     * {@link RWWriteCacheService#clearWrite(long)} currently uses this field,
     * but I do not believe that it does so in a safe manner.
     */
    final protected ConcurrentMap<Long/* offset */, WriteCache> recordMap;

    /**
     * An immutable array of the {@link WriteCache} buffer objects owned by the
     * {@link WriteCacheService} (in contract to those owner by the caller but
     * placed onto the {@link #dirtyList} by
     * {@link #writeChk(long, ByteBuffer, int)}).
     */
    final private WriteCache[] buffers;

    /**
     * Allocates N buffers from the {@link DirectBufferPool}.
     * 
     * @param nbuffers
     *            The #of buffers to allocate.
     * @param useChecksum
     *            <code>true</code> iff record level checksums are enabled.
     * @param opener
     *            The object which knows how to (re-)open the channel to which
     *            cached writes are flushed.
     * 
     * @throws InterruptedException
     */
    public WriteCacheService(final int nbuffers,
            final boolean useChecksum,
            final IReopenChannel<? extends Channel> opener,
            final QuorumManager quorumManager)
            throws InterruptedException {

        if (nbuffers <= 0)
            throw new IllegalArgumentException();

        if (opener == null)
            throw new IllegalArgumentException();

        if (quorumManager == null)
            throw new IllegalArgumentException();

        this.useChecksum = useChecksum;
        
        this.opener = opener;

        this.quorumManager = quorumManager;
        
        dirtyList = new LinkedBlockingQueue<WriteCache>();

        cleanList = new LinkedBlockingQueue<WriteCache>();
        
        buffers = new WriteCache[nbuffers];

        // N-1 WriteCache instances.
        for (int i = 0; i < nbuffers - 1; i++) {

            final WriteCache tmp = newWriteCache(null/* buf */, useChecksum,
                    opener);

            buffers[i] = tmp;

            cleanList.add(tmp);

        }

        // One more WriteCache for [current].
        current.set(buffers[nbuffers - 1] = newWriteCache(null/* buf */,
                useChecksum, opener));

        // assume capacity is the same for each buffer instance.
        capacity = current.get().capacity();

        // set initial capacity based on an assumption of 1k buffers.
        recordMap = new ConcurrentHashMap<Long, WriteCache>(nbuffers
                * (capacity / 1024));

        // start service to write on the backing channel.
        localWriteService = Executors
                .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                        .getName()));

        if (quorumManager.replicationFactor() > 1) {
            // service used to write on the downstream node in the quorum.
            remoteWriteService = Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                            .getName()));
        } else {
            remoteWriteService = null;
        }
        
        // run the write task
        localWriteFuture = localWriteService.submit(new WriteTask());

    }

    /**
     * The task responsible for writing dirty buffers onto the backing channel
     * and onto the downstream {@link Quorum} member if the service is highly
     * available.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class WriteTask implements Callable<Void> {

        public Void call() throws Exception {
            while (true) {
                assert !halt;
                dirtyListLock.lockInterruptibly();
                try {
                    /*
                     * Note: If there is an error in this thread then it needs
                     * to be propagated to the threads write()ing on the cache
                     * or awaiting flush() and from there back to the caller and
                     * an abort(). We do not need to bother the readers since
                     * the read() methods all allow for concurrent close() and
                     * will return null rather than bad data. The reprovisioning
                     * of the write cache service (e.g., by reset()) must hold
                     * the writeLock so as to occur when there are no
                     * outstanding reads executing against the write cache
                     * service.
                     */
                    while (dirtyList.isEmpty() && !halt) {
                        dirtyListNotEmpty.await();
                    }
                    if (halt)
                        throw new RuntimeException(firstCause.get());
                    
                    // Guaranteed available.
                    final WriteCache cache = dirtyList.take();

                    if (dirtyList.isEmpty()) {
                        /*
                         * Signal Condition when we release the dirtyListLock.
                         */
                        dirtyListEmpty.signalAll();
                    }

                    if (remoteWriteService != null) {
                        // Start downstream IOs (network IO).
                        remoteWriteFuture = remoteWriteService.submit(cache
                                .getDownstreamWriteRunnable(quorumManager));
                    }

                    // Do the local IOs.
                    cache.flush(false/* force */);

                    {
                        final Future<?> tmp = remoteWriteFuture;
                        if (tmp != null) {
                            // Wait for the downstream IOs to finish.
                            tmp.get();
                        }
                    }

                    /*
                     * Add to the cleanList (all buffers are our buffers).
                     */
                    cleanListLock.lockInterruptibly();
                    try {
                        cleanList.add(cache);
                        /*
                         * Since we a holding the lock for both the clean and
                         * the dirty buffer, make sure that the sizes of these
                         * two lists are LTE to the #of buffers (one buffer may
                         * be on [current] as well).
                         */
                        assert dirtyList.size() + cleanList.size() <= buffers.length : "#dirty="
                                + dirtyList.size()
                                + ", #clean="
                                + cleanList.size()
                                + ", #buffers="
                                + buffers.length;
                        cleanListNotEmpty.signalAll();
                    } finally {
                        cleanListLock.unlock();
                    }

                } catch (InterruptedException t) {
                    /*
                     * This task can only be interrupted by a thread with its
                     * Future, so this interrupt is a clear signal that the
                     * write cache service is closing down.
                     */
                    return null;
                } catch (Throwable t) {
                    /*
                     * Anything else is an error and halts processing. Error
                     * processing MUST a high-level abort() and MUST do a
                     * reset() if this WriteCacheService instance will be
                     * reused.
                     * 
                     * Note: If a WriteCache was taken from the dirtyList above
                     * then it will have been dropped. However, all of the
                     * WriteCache instances owned by the WriteCacheService are
                     * in [buffers] and reset() is written in terms of [buffers]
                     * precisely so we do not loose buffers here.
                     */
                    if(firstCause.compareAndSet(null/*expect*/, t/*update*/)) {
                        halt = true;
                    }
                    /*
                     * Signal anyone blocked on the dirtyList or cleanList
                     * Conditions. They need to notice the change in [halt]
                     * and wrap and rethrow [firstCause].
                     */
                    dirtyListEmpty.signalAll();
                    dirtyListNotEmpty.signalAll();
                    cleanListLock.lock();
                    try {
                        cleanListNotEmpty.signalAll();
                    } finally {
                        cleanListLock.unlock();
                    }
                    log.error(t, t);
                    /*
                     * Halt processing. The WriteTask must be restarted by
                     * reset.
                     */
                    return null;
                } finally {
                    dirtyListLock.unlock();
                }

            } // while(true)

        } // call()

    } // class WriteTask

    /**
     * Factory for {@link WriteCache} implementations.
     * 
     * @param buf
     *            The backing buffer (optional).
     * @param useChecksum
     *            <code>true</code> iff record level checksums are enabled.
     * @param opener
     *            The object which knows how to re-open the backing channel
     *            (required).
     * 
     * @return A {@link WriteCache} wrapping that buffer and able to write on
     *         that channel.
     * 
     * @throws InterruptedException
     */
    abstract protected WriteCache newWriteCache(ByteBuffer buf,
            boolean useChecksum, IReopenChannel<? extends Channel> opener)
            throws InterruptedException;

    /**
     * {@inheritDoc}
     * <p>
     * This implementation calls {@link IWriteCache#reset()} on all
     * {@link #buffers} and moves them onto the {@link #cleanList}. Note that
     * this approach deliberately does not cause any buffers belonging to the
     * caller of {@link #writeChk(long, ByteBuffer, int)} to become part of the
     * {@link #cleanList}.
     */
    public void reset() throws InterruptedException {
        final WriteLock writeLock = lock.writeLock();
        writeLock.lockInterruptibly();
        try {
            if(!open) {
                // Reset can not recover from close().
                throw new IllegalStateException();
            }

            // cancel the current WriteTask.
            localWriteFuture.cancel(true/* mayInterruptIfRunning */);
            final Future<?> rwf = remoteWriteFuture;
            if (rwf != null) {
                rwf.cancel(true/* mayInterruptIfRunning */);
            }

            // drain the dirty list.
            dirtyListLock.lockInterruptibly();
            try {
                dirtyList.drainTo(new LinkedList<WriteCache>());
                dirtyListEmpty.signalAll();
                dirtyListNotEmpty.signalAll(); // NB: you must verify Condition once signaled!
            } finally {
                dirtyListLock.unlock();
            }
            
            // re-populate the clean list with our buffers.
            cleanListLock.lockInterruptibly();
            try {
                cleanList.drainTo(new LinkedList<WriteCache>());
                cleanListNotEmpty.signalAll();
            } finally {
                cleanListLock.unlock();
            }
            
            // reset each buffer.
            for(WriteCache t : buffers) {
                t.reset();
            }
            
            // re-populate the clean list with N-1 of our buffers
            for (int i = 0; i < buffers.length - 1; i++) {
                cleanList.put(buffers[i]);
            }
            
            // clear the service record map.
            recordMap.clear();
            
            // set the current buffer.
            current.set(buffers[buffers.length - 1]);

            // Restart the WriteTask.
            try {
                localWriteFuture.get();
            } catch (Throwable t) {
                // ignored.
            }
            if (rwf != null) {
                try {
                    rwf.get();
                } catch (Throwable t) {
                    // ignored.
                }
            }
            this.localWriteFuture = localWriteService.submit(new WriteTask());
            this.remoteWriteFuture = null;
            
        } finally {
            writeLock.unlock();
        }
    }

    public void close() throws InterruptedException {
        /*
         * Note: The write lock prevents concurrent close by another thread and
         * is also required for the operations we take on the dirtyList, the
         * cleanList, and current.
         */
        final WriteLock writeLock = lock.writeLock();
        writeLock.lockInterruptibly();
        try {

            if (!open) {
                // Already closed, so this is a NOP.
                return;
            }

            // Interrupt the write task.
            localWriteFuture.cancel(true/* mayInterruptIfRunning */);
            final Future<?> rwf = remoteWriteFuture;
            if (rwf != null) {
                rwf.cancel(true/* mayInterruptIfRunning */);
            }

            // Immediate shutdown of the write service.
            localWriteService.shutdownNow();

            if (remoteWriteService != null) {

                // Immediate shutdown of the write service.
                remoteWriteService.shutdownNow();

            }

            /*
             * Ensure that the WriteCache buffers are close()d in a timely
             * manner.
             */

            // reset buffers on the dirtyList.
            dirtyListLock.lockInterruptibly();
            try {
                dirtyList.drainTo(new LinkedList<WriteCache>());
                dirtyListEmpty.signalAll();
                dirtyListNotEmpty.signalAll();
            } finally {
                dirtyListLock.unlock();
            }

            // close() buffers on the cleanList.
            cleanListLock.lockInterruptibly();
            try {
                cleanList.drainTo(new LinkedList<WriteCache>());
            } finally {
                cleanListLock.unlock();
            }

            // close all buffers.
            for(WriteCache t : buffers) {
                t.close();
            }

            // clear reference to the current buffer.
            current.getAndSet(null);
            
            // clear the service record map.
            recordMap.clear();
            
            // Closed.
            open = false;

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Ensures that {@link #close()} is eventually invoked so the buffers can be
     * returned to the {@link DirectBufferPool}.
     * 
     * @throws Throwable
     */
    protected void finalized() throws Throwable {

        close();
        
    }

    /**
     * This method is called ONLY by write threads and verifies that the service
     * is {@link #open}, that the {@link WriteTask} has not been {@link #halt
     * halted}, and that the {@link WriteTask} is still executing (in case any
     * uncaught errors are thrown out of {@link WriteTask#call()}.
     * <p>
     * Note: {@link #read(long)} DOES NOT throw an exception if the service is
     * closed, asynchronously closed, or even just plain dead. It just returns
     * <code>null</code> to indicate that the desired record is not available
     * from the cache.
     * 
     * @throws IllegalStateException
     *             if the service is closed.
     * @throws RuntimeException
     *             if the {@link WriteTask} has failed.
     */
    private void assertOpenForWriter() {

        if (!open)
            throw new IllegalStateException();

        if (halt)
            throw new RuntimeException(firstCause.get());
        
        if (localWriteFuture.isDone()) {

            /*
             * If the write task terminates abnormally then throw the exception
             * out here.
             */

            try {

                localWriteFuture.get();

            } catch (Throwable t) {

                throw new RuntimeException(t);

            }

        }

    }

    /**
     * Return the current buffer to a write thread. Once they are done, the
     * caller MUST call {@link #release()}.
     * 
     * @return The buffer.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the {@link WriteCacheService} is closed.
     * @throws RuntimeException
     *             if the service has been {@link #halt halted}
     */
    private WriteCache acquireForWriter() throws InterruptedException,
            IllegalStateException {

        final ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();
        
        try {

            /*
             * We only want to throw errors from the WriteTask out of write()
             * and flush(). However, this method is NOT invoked by read() which
             * uses a different non-blocking protocol to access the record if it
             * is in a cache buffer.
             */
            assertOpenForWriter();

            /*
             * Note: acquire() does not block since it holds the ReadLock.
             * Methods which change [current] MUST hold the WriteLock across
             * that operation to ensure that [current] is always non-null since
             * acquire() will not block once it acquires the ReadLock.
             */
            final WriteCache tmp = current.get();

            if (tmp == null) {

                throw new RuntimeException();

            }

            // Note: The ReadLock is still held!
            return tmp;

        } catch(Throwable t){

            /*
             * Note: release the lock only on the error path.
             */

            readLock.unlock();

            if (t instanceof InterruptedException)
                throw (InterruptedException) t;

            if (t instanceof IllegalStateException)
                throw (IllegalStateException) t;

            throw new RuntimeException(t); 
            
        }

    }

    /**
     * Release the latch on an acquired buffer.
     */
    private void release() {

        /*
         * Note: This is releasing the ReadLock which was left open by
         * acquire().
         */
        lock.readLock().unlock();

    }

    /**
     * Flush the current write set through to the backing channel.
     * 
     * @throws InterruptedException
     */
    public void flush(final boolean force) throws InterruptedException {

        try {

            if (!flush(force, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {

                throw new RuntimeException();

            }

        } catch (TimeoutException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * flush() is a blocking method. At most one flush() operation may run at a
     * time. The {@link #current} buffer is moved to the {@link #dirtyList}
     * while holding the {@link WriteLock} and flush() then waits until the
     * dirtyList becomes empty, at which point all dirty records have been
     * written through to the backing file.
     * <p>
     * Note: Any exception thrown from this method MUST trigger error handling
     * resulting in a high-level abort() and {@link #reset()} of the
     * {@link WriteCacheService}.
     * 
     * @see WriteTask
     * @see #dirtyList
     * @see #dirtyListEmpty
     * 
     * @todo Note: flush() is currently designed to block concurrent writes() in
     *       order to give us clean decision boundaries for the HA write
     *       pipeline and also to simplify the internal locking design. Once we
     *       get HA worked out cleanly we should explore whether or not we can
     *       relax this constraint such that writes can run concurrently with
     *       flush(). That would have somewhat higher throughput since mutable
     *       B+Tree evictions would no longer cause concurrent tasks to block
     *       during the commit protocol or the file extent protocol.
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit units) throws TimeoutException, InterruptedException {

        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos; 

        final WriteLock writeLock = lock.writeLock();
        if (!writeLock.tryLock(remaining, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            final WriteCache tmp = current.get();
            // remaining := (total - elapsed).
            remaining = nanos - (System.nanoTime() - begin);
            if (!dirtyListLock.tryLock(remaining, TimeUnit.NANOSECONDS))
                throw new TimeoutException();
            try {
                /*
                 * Note: [tmp] may be empty, but there is basically zero cost in
                 * WriteTask to process and empty buffer and, done this way, the
                 * code is much less complex here.
                 */
                dirtyList.add(tmp);
                dirtyListNotEmpty.signalAll();
                while (!dirtyList.isEmpty() && !halt) {
                    // remaining := (total - elapsed).
                    remaining = nanos - (System.nanoTime() - begin);
                    if (!dirtyListEmpty.await(remaining, TimeUnit.NANOSECONDS)) {
                        throw new TimeoutException();
                    }
                }
                if (halt)
                    throw new RuntimeException(firstCause.get());
            } finally {
                dirtyListLock.unlock();
            }
            /*
             * Replace [current] with a clean cache buffer.
             */
            // remaining := (total - elapsed).
            remaining = nanos - (System.nanoTime() - begin);
            if (!cleanListLock.tryLock(remaining, TimeUnit.NANOSECONDS))
                throw new TimeoutException();
            try {
                // Note: use of Condition let's us notice [halt].
                while (cleanList.isEmpty() && !halt) {
                    // remaining := (total - elapsed).
                    remaining = nanos - (System.nanoTime() - begin);
                    if (!cleanListNotEmpty.await(remaining,TimeUnit.NANOSECONDS)) {
                        throw new TimeoutException();
                    }
                    if (halt)
                        throw new RuntimeException(firstCause.get());
                }
                // Guaranteed available hence non-blocking. 
                final WriteCache nxt = cleanList.take();
                nxt.resetWith(recordMap);
                current.set(nxt);
                return true;
            } finally {
                cleanListLock.unlock();
            }
        } finally {
            writeLock.unlock();
        }
    }

//    public boolean write(long offset, ByteBuffer data)
//            throws IllegalStateException, InterruptedException {
//
//        return writeChk(offset, data, 0);
//
//    }

    /**
     * Write the record onto the cache. If the record is too large for the cache
     * buffers, then it is written synchronously onto the backing channel.
     * Otherwise it is written onto a cache buffer which is lazily flushed onto
     * the backing channel. Cache buffers are written in order once they are
     * full. This method does not impose synchronization on writes which fit the
     * capacity of a cache buffer.
     * <p>
     * When integrating with the {@link RWStrategy} or the {@link WORMStrategy}
     * there needs to be a read/write lock such that file extension is mutually
     * exclusive with file read/write operations (due to a Sun bug). The caller
     * can override {@link #newWriteCache(ByteBuffer, IReopenChannel)} to
     * acquire the necessary lock (the read lock of a {@link ReadWriteLock}).
     * This is even true when the record is too large for the cache since we
     * delegate the write to a temporary {@link WriteCache} wrapping the
     * caller's buffer.
     * <p>
     * Note: Any exception thrown from this method MUST trigger error handling
     * resulting in a high-level abort() and {@link #reset()} of the
     * {@link WriteCacheService}.
     * 
     * @return <code>true</code> since the record is always accepted by the
     *         {@link WriteCacheService} (unless an exception is thrown).
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
     * 
     * @todo The WORM serializes invocations on this method because it must put
     *       each record at a specific offset into the user extent of the file.
     *       However, the RW store does not do this. Therefore, for the RW store
     *       only, we could use a queue with lost cost access and scan for best
     *       fit packing into the write cache buffer. When a new buffer is set
     *       as [current], we could pack the larger records in the queue onto
     *       that buffer first. This might provide better throughput for the RW
     *       store but would require an override of this method specific to that
     *       implementation.
     */
    public boolean write(final long offset, final ByteBuffer data, final int chk)
            throws InterruptedException, IllegalStateException {

        if (log.isInfoEnabled()) {
            log.info("offset: " + offset + ", length: " + data.limit()
                    + ", chk=" + chk+", useChecksum="+useChecksum);
        }
    	
        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        // #of bytes in the record.
        final int remaining = data.remaining();

        // #of bytes to be written.
        final int nwrite = remaining + (useChecksum ? 4 : 0);
        
        if (remaining == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        if (nwrite > capacity) {

            /*
             * Handle large records.
             */
            return writeLargeRecord(offset, data, chk);
            
        }

        /*
         * The record can fit into a cache instance, so try and acquire one and
         * write the record onto it.
         * 
         * @todo this could be refactored to use moveBufferToDirtyList()
         */
        {

            final WriteCache cache = acquireForWriter();

            try {

                // write on the cache.
                if (cache.write(offset, data, chk)) {

                    /*
                     * Note: We MUST use put() here rather than putIfAbsent()
                     * and the return value MAY be non-null. This condition
                     * arises when there exists a record on a clean buffer which
                     * was part of an aborted write set. Such records may be
                     * rewritten following the abort. Since the record was
                     * already laid down on the backing channel, there is no
                     * point clearing it from the clean write cache buffers,
                     * which would require us to track all records in a given
                     * write set (not very scalable).
                     * 
                     * Note: put() SHOULD return non-null further down in this
                     * method since we will always be looking at a new buffer in
                     * those code paths.
                     * 
                     * FIXME The first paragraph above does not seem to be
                     * correct to me and is inconsistent with throwing an
                     * assertion if put() returns non-null (which is what we are
                     * doing immediately below this comment).
                     */
                    if (recordMap.put(offset, cache) != null) {
                        throw new AssertionError(
                                "Record already in cache: offset=" + offset);
                    }

                    return true;

                }

            } finally {

                release();

            }

        }

        /*
         * The record did not fit into the current buffer but it is small enough
         * to fit into an empty buffer. Grab the write lock and then try again.
         * If it still does not fit, then put the current buffer onto the dirty
         * list and take a buffer from the clean list and then write the record
         * onto that buffer while we are holding the lock. This last step must
         * succeed since the buffer will be empty and the record can fit into an
         * empty buffer.
         */
        {

            final Lock writeLock = lock.writeLock();

            writeLock.lockInterruptibly();

            try {

                /*
                 * While holding the write lock, see if the record can fit into
                 * the current buffer. Note that the buffer we acquire here MAY
                 * be a different buffer since a concurrent write could have
                 * already switched us to a new buffer. In that case, the record
                 * might fit into the new buffer.
                 */

                // Acquire a buffer. Maybe the same one, maybe different.
                WriteCache cache = acquireForWriter();

                try {

                    // While holding the write lock, see if the record fits.
                    if (cache.write(offset, data, chk)) {

                        /*
                         * It fits: someone already changed to a new cache,
                         * which is fine.
                         */
                        if (recordMap.put(offset, cache) != null) {
                            // The record should not already be in the cache.
                            throw new AssertionError(
                                    "Record already in cache: offset=" + offset);
                        }

                        return true;

                    }

                    /*
                     * There is not enough room in the current buffer for this
                     * record, so put the buffer onto the dirty list. Then take
                     * a new buffer from the clean list (block), reset the
                     * buffer to clear the old writes, and set it as current. At
                     * that point, the record should always fit.
                     * 
                     * Note: When we take a cache instances from the cleanList
                     * we need to remove any entries in our recordMap which are
                     * in its record map.
                     * 
                     * Note: We move the current buffer to the dirty list before
                     * we take a buffer from the clean list. This is absolutely
                     * necessary since the code will otherwise deadlock if there
                     * is only one buffer.
                     * 
                     * Note: Do NOT yield the WriteLock here. That would make it
                     * possible for another thread to acquire() the current
                     * buffer, which has already been placed onto the dirtyList
                     * by this thread!!!
                     */

                    /*
                     * Move the current buffer to the dirty list.
                     * 
                     * Note: The lock here is required to give flush() atomic
                     * semantics with regard to the set of dirty write buffers
                     * when flush() gained the writeLock [in fact, we only need
                     * the dirtyListLock for the dirtyListEmpty Condition].
                     */
                    dirtyListLock.lockInterruptibly();
                    try {
                        dirtyList.add(cache);
                        dirtyListNotEmpty.signalAll();
                    } finally {
                        dirtyListLock.unlock();
                    }

                    /*
                     * Take the buffer from the cleanList and set it has the
                     * [current] buffer.
                     * 
                     * Note: We use the [cleanListNotEmpty] Condition so we can
                     * notice a [halt].
                     */
                    cleanListLock.lockInterruptibly();

                    try {
                    
                        while (cleanList.isEmpty() && !halt) {
                            cleanListNotEmpty.await();
                        }

                        if (halt)
                            throw new RuntimeException(firstCause.get());
                        
                        // Take a buffer from the cleanList (guaranteed avail).
                        final WriteCache newBuffer = cleanList.take();

                        // Clear the state on the new buffer and remove from
                        // cacheService map
                        newBuffer.resetWith(recordMap);

                        // Set it as the new buffer.
                        current.set(cache = newBuffer);

                        // Try to write on the new buffer.
                        if (cache.write(offset, data, chk)) {

                            // This must be the only occurrence of this record.
                            if (recordMap.put(offset, cache) != null) {
                                throw new AssertionError(
                                        "Record already in cache: offset="
                                                + offset);
                            }

                            return true;

                        }

                    } finally {
                        
                        cleanListLock.unlock();
                        
                    }

                    /*
                     * Should never happen.
                     */
                    throw new IllegalStateException("Unable to write into current WriteCache");

                } finally {

                    release();

                }

            } finally {

                writeLock.unlock();
                
            }

        }

    }

    /**
     * Write a record whose size (when combined with the optional checksum) is
     * larger than the capacity of an individual {@link WriteCache} buffer. This
     * operation is synchronous (to protect the ByteBuffer from concurrent
     * modification by the caller). It will block until the record has been
     * written.
     * <p>
     * This implementation will write the record onto a sequence of
     * {@link WriteCache} objects and wait until all of those objects have been
     * written through to the backing file and the optional HA write pipeline. A
     * checksum will be appended after the last chunk of the record. This
     * strategy works for the WORM since the bytes will be laid out in a
     * contiguous region on the disk.
     * <p>
     * Note: For the WORM, this code MUST NOT allow the writes to proceed out of
     * order or the data will not be laid out correctly on the disk !!!
     * <p>
     * Note: The RW store MUST NOT permit individual allocations whose size on
     * the disk is greater than the capacity of an individual {@link WriteCache}
     * buffer (@todo Or is this Ok? Perhaps it is if the RW store holds a lock
     * across the write for a large record? Maybe if we also add a low-level
     * method for inserting an entry into the record map?)
     * <p>
     * Note: This method DOES NOT register the record with the shared
     * {@link #recordMap}. Since the record spans multiple {@link WriteCache}
     * objects it can not be directly recovered without reading it from the
     * backing file.
     * 
     * <h2>Dialog on large records</h2>
     * 
     * It seems to me that the RW store is designed to break up large records
     * into multiple allocations. If we constrain the size of the largest
     * allocation slot on the RW store to be the capacity of a WriteCache buffer
     * (including the bytes for the checksum and other record level metadata)
     * then we do not have a problem with breaking up large records for it in
     * the WriteCacheService and it will automatically benefit from HA using the
     * write replication logic.
     * <p>
     * The WORM does not have these limits on the allocation size, so it seems
     * likely that breaking it up across multiple WriteCache buffer instances
     * would have to be done inside of the WriteCacheService in order to prevent
     * checksums from being interleaved with each WriteCache worth of data it
     * emits for a large record. We can't raise this out of the
     * WriteCacheService because the large record would not be replicated for
     * HA.
     */
    protected boolean writeLargeRecord(final long offset,
            final ByteBuffer data, final int chk) throws InterruptedException,
            IllegalStateException {

        if (log.isInfoEnabled()) {
            log.info("offset: " + offset + ", length: " + data.limit()
                    + ", chk=" + chk + ", useChecksum=" + useChecksum);
        }
        
        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        // #of bytes in the record.
        final int remaining = data.remaining();
        
        if (remaining == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        // Small records should not take this code path.
        if (remaining < capacity)
            throw new RuntimeException();

        /*
         * Put as much into each WriteCache instance as well fit, then transfer
         * the WriteCache onto the dirtyList, take a new WriteCache from the
         * cleanList, and continue until all data as been transferred. If
         * checksums are enabled, add a 4 byte checksum afterwards.
         * 
         * Note: We hold the WriteLock across this operation since we will be
         * changing out [current] each time it fills up. This has the
         * side-effect of guaranteeing that the writes are emitted without
         * intervening writes of other record.
         * 
         * while(r > 0) {
         * 
         * cache = acquire();
         * 
         * copy up to [r] bytes into the buffer.
         * 
         * if the buffer is full, then transfer it to the dirty list.
         * 
         * release()
         * 
         * }
         * 
         * write checksum on buffer
         */

        final Lock writeLock = lock.writeLock();
        writeLock.lockInterruptibly();
        try {
            // the offset of the next byte to transfer to a cache buffer.
            int p = 0;
            // #of bytes remaining in the large record (w/o the checksum).
            int r = remaining;
            while (r > 0) {
                // Acquire a buffer.
                final WriteCache cache = acquireForWriter();
                try {
                    // #of bytes to copy onto the write cache.
                    final int ncpy = Math.min(r, cache.remaining());
                    if (ncpy > 0) {
                        // create view of the data to be copied.
                        final ByteBuffer tmp = data.duplicate();
                        tmp.limit(p + ncpy);
                        tmp.position(p);
                        // Note: For WORM, this MUST NOT add the checksum except
                        // for the last chunk!
                        if (!cache
                                .write(offset + p, tmp, chk, false/* writeChecksum */))
                            throw new AssertionError();
                        r -= ncpy;
                        p += ncpy;
                    }
                    if (cache.remaining() == 0) {
                        moveBufferToDirtyList();
                    }
                } finally {
                    release();
                }
            } // while( remaining > 0 )
            /*
             * Now we need to write out the optional checksum. We do not have to
             * flush this write through. The buffer can remain partly full.
             */
            if (useChecksum) {
                // Acquire a buffer.
                final WriteCache cache = acquireForWriter();
                try {
                    // Allocate a small buffer
                    final ByteBuffer t = ByteBuffer.allocate(4);
                    // Add in the record checksum.
                    t.putInt(chk);
                    // Prepare for reading.
                    t.flip();
                    // Note: [t] _is_ the checksum.
                    if (!cache
                            .write(offset + p, t, chk, false/* writeChecksum */))
                        throw new AssertionError();
                } finally {
                    release();
                }
            }
            /*
             * If the current cache buffer is dirty then we need to move it to
             * the dirty list since the caller MUST be able to read the record
             * back from the file by the time this method returns.
             */
            final WriteCache cache = acquireForWriter();
            try {
                if (!cache.isEmpty()) {
                    moveBufferToDirtyList();
                }
            } finally {
                release();
            }
            /*
             * In order to guarantee that the caller can read the record back
             * from the file we now flush the dirty list to the backing store.
             * When this method returns, the record will be on the disk and can
             * be read back safely from the disk.
             */
            flush(false/*force*/);
            // done.
            return true;
        } finally {
            writeLock.unlock();
        }
        
//        try {
//
//            /*
//             * Wrap the caller's record as a ByteBuffer.
//             * 
//             * Note: This code path must add the checksum to the record if
//             * checksums are enabled.
//             */
//            final ByteBuffer t;
//            if(useChecksum) {
//                // Allocate a larger buffer
//                t = ByteBuffer.allocate(nwrite);
//                // Copy the caller's data.
//                t.put(data);
//                // Add in the record checksum.
//                t.putInt(chk);
//                // Prepare for reading.
//                t.flip();
//            } else {
//                // just use the caller's data.
//                t = data;
//            }
//            
//            final WriteCache tmp = newWriteCache(t, useChecksum, opener);
//
//            /*
//             * Write the record on the channel using write cache factory.
//             * 
//             * Note: We need to pass in the offset of the sole record in
//             * order to have it written at the correct offset in the backing
//             * file (getFirstOffset() on [tmp] will be -1L since we never
//             * added anything to [tmp] but instead initialized it with the
//             * data already in the buffer.
//             */
//
//            // A singleton map for that record.
//            final Map<Long, RecordMetadata> recordMap = Collections
//                    .singletonMap(offset, new RecordMetadata(offset,
//                            0/* bufferOffset */, nwrite));
//
//            if (!tmp.writeOnChannel(t, offset/* firstOffset */, recordMap,
//                    Long.MAX_VALUE/* nanos */))
//                throw new RuntimeException();
//
//            return true;
//
//        } catch (Throwable e) {
//
//            throw new RuntimeException(e);
//
//        }

    }

    /**
     * Move the {@link #current} buffer to the dirty list and await a clean
     * buffer. The clean buffer is set as the {@link #current} buffer and
     * returned to the caller.
     * <p>
     * Note: If there is buffer available on the {@link #cleanList} then this
     * method can return immediately. Otherwise, this method will block until
     * a clean buffer becomes available.
     * 
     * @return A clean buffer.
     * 
     * @throws InterruptedException
     * @throws IllegalMonitorStateException
     *             unless the current thread is holding the {@link WriteLock}
     *             for {@link #lock}.
     */
    private WriteCache moveBufferToDirtyList() throws InterruptedException {
        
        if(!lock.isWriteLockedByCurrentThread())
            throw new IllegalMonitorStateException();
        
        final WriteCache cache = current.get();
        assert cache != null;
        /*
         * Note: The lock here is required to give flush() atomic
         * semantics with regard to the set of dirty write buffers
         * when flush() gained the writeLock [in fact, we only need
         * the dirtyListLock for the dirtyListEmpty Condition].
         */
        dirtyListLock.lockInterruptibly();
        try {
            dirtyList.add(cache);
            dirtyListNotEmpty.signalAll();
        } finally {
            dirtyListLock.unlock();
        }

        /*
         * Take the buffer from the cleanList and set it has the
         * [current] buffer.
         * 
         * Note: We use the [cleanListNotEmpty] Condition so we can
         * notice a [halt].
         */
        cleanListLock.lockInterruptibly();

        try {

            while (cleanList.isEmpty() && !halt) {
                cleanListNotEmpty.await();
            }

            if (halt)
                throw new RuntimeException(firstCause.get());

            // Take a buffer from the cleanList (guaranteed avail).
            final WriteCache newBuffer = cleanList.take();

            // Clear state on new buffer and remove from cacheService map
            newBuffer.resetWith(recordMap);

            // Set it as the new buffer.
            current.set(newBuffer);

            return newBuffer;
            
        } finally {

            cleanListLock.unlock();

        }

    }

    /**
     * This is a non-blocking query of all write cache buffers (current, clean
     * and dirty).
     * <p>
     * This implementation DOES NOT throw an {@link IllegalStateException} if
     * the service is already closed NOR if there is an asynchronous close of
     * the service. Instead it just returns <code>null</code> to indicate a
     * cache miss.
     */
    public ByteBuffer read(final long offset) throws InterruptedException,
            ChecksumError {

        if (!open) {
            /*
             * Not open. Return [null] rather than throwing an exception per the
             * contract for this implementation.
             */
            return null;
            
        }

        final Long off = Long.valueOf(offset);

        final WriteCache cache = recordMap.get(off);

        if (cache == null) {

            // No match.
            return null;

        }

        /*
         * Ask the cache buffer if it has the record still. It will not if the
         * cache buffer has been concurrently reset.
         */

        return cache.read(off.longValue());

    }

}
