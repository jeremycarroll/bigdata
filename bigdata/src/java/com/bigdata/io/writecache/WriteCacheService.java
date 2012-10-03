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

package com.bigdata.io.writecache;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumPipeline;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.WriteCache.WriteCacheCounters;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.InnerCause;
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
 *       the {@link WriteCache} (a write at the same offset in the backing file,
 *       but at a different position in the {@link WriteCache} buffer). It is
 *       possible for this to occur with the {@link RWStore} if a record is
 *       written, deleted, and the immediately reallocated. Whether or not this
 *       is a likely event depends on how aggressively the {@link RWStore}
 *       reallocates addresses which were allocated and then deleted within the
 *       same native transaction.
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

    protected static final Logger log = Logger.getLogger(WriteCacheService.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.ha");

    /**
     * <code>true</code> until the service is {@link #close() closed}.
     */
//  private volatile boolean open = true;
    private final AtomicBoolean open = new AtomicBoolean(true);

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
     * The {@link Future} of the task running on the {@link #remoteWriteService}
     * .
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
     * The read lock allows concurrent {@link #acquireForWriter()}s while the
     * write lock prevents {@link #acquireForWriter()} when we must either reset
     * the {@link #current} cache buffer or change the {@link #current}
     * reference. E.g., {@link #flush(boolean, long, TimeUnit)}.
     * <p>
     * Note: {@link #read(long)} is non-blocking. It does NOT use this lock!!!
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
     * Flag set if {@link WriteTask} encounters an error. The cause is set
     * on {@link #firstCause} as well.
     * <p>
     * Note: Error handling MUST cause the write cache service buffers to be
     * {@link #reset()} and make sure the HA write pipeline is correctly
     * configured. This is handled by a high-level abort() on the journal. It is
     * NOT Ok to simply re-try writes of partly filled buffers since they may
     * already have been partly written to the disk. A high-level abort() is
     * necessary to ensure that we discard any bad writes. The abort() will need
     * to propagate to all members of the {@link Quorum} so they are all reset
     * to the last commit point and have reconfigured write cache services and
     * write pipelines.
     */
    private volatile boolean halt = false;

    /**
     * The first cause of an error within the asynchronous
     * {@link WriteTask}.
     */
    private final AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>();

    /**
     * The capacity of the cache buffers. This is assumed to be the same for
     * each buffer.
     */
    final private int capacity;

//  /**
//   * Object knows how to (re-)open the backing channel.
//   */
//  final private IReopenChannel<? extends Channel> opener;

    /**
     * A map from the offset of the record on the backing file to the cache
     * buffer on which that record was written.
     */
    final private ConcurrentMap<Long/* offset */, WriteCache> recordMap;

    /**
     * An immutable array of the {@link WriteCache} buffer objects owned by the
     * {@link WriteCacheService} (in contract to those owner by the caller but
     * placed onto the {@link #dirtyList} by
     * {@link #writeChk(long, ByteBuffer, int)}).
     */
    final private WriteCache[] buffers;
    
    /**
     * Debug arrays to chase down write/removal errors.
     * 
     * Toggle comment appropriately to activate/deactivate
     */
//  final long[] addrsUsed = new long[4024 * 1024];
//  int addrsUsedCurs = 0;
//  final char[] addrActions = new char[addrsUsed.length];
//  final int[] addrLens = new int[addrsUsed.length];
    private final long[] addrsUsed = null;
    private int addrsUsedCurs = 0;
    private final char[] addrActions = null;
    private final int[] addrLens = null;
    
    /**
     * The current file extent.
     */
    final private AtomicLong fileExtent = new AtomicLong(-1L);

//  /**
//   * The environment in which this object participates
//   */
//  protected final Environment environment;

    /**
     * The object which manages {@link Quorum} state changes on the behalf of
     * this service.
     */
    final private Quorum<HAPipelineGlue, QuorumMember<HAPipelineGlue>> quorum;

//    /**
//     * The {@link UUID} of the highly available service.
//     */
//    final private UUID serviceId;
    
    /**
     * The {@link Quorum} token under which this {@link WriteCacheService}
     * instance is valid. This is fixed for the life cycle of the
     * {@link WriteCacheService}. This ensures that all writes are buffered
     * under a consistent quorum meet.
     */
    final private long quorumToken;
    
    /**
     * The object which manages {@link Quorum} state changes on the behalf of
     * this service.
     */
    protected Quorum<HAPipelineGlue, QuorumMember<HAPipelineGlue>> getQuorum() {

        return quorum;
        
    }

    /**
     * The #of times the leader in a highly available quorum will attempt to
     * retransmit the current write cache block if there is an error when
     * sending that write cache block to the downstream node.
     */
    protected final int RETRY_COUNT = 3;

    /**
     * Allocates N buffers from the {@link DirectBufferPool}.
     * 
     * @param nbuffers
     *            The #of buffers to allocate.
     * @param useChecksum
     *            <code>true</code> iff record level checksums are enabled.
     * @param fileExtent
     *            The current extent of the backing file.
     * @param opener
     *            The object which knows how to (re-)open the channel to which
     *            cached writes are flushed.
     * @param quorumManager
     *            The object which manages {@link Quorum} state changes on the
     *            behalf of this service.
     * 
     * @throws InterruptedException
     */
    public WriteCacheService(final int nbuffers, final boolean useChecksum,
            final long fileExtent,
            final IReopenChannel<? extends Channel> opener,
            final Quorum quorum) throws InterruptedException {

        if (nbuffers <= 0)
            throw new IllegalArgumentException();

        if (fileExtent < 0L)
            throw new IllegalArgumentException();

        if (opener == null)
            throw new IllegalArgumentException();

//        if (quorum == null)
//            throw new IllegalArgumentException();

        this.useChecksum = useChecksum;

//      this.opener = opener;

        // the token under which the write cache service was established.
        if ((this.quorum = quorum) != null) {
            this.quorumToken = quorum.token();
        } else {
            this.quorumToken = Quorum.NO_QUORUM;
        }
        
        dirtyList = new LinkedBlockingQueue<WriteCache>();

        cleanList = new LinkedBlockingQueue<WriteCache>();

        buffers = new WriteCache[nbuffers];

        // save the current file extent.
        this.fileExtent.set(fileExtent);

        // Add [current] WriteCache.
        current.set(buffers[0] = newWriteCache(null/* buf */,
                useChecksum, false/* bufferHasData */, opener, fileExtent));

        // add remaining buffers.
        for (int i = 1; i < nbuffers; i++) {

            final WriteCache tmp = newWriteCache(null/* buf */, useChecksum,
                    false/* bufferHasData */, opener, fileExtent);

            buffers[i] = tmp;

            cleanList.add(tmp);

        }


        // Set the same counters object on each of the write cache instances.
        final WriteCacheServiceCounters counters = new WriteCacheServiceCounters(
                nbuffers);

        for (int i = 0; i < buffers.length; i++) {
        
            buffers[i].setCounters(counters);
            
        }
        
        this.counters = new AtomicReference<WriteCacheServiceCounters>(counters);

        // assume capacity is the same for each buffer instance.
        capacity = current.get().capacity();

        // set initial capacity based on an assumption of 1k buffers.
        recordMap = new ConcurrentHashMap<Long, WriteCache>(nbuffers
                * (capacity / 1024));

        // start service to write on the backing channel.
        localWriteService = Executors
                .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                        .getName()));

        // run the write task
        localWriteFuture = localWriteService.submit(newWriteTask());

    }
    
    /**
     * Called from {@link IBufferStrategy#commit()} and {@link #reset()} to
     * reset WriteCache sequence for HA synchronization.
     */
    public void resetSequence() {
        cacheSequence = 0;
    }
    private volatile long cacheSequence = 0;

    protected Callable<Void> newWriteTask() {

        return new WriteTask();

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

        /**
         * Note: If there is an error in this thread then it needs to be
         * propagated to the threads write()ing on the cache or awaiting flush()
         * and from there back to the caller and an abort(). We do not need to
         * bother the readers since the read() methods all allow for concurrent
         * close() and will return null rather than bad data. The reprovisioning
         * of the write cache service (e.g., by reset()) must hold the writeLock
         * so as to occur when there are no outstanding reads executing against
         * the write cache service.
         * 
         * @todo If resynchronization rolls back the lastCommitTime for a store,
         *       then we need to interrupt or otherwise invalidate any readers
         *       with access to historical data which is no longer part of the
         *       quorum.
         */
        public Void call() throws Exception {
            while (true) {
                assert !halt;
                try {
                    /*
                     * Get a dirty cache buffer.
                     */
                    final WriteCache cache;
                    dirtyListLock.lockInterruptibly();
                    try {
                        while (dirtyList.isEmpty() && !halt) {
                            dirtyListNotEmpty.await();
                        }
                        if (halt)
                            throw new RuntimeException(firstCause.get());

                        // update counters.
                        final WriteCacheServiceCounters c = counters.get();
                        c.ndirty = dirtyList.size();
                        if (c.maxdirty < c.ndirty)
                            c.maxdirty = c.ndirty;

                        // Guaranteed available.
                        cache = dirtyList.peek();

                    } finally {
                        dirtyListLock.unlock();
                    }
                    
                    /*
                     * Ensure nothing will modify this buffer before written to disk or HA pipeline
                     */
                    cache.closeForWrites();

                    if (!cache.isEmpty()) {
                        
                        /*
                         * Only process non-empty cache buffers.
                         */
                    	
                        // increment writeCache sequence
                        cache.setSequence(cacheSequence++);

                        if (quorum != null && quorum.isHighlyAvailable()) {

//                            final QuorumMember<?> client = (QuorumMember<?>) quorum
//                                    .getClient();
//
//                            // Verify quorum still valid (protects against NPE on [client]).
//                            quorum.assertQuorum(quorumToken);
//                            
//                            final UUID serviceId = client.getServiceId();
//
//                            // Get prior and next serviceIds in the pipeline.
//                            final UUID[] priorAndNext = quorum
//                                    .getPipelinePriorAndNext(serviceId);
//
//                            // The next serviceId in the pipeline (if any).
//                            final UUID nextId = (priorAndNext == null ? null
//                                    : priorAndNext[1]);

                            // Verify quorum still valid and we are the leader.
                            quorum.assertLeader(quorumToken);

                            /*
                             * Replicate from the leader to the first follower.
                             * Each non-final follower will receiveAndReplicate
                             * the write cache buffer. The last follower will
                             * receive the buffer.
                             */
                            // duplicate the write cache's buffer.
                            final ByteBuffer b = cache.peek().duplicate();
                            // flip(limit=pos;pos=0)
                            b.flip();
                            assert b.remaining() > 0 : "Empty cache: " + cache; 
           
                            // send to 1st follower.
                            @SuppressWarnings("unchecked")
                            final QuorumPipeline<HAPipelineGlue> quorumMember = (QuorumPipeline<HAPipelineGlue>) quorum
                                    .getMember();

                            assert quorumMember != null : "Not quorum member?";
                            
                            final HAWriteMessage msg = cache.newHAWriteMessage(
                                    quorumToken,//
                                    quorumMember.getLastCommitCounter(),//
                                    quorumMember.getLastCommitTime()//
                                    );

                            /*
                             * The quorum leader must log the write cache block.
                             * 
                             * Note: It will be logged exactly once (retry send
                             * will not re-log the block).
                             */
                            quorumMember.logWriteCacheBlock(msg, b.duplicate());
                            
                            remoteWriteFuture = quorumMember.replicate(msg, b);

                            counters.get().nsend++;
                            
                        }

                        /*
                         * Do the local IOs (concurrent w/ remote replication).
                         * 
                         * Note: This will not throw out an InterruptedException
                         * unless this thread is actually interrupted. The local
                         * storage managers all trap asynchronous close
                         * exceptions arising from the interrupt of a concurrent
                         * IO operation and retry until they succeed.
                         */
                        cache.flush(false/* force */);

                        // Wait for the downstream IOs to finish.
                        if (remoteWriteFuture != null) {
                            try {
                                remoteWriteFuture.get();
                            } catch (ExecutionException ex) {
                                 retrySend(quorum, cache, ex);
                            }
                        }

                    }

                    // Now written, remove from dirtylist.
                    dirtyList.take();
                    counters.get().ndirty--;

                    dirtyListLock.lockInterruptibly();
                    try {
                        if (dirtyList.isEmpty()) {
                        /*
                         * Signal Condition when we release the
                         * dirtyListLock.
                         */
                        dirtyListEmpty.signalAll();
                        }
                    } finally {
                        dirtyListLock.unlock();
                    }

                    /*
                     * Add to the cleanList (all buffers are our buffers).
                     */
                    cleanListLock.lockInterruptibly();
                    try {
                        cleanList.add(cache);
                        cleanListNotEmpty.signalAll();
                        counters.get().nclean = cleanList.size();
                    } finally {
                        cleanListLock.unlock();
                    }
                    if(log.isInfoEnabled()) {
                        final WriteCacheServiceCounters tmp = counters.get();
                        final long nhit = tmp.nhit.get();
                        final long ntests = nhit + tmp.nmiss.get();
                        final double hitRate=(ntests == 0L ? 0d : (double) nhit / ntests);
                        log.info("WriteCacheService: bufferCapacity="
                                + buffers[0].capacity() + ",nbuffers="
                                + tmp.nbuffers + ",nclean=" + tmp.nclean
                                + ",ndirty=" + tmp.ndirty + ",maxDirty="
                                + tmp.maxdirty + ",nflush=" + tmp.nflush
                                + ",nwrite=" + tmp.nwrite + ",hitRate="
                                + hitRate);
                    }

                } catch (InterruptedException t) {
                    /*
                     * This task can only be interrupted by a thread with its
                     * Future (or by shutting down the thread pool on which it
                     * is running), so this interrupt is a clear signal that the
                     * write cache service is closing down.
                     */
                    return null;
                } catch (Throwable t) {
                    if (InnerCause.isInnerCause(t,
                            AsynchronousCloseException.class)) {
                        /*
                         * The service was shutdown. We do not want to log an
                         * error here since this is normal shutdown. close()
                         * will handle all of the Condition notifies.
                         */
                        return null;
                    }
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
                    if (firstCause.compareAndSet(null/* expect */, t/* update */)) {
                        halt = true;
                    }
                    /*
                     * Signal anyone blocked on the dirtyList or cleanList
                     * Conditions. They need to notice the change in [halt] and
                     * wrap and rethrow [firstCause].
                     */
                    dirtyListLock.lock();
                    try {
                        dirtyListEmpty.signalAll();
                        dirtyListNotEmpty.signalAll();
                    } finally {
                        dirtyListLock.unlock();
                    }
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
                }

            } // while(true)

        } // call()

        /**
         * Robust retransmit of the current cache block. This method is designed
         * to handle several kinds of recoverable errors, including:
         * <ul>
         * <li>downstream service leaves the pipeline</li>
         * <li>intermittent failure sending the RMI message</li>
         * <li>intermittent failure sending the payload</li>
         * </ul>
         * The basic pattern is that it will retry the operation a few times to
         * see if there is a repeatable error. Each time it attempts the
         * operation it will discover the current downstream serviceId and
         * verify that the quorum is still valid. Each error (including the
         * first) is logged. If the operation fails, the original error is
         * rethrown. If the operation succeeds, then the cache block was
         * successfully transmitted to the current downstream service and this
         * method returns without error.
         * 
         * @param cache
         *            The cache block.
         * @param t
         *            The thrown exception.
         */
//        * @param nextIdBefore
//        *            The {@link UUID} of the downstream service the first time
//        *            we attempted to send the cache block downstream.
        private void retrySend(
                final Quorum<HAPipelineGlue, QuorumMember<HAPipelineGlue>> quorum,
                final WriteCache cache, final Throwable t) throws Throwable {

            // Log the caller's error.
            log.error(t,t);
            
            // we already tried once.
            int tryCount = 1;
            
            // now try some more times.
            for (; tryCount < RETRY_COUNT; tryCount++) {

//                // Get prior and next serviceIds in the pipeline.
//                final UUID[] priorAndNext = quorum
//                        .getPipelinePriorAndNext(serviceId);
//
//                // The next serviceId in the pipeline (if any).
//                final UUID nextId = (priorAndNext == null ? null
//                        : priorAndNext[1]);
//
//                if (nextIdBefore.equals(nextId)) {
//
//                    /*
//                     * The downstream service has not changed.
//                     */
//                    log.error("The downstream service is unchanged : retry="
//                            + tryCount);
//
//                }

                // Verify quorum still valid and we are the leader.
                quorum.assertLeader(quorumToken);

                /*
                 * Replicate from the leader to the first follower. Each
                 * non-final follower will receiveAndReplicate the write cache
                 * buffer. The last follower will receive the buffer.
                 */

                // duplicate the write cache's buffer.
                final ByteBuffer b = cache.peek().duplicate();
                // final ByteBuffer b = ByteBuffer.allocate(0);
                
                // flip(limit=pos;pos=0)
                b.flip();
                
                assert b.remaining() > 0 : "Empty cache: " + cache;

                @SuppressWarnings("unchecked")
                final QuorumPipeline<HAPipelineGlue> quorumMember = (QuorumPipeline<HAPipelineGlue>) quorum
                        .getMember();

                final HAWriteMessage msg = cache.newHAWriteMessage(//
                        quorumToken,//
                        quorumMember.getLastCommitCounter(),//
                        quorumMember.getLastCommitTime()//
                        );
                
                // send to 1st follower.
                remoteWriteFuture = quorumMember.replicate(msg, b);
                
                counters.get().nsend++;

                try {
                    // wait on the future.
                    remoteWriteFuture.get();
                } catch (ExecutionException ex) {
                    // log and retry.
                    log.error("retry=" + tryCount + " : " + ex, ex);
                    continue;
                }
                
            }

            // Rethrow the original exception.
            throw new RuntimeException("Giving up. Could not send after "
                    + tryCount + " attempts : " + t, t);

        }
        
    } // class WriteTask

    /**
     * Factory for {@link WriteCache} implementations.
     * 
     * @param buf
     *            The backing buffer (optional).
     * @param useChecksum
     *            <code>true</code> iff record level checksums are enabled.
     * @param bufferHasData
     *            <code>true</code> iff the buffer has data to be written onto
     *            the local persistence store (from a replicated write).
     * @param opener
     *            The object which knows how to re-open the backing channel
     *            (required).
     * @param fileExtent
     *            The then current extent of the backing file.
     * 
     * @return A {@link WriteCache} wrapping that buffer and able to write on
     *         that channel.
     * 
     * @throws InterruptedException
     */
    abstract public WriteCache newWriteCache(IBufferAccess buf,
            boolean useChecksum, boolean bufferHasData,
            IReopenChannel<? extends Channel> opener, final long fileExtent)
            throws InterruptedException;

	/**
     * {@inheritDoc}
     * <p>
     * This implementation calls {@link IWriteCache#reset()} on all
     * {@link #buffers} and moves them onto the {@link #cleanList}. Note that
     * this approach deliberately does not cause any buffers belonging to the
     * caller of {@link #writeChk(long, ByteBuffer, int)} to become part of the
     * {@link #cleanList}.
     * <p>
     * Note: <strong>You MUST set the {@link #setExtent(long) file extent}
     * </strong> after {@link #reset() resetting} the {@link WriteCacheService}.
     * This is necessary in order to ensure that the correct file extent is
     * communicated along the write replication pipeline when high availability
     * is enabled.
     */
    public void reset() throws InterruptedException {
        final WriteLock writeLock = lock.writeLock();
        writeLock.lockInterruptibly();
        try {
            if (!open.get()) {
                // Reset can not recover from close().
                throw new IllegalStateException(firstCause.get());
            }

            // cancel the current WriteTask.
            localWriteFuture.cancel(true/* mayInterruptIfRunning */);
            final Future<?> rwf = remoteWriteFuture;
            if (rwf != null) {
                // Note: Cancel of remote Future is RMI!
                try {
                    rwf.cancel(true/* mayInterruptIfRunning */);
                } catch (Throwable t) {
                    log.warn(t, t);
                }
            }

            // drain the dirty list.
            dirtyListLock.lockInterruptibly();
            try {
                dirtyList.drainTo(new LinkedList<WriteCache>());
                dirtyListEmpty.signalAll();
                dirtyListNotEmpty.signalAll(); // NB: you must verify Condition
                                                // once signaled!
            } finally {
                dirtyListLock.unlock();
            }

            // drain the clean list.
            cleanListLock.lockInterruptibly();
            try {
                cleanList.drainTo(new LinkedList<WriteCache>());
                cleanListNotEmpty.signalAll();
            } finally {
                cleanListLock.unlock();
            }

            /*
             * Now that we have sent all the signal()s we know how to send, go
             * ahead and wait for the WriteTask to notice and terminate.
             */
            try {
                // wait for it
                localWriteFuture.get();
            } catch (Throwable t) {
                // ignored.
            }

            // reset each buffer.
            for (WriteCache t : buffers) {
                t.reset();
            }

            // clear the service record map.
            recordMap.clear();

            // set the current buffer.
            current.set(buffers[0]);

            // re-populate the clean list with remaining buffers
            for (int i = 1; i < buffers.length; i++) {
                cleanList.put(buffers[i]);
            }

            // reset the counters.
            {
                final WriteCacheServiceCounters c = counters.get();
                c.ndirty = 0;
                c.nclean = buffers.length-1;
                c.nreset++;
            }
            
            // reset cacheSequence for HA
            cacheSequence = 0;

            /*
             * Restart the WriteTask
             * 
             * Note: don't do Future#get() for the remote Future. The task was
             * cancelled above and we don't want to wait on RMI (for the remote
             * Future). The remote service will have to handle any problems on
             * its end when resynchronizing if it was disconnected and did not
             * see our cancel() message.
             */
            // if (rwf != null) {
            // try {
            // rwf.get();
            // } catch (Throwable t) {
            // // ignored.
            // }
            // }
            this.localWriteFuture = localWriteService.submit(newWriteTask());
            this.remoteWriteFuture = null;

            // clear the file extent to an illegal value.
            fileExtent.set(-1L);

            counters.get().nreset++;

        } finally {
            writeLock.unlock();
        }
    }

    public void close() { //throws InterruptedException {

        if (!open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed, so this is a NOP.
            return;
        }

        /*
         * Set [firstCause] and [halt] to ensure that other threads report
         * errors.
         * 
         * Note: If the firstCause has not yet been set, then we set it now to a
         * stack trace which will indicate that the WriteCacheService was
         * asynchronously closed (that is, it was closed by another thread).
         */
        if (firstCause.compareAndSet(null/* expect */,
                new AsynchronousCloseException()/* update */)) {
            halt = true;
        }
        
        // Interrupt the write task.
        localWriteFuture.cancel(true/* mayInterruptIfRunning */);
        final Future<?> rwf = remoteWriteFuture;
        if (rwf != null) {
            // Note: Cancel of remote Future is RMI!
            try {
                rwf.cancel(true/* mayInterruptIfRunning */);
            } catch (Throwable t) {
                log.warn(t, t);
            }
        }

        // Immediate shutdown of the write service.
        localWriteService.shutdownNow();

//          // Immediate shutdown of the remote write service (if running).
//          if (remoteWriteService != null) {
//              remoteWriteService.shutdownNow();
//          }

        /*
         * Ensure that the WriteCache buffers are close()d in a timely
         * manner.
         */

        // reset buffers on the dirtyList.
        dirtyListLock.lock/*Interruptibly*/();
        try {
            dirtyList.drainTo(new LinkedList<WriteCache>());
            dirtyListEmpty.signalAll();
            dirtyListNotEmpty.signalAll();
        } finally {
            dirtyListLock.unlock();
        }

        // close() buffers on the cleanList.
        cleanListLock.lock/*Interruptibly*/();
        try {
            cleanList.drainTo(new LinkedList<WriteCache>());
        } finally {
            cleanListLock.unlock();
        }

        /*
         * Note: The lock protects the [current] reference.
         */
        final WriteLock writeLock = lock.writeLock();
        writeLock.lock/*Interruptibly*/();
        try {

            // close all buffers.
            boolean interrupted = false;
            for (WriteCache t : buffers) {
                try {
                    t.close();
                } catch (InterruptedException ex) {
                    interrupted = true;
                    continue;
                }
            }

            // clear reference to the current buffer.
            current.getAndSet(null);

            // clear the service record map.
            recordMap.clear();

            // clear the file extent to an illegal value.
            fileExtent.set(-1L);

            if(interrupted)
                Thread.currentThread().interrupt();

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
     * is {@link #open}, that the {@link WriteTask} has not been
     * {@link #halt halted}, and that the {@link WriteTask} is still
     * executing (in case any uncaught errors are thrown out of
     * {@link WriteTask#call()}.
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

        if (!open.get())
            throw new IllegalStateException(firstCause.get());

        if (halt)
            throw new RuntimeException(firstCause.get());

        if (localWriteFuture.isDone()) {

            /*
             * If the write task terminates abnormally then throw the exception
             * out here.
             */

            try {
                // @todo don't do get() all the time...?
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
    private WriteCache acquireForWriter() throws InterruptedException, IllegalStateException {

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

        } catch (Throwable t) {

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

        if (haLog.isInfoEnabled()) {
            /*
             * Note: This is an important event for HA. The write cache is
             * flushed to ensure that the entire write set is replicated on the
             * followers. Once that has been done, HA will do a 2-phase commit
             * to verify that there is a quorum that agrees to write the root
             * block. Writing the root block is the only thing that the nodes in
             * the quorum need to do once the write cache has been flushed.
             */
            haLog.info("Flushing the write cache.");
        }

        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;

        final WriteLock writeLock = lock.writeLock();
        if (!writeLock.tryLock(remaining, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            final WriteCache tmp = current.getAndSet(null);
            if (tmp.remaining() == 0) {
                /*
                 * Handle an empty buffer by waiting until the dirtyList is
                 * empty.
                 */
                // remaining := (total - elapsed).
                remaining = nanos - (System.nanoTime() - begin);
                if (!dirtyListLock.tryLock(remaining, TimeUnit.NANOSECONDS))
                    throw new TimeoutException();
                try {
                    while (!dirtyList.isEmpty() && !halt) {
                        // remaining := (total - elapsed).
                        remaining = nanos - (System.nanoTime() - begin);
                        if (!dirtyListEmpty.await(remaining,
                                TimeUnit.NANOSECONDS)) {
                            throw new TimeoutException();
                        }
                    }
                    if (halt)
                        throw new RuntimeException(firstCause.get());
                } finally {
                    dirtyListLock.unlock();
                }
                return true;
            }
            /*
             * Otherwise, the current buffer is non-empty.
             */
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
                counters.get().ndirty++;
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
                    if (!cleanListNotEmpty.await(remaining, TimeUnit.NANOSECONDS)) {
                        throw new TimeoutException();
                    }
                    if (halt)
                        throw new RuntimeException(firstCause.get());
                }
                // Guaranteed available hence non-blocking.
                final WriteCache nxt = cleanList.take();
                counters.get().nclean--;
                nxt.resetWith(recordMap, fileExtent.get());
                current.set(nxt);
                return true;
            } finally {
                cleanListLock.unlock();
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    /**
     * Set the extent of the file on the current {@link WriteCache}. The then
     * current value of the extent will be communicated together with the rest
     * of the {@link WriteCache} state if it is written onto another service
     * using the write replication pipeline (HA only). The receiver will use the
     * value read from the {@link WriteCache} message to adjust the extent of
     * its backing file.
     * <p>
     * Note: Changes in the file extent for persistence store implementations
     * MUST (a) be mutually exclusive with reads and writes on the backing file
     * (due to a JVM bug); and (b) force the file data and the file metadata to
     * the disk. Thus any change in the {@link #fileExtent} MUST be followed by
     * a {@link #flush(boolean, long, TimeUnit)}.
     * <p>
     * Note: You MUST set the file extent each time you invoke {@link #reset()}
     * so the {@link WriteCacheService} is always aware of the correct file
     * extent.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    public void setExtent(final long fileExtent) throws IllegalStateException,
            InterruptedException {

        if (fileExtent < 0L)
            throw new IllegalArgumentException();

        final WriteCache cache = acquireForWriter();

        try {
        	if (log.isDebugEnabled())
        		log.debug("Set fileExtent: " + fileExtent);

            // make a note of the current file extent.
            this.fileExtent.set(fileExtent);

            // set the current file extent on the WriteCache.
            cache.setFileExtent(fileExtent);

        } finally {

            release();

        }

    }

    public boolean write(final long offset, final ByteBuffer data, final int chk)
            throws InterruptedException, IllegalStateException {
     
        return write(offset, data, chk, useChecksum, 0/* latchedAddr */);
        
    }
    
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
     * @param latchedAddr The latched address (RWStore only).
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
    public boolean write(final long offset, final ByteBuffer data, final int chk, final boolean useChecksum,final int latchedAddr)
            throws InterruptedException, IllegalStateException {

        if (log.isTraceEnabled()) {
            log.trace("offset: " + offset + ", length: " + data.limit()
                    + ", chk=" + chk + ", useChecksum=" + useChecksum);
        }
        
        if (!open.get())
            throw new IllegalStateException(firstCause.get());

        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        // maintain nwrites
        counters.get().nwrites++;

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
            return writeLargeRecord(offset, data, chk, useChecksum);

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
                debugAddrs(offset, data.remaining(), 'A');

                // write on the cache.
                if (cache.write(offset, data, chk, useChecksum, latchedAddr)) {
                    WriteCache old = recordMap.put(offset, cache);
                    // There should be no duplicate address in the record
                    //  map since these entries should be removed, although
                    //  write data may still exist in an old WriteCache.
                    // A duplicate may also be indicative of an allocation
                    //  error, which we need to be pretty strict about!
                    if (old == cache) {
                        throw new AssertionError("Record already in cache: offset=" + offset + " " + addrDebugInfo(offset));
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
                    if (cache.write(offset, data, chk, useChecksum, latchedAddr)) {

                        /*
                         * It fits: someone already changed to a new cache,
                         * which is fine.
                         */
                        if (recordMap.put(offset, cache) != null) {
                            // The record should not already be in the cache.
                            throw new AssertionError("Record already in cache: offset=" + offset + " "  + addrDebugInfo(offset));
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

                    	if (log.isInfoEnabled() && cleanList.isEmpty())
                    		log.info("Waiting for clean buffer");
                    	
                        while (cleanList.isEmpty() && !halt) {
                            cleanListNotEmpty.await();
                        }

                        if (halt)
                            throw new RuntimeException(firstCause.get());

                        // Take a buffer from the cleanList (guaranteed avail).
                        final WriteCache newBuffer = cleanList.take();
                        counters.get().nclean--;
                        // Clear the state on the new buffer and remove from
                        // cacheService map
                        newBuffer.resetWith(recordMap, fileExtent.get());

                        // Set it as the new buffer.
                        current.set(cache = newBuffer);

                        // Try to write on the new buffer.
                        if (cache.write(offset, data, chk, useChecksum, latchedAddr)) {

                            // This must be the only occurrence of this record.
                            if (recordMap.put(offset, cache) != null) {
                                throw new AssertionError("Record already in cache: offset=" + offset + " " + addrDebugInfo(offset));
                            }

                            return true;

                        }

                    } finally {

                        cleanListLock.unlock();

                    }

                    /*
                     * Should never happen.
                     */
                    throw new AssertionError("Unable to write into current WriteCache " + offset + " " + addrDebugInfo(offset));

                } finally {

                    release();

                }

            } finally {

                writeLock.unlock();

            }

        }

    }

    public void debugAddrs(long offset, int length, char c) {
        if (addrsUsed != null) {
            addrsUsed[addrsUsedCurs] = offset;
            addrActions[addrsUsedCurs] = c;
            addrLens[addrsUsedCurs] = length;
            
            addrsUsedCurs++;
            if (addrsUsedCurs >= addrsUsed.length) {
                addrsUsedCurs = 0;
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
    protected boolean writeLargeRecord(final long offset, final ByteBuffer data, final int chk, final boolean useChecksum)
            throws InterruptedException, IllegalStateException {

        if (log.isTraceEnabled()) {
            log.trace("offset: " + offset + ", length: " + data.limit() + ", chk=" + chk + ", useChecksum="
                    + useChecksum);
        }

        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_NULL);

        // #of bytes in the record.
        final int remaining = data.remaining();

        if (remaining == 0)
            throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        // Small records should not take this code path.
        if (remaining < capacity)
            throw new AssertionError();

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
                        if (!cache.write(offset + p, tmp, chk, false/* writeChecksum */,0/*latchedAddr*/))
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
                    if (!cache.write(offset + p, t, chk, false/* writeChecksum */,0/*latchedAddr*/))
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
            if (log.isTraceEnabled())
                log.trace("FLUSHING LARGE RECORD");
            
            flush(false/* force */);
            // done.
            return true;
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * Move the {@link #current} buffer to the dirty list and await a clean
     * buffer. The clean buffer is set as the {@link #current} buffer and
     * returned to the caller.
     * <p>
     * Note: If there is buffer available on the {@link #cleanList} then this
     * method can return immediately. Otherwise, this method will block until a
     * clean buffer becomes available.
     * 
     * @return A clean buffer.
     * 
     * @throws InterruptedException
     * @throws IllegalMonitorStateException
     *             unless the current thread is holding the {@link WriteLock}
     *             for {@link #lock}.
     */
    private WriteCache moveBufferToDirtyList() throws InterruptedException {

        if (!lock.isWriteLockedByCurrentThread())
            throw new IllegalMonitorStateException();

        final WriteCache cache = current.getAndSet(null);
        assert cache != null;
        
        /*
         * Note: The lock here is required to give flush() atomic semantics with
         * regard to the set of dirty write buffers when flush() gained the
         * writeLock [in fact, we only need the dirtyListLock for the
         * dirtyListEmpty Condition].
         */
        dirtyListLock.lockInterruptibly();
        try {
            dirtyList.add(cache);
            dirtyListNotEmpty.signalAll();
        } finally {
            dirtyListLock.unlock();
        }

        /*
         * Take the buffer from the cleanList and set it has the [current]
         * buffer.
         * 
         * Note: We use the [cleanListNotEmpty] Condition so we can notice a
         * [halt].
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
            counters.get().nclean--;
            
            // Clear state on new buffer and remove from cacheService map
            newBuffer.resetWith(recordMap, fileExtent.get());

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

        if (!open.get()) {
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

            counters.get().nmiss.increment();
            
            return null;

        }

        /*
         * Ask the cache buffer if it has the record still. It will not if the
         * cache buffer has been concurrently reset.
         */
        try {
            return cache.read(off.longValue());
        } catch (IllegalStateException ex) {
            /*
             * The write cache was closed. Per the API for this method, return
             * [null] so that the caller will read through to the backing store.
             */
            assert !open.get();
            return null;
        }

    }

    /**
     * Called to check if a write has already been flushed. This is only made if
     * a write has been made to previously committed data (in the current RW
     * session).
     * <p>
     * If dirty {@link WriteCache}s are flushed in order then it does not
     * matter, however, if we want to be able to combine {@link WriteCache}s
     * then it makes sense that there are no duplicate writes.
     * <p>
     * On reflection this is more likely needed since for the {@link RWStore},
     * depending on session parameters, the same cached area could be
     * overwritten. We could still maintain multiple writes but we need a
     * guarantee of order when retrieving data from the write cache (newest
     * first).
     * <p>
     * So the question is, whether it is better to keep cache consistent or to
     * constrain with read order?
     * 
     * @param offset
     *            the address to check
     */
    public boolean clearWrite(final long offset,final int latchedAddr) {
        try {
            counters.get().nclearRequests++;
            final WriteCache cache = recordMap.remove(offset);
            if (cache == null)
                return false;
            
            // Is there any point in acquiring for writer (with the readLock)?
            // It prevents concurrent access with the write method that takes
            //  the writeLock, but is this a problem?
            //final WriteCache cur = acquireForWriter(); // in case current
            counters.get().nclears++;
            //try {
            debugAddrs(offset, 0, 'F');
                cache.clearAddrMap(offset, latchedAddr);
                
                return true;
            //} finally {
            //  release();
            //}
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * An array of writeCache actions is maintained that can be used
     * to provide a breadcrumb of how that address has been written, saved,
     * freed or removed.
     * <p>
     * Write errors often show up as a checksum error, so the length of
     * data written to the address cab be crucial information in determining the
     * root of any problem.
     * 
     * @param address for which info requested
     * @return summary of writeCache actions
     */
    public String addrDebugInfo(final long paddr) {
        if (addrsUsed == null) {
            return "No WriteCache debug info";
        }
        
        final StringBuffer ret = new StringBuffer();
//      // first see if address was ever written
//      boolean written = false;
        for (int i = 0; i < addrsUsed.length; i++) {
            if (i == addrsUsedCurs) {
                ret.append("|...|");
            }
            if (addrsUsed[i] == paddr) {
                ret.append(addrActions[i]);
                if (addrActions[i]=='A') {
                    ret.append("[" + addrLens[i] + "]");
                }
            }
        }
        /*
         * Note: I've added in the write cache service counters here for
         * information about the maximum #of buffers from the pool which have
         * been in use, #of flushes, etc.
         */
        ret.append(":");
        ret.append(getCounters().toString());
        return ret.toString();
    }

    /**
     * Return <code>true</code> iff the address is in the write
     * cache at the moment which the write cache is checked.
     * <p>
     * Note: Unless the caller is holding an appropriate lock
     * across this operation, the result is NOT guaranteed to
     * be correct at any time other than the moment when the
     * cache was tested. 
     */
	public boolean isPresent(final long addr) {
		// System.out.println("Checking address: " + addr);
		
		return recordMap.get(addr) != null;
	}
	
	/**
     * Performance counters for the {@link WriteCacheService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class WriteCacheServiceCounters extends WriteCacheCounters {

        /** #of configured buffers (immutable). */
        public final int nbuffers;

        /**
         * #of dirty buffers (instantaneous).
         * <p>
         * Note: This is set by the {@link WriteTask} thread and by
         * {@link WriteCacheService#reset()}. It is volatile so it is visible
         * from a thread which looks at the counters and for correct publication
         * from reset().
         */
        public volatile int ndirty;

        /**
         * #of clean buffers (instantaneous).
         * <p>
         * Note: This is set by the {@link WriteTask} thread and by
         * {@link WriteCacheService#reset()}. It is volatile so it is visible
         * from a thread which looks at the counters and for correct publication
         * from reset().
         */
        public volatile int nclean;

        /**
         * The maximum #of dirty buffers observed by the {@link WriteTask} (its
         * maximum observed backlog). This is only set by the {@link WriteTask}
         * thread, but it is volatile so it is visible from a thread which looks
         * at the counters.
         */
        public volatile int maxdirty;

        /**
         * #of times the {@link WriteCacheService} was reset (typically to
         * handle an error condition).
         * <p>
         * Note: This is set by {@link WriteCacheService#reset()}. It is
         * volatile so it is visible from a thread which looks at the counters
         * and for correct publication from reset().
         */
        public volatile long nreset;

        /**
         * The #of {@link WriteCache} blocks sent by the leader to the first
         * downstream follower.
         */
        public volatile long nsend;        
        /**
         * The #of writes made to the writeCacheService.
         */
        public volatile long nwrites;
        /**
         * The #of addresses cleared by the writeCacheService.
         */
        public volatile long nclearRequests;
        /**
         * The #of addresses cleared by the writeCacheService.
         */
        public volatile long nclears;
        
        public WriteCacheServiceCounters(final int nbuffers) {
            
            this.nbuffers = nbuffers;
            
        }
        
        public CounterSet getCounters() {

            final CounterSet root = super.getCounters();

            root.addCounter("nbuffers",
                    new OneShotInstrument<Integer>(nbuffers));

            root.addCounter("ndirty", new Instrument<Integer>() {
                public void sample() {
                    setValue(ndirty);
                }
            });

            root.addCounter("maxDirty", new Instrument<Integer>() {
                public void sample() {
                    setValue(maxdirty);
                }
            });

            root.addCounter("nclean", new Instrument<Integer>() {
                public void sample() {
                    setValue(nclean);
                }
            });

            root.addCounter("nreset", new Instrument<Long>() {
                public void sample() {
                    setValue(nreset);
                }
            });

            root.addCounter("nsend", new Instrument<Long>() {
                public void sample() {
                    setValue(nsend);
                }
            });

            root.addCounter("nwrites", new Instrument<Long>() {
                public void sample() {
                    setValue(nwrites);
                }
            });

            root.addCounter("nclearRequests", new Instrument<Long>() {
                public void sample() {
                    setValue(nclearRequests);
                }
            });

            root.addCounter("nclears", new Instrument<Long>() {
                public void sample() {
                    setValue(nclears);
                }
            });

            root.addCounter("mbPerSec", new Instrument<Double>() {
                public void sample() {
                    final double mbPerSec = (((double) bytesWritten)
                            / Bytes.megabyte32 / (TimeUnit.NANOSECONDS
                            .toSeconds(elapsedWriteNanos)));
                    setValue(((long) (mbPerSec * 100)) / 100d);
                    
                }
            });

            return root;

        }

    } // class WriteCacheServiceCounters

    /**
     * Note: Atomic reference is used so the counters may be imposed from
     * outside.
     */
    private final AtomicReference<WriteCacheServiceCounters> counters;

    /**
     * Return the performance counters for the {@link WriteCacheService}.
     */
    public CounterSet getCounters() {

        return counters.get().getCounters();

    }
    
    /**
     * Return the #of {@link WriteCache} blocks sent by the quorum leader to
     * the first downstream follower.
     */
    public long getSendCount() {

        return counters.get().nsend;

    }

    /**
     * An instance of this exception is thrown if a thread notices that the
     * {@link WriteCacheService} was closed by a concurrent process.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class AsynchronousCloseException extends IllegalStateException {

        private static final long serialVersionUID = 1L;
        
    }
    
}
