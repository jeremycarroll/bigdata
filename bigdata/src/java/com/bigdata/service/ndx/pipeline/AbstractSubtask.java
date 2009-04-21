/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.ChunkMergeSortHelper;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Abstract implementation of a subtask for the {@link AbstractMasterTask}
 * handles the protocol for startup and termination of the subtask. A concrete
 * implementation must handle the chunks of elements being drained from the
 * subtask's {@link #buffer} via {@link #handleChunk(Object[])}.
 * 
 * @param <HS>
 *            The generic type of the value returned by {@link Callable#call()}
 *            for the subtask.
 * @param <M>
 *            The generic type of the master task implementation class.
 * @param <E>
 *            The generic type of the elements in the chunks stored in the
 *            {@link BlockingBuffer}.
 * @param <L>
 *            The generic type of the key used to lookup a subtask in the
 *            internal map (must be unique and must implement hashCode() and
 *            equals() per their contracts).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractSubtask<//
HS extends AbstractSubtaskStats,//
M extends AbstractMasterTask<? extends AbstractMasterStats<L, HS>, E, ? extends AbstractSubtask, L>,//
E,//
L>//
        implements Callable<HS> {

    protected static transient final Logger log = Logger
            .getLogger(AbstractSubtask.class);

    protected transient final boolean INFO = log.isInfoEnabled(); 
    protected transient final boolean DEBUG = log.isDebugEnabled(); 

    /**
     * The master.
     */
    protected final M master;

    /**
     * The unique key for the subtask.
     */
    protected final L locator;
    
    /**
     * The buffer on which the {@link #master} is writing.
     */
    protected final BlockingBuffer<E[]> buffer;

    /**
     * The iterator draining the {@link #buffer}.
     * <p>
     * Note: DO NOT close this iterator from within {@link #call()} as that
     * would cause this task to interrupt itself!
     */
    protected final IAsynchronousIterator<E[]> src;

    /**
     * The statistics used by this task.
     */
    protected final HS stats;
    
    /**
     * The timestamp at which a chunk was last written on the output buffer for
     * this sink by the master.
     */
    protected volatile long lastChunkNanos = System.nanoTime();

    public AbstractSubtask(final M master, final L locator,
            final BlockingBuffer<E[]> buffer) {

        if (master == null)
            throw new IllegalArgumentException();

        if (locator == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.master = master;
        
        this.locator = locator;

        this.buffer = buffer;

        this.src = buffer.iterator();

        this.stats = (HS) master.stats.getSubtaskStats(locator);

    }

    public HS call() throws Exception {

        try {

            final NonBlockingChunkedIterator itr = new NonBlockingChunkedIterator(
                    src);
            
            /*
             * Timestamp of the last chunk handled (written out on the index
             * partition). This is used to compute the average time between
             * chunks written on the index partition by this sink and across all
             * sinks.
             */
            long lastHandledChunkNanos = System.nanoTime();
            
            while(itr.hasNext()) {

                final E[] chunk = itr.next();

                // how long we waited for this chunk.
                final long elapsedChunkWaitNanos = System.nanoTime() - lastHandledChunkNanos;

                synchronized (master.stats) {
                    master.stats.elapsedChunkWaitingNanos += elapsedChunkWaitNanos;
                }
                stats.elapsedChunkWaitingNanos += elapsedChunkWaitNanos;
                
                if (handleChunk(chunk)) {

                    if (INFO)
                        log.info("Eager termination.");

                    // Done (eager termination).
                    break;

                }

                // reset the timestamp now that we will wait again.
                lastHandledChunkNanos = System.nanoTime();

            }
            
            // normal completion.
            master.removeOutputBuffer(locator, this);

            if (INFO)
                log.info("Done: " + locator);

            // done.
            return stats;

        } catch (Throwable t) {

            if (INFO) {
                // show stack trace @ INFO
                log.warn(this, t);
            } else {
                // else only abbreviated warning.
                log.warn(this + " : " + t);
            }
            
            /*
             * Halt processing.
             * 
             * Note: This is responsible for propagating any errors such that
             * the master halts in a timely manner. This is necessary since no
             * one is checking the Future for the sink tasks (except when we
             * wait for them to complete before we reopen an output buffer).
             */
            master.halt(t);

            throw new RuntimeException(t);

        } finally {

            master.lock.lock();
            try {
                master.subtask.signalAll();
            } finally {
                master.lock.unlock();
            }

        }

    }
    
    /**
     * Inner class is responsible for combining chunks as they become avaiable
     * from the {@link IAsynchronousIterator} while maintaining liveness. It
     * works with the {@link IAsynchronousIterator} API internally and polls the
     * {@link AbstractSubtask#src}. If a chunk is available, then it is added
     * to an ordered list of chunks which is maintained internally by this
     * class. {@link #next()} combines those chunks, using a merge sort to
     * maintain their order, and returns their data in a single chunk.
     * <p>
     * Note: This does not implement {@link Iterable} since its methods throw
     * {@link InterruptedException}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class NonBlockingChunkedIterator {

        /**
         * The source iterator that is being drained.
         */
        private final IAsynchronousIterator<E[]> src;
        
        /** #of elements across the set of {@link #chunks}. */
        private int chunkSize;

        /**
         * The set of chunks that have been buffered so far.
         */
        private LinkedList<E[]> chunks = new LinkedList<E[]>();

        /**
         * Clear the internal state after returning a chunk to the caller.
         */
        private void clear() {
            
            chunkSize = 0;

            chunks = new LinkedList<E[]>();
            
        }
        
        public String toString() {
            
            return AbstractSubtask.this.toString() + "{chunkSize=" + chunkSize
                    + "}";
            
        }
        
        public NonBlockingChunkedIterator(final IAsynchronousIterator<E[]> src) {

            if (src == null)
                throw new IllegalArgumentException();
            
            this.src = src;
            
        }
        
        public boolean hasNext() throws InterruptedException {

            // The thread in which this method runs.
            final Thread t = Thread.currentThread();

            // when we start looking for a chunk.
            final long begin = System.nanoTime();

            while (true) {
            
                // halt?
                master.halted();

                // interrupted?
                if (t.isInterrupted()) {

                    throw master.halt(new InterruptedException(toString()));

                }

                // current time.
                final long now = System.nanoTime();
                
                // elapsed since we entered hasNext.
                final long elapsedNanos = now - begin;

                // elapsed since the master last wrote a chunk on this sink.
                final long elapsedSinceLastChunk = now - lastChunkNanos;

                // true iff the sink has become idle.
                final boolean idle = elapsedSinceLastChunk > master.sinkIdleTimeoutNanos;
                
                if ((idle || (master.src.isExhausted()) && buffer.isOpen())) {
                    master.lock.lockInterruptibly();
                    try {
                        if (buffer.isEmpty()) {
                            /*
                             * Close out buffer. Since the buffer is empty the
                             * iterator will be quickly be exhausted (it is
                             * possible there is one chunk waiting in the
                             * iterator) and the subtask will quit the next time
                             * through the loop.
                             * 
                             * Note: This can happen either if the master is
                             * closed or if idle too long.
                             */
                            if (INFO)
                                log.info("Closing buffer: idle=" + idle + " : "
                                        + this);
                            if (idle) {
                                // stack trace here if closed by idle timeout.
                                buffer.close();
                                synchronized (master.stats) {
                                    master.stats.subtaskIdleTimeout++;
                                }
                            } else {
                                // stack trace here if master exhausted.
                                buffer.close();
                            }
                            if (chunkSize == 0 && !src.hasNext()) {
                                /*
                                 * The iterator is already exhausted so we break
                                 * out of the loop now.
                                 */
                                if (INFO)
                                    log.info("No more data: " + this);
                                return false;
                            }
                        }
                    } finally {
                        master.lock.unlock();
                    }
                }

                if (chunkSize >= buffer.getChunkSize()) {
                    /*
                     * We have a full chunk worth of data so do not wait longer.
                     */
                    if (log.isInfoEnabled())
                        log.info("Full chunk: " + chunkSize + ", elapsed="
                                + TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
                    return true;
                }

                if (chunkSize > 0
                        && ((elapsedNanos > buffer.getChunkTimeout()) || (!buffer
                                .isOpen() && !src.hasNext()))) {
                    /*
                     * We have SOME data and either (a) the chunk timeout has
                     * expired -or- (b) the buffer is closed and there is
                     * nothing more to be read from the iterator.
                     */
                    if (log.isInfoEnabled())
                        log.info("Partial chunk: " + chunkSize + ", elapsed="
                                + TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
                    // Done.
                    return true;
                }
                
                // poll the source iterator for another chunk.
                if (src.hasNext(master.sinkPollTimeoutNanos,
                        TimeUnit.NANOSECONDS)) {

                    /*
                     * Take whatever is already buffered but do allow the source
                     * iterator to combine chunks since that would increase our
                     * blocking time by whatever the chunkTimeout is.
                     */
                    final E[] a = src.next(1L, TimeUnit.NANOSECONDS);

                    assert a != null;
                    assert a.length != 0;

                    // add to the list of chunks which are already available.
                    chunks.add(a);

                    // track the #of elements available across those chunks.
                    chunkSize += a.length;

                    if (log.isDebugEnabled())
                        log.debug("Combined another chunk: chunkSize="
                                + a.length
                                + ", ncombined="
                                + chunks.size()
                                + ", elapsed="
                                + TimeUnit.NANOSECONDS.toMillis(System
                                        .nanoTime()
                                        - begin));
                    
                    continue;

                }
                
                if (chunkSize == 0 && !buffer.isOpen() && !src.hasNext()) {
                    // We are done.
                    if (INFO)
                        log.info("No more data: " + this);
                    return false;
                }

                // poll the itr again.

            } // while(true)

        }

        /**
         * Return the buffered chunk(s) as a single combined chunk. If more than
         * one chunk is combined to produce the returned chunk, then a merge
         * sort is applied to the the elements of the chunk before it is
         * returned to the caller in order to keep the data in the chunk fully
         * ordered.
         */
        public E[] next() {

            if (chunkSize == 0) {

                // nothing buffered.
                throw new NoSuchElementException();

            }

            // Dynamic instantiation of array of the same component type.
            final E[] a = (E[]) java.lang.reflect.Array.newInstance(chunks
                    .get(0)[0].getClass(), chunkSize);

            // Combine the chunk(s) into a single chunk.
            int dstpos = 0;
            int ncombined = 0;
            for (E[] t : chunks) {

                final int len = t.length;

                System.arraycopy(t, 0, a, dstpos, len);

                dstpos += len;

                ncombined++;

            }

            if (ncombined > 0) {

                ChunkMergeSortHelper.mergeSort(a);

            }

            // clear internal state.
            clear();

            return a;
            
        }

    }
    
    /**
     * Process a chunk from the buffer.
     * 
     * @return <code>true</code> iff the task should exit immediately.
     */
    abstract protected boolean handleChunk(E[] chunk) throws Exception;

}
