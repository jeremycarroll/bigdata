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

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Abstract implementation of a subtask for the {@link AbstractMasterTask}
 * handles the protocol for startup and termination of the subtask. A concrete
 * implementation must handle the chunks of elements being drained from the
 * subtask's {@link #buffer} via {@link #nextChunk(Object[])}.
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

            /*
             * Poll the iterator with a timeout to avoid deadlock with
             * awaitAll().
             * 
             * Note: In order to ensure termination the subtask MUST poll
             * the iterator with a timeout so that a subtask which was
             * created in response to a StaleLocatorException during
             * master.awaitAll() can close its own blocking buffer IF: (a)
             * the top-level blocking buffer is closed; and (b) the
             * subtask's blocking buffer is empty. This operation needs to
             * be coordinated using the master's [lock], as does any
             * operation which writes on the subtask's buffer. Otherwise we
             * can wait forever for a subtask to complete. The subtask uses
             * the [subtask] Condition signal the master when it is
             * finished.
             */
            while (true) {

                master.halted();

                // nothing available w/in timeout?
                if (!src.hasNext(
                        // @todo config timeout
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT)) {

                    // are we done? should we close our buffer?
                    master.lock.lockInterruptibly();
                    try {
                        if (!buffer.isOpen() && !src.hasNext()) {
                            // We are done.
                            if (log.isInfoEnabled())
                                log.info("No more data: " + this);
                            break;
                        }
                        if (master.src.isExhausted()) {
                            if (buffer.isEmpty()) {
                                // close our buffer.
                                buffer.close();
                                if (log.isInfoEnabled())
                                    log.info("Closed buffer: " + this);
                            }
                        }
                    } finally {
                        master.lock.unlock();
                    }
                    continue;

                }

                if (Thread.interrupted()) {

                    throw master.halt(new InterruptedException(toString()));

                }

                if (!nextChunk(src.next())) {

                    // Done (eager termination).
                    break;

                }

            }

            // normal completion.
            master.removeOutputBuffer(locator, this);

            if (log.isInfoEnabled())
                log.info("Done: " + stats);

            // done.
            return stats;

        } catch (Throwable t) {

            // halt processing.
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
     * Process a chunk from the buffer.
     * 
     * @return <code>true</code> iff the task should exit immediately.
     */
    abstract protected boolean nextChunk(E[] chunk) throws Exception;

}
