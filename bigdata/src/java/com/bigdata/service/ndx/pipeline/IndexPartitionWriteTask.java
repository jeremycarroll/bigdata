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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.Split;
import com.bigdata.util.InnerCause;

/**
 * Class drains a {@link BlockingBuffer} writing on a specific index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexPartitionWriteTask<T extends IKeyArrayIndexProcedure, O, R, A>
        implements Callable<IndexPartitionWriteStats> {

    static protected transient final Logger log = Logger
            .getLogger(IndexPartitionWriteTask.class);

    /**
     * The master.
     */
    private final IndexWriteTask<T, O, R, A> master;

    /**
     * The index partition locator.
     */
    public final PartitionLocator locator;

    /**
     * The data service on which the index partition resides.
     */
    public final IDataService dataService;

    /**
     * The buffer on which the {@link IndexWriteTask} writes tuples to be
     * written onto the index partition associated with this task.
     */
    protected final BlockingBuffer<KVO<O>[]> buffer;

    /**
     * The iterator draining the {@link #buffer}.
     * <p>
     * Note: DO NOT close this iterator from within {@link #call()} as that
     * would cause this task to interrupt itself!
     */
    protected final IAsynchronousIterator<KVO<O>[]> src;

    /**
     * The timestamp associated with the index view.
     */
    public final long timestamp;

    /**
     * The index partition identifier.
     */
    public final int partitionId;

    /**
     * The name of the index partition.
     */
    private final String indexPartitionName;

    /**
     * The statistics for writes on this index partition for the
     * {@link #master}.
     */
    private final IndexPartitionWriteStats stats;

    public String toString() {

        return getClass().getName() + "{indexPartition=" + indexPartitionName
                + ", open=" + buffer.isOpen() + "}";

    }

    public IndexPartitionWriteTask(final IndexWriteTask<T, O, R, A> master,
            final PartitionLocator locator, final IDataService dataService,
            final BlockingBuffer<KVO<O>[]> buffer) {

        if (master == null)
            throw new IllegalArgumentException();

        if (locator == null)
            throw new IllegalArgumentException();

        if (dataService == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.master = master;

        this.locator = locator;

        this.dataService = dataService;

        this.buffer = buffer;

        this.src = buffer.iterator();

        this.timestamp = master.ndx.getTimestamp();

        this.partitionId = locator.getPartitionId();

        this.indexPartitionName = DataService.getIndexPartitionName(master.ndx
                .getName(), partitionId);

        this.stats = master.stats.getStats(partitionId);

    }

    public IndexPartitionWriteStats call() throws Exception {

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

                if (!nextChunk()) {

                    // Done (handled a stale locator).
                    break;

                }

            }

            // normal completion.
            master.removeOutputBuffer(this);

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
     * Reads the next chunk from the buffer (there MUST be a chunk waiting
     * in the buffer).
     * 
     * @return <code>true</code> iff a {@link StaleLocatorException} was
     *         handled, in which case the task should exit immediately.
     * 
     * @throws IOException
     *             RMI error.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private boolean nextChunk() throws ExecutionException,
            InterruptedException, IOException {

        // A chunk in sorted order.
        final KVO<O>[] sourceChunk = src.next();

        /*
         * Remove duplicates in a caller specified manner (may be a NOP).
         */
        final KVO<O>[] chunk;
        final int duplicateCount;
        if (master.duplicateRemover == null) {
            
            chunk = sourceChunk;
            
            duplicateCount = 0;
            
        } else {
            
            // filter out duplicates.
            chunk = master.duplicateRemover.filter(sourceChunk);
            
            // #of duplicates that were filtered out.
            duplicateCount = sourceChunk.length - chunk.length;
            
        }

        if (chunk.length == 0) {

            // empty chunk after duplicate elimination (keep reading).
            return false;

        }

        // size of the chunk to be processed.
        final int chunkSize = chunk.length;

        /*
         * Change the shape of the data for RMI.
         */

        final boolean sendValues = master.ctor.sendValues();

        final byte[][] keys = new byte[chunkSize][];

        final byte[][] vals = sendValues ? new byte[chunkSize][] : null;

        for (int i = 0; i < chunkSize; i++) {

            keys[i] = chunk[i].key;

            if (sendValues)
                vals[i] = chunk[i].val;

        }

        /*
         * Instantiate the procedure using the data from the chunk and
         * submit it to be executed on the DataService using an RMI call.
         */
        final long beginNanos = System.nanoTime();
        try {

            final T proc = master.ctor.newInstance(master.ndx,
                    0/* fromIndex */, chunkSize/* toIndex */, keys, vals);

            // submit and await Future
            final R result = ((Future<R>) dataService.submit(timestamp,
                    indexPartitionName, proc)).get();

            if (master.resultHandler != null) {

                // aggregate results.
                master.resultHandler.aggregate(result, new Split(locator,
                        0/* fromIndex */, chunkSize/* toIndex */));

            }

            if (log.isDebugEnabled())
                log.debug(stats);

            // keep reading.
            return false;

        } catch (ExecutionException ex) {

            final StaleLocatorException cause = (StaleLocatorException) InnerCause
                    .getInnerCause(ex, StaleLocatorException.class);

            if (cause != null) {

                /*
                 * Handle a stale locator.
                 * 
                 * Note: The master has to (a) close the output buffer for
                 * this subtask; (b) update its locator cache such that no
                 * more work is assigned to this output buffer; (c) re-split
                 * the chunk which failed with the StaleLocatorException;
                 * and (d) re-split all the data remaining in the output
                 * buffer since it all needs to go into different output
                 * buffer(s).
                 */

                if (log.isInfoEnabled())
                    log.info("Stale locator: name=" + cause.getName()
                            + ", reason=" + cause.getReason());

                master.handleStaleLocator(this, chunk, cause);

                // done.
                return true;

            } else {

                throw ex;

            }

        } finally {

            final long elapsedNanos = System.nanoTime() - beginNanos;

            // update the local statistics.
            stats.chunksOut++;
            stats.elementsOut += chunkSize;
            stats.elapsedNanos += elapsedNanos;

            // update the master's statistics.
            synchronized (master.stats) {
                master.stats.chunksOut++;
                master.stats.elementsOut += chunkSize;
                master.stats.duplicateCount += duplicateCount;
                master.stats.elapsedNanos += elapsedNanos;
            }

        }

    }

}
