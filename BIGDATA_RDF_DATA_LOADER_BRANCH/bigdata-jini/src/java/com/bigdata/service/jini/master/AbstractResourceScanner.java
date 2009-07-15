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
 * Created on Jul 11, 2009
 */

package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Abstract base class for the scanner for a mapped master job. The
 * {@link Callable} should return the #of resources which were accepted for
 * processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractResourceScanner<V> implements Callable<Long> {

    /**
     * The master buffer onto which the scanner drops chunks of resources
     * for processing.
     */
    private final BlockingBuffer<V[]> buffer;

    /**
     * A queue used to combine the individual resources reported to
     * {@link #accept(Object)} into chunks before they are added to the
     * {@link #buffer}.
     */
    private final ArrayBlockingQueue<V> queue;

    /**
     * Lock used to serialize the decision to transfer a chunk from the queue to
     * the buffer.
     */
    private final ReentrantLock queueLock = new ReentrantLock();
    
    /**
     * The #of resources accepted by the scanner.
     */
    private final AtomicLong acceptCount = new AtomicLong();

    /**
     * Return the #of accepted resources.
     */
    final public long getAcceptCount() {

        return acceptCount.get();

    }

    /**
     * @param buffer
     *            The buffer to which the resources should be added.
     */
    protected AbstractResourceScanner(final BlockingBuffer<V[]> buffer) {

        if (buffer == null)
            throw new IllegalArgumentException();

        this.buffer = buffer;

        this.queue = new ArrayBlockingQueue<V>(2 * buffer.getMinimumChunkSize());
        
    }

    /**
     * Invokes {@link #runScanner()} and waits until all resources identified by
     * the scanner have been processed.
     * <p>
     * During normal execution, the {@link BlockingBuffer} will be closed and
     * this method will await its {@link Future} before returning. If the thread
     * is interrupted or if any exception is thrown, then
     * {@link BlockingBuffer#abort(Throwable)} is used to terminate buffer
     * processing and the exception is rethrown out of this method.
     * 
     * @return The #of resources accepted by the scanner.
     */
    final public Long call() throws Exception {

        try {

            // run the scanner.
            runScanner();

            // flush the last chunk to the blocking buffer.
            flushQueue();
            
            // close the buffer - no more resources will be queued.
            buffer.close();
            
            // await the completion of the work for the queued resources.
            buffer.getFuture().get();
            
            // #of resources accepted by the scanner.
            return acceptCount.get();
            
        } catch (Throwable t) {

            // interrupt buffer.
            buffer.abort(t);

            // rethrow exception.
            throw new RuntimeException(t);
            
        }

    }
    
    /**
     * Run the scanner.
     * 
     * @throws Exception
     */
    protected abstract void runScanner() throws Exception;
    
    /**
     * Accept a resource for processing.
     * 
     * @param resource
     *            The resource.
     */
    public void accept(final V resource) throws InterruptedException {

        if (resource == null)
            throw new IllegalArgumentException();

        this.acceptCount.incrementAndGet();

        // add the resource to the queue.
        queue.add(resource);

        /*
         * Synchronize callers. If there are multiple threads accepting
         * resources then only one thread at a time will cause the chunk to be
         * drained from the queue and placed onto the buffer.
         */
        queueLock.lockInterruptibly();
        try {

            if (queue.size() >= buffer.getMinimumChunkSize()) {

                // drain a chunk, transferring it to the buffer.
                transferChunk();

            }

        } finally {
         
            queueLock.unlock();
            
        }

    }
    
    /**
     * Drain a chunk from the queue, transferring it to the buffer.
     */
    @SuppressWarnings("unchecked")
    private void transferChunk() {
        
        final LinkedList<V> c = new LinkedList<V>();

        // drain chunk containing up to the desired chunk size.
        queue.drainTo(c, buffer.getMinimumChunkSize());

        // allocate array of the appropriate component type.
        final V[] a = (V[]) java.lang.reflect.Array.newInstance(c.getFirst()
                .getClass(), c.size());

        // copy the chunk onto the array.
        int i = 0;
        for (V v : c) {

            a[i++] = v;

        }
        
        /*
         * Add the chunk to the buffer.
         * 
         * Note: this will block if the queue is full.
         */
        buffer.add(a);

    }
    
    /**
     * Drain anything left in the queue, transferring it in chunks to the
     * buffer.
     */
    private void flushQueue() {

        while(!queue.isEmpty()) {

            // transfer a chunk from the queue to the buffer.
            transferChunk();
        
        }

    }

}
