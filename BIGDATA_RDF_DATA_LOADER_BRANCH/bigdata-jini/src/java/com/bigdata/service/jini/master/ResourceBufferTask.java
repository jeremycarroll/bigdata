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
 * Created on Apr 15, 2009
 */

package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import net.jini.core.lookup.ServiceItem;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BigdataMap;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.FederationCallable;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.ndx.pipeline.AbstractPendingSetMasterTask;
import com.bigdata.service.ndx.pipeline.AbstractSubtask;

/**
 * Task drains a {@link BlockingBuffer} containing resources (really, resource
 * identifiers) to be processed by the clients and uses hash partitioning to
 * assign the resources to client tasks for processing.
 * <p>
 * If the task is interrupted, it will refuse additional writes by closing its
 * {@link BlockingBuffer} and will cancel any sub-tasks and discard any buffered
 * writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <H>
 *            The generic type of the value returned by {@link Callable#call()}
 *            for the master.
 * @param <E>
 *            The generic type of the elements in the chunks stored in the
 *            {@link BlockingBuffer}.
 * @param <S>
 *            The generic type of the subtask implementation class.
 * @param <L>
 *            The generic type of the key used to lookup a subtask in the
 *            internal map (must be unique and must implement hashCode() and
 *            equals() per their contracts).
 * @param <HS>
 *            The generic type of the value returned by {@link Callable#call()}
 *            for the subtask.
 * 
 * @todo finish up should use round robin multiple assignment of resources to
 *       clients to get done faster. this will have to assign the last resources
 *       to multiple clients (ideally based on client load) rather than
 *       assigning them based on the {@link #hashFunction(Serializable)}. The
 *       pendingMap and pendingSet logic for the master and subtask should be
 *       ready to go as they allow for the possibility that an item is tasked to
 *       more than one client.
 */
abstract public class ResourceBufferTask <//
H extends ResourceBufferStatistics<L, HS>, //
E extends Serializable, //
S extends ResourceBufferSubtask, //
L extends ClientLocator, //
HS extends ResourceBufferSubtaskStatistics //
> extends AbstractPendingSetMasterTask<H, E, S, L> 
{

    // from the ctor.
    
    protected final MappedTaskMaster taskMaster;
    
    protected final int sinkQueueCapacity;

    protected final int sinkChunkSize;

    protected final long sinkChunkTimeoutNanos;

    public String toString() {
        
        return getClass().getName() + "{jobName="
                + taskMaster.getJobState().jobName + ", open="
                + buffer.isOpen() + "}";
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @param sinkQueueCapacity
     *            The capacity of the internal queue for the per-sink output
     *            buffer.
     * @param sinkChunkSize
     *            The desired size of the chunks written that will be written by
     *            the {@link AbstractSubtask sink}.
     * @param sinkChunkTimeoutNanos
     *            The maximum amount of time in nanoseconds that a sink will
     *            combine smaller chunks so that it can satisfy the desired
     *            <i>sinkChunkSize</i>.
     */
    public ResourceBufferTask(//
            final MappedTaskMaster taskMaster,
            final long sinkIdleTimeoutNanos,
            final long sinkPollTimeoutNanos,
            final int sinkQueueCapacity,
            final int sinkChunkSize,
            final long sinkChunkTimeoutNanos,
            final H stats,
            final BlockingBuffer<E[]> buffer) {

        super(taskMaster.getFederation(), stats, buffer, sinkIdleTimeoutNanos,
                sinkPollTimeoutNanos);

        if (taskMaster == null)
            throw new IllegalArgumentException();
        
        if (sinkQueueCapacity <= 0)
            throw new IllegalArgumentException();

        if (sinkChunkSize <= 0)
            throw new IllegalArgumentException();
        
        if (sinkChunkTimeoutNanos <= 0)
            throw new IllegalArgumentException();

        this.taskMaster = taskMaster;

        this.sinkQueueCapacity = sinkQueueCapacity;

        this.sinkChunkSize = sinkChunkSize;
        
        this.sinkChunkTimeoutNanos = sinkChunkTimeoutNanos;
        
    }

    /**
     * Hash partitions the chunk among the clients.
     */
    @SuppressWarnings("unchecked")
    protected void handleChunk(final E[] chunk, final boolean reopen)
            throws InterruptedException {

        final long begin = System.nanoTime();
        
        try {

            final long beforeSplit = System.nanoTime();
            
            // #of partitions.
            final int N = taskMaster.getJobState().nclients;
            
            // array of ordered containers for each partition.
            final List<E>[] v = new List[N];

            for (E e : chunk) {

                // Note: Could have hashed on the Object value as easily as the
                // key, which would make sense for some applications.
                final int i = taskMaster.hashFunction(e) % N;

                if (v[i] == null) {

                    v[i] = new LinkedList<E>();

                }

                v[i].add(e);

            }
            
            final long splitNanos = System.nanoTime() - beforeSplit;

            synchronized (stats) {

                stats.elapsedSplitChunkNanos += splitNanos;

            }
            
            // Break the chunk into the splits
            for (int i = 0; i < v.length; i++) {

                final List<E> t = v[i];

                if (t == null) {
                    // no data for this partition.
                    continue;
                }

                // allocate array of an appropriate type.
                final E[] split = (E[]) java.lang.reflect.Array.newInstance(
                        chunk[0].getClass(), t.size());

                if (t.toArray(chunk) != chunk) {

                    /*
                     * This assumes that the data is inserted into the given
                     * array.
                     */
                    
                    throw new AssertionError();

                }

                halted();

                addToOutputBuffer((L) new ClientLocator(i), split,
                        0/* fromIndex */, chunk.length/* toIndex */, reopen);

            }

        } finally {

            synchronized (stats) {
             
                stats.handledChunkCount.incrementAndGet();
                
                stats.elapsedHandleChunkNanos += System.nanoTime() - begin;
                
            }
            
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected S newSubtask(final L locator, final BlockingBuffer<E[]> out) {

        final ServiceItem serviceItem = taskMaster.getJobState().clientServiceMap
                .getServiceItem(locator.getClientNo());

        assert serviceItem != null;

        final IRemoteExecutor service = (IRemoteExecutor) serviceItem.service;

        assert service != null;

        try {

            /*
             * Submit a factory task whose Future evaluates to the proxy for the
             * client task running on the remote service. The factory task ctor
             * accepts the Serializable client task object. The factory task is
             * executed on the IRemoteExecutor and returns the proxy for the
             * client task from that machine.
             */
            final IAsynchronousClientTask<?, E> clientTask = (IAsynchronousClientTask<?, E>) service
                    .submit(
                            new ClientTaskFactory(taskMaster.newClientTask(
                                    masterProxy, locator))).get();

            // Verify that the client task is still running.
            if (clientTask.getFuture().isDone()) {

                // Not running? Throw error (if any) from Future.
                clientTask.getFuture().get();

                // If no error, then throw the exception ourselves.
                throw new RuntimeException(
                        "Could not start task on remote service: " + locator);

            }

            return (S) new ResourceBufferSubtask(this, locator, clientTask, out);

        } catch (Throwable t) {

            halt(t);

            throw new RuntimeException(t);

        }

    }
    
    /**
     * Factory object used to start a {@link AbstractAsynchronousClientTask} on an
     * {@link IRemoteExecutor} service. The factory returns the proxy for the
     * {@link AbstractAsynchronousClientTask}. By using {@link AbstractAsynchronousClientTask#getFuture()},
     * the caller can also obtain the proxy for the task's {@link Future}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class ClientTaskFactory extends FederationCallable<IAsynchronousClientTask> {

        /**
         * 
         */
        private static final long serialVersionUID = -5106901692329093593L;

        private final AbstractAsynchronousClientTask task;

        public ClientTaskFactory(final AbstractAsynchronousClientTask task) {

            if (task == null)
                throw new IllegalArgumentException();

            this.task = task;

        }
        
        /**
         * Submit the {@link AbstractAsynchronousClientTask} for execution, set
         * its {@link Future} and return the proxy for the
         * {@link AbstractAsynchronousClientTask}. The proxy for the
         * {@link Future} is available from the {@link IAsynchronousClientTask}
         * proxy.
         */
        @SuppressWarnings("unchecked")
        public IAsynchronousClientTask call() throws Exception {

            final Future future = getFederation().getExecutorService().submit(
                    task);

            task.setFuture(future);

            return (IAsynchronousClientTask) ((JiniFederation) getFederation())
                    .getProxy(task, true/* enableDGC */);

        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The queue capacity, chunk size and chunk timeout are taken from the ctor
     * parameters.
     */
    protected BlockingBuffer<E[]> newSubtaskBuffer() {
        
        return new BlockingBuffer<E[]>(//
                new LinkedBlockingDeque<E[]>(sinkQueueCapacity),//
                sinkChunkSize,// 
                sinkChunkTimeoutNanos,//
                TimeUnit.NANOSECONDS,//
                buffer.isOrdered()
        );
        
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Future<HS> submitSubtask(final S subtask) {

        return (Future<HS>) getFederation().getExecutorService().submit(
                subtask);
        
    }

    protected Map<E, Collection<L>> newPendingMap() {

        final int initialCapacity = ((MappedTaskMaster.JobState) taskMaster
                .getJobState()).pendingSetMasterInitialCapacity;

        if (initialCapacity == Integer.MAX_VALUE) {

            final IRawStore store = getFederation().getTempStore();
            
            final IndexMetadata metadata = new IndexMetadata("pendingMap", UUID
                    .randomUUID());

            final BTree ndx = BTree.create(store, metadata);

            return new BigdataMap<E, Collection<L>>(ndx);
            
        } else {

            return new LinkedHashMap<E, Collection<L>>(initialCapacity);

        }

    }

    /**
     * Concrete master hides most of the generic types leaving you with only
     * those that are meaningful to parameterize.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class M<E extends Serializable> extends ResourceBufferTask<//
            ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics>, // H
            E, // E
            ResourceBufferSubtask, // S
            ClientLocator, // L
            ResourceBufferSubtaskStatistics // HS
            > {

        /**
         * {@inheritDoc}
         */
        public M(
                final MappedTaskMaster taskMaster,
                final long sinkIdleTimeoutNanos,
                final long sinkPollTimeoutNanos,
                final int sinkQueueCapacity,
                final int sinkChunkSize,
                final long sinkChunkTimeoutNanos,
                final ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics> stats,
                final BlockingBuffer<E[]> buffer) {
            
            super(taskMaster, sinkIdleTimeoutNanos, sinkPollTimeoutNanos,
                    sinkQueueCapacity, sinkChunkSize, sinkChunkTimeoutNanos,
                    stats, buffer);

        }

    }

}
