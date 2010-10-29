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
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;


import com.bigdata.relation.accesspath.BlockingBuffer;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.ClientService;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.IBigdataFederation;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.IClientService;
import com.bigdata.service.IClientServiceCallable;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.ndx.pipeline.AbstractPendingSetMasterTask;
import com.bigdata.service.ndx.pipeline.AbstractSubtask;

//BTM - FOR_PRE_CLIENT_SERVICE
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.service.CallableExecutor;
import com.bigdata.util.Util;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.tcp.TcpServerEndpoint;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.resources.ILocalResourceManagement;

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
 * @todo Isolate the pending set buffer logic and write unit tests for it. This
 *       should also cover the acceleration of the final tasks during normal
 *       shutdown.
 */
abstract public class ResourceBufferTask<//
H extends ResourceBufferStatistics<L, HS>, //
E extends Serializable, //
S extends ResourceBufferSubtask, //
L extends ClientLocator, //
HS extends ResourceBufferSubtaskStatistics //
> extends AbstractPendingSetMasterTask<H, E, S, L> {

    // from the ctor.

    protected final MappedTaskMaster taskMaster;

    protected final int sinkQueueCapacity;

    protected final int sinkChunkSize;

    protected final long sinkChunkTimeoutNanos;

//BTM - FOR_CLIENT_SERVICE
    private ILocalResourceManagement localResourceManager;

    /**
     * Internal state reflecting the resources which are in process. Resources
     * are added to this collection when they are posted to a client for
     * processing and are removed when the client asynchronously reports success
     * or failure for the resource.
     */
    final private Map<E, Collection<L>> pendingMap;

    protected Map<E,Collection<L>> getPendingMap() {

        return pendingMap;

    }

    @Override
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
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public ResourceBufferTask(
//BTM - PRE_CLIENT_SERVICE            //
//BTM - PRE_CLIENT_SERVICE            final MappedTaskMaster taskMaster, final long sinkIdleTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE            final long sinkPollTimeoutNanos, final int sinkQueueCapacity,
//BTM - PRE_CLIENT_SERVICE            final int sinkChunkSize, final long sinkChunkTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE            final H stats, final BlockingBuffer<E[]> buffer) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        super(taskMaster.getFederation(), stats, buffer, sinkIdleTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE                sinkPollTimeoutNanos);
    public ResourceBufferTask
               (final MappedTaskMaster taskMaster,
                final long sinkIdleTimeoutNanos,
                final long sinkPollTimeoutNanos,
                final int sinkQueueCapacity,
                final int sinkChunkSize,
                final long sinkChunkTimeoutNanos,
                final H stats,
                final BlockingBuffer<E[]> buffer,
                final ILocalResourceManagement localResourceManager)
    {
        super(taskMaster.getScaleOutIndexManager(), stats, buffer,
              sinkIdleTimeoutNanos, sinkPollTimeoutNanos);

        if (localResourceManager == null) {
            throw new NullPointerException("null localResourceManager");
        }
        this.localResourceManager = localResourceManager;
//BTM - PRE_CLIENT_SERVICE - END

//        if (taskMaster == null)
//            throw new IllegalArgumentException();

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

        this.pendingMap = newPendingMap();

    }

    /**
     * Accelerate shutdown protocol by mapping the pending set across the
     * remaining clients. Each resource in the pending set is assigned to
     * multiple clients. The assignments are made in random orderings to
     * minimize the likelihood that each client will perform the same work.
     *
     * FIXME Finish up should use round robin multiple assignment of resources
     * to clients to get done faster.
     */
    @Override
    protected void willShutdown() throws InterruptedException {
        /*
         * TODO visit the sinks and determine which are fast and which are slow.
         * This could be done based on the pending set size of the sink, but the
         * service rate is a better indicator since the asynchronous write API
         * tends to complete large chunks of work items at the same time. For
         * the slow sinks, map their pendingSet across the fast sinks using a
         * round robin approach and a non-blocking add to the sink's input
         * buffer. We can reopen fast sinks which have been closed for this
         * purpose since we know that there is no workload on those clients. Be
         * careful not to block here, but note that we are NOT holding any locks
         * on entry to this method.
         */
//        final SubtaskOp<S> op = new SubtaskOp<S>() {
//            public void call(S s) throws Exception {
//            }
//        };
//        try {
//            mapOperationOverSubtasks(op);
//        } catch (ExecutionException ex) {
//            throw new RuntimeException(ex);
//        }
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

            // The hash function used to assign resources to client tasks.
            final IHashFunction<E> hashFunction = ((MappedTaskMaster.JobState) taskMaster
                    .getJobState()).clientHashFunction;

            // array of ordered containers for each partition.
            final List<E>[] v = new List[N];

            for (E e : chunk) {

                final int h = hashFunction.hashFunction(e);

                // note: hash function can be negative, but we want a non-neg index.
                final int i = Math.abs(h % N);

//                assert i >= 0 && i < N : "hashFunction out of range: e=" + e
//                        + ", h(e)=" + h + ", N=" + N + ", i=" + i
//                        + ", hashFunction=" + hashFunction;

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

                if (t.toArray(split) != split) {

                    /*
                     * This assumes that the data is inserted into the given
                     * array.
                     */

                    throw new AssertionError();

                }

                halted();

                addToOutputBuffer((L) new ClientLocator(i), split,
                        0/* fromIndex */, split.length/* toIndex */, reopen);

            }

        } finally {

            synchronized (stats) {

                stats.handledChunkCount.incrementAndGet();

                stats.elapsedHandleChunkNanos += System.nanoTime() - begin;

            }

        }

    }

    /**
     * @todo In order to handle client failure once the master is shutting down
     *       we must start a NEW client task. Prior to that it is sufficient to
     *       redistribute the work among the remaining clients (in this case we
     *       could also start a replacement client task).
     */
    @SuppressWarnings("unchecked")
    @Override
    protected S newSubtask(final L locator, final BlockingBuffer<E[]> out) {

//BTM - PRE_CLIENT_SERVICE  IClientService service = taskMaster.getJobState().clientServiceMap
        CallableExecutor service =
            taskMaster.getJobState().clientServiceMap.getService
                                                      (locator.getClientNo());
        assert service != null;

        try {

            /*
             * Submit a factory task whose Future evaluates to the proxy for the
             * client task running on the remote service. The factory task ctor
             * accepts the Serializable client task object. The factory task is
             * executed on the callable executor service and returns the proxy
             * for the client task from that machine.
             */
            final IAsynchronousClientTask<?, E> clientTask = 
                (IAsynchronousClientTask<?, E>) service.submit
                    (new ClientTaskFactory(taskMaster.newClientTask
                                               (masterProxy, locator))).get();

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
     * Factory object used to start a {@link AbstractAsynchronousClientTask} on
     * a callable executor service. The factory returns the proxy for the
     * {@link AbstractAsynchronousClientTask}. By using
     * {@link AbstractAsynchronousClientTask#getFuture()}, the caller can also
     * obtain the proxy for the task's {@link Future}.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class ClientTaskFactory
        implements IClientServiceCallable<IAsynchronousClientTask> {

        /**
         *
         */
        private static final long serialVersionUID = -5106901692329093593L;

        private final AbstractAsynchronousClientTask task;

        private static final boolean ENABLE_DGC = true;
        private static final boolean KEEP_ALIVE = false;

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
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        public IAsynchronousClientTask startClientTask(
//BTM - PRE_CLIENT_SERVICE                IBigdataFederation federation,
//BTM - PRE_CLIENT_SERVICE                ClientService clientService)
//BTM - PRE_CLIENT_SERVICE            throws Exception {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            JiniFederation jiniFederation = (JiniFederation) federation;
//BTM - PRE_CLIENT_SERVICE            final Future future =
//BTM - PRE_CLIENT_SERVICE                jiniFederation.getProxy(clientService.submit(task));
//BTM - PRE_CLIENT_SERVICE            task.setFuture(future);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            return (IAsynchronousClientTask)
//BTM - PRE_CLIENT_SERVICE                    jiniFederation.getProxy(task, true/* enableDGC */);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    }

        public IAsynchronousClientTask startClientTask
                               (IIndexManager indexManager,
                                ILocalResourceManagement localResourceManager,
                                CallableExecutor embeddedCallableExecutor,
                                ZooKeeper zookeeperClient,
                                List<ACL> zookeeperAcl,
                                String zookeeperRoot)
                                           throws Exception
        {
            Exporter futureExporter =
                       new BasicJeriExporter(TcpServerEndpoint.getInstance(0),
                                             new BasicILFactory(),
                                             ENABLE_DGC, KEEP_ALIVE);
            Exporter masterExporter =
                       new BasicJeriExporter(TcpServerEndpoint.getInstance(0),
                                             new BasicILFactory(),
                                             ENABLE_DGC, KEEP_ALIVE);
            Future futureStub = 
                Util.wrapFuture( futureExporter,
                                 embeddedCallableExecutor.submit(task) );
            task.setFuture(futureStub);
            return ((IAsynchronousClientTask)masterExporter.export(task));
        }
    }
//BTM - PRE_CLIENT_SERVICE - END

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
                buffer.isOrdered());

    }

    @SuppressWarnings("unchecked")
    @Override
    protected Future<HS> submitSubtask(final S subtask) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        return (Future<HS>) getFederation().getExecutorService()
//BTM - PRE_CLIENT_SERVICE                .submit(subtask);
        return (Future<HS>) localResourceManager.getThreadPool().submit
                                                                     (subtask);
//BTM - PRE_CLIENT_SERVICE - END

    }

    protected Map<E, Collection<L>> newPendingMap() {

        final int initialCapacity = ((MappedTaskMaster.JobState) taskMaster
                .getJobState()).pendingSetMasterInitialCapacity;

        if (initialCapacity == Integer.MAX_VALUE) {

            /*
             * FIXME The use of a BigdataMap/Set here has not been tested. One
             * know point of failure is that the keys of the map may be File,
             * URL, or other objects that are not handled by the KeyBuilder.
             * Those objects will need to be consistently converted into an
             * appropriate object, e.g., a String. The conversion must be
             * consistent to ensure that we recognize the resource as initially
             * queued and when its success/failure event comes along.
             */

            throw new UnsupportedOperationException();

//            final IRawStore store = getFederation().getTempStore();
//
//            // anonymous index (unnamed).
//            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
//
//            final BTree ndx = BTree.create(store, metadata);
//
//            return new BigdataMap<E, Collection<L>>(ndx);

        } else {

            return new LinkedHashMap<E, Collection<L>>(initialCapacity);

        }

    }

    /**
     * Concrete master hides most of the generic types leaving you with only
     * those that are meaningful to parameterize.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class M<E extends Serializable>
            extends
            ResourceBufferTask<//
            ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics>, // H
            E, // E
            ResourceBufferSubtask, // S
            ClientLocator, // L
            ResourceBufferSubtaskStatistics // HS
            > {

        /**
         * {@inheritDoc}
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        public M(
//BTM - PRE_CLIENT_SERVICE                final MappedTaskMaster taskMaster,
//BTM - PRE_CLIENT_SERVICE                final long sinkIdleTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE                final long sinkPollTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE                final int sinkQueueCapacity,
//BTM - PRE_CLIENT_SERVICE                final int sinkChunkSize,
//BTM - PRE_CLIENT_SERVICE                final long sinkChunkTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE                final ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics> stats,
//BTM - PRE_CLIENT_SERVICE                final BlockingBuffer<E[]> buffer) {
//BTM - PRE_CLIENT_SERVICE            super(taskMaster, sinkIdleTimeoutNanos, sinkPollTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE                    sinkQueueCapacity, sinkChunkSize, sinkChunkTimeoutNanos,
//BTM - PRE_CLIENT_SERVICE                    stats, buffer);
//BTM - PRE_CLIENT_SERVICE        }
        public M
            (final MappedTaskMaster taskMaster,
             final long sinkIdleTimeoutNanos,
             final long sinkPollTimeoutNanos,
             final int sinkQueueCapacity,
             final int sinkChunkSize,
             final long sinkChunkTimeoutNanos,
             final ResourceBufferStatistics<ClientLocator,
                                            ResourceBufferSubtaskStatistics> stats,
             final BlockingBuffer<E[]> buffer,
             final ILocalResourceManagement localResourceManager)
        {
            super(taskMaster,
                  sinkIdleTimeoutNanos,
                  sinkPollTimeoutNanos,
                  sinkQueueCapacity,
                  sinkChunkSize,
                  sinkChunkTimeoutNanos,
                  stats,
                  buffer,
                  localResourceManager);
        }
//BTM - PRE_CLIENT_SERVICE - END
    }

}
