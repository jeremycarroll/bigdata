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
 * Created on Jul 10, 2009
 */

package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.btree.AsynchronousIndexWriteConfiguration;
import com.bigdata.btree.BigdataMap;
import com.bigdata.btree.BigdataSet;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.rdf.load.RDFDataLoadMaster;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IRunnableBuffer;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.ndx.pipeline.AbstractMasterTask;

/**
 * Extends the {@link TaskMaster} to assign chunks of resources for processing
 * to the client tasks.
 * <p>
 * The master scans a source such as the file system, a distributed file system,
 * or a scale-out index to identify the resources to be processed. The resources
 * are placed onto a hash-partitioned buffer, which is drained by the various
 * clients. If a client succeeds, then the resources have been handled
 * successfully and a new set of resources is assigned to that client. If a
 * client fails, the resources are reassigned to a different client. If all
 * clients fail, the job is aborted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic type of the {@link JobState}.
 * @param <T>
 *            The generic type of the client task.
 * @param <U>
 *            The generic type of the value returned by the client task.
 * @param <V>
 *            The generic type of the resources to be tasked.
 */
public abstract class MappedTaskMaster<//
S extends MappedTaskMaster.JobState,//
T extends AbstractAsynchronousClientTask<U, V, L>, //
L extends ClientLocator, //
U, //
V extends Serializable//
>//
        extends TaskMaster<S, T, U> {

    /**
     * {@link Configuration} options for the {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions extends TaskMaster.ConfigurationOptions {

        /**
         * Option specifies the {@link AsynchronousIndexWriteConfiguration} used
         * to provision the buffer on which the master queues the resources to
         * be processed by the client tasks.
         * <p>
         * Note: The configuration values for the indices are stored in the
         * {@link IndexMetadata} on the {@link IMetadataService}. The easiest
         * way to tweak things is just to update the {@link IndexMetadata}
         * objects on the {@link IMetadataService}.
         */
        String RESOURCE_BUFFER_CONFIG = "resourceBufferConfig";
        
        /**
         * Option specifies the {@link IResourceScannerFactory} used to
         * instantiate the scanner which will select the resources to be
         * processed by the job.
         */
        String RESOURCE_SCANNER_FACTORY = "resourceScannerFactory";

        /**
         * The initial capacity of the pending {@link Map} for the job -or-
         * {@link Integer#MAX_VALUE} to use a {@link BigdataMap}.
         * <p>
         * The pending set contains all resources for which there is an
         * outstanding asynchronous work request. There is a pending set for the
         * {@link ResourceBufferTask} (per job) and the
         * {@link ResourceBufferSubtask} (per client).
         */
        String PENDING_SET_MASTER_INITIAL_CAPACITY = "pendingSetMasterInitialCapacity";

        /**
         * The initial capacity of the pending {@link Set} for each client -or-
         * {@link Integer#MAX_VALUE} to use a {@link BigdataSet}.
         */
        String PENDING_SET_SUBTASK_INITIAL_CAPACITY = "pendingSetSubtaskInitialCapacity";
        
    }
    
    /**
     * The job description for an {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JobState extends TaskMaster.JobState {
    
        /**
         * 
         */
        private static final long serialVersionUID = 1395850823170993889L;

        /**
         * Configuration for the buffer used to pass resources to the client
         * tasks.
         * 
         * @see ConfigurationOptions#RESOURCE_BUFFER_CONFIG
         */
        protected final AsynchronousIndexWriteConfiguration conf;

        /**
         * Factory for the scanner selecting the resources to be processed.
         * 
         * @see ConfigurationOptions#RESOURCE_SCANNER_FACTORY
         */
        protected final IResourceScannerFactory<?> scannerFactory;

        /**
         * @see ConfigurationOptions#PENDING_SET_MASTER_INITIAL_CAPACITY
         */
        public final int pendingSetMasterInitialCapacity;

        /**
         * @see ConfigurationOptions#PENDING_SET_SUBTASK_INITIAL_CAPACITY
         */
        public final int pendingSetSubtaskInitialCapacity;
        
        @Override
        protected void toString(final StringBuilder sb) {

            super.toString(sb);

            sb.append(", " + ConfigurationOptions.RESOURCE_BUFFER_CONFIG + "="
                    + conf);

            sb.append(", " + ConfigurationOptions.RESOURCE_SCANNER_FACTORY
                    + "=" + scannerFactory);

            sb.append(", "
                    + ConfigurationOptions.PENDING_SET_MASTER_INITIAL_CAPACITY
                    + "=" + pendingSetMasterInitialCapacity);

            sb.append(", "
                    + ConfigurationOptions.PENDING_SET_SUBTASK_INITIAL_CAPACITY
                    + "=" + pendingSetSubtaskInitialCapacity);

        }

        /**
         * {@inheritDoc}
         */
        public JobState(final String component, final Configuration config)
                throws ConfigurationException {

            super(component, config);
            
            conf = (AsynchronousIndexWriteConfiguration) config.getEntry(component,
                    ConfigurationOptions.RESOURCE_BUFFER_CONFIG, AsynchronousIndexWriteConfiguration.class,
                    new AsynchronousIndexWriteConfiguration(//
                            100, // masterQueueCapacity,
                            1000, // masterChunkSize
                            TimeUnit.SECONDS.toNanos(5),// masterChunkTimeoutNanos
                            Long.valueOf(IndexMetadata.Options.SINK_IDLE_TIMEOUT_NANOS).longValue(),//
                            Long.valueOf(IndexMetadata.Options.SINK_POLL_TIMEOUT_NANOS).longValue(),//
                            100, // sinkQueueCapacity
                            1000, // sinkChunkSize,
                            TimeUnit.SECONDS.toNanos(20)// sinkChunkTimeoutNanos
                    ));

            scannerFactory = (IResourceScannerFactory) config.getEntry(
                    component, ConfigurationOptions.RESOURCE_SCANNER_FACTORY,
                    IResourceScannerFactory.class);

            pendingSetMasterInitialCapacity = (Integer) config.getEntry(
                    component,
                    ConfigurationOptions.PENDING_SET_MASTER_INITIAL_CAPACITY,
                    Integer.TYPE, Integer.MAX_VALUE);

            pendingSetSubtaskInitialCapacity = (Integer) config.getEntry(
                    component,
                    ConfigurationOptions.PENDING_SET_SUBTASK_INITIAL_CAPACITY,
                    Integer.TYPE, Integer.MAX_VALUE);

        }

    }

    /**
     * {@inheritDoc}
     */
    public MappedTaskMaster(JiniFederation fed) throws ConfigurationException {

        super(fed);

    }

    /**
     * Return the hash code for a resource to be assigned to a client task. The
     * default implementation uses {@link Object#hashCode()}.
     * 
     * @param resource
     *            The resource.
     *            
     * @return The hash code.
     */
    public int hashFunction(V resource) {
        
        return resource.hashCode();
        
    }

    /**
     * Extended to run the scanner, handing off resources to clients for
     * processing. The clients should run until they are interrupted by the
     * master.
     * <p>
     * After the scanner closes the master buffer, the master will continue to
     * run until the pendingSet is empty. In order to prevent workload
     * starvation, the sinks will be flushed aggressively once the master
     * resource buffer is empty.
     * 
     * FIXME Termination conditions are still not quite correct. The resource
     * buffer sinks must {@link IAsynchronousClientTask#close()} the client task
     * once if the master is signaling that they should flush work agressively.
     * The client tasks will then invoke {@link AbstractMasterTask#awaitAll()}.
     * 
     * FIXME In order to handle client failure once the master has begun to
     * flush work aggressively we must start a NEW client task. Prior to that it
     * is sufficient to redistribute the work among the remaining clients (in
     * this case we could also start a replacement client task).
     * 
     * FIXME The resources remaining in the pendingSet should be pushed to all
     * clients in AbstractMasterTask#awaitAll(). The resources should be placed
     * into random orderings to maximize the chance that the clients will
     * accelerate the work.
     */
    @Override
    protected void runJob() throws Exception {
        
        try {

            // instantiate scanner backed by the resource buffer.
            final AbstractResourceScanner<?> scanner = getJobState().scannerFactory
                    .newScanner((IRunnableBuffer) resourceBuffer);

            // start scanner.
            final Future<Long> scannerFuture = fed.getExecutorService().submit(
                    scanner);

            // await scanner future.
            scannerFuture.get();

        } finally {

            /*
             * The buffer should have been closed by the scanner. We close the
             * buffer here as a failsafe in case the scanner task did not start
             * or in case the scanner it is interrupted (in which case
             * Future#get() may return before the buffer is closed).
             */
            if (resourceBuffer.isOpen()) {

                resourceBuffer.close();

            }
            
        }
        
    }

    /**
     * Buffer used to assign resources to the client tasks.
     */
    private final BlockingBuffer<V[]> resourceBuffer = newResourceBuffer();
    
    /**
     * Return the buffer used to hand off resources for processing to the
     * clients.
     * 
     * @todo Specializing the behavior of the {@link MappedTaskMaster} requires
     *       overriding the behavior of the {@link ResourceBufferTask}. It
     *       should be parameterized if possible with interfaces for interesting
     *       things, however its termination logic is tricky and overrides
     *       should not mess with that.
     */
    public BlockingBuffer<V[]> newResourceBuffer() {

        final AsynchronousIndexWriteConfiguration conf = getJobState().conf;

        final ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics> stats //
            = new ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics>(
                getFederation());
        
        final BlockingBuffer<V[]> resourceBuffer = new BlockingBuffer<V[]>(
                new LinkedBlockingDeque<V[]>(conf.getMasterQueueCapacity()),//
                conf.getMasterChunkSize(),//
                conf.getMasterChunkTimeoutNanos(),// 
                TimeUnit.NANOSECONDS,//
                false// NOT ordered data.
        );

        final ResourceBufferTask.M<V> task = new ResourceBufferTask.M<V>(
                this, //
                conf.getSinkIdleTimeoutNanos(),//
                conf.getSinkPollTimeoutNanos(),//
                conf.getSinkQueueCapacity(), //
                conf.getSinkChunkSize(), //
                conf.getSinkChunkTimeoutNanos(),//
                stats,//
                resourceBuffer//
        );

        final Future<? extends ResourceBufferStatistics> future = getFederation()
                .getExecutorService().submit(task);

        resourceBuffer.setFuture(future);

        return task.getBuffer();

    }

    /**
     * Unsupported -- see {@link #newClientTask(INotifyOutcome, int)}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    protected T newClientTask(final int clientNum) {
        throw new UnsupportedOperationException();
    }

    /**
     * Factory for new client tasks.
     * 
     * @param notifyProxy
     *            The proxy for the object to which the client must deliver
     *            notice of success or failure for each processed resource.
     * @param locator
     *            The locator for the client on which the task will be executed.
     * 
     * @return The client task.
     */
    abstract protected T newClientTask(INotifyOutcome<V, L> notifyProxy,
            final L locator);
}
