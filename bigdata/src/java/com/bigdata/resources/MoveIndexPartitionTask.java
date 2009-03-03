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
 * Created on Feb 29, 2008
 */

package com.bigdata.resources;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.RawDataServiceTupleIterator;

/**
 * Task moves an index partition to another {@link IDataService}.
 * <p>
 * This task runs as a historical read operation and copy the view of the index
 * partition as of the lastCommitTime of old journal to another
 * {@link IDataService}. Once that historical view has been copied, this task
 * then submits an {@link AtomicUpdateMoveIndexPartitionTask}. The atomic
 * update is an {@link ITx#UNISOLATED} operation. It is responsible copying any
 * writes buffered for the index partition on the live journal to the target
 * {@link IDataService} and then updating the {@link MetadataIndex}. Once the
 * atomic update task is finished, clients will discover that the source index
 * partition does not exist. When they query the {@link MetadataService} they
 * will discover that the key(-range) is now handled by the new index partition
 * on the target {@link IDataService}.
 * <p>
 * Note: This task is run on the target {@link IDataService} and it copies the
 * data from the source {@link IDataService}. This allows us to use standard
 * {@link IRangeQuery} operations to copy the historical view. However, the
 * {@link AtomicUpdateMoveIndexPartitionTask} is run on the source
 * {@link IDataService} since it needs to obtain an exclusive lock on the index
 * partition that is being moved in order to prevent concurrent writes during
 * the atomic cutover. For the same reason, the
 * {@link AtomicUpdateMoveIndexPartitionTask} can not use standard
 * {@link IRangeQuery} operations. Instead, it initiates a series of data
 * transfers while holding onto the exclusive lock until the target
 * {@link IDataService} has the current state of the index partition. At that
 * point it notifies the {@link IMetadataService} to perform the atomic cutover
 * to the new index partition.
 * <p>
 * Note: This task does NOT cause any resources associated with the current view
 * of the index partition to be released on the source {@link IDataService}.
 * The reason is two-fold. First, the {@link IndexSegment}(s) associated with
 * that view MAY be in used by historical views. Second, there MAY be historical
 * commit points for the index partition on the live journal before the atomic
 * cutover to the new {@link IDataService} - those historical commit points MUST
 * be preserved until the release policy for those views has been satisified.
 * <p>
 * Note: The MOVE task MUST be explicitly coordinated with the target
 * {@link IDataService}. Failure to coordinate the move results in an error
 * message reported by the {@link MetadataService} indicating that the wrong
 * partition locator was found under the key. The cause is a MOVE operation
 * during which the target data service undergoes concurrent synchronous (and
 * then asynchronous) overflow. What happens is the
 * {@link MoveIndexPartitionTask} registers the new index partition on the
 * target data service. One registered on the {@link IDataService}, the index
 * partition it is visible during synchronous overflow BEFORE the MOVE is
 * complete and BEFORE the index is registered with the {@link MetadataService}
 * and hence discoverable to clients. If the target {@link IDataService} then
 * undergoes synchronous and asynchronous overflow and chooses an action which
 * would change the index partition definition (split, join, or move) WHILE the
 * index partition is still being moved onto the target {@link IDataService}
 * THEN the MOVE is not atomic and the definition of the index partition in the
 * {@link MetadataService} will not coherently reflect either the MOVE or the
 * action choosen by the target {@link IDataService}, depending on which one
 * makes its atomic update first.
 * <p>
 * The target {@link IDataService} MAY undergo both synchronous and asynchronous
 * overflow as {@link IDataService}s are designed to allow continued writes
 * during those operations. Further, it MAY choose to copy, build, or compact
 * the index partition while it is being moved. However, it MUST NOT choose any
 * action (split, join, or move) that would change the index partition
 * definition until the move is complete (whether it ends in success or
 * failure).
 * <p>
 * This issue is addressed by the following protocol:
 * <ol>
 * 
 * <li>The {@link MoveIndexPartitionTask} set the
 * <code>sourcePartitionId</code> on the {@link LocalPartitionMetadata} when
 * it registers the index partition on the target {@link IDataService}. When
 * <code>sourcePartitionId != -1</code>. the target {@link IDataService} is
 * restricted to for that index partition to overflows actions which do not
 * change the index partition definition (copy, build, or merge). Further, any
 * index partition found on restart whose by the target {@link IDataService}
 * whose <code>sourcePartitionId != -1</code> is deleted as it was never
 * successfully put into play (this prevents partial moves from accumulating
 * state which could not otherwise be released.)</li>
 * 
 * <li>The atomic update task causes the <code>sourcePartitionId</code> to be
 * set to <code>-1</code> as one of its last actions, thereby allowing the
 * target {@link IDataService} to use operations that could re-define the index
 * parition (split, join, move) and also preventing the target index partition
 * from being deleted on restart. </li>
 * 
 * </ol>
 * 
 * @todo In order to make MOVE atomic we need a more robust mechanism. This
 *       could be addressed w/ zookeeper. In particular, there must be a global
 *       lock in order to prevent clients from accessing the new index partition
 *       definition on the {@link MetadataService} before the atomic update task
 *       completes. Likewise, if the atomic update task fails and the
 *       {@link MetadataService} has already been updated, then we need to
 *       rollback the change to the {@link MetadataService} (replacing the new
 *       index partition locator with the old one). Note that the source index
 *       partition is not dropped until the atomic update task completes
 *       successfully.
 * 
 * @todo MOVE could also be a (full or partial) build followed by a copy of the
 *       index segment(s) in the view to the target data service and finally
 *       followed by the atomic update, which would copy over any tuples that
 *       have been buffered on the live journal. This could be much less
 *       expensive (depending on the state of the source index partition) and
 *       the data to be moved could be blasted across a socket. Try it out both
 *       ways.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MoveIndexPartitionTask extends AbstractPrepareTask<MoveResult> {
    
    private final ViewMetadata vmd;
    
    /**
     * {@link UUID} of the target {@link IDataService} (the one to which the index
     * partition will be moved).
     */
    private final UUID targetDataServiceUUID;

    /**
     * The partition identifier for the target index partition that will be
     * created by the move (RMI).
     */
    final int newPartitionId;

    /**
     * The name of the new index partition on the target data service.
     */
    final String targetIndexName; 
    
    /**
     * The summary used for the event description and the partition history
     * record.
     */
    private final String summary;
    
    /**
     * The event corresponding to this action.
     */
    private final Event e;
    
    /**
     * Note: The <i>newPartitionId</i> is passed into this task, rather than
     * being obtained during the execution of this task, in order to increase
     * the readability of the trace of the choosen tasks for the
     * {@link PostProcessOldJournalTask}.
     * 
     * @param targetDataServiceUUID
     *            The UUID for the target data service.
     */
    public MoveIndexPartitionTask(//
            final ViewMetadata vmd,//
            final UUID targetDataServiceUUID//
            ) {
        
        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);

        if (targetDataServiceUUID == null)
            throw new IllegalArgumentException();

        if (resourceManager.getDataServiceUUID().equals(targetDataServiceUUID)) {

            throw new IllegalArgumentException("Same data service: "
                    + targetDataServiceUUID);

        }

        this.vmd = vmd;

        this.targetDataServiceUUID = targetDataServiceUUID;

        this.newPartitionId = resourceManager.nextPartitionId(vmd.indexMetadata
                .getName());

        this.targetIndexName = DataService.getIndexPartitionName(
                vmd.indexMetadata.getName(), newPartitionId);

        this.summary = OverflowActionEnum.Move + "(" + vmd.name + "->"
                + targetIndexName + ")";

        this.e = new Event(resourceManager.getFederation(), new EventResource(
                vmd.indexMetadata), OverflowActionEnum.Move, summary + " : "
                + vmd);

    }

    @Override
    protected void clearRefs() {
        
        vmd.clearRef();
        
    }
    
    /**
     * Copies the historical writes to the target data service and then issues
     * the atomic update task to copy any buffered writes on the live journal
     * and update the {@link MetadataService}.
     */
    @Override
    protected MoveResult doTask() throws Exception {

        e.start();

        try {

            final IDataService targetDataService;
            final MoveResult moveResult;

            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();
                
                // view of the source index partition.
                final ILocalBTreeView src = (ILocalBTreeView) getIndex(getOnlyResource());
                
                moveResult = registerNewPartitionOnTargetDataService(
                        resourceManager, targetDataServiceUUID, vmd.name,
                        newPartitionId, e);
                
                /*
                 * FIXME this is reporting the time of the copy history task on
                 * the queue in addition to its execution time. The only way to
                 * report just its execution time is to report it from the
                 * target data service!
                 */ 
                final Event copyEvent = e.newSubEvent(
                        OverflowSubtaskEnum.CopyHistory, summary + " : " + vmd)
                        .start();

                try {

                    /*
                     * Name of the newly registered index partition.
                     */
                    final String targetIndexName = DataService
                            .getIndexPartitionName(src.getIndexMetadata()
                                    .getName(), moveResult.newPartitionId);
            
                    /*
                     * The data service on which we will register the new index
                     * partition.
                     */
                    targetDataService = resourceManager.getFederation()
                            .getDataService(targetDataServiceUUID);

                    /*
                     * Run procedure on the target data service that will copy
                     * data from the old index partition (on this data service)
                     * to the new index partition (on the target data service)
                     * as of the [lastCommitTime] of the old journal.
                     */
                    targetDataService.submit(ITx.UNISOLATED, targetIndexName,
                            new CopyIndexPartitionProcedure(resourceManager
                                    .getDataServiceUUID(), vmd.name,
                                    vmd.commitTime)).get();
                    
                } finally {

                    copyEvent.end();
                    
                }

            } finally {

                /*
                 * While we still need to copy the buffered writes on the live
                 * journal to the target index partition, at this point we no
                 * longer require the source index partition view (the view on
                 * the old journal) so we clear our references for that index.
                 */

                clearRefs();

            }

            moveBufferedWritesAndGoLive(resourceManager, moveResult, e);
            
            if (INFO)
                log.info("Successfully moved index partition: "+summary);

            return moveResult;

        } finally {

            e.end();

        }
        
    }

    /**
     * 
     * @param resourceManager
     * @param targetDataServiceUUID
     * @param sourceIndexName
     * @param indexMetadata
     * @param newPartitionId
     * 
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    static MoveResult registerNewPartitionOnTargetDataService(
            final ResourceManager resourceManager,
            final UUID targetDataServiceUUID,//
            final String sourceIndexName,
            final int newPartitionId,//
            final Event parentEvent
            ) throws IOException, InterruptedException, ExecutionException {

        if (resourceManager == null)
            throw new IllegalArgumentException();
        
        if (targetDataServiceUUID == null)
            throw new IllegalArgumentException();
        
        if (sourceIndexName == null)
            throw new IllegalArgumentException();
        
        if (parentEvent == null)
            throw new IllegalArgumentException();
        
        /*
         * @todo Because the registration of the new index partition is based on
         * an historical view of the old index partition it is possible that we
         * will miss an update to the index partition metadata. This really
         * should be modified to grap the current index metadata, which would
         * mean either running as an unisolated operation (which would mean in
         * turn that we could not copy historical tuples separately) or settling
         * for the read-committed copy of the index metadata from the live
         * journal for the sourceIndexName.
         * 
         * FIXME The only way to really close this hole is to NOT copy the
         * historical data to the target data service but to do everything in a
         * single unisolated operation which obtains the necessary exclusive
         * lock on the index partition to be moved. It is doubtless better to be
         * correct even at the expense of perhaps doing more work. Also, we can
         * use tail splits and other devices to choose moves that will achieve
         * more impact while doing less work.
         */
        final IndexMetadata indexMetadata;
        {
            
            final AbstractJournal journal = resourceManager.getLiveJournal();

            // just the mutable BTree (last committed state).
            indexMetadata = resourceManager.getIndexOnStore(sourceIndexName,
                    ITx.READ_COMMITTED, journal).getIndexMetadata();
            
        }
        
        /*
         * The name of the target index partition on the target data service.
         * This is formed using the name of the scale-out index and the
         * partition identifier that was assigned to the new index partition.
         */
        final String targetIndexName = DataService.getIndexPartitionName(
                indexMetadata.getName(), newPartitionId);

        final String summary = OverflowActionEnum.Move + "(" + sourceIndexName
                + "->" + targetIndexName + ")";

        /*
         * @todo this is reporting the time required to formulate the task and
         * wait for it to begin executing not just the time for it to execute on
         * the target data service.  since the time register an index is so short
         * this is not that important.
         */ 
        final Event e = parentEvent.newSubEvent(
                OverflowSubtaskEnum.RegisterIndex, targetIndexName).start();

        try {

            /*
             * The data service on which we will register the new index
             * partition.
             */
            final IDataService targetDataService = resourceManager
                    .getFederation().getDataService(targetDataServiceUUID);

            // clone metadata.
            final IndexMetadata newMetadata = indexMetadata.clone();

            // the partition metadata for the source index partition.
            final LocalPartitionMetadata oldpmd = newMetadata
                    .getPartitionMetadata();

            newMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    newPartitionId,//
                    // The source partition identifier.
                    oldpmd.getPartitionId(), oldpmd.getLeftSeparatorKey(),//
                    oldpmd.getRightSeparatorKey(),//
                    /*
                     * Note: This is [null] to indicate that the resource
                     * metadata needs to be filled in by the target data service
                     * when the new index partition is registered. It will be
                     * populated with the resource metadata description for the
                     * live journal on that data service.
                     */
                    null,
                    // history line.
                    oldpmd.getHistory() + summary + " "));

            if (INFO)
                log.info(summary);

            /*
             * Register new index partition on the target data service.
             * 
             * Note: The correct resource metadata for the new index partition
             * will be assigned when it is registered on the target data
             * service. See above and RegisterIndexTask.
             */
            targetDataService.registerIndex(targetIndexName, newMetadata);

            if (INFO)
                log
                        .info("Registered new index partition on target data service: targetIndexName="
                                + targetIndexName);

            /*
             * At this point the historical view as of the [lastCommitTime] has
             * been copied to the target data service.
             * 
             * The MoveResult contains the information that we need to run the
             * atomic move update task which will bring that view up to the
             * current state of the index partition and then atomically switch
             * over to the new index partition.
             */

            final LocalPartitionMetadata pmd = indexMetadata
                    .getPartitionMetadata();

            final PartitionLocator oldLocator = new PartitionLocator(//
                    pmd.getPartitionId(),//
                    resourceManager.getDataServiceUUID(),//
                    pmd.getLeftSeparatorKey(),//
                    pmd.getRightSeparatorKey()//
            );

            final PartitionLocator newLocator = new PartitionLocator(
                    newPartitionId,//
                    targetDataServiceUUID,//
                    pmd.getLeftSeparatorKey(),//
                    pmd.getRightSeparatorKey()//
            );

            return new MoveResult(sourceIndexName, indexMetadata,
                    targetDataServiceUUID, newPartitionId, oldLocator,
                    newLocator);

        } finally {

            e.end();

        }

    }

    /**
     * Copy any writes buffered by the live journal for the index partition that
     * is being moved to the new index partition on the target data service and
     * cutover to the new index partition. If the operation fails then this
     * cleans up before rethrowing the exception.
     * 
     * @param resourceManager
     * @param moveResult
     *            This contains all the metadata that we need to execute this
     *            task.
     * @param parentEvent
     *            Used to report the atomic update event.
     * 
     * @todo If this task (the caller) is interrupted while the atomic update
     *       task is running then the atomic update task will not notice the
     *       interrupt since it runs in a different thread. This is true for all
     *       of the atomic update tasks when we chain them from the task that
     *       prepares for the update. This is mainly an issue for responsiveness
     *       to the timeout since this task can not notice its interrupt (and
     *       therefore the post-processing will not terminate) until its atomic
     *       update task completes.
     */
    static void moveBufferedWritesAndGoLive(
            final ResourceManager resourceManager,//
            final MoveResult moveResult,//
            final Event parentEvent//
            ) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (moveResult == null)
            throw new IllegalArgumentException();

        if (parentEvent == null)
            throw new IllegalArgumentException();

        try {

            resourceManager.getConcurrencyManager().submit(
                    new AtomicUpdateMoveIndexPartitionTask(resourceManager,
                            moveResult.name, moveResult, parentEvent)).get();

        } catch (Throwable t) {

            if (moveResult.registeredInMDS.get()) {

                /*
                 * The move operation got as far as registering the target index
                 * partition in the metadata service before the move failed.
                 * This means that clients are now being directed to the target
                 * index partition and that the commit of the unisolated task
                 * failed implying that the source index partition was not
                 * deleted on the local data service.
                 * 
                 * Note: This is safe as all data has been replicated to the
                 * target index partition.
                 * 
                 * At this point we could drop the source index partition since
                 * the move is complete even though atomic update task failed.
                 * 
                 * Note: This is just cleaning up the source data service. The
                 * only consequence of failing to delete the source at this
                 * point is that the source index partition will hang around
                 * forever (or at least until a service restart).
                 * 
                 * @todo The task in which we are running does not have access
                 * to to the unisolated view of the source index partition (it
                 * is a read-historical task). Rather than attempting gain the
                 * exclusive lock I am letting the drop of the source index
                 * partition slide for now. The reason is that error handling
                 * here is not really all that robust since we are not using a
                 * distributed lock service and it will all have to be redone
                 * anyway. Trying to drop the source index partition here is
                 * just trying too hard in my opinion. By the time the data has
                 * been copied to the target data service and the target index
                 * partition has been registered with the metadata service about
                 * the only thing left that can go wrong is running out of disk
                 * or ram on the local data service and error correcting actions
                 * are likely to fail themselves under those extreme conditions
                 * (another cause could be the shutdown of the source data
                 * service or interrupting the atomic update task at just the
                 * wrong moment).
                 */

            } else {

                /*
                 * The move operation did not succeed in registering the target
                 * index partition on the metadata service. This means that
                 * clients are still being directed to the source index
                 * partition on this data service.
                 * 
                 * Note: This is safe as all data is still on the source index
                 * partition.
                 */

                try {

                    /*
                     * Drop the target index partition since the move failed and
                     * we will continue to use the source index partition.
                     * 
                     * Note: This is just cleaning up the target data service.
                     * The only consequence of failing to delete the target
                     * index partition after a failed move is that the data will
                     * hang around forever on the target data service (or at
                     * least until a service restart).
                     */

                    /*
                     * The data service on which we will register the new index
                     * partition.
                     */
                    final IDataService targetDataService = resourceManager
                            .getFederation().getDataService(
                                    moveResult.targetDataServiceUUID);

                    targetDataService.dropIndex(moveResult.targetIndexName);

                    log
                            .warn("Dropped target index partition after failed move: name="
                                    + moveResult.targetIndexName);

                } catch (Throwable t2) {

                    log.warn(
                            "Could not drop target index partition after failed move: name="
                                    + moveResult.targetIndexName + ", locator"
                                    + moveResult.newLocator, t2);

                }

            }

            // the move failed - rethrow the exception.
            throw new RuntimeException(t);

        }

    }
    
    /**
     * Procedure copies data from the old index partition to the new index
     * partition. This runs as an {@link ITx#UNISOLATED} operation on the target
     * {@link IDataService}. It is {@link ITx#UNISOLATED} because it must write
     * on the new index partition.
     * <p>
     * Note: This procedure only copies the view as of the lastCommitTime. It
     * does NOT copy deleted tuples since the historical views for the source
     * index partition will remain on the source data service. In this sense,
     * this operation is much like a compacting merge.
     * <p>
     * Note: This is an {@link IDataServiceAwareProcedure} so it can resolve the
     * {@link UUID} of the source {@link IDataService} and issue requests to
     * that source {@link IDataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyIndexPartitionProcedure implements
            IDataServiceAwareProcedure, IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 8024224972159862036L;

        private final UUID sourceDataServiceUUID;
        private final String sourceIndexName;
        private final long lastCommitTime;

        private transient DataService dataService;
        
        public void setDataService(final DataService dataService) {

            if (dataService == null)
                throw new IllegalArgumentException();

            if (this.dataService != null)
                throw new IllegalStateException();

            if(INFO)
                log.info("Set dataService: " + dataService);

            this.dataService = dataService;

        }

        /**
         * The {@link DataService} on which the procedure is executing.
         */
        final protected DataService getDataService() {

            if (dataService == null)
                throw new IllegalStateException();

            return dataService;
            
        }
        
        public final boolean isReadOnly() {
            
            return false;
            
        }
        
        /**
         * @param sourceDataServiceUUID
         * @param sourceIndexName
         * @param lastCommitTime
         * 
         */
        public CopyIndexPartitionProcedure(final UUID sourceDataServiceUUID,
                final String sourceIndexName, final long lastCommitTime) {

            if (sourceDataServiceUUID == null)
                throw new IllegalArgumentException();
            
            if (sourceIndexName == null)
                throw new IllegalArgumentException();
            
            this.sourceDataServiceUUID = sourceDataServiceUUID;
            
            this.sourceIndexName = sourceIndexName; 
            
            this.lastCommitTime = lastCommitTime;
            
         }
        
        /**
         * Copy the non-deleted index entries from the source index view.
         * <P>
         * Note: We copy only the non-deleted keys and values since this is the
         * initial state of this view on this data service.
         * <p>
         * Note: we take care to preserve the timestamps in case the index is
         * being used to support transactions.
         * 
         * @return The #of index entries copied.
         * 
         * FIXME Must apply overflowHandler - see AbstractBTree#rangeCopy.
         */
        public Object apply(final IIndex ndx) {

            // the live index on which we are writing.
            final BTree dst = (BTree)ndx;
            
            // @todo handle failover / read from secondaries.
            final IBigdataFederation fed = getDataService().getFederation();
            final IMetadataService metadataService = fed.getMetadataService();
            assert metadataService != null;
            final IDataService sourceDataService = fed.getDataService(sourceDataServiceUUID);
            assert sourceDataService != null;
            
            /*
             * An iterator reading from the (remote) source index partition.
             * 
             * Note: We obtain read-consistent semantics here because the
             * lastCommitTime of the source is fixed. (The source is a journal
             * that has been closed for writes.)
             */
            final ITupleIterator itr = new RawDataServiceTupleIterator(
                    sourceDataService, //
                    sourceIndexName, //
                    TimestampUtility.asHistoricalRead(lastCommitTime),// Note: historical read.
                    false, // readConsistent (does not matter since lastCommitTime is fixed),
                    null, // fromKey
                    null, // toKey
                    0,    // capacity
                    IRangeQuery.KEYS | IRangeQuery.VALS,// Note: deleted entries NOT copied!
                    null  // filter
                    );

            final boolean isIsolatable = ndx.getIndexMetadata().isIsolatable();

            long ncopied = 0L;

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final byte[] key = tuple.getKey();

                final byte[] val = tuple.getValue();

                if (isIsolatable) {

                    final long timestamp = tuple.getVersionTimestamp();

                    dst.insert(key, val, false/* delete */, timestamp,
                                    null/* tuple */);

                } else {

                    dst.insert(key, val);

                }

                ncopied++;

            }

            if(INFO)
                log.info("Copied " + ncopied + " index entries from "
                    + sourceIndexName);
            
            return ncopied;
            
        }

    }

    /**
     * Unisolated task is executed on the source data service once the
     * historical state of an index partitions as of the lastCommitTime of the
     * old journal has been copied to the target data service. The target index
     * partition identified by the {@link MoveResult} MUST already exist and
     * MUST have been populated with the state of the source index partition
     * view as of the lastCommitTime of the old journal. The task copies any
     * writes that have been buffered for the index partition on the source data
     * service to the target data service and then does an atomic update of the
     * {@link MetadataService} while it is holding an exclusive lock on the
     * source index partition.
     * <p>
     * Tasks executing after this one will discover that the source index
     * partition no longer exists as of the timestamp when this task commits.
     * Clients that submit tasks for the source index partition will be notified
     * that it no longer exists. When the client queries the
     * {@link MetadataService} it will discover that the key range has been
     * assigned to a new index partition - the one on the target data service.
     * <p>
     * Note: This task runs as an {@link ITx#UNISOLATED} operation since it MUST
     * have an exclusive lock in order to ensure that the buffered writes are
     * transferred to the target index partition without allowing concurrent
     * writes on the source index partition. This means that the target index
     * partition WILL NOT be able to issue read-requests against the live view
     * of source index partition since this task already holds the necessary
     * lock. Therefore this task instead sends zero or more {@link ResultSet}s
     * containing the buffered writes to the target index partition.
     * <p>
     * Note: The {@link ResultSet} data structure is used since it carries
     * "delete markers" and version timestamps as well as the keys and values.
     * The delete markers MUST be transferred in case the buffered writes
     * include deletes of index entries as of the lastCommitTime on the old
     * journal. If the index supports transactions then the version timestamps
     * MUST be preserved since they are the basis for validation of transaction
     * write sets.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class AtomicUpdateMoveIndexPartitionTask extends
            AbstractAtomicUpdateTask<Long> {

        final private MoveResult moveResult;

        final private Event updateEvent;
        
        /**
         * @param resourceManager
         * @param resource
         *            The source index partition.
         * @param moveResult
         *            The target index partition.
         */
        public AtomicUpdateMoveIndexPartitionTask(
                final ResourceManager resourceManager, //
                final String resource,//
                final MoveResult moveResult, //
                final Event parentEvent
                ) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (moveResult == null)
                throw new UnsupportedOperationException();
            
            if (parentEvent == null)
                throw new UnsupportedOperationException();
            
            this.moveResult = moveResult;

            this.updateEvent = parentEvent.newSubEvent(OverflowSubtaskEnum.AtomicUpdate,
                    moveResult.toString());

        }

        /**
         * @return The #of tuples copied.
         */
        @Override
        protected Long doTask() throws Exception {

            updateEvent.start();
            
            try {
            
            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final IDataService targetDataService = resourceManager
                    .getFederation().getDataService(moveResult.targetDataServiceUUID);
            
            final IIndex src = getIndex(getOnlyResource());

            final String scaleOutIndexName = src.getIndexMetadata().getName();
            
            final String targetIndexName = DataService.getIndexPartitionName(
                    scaleOutIndexName, moveResult.newPartitionId);

            if (INFO)
                log.info("Copying buffered writes: targeIndexName="
                        + targetIndexName + ", oldLocator="
                        + moveResult.oldLocator + ", newLocator="
                        + moveResult.newLocator);

            int nchunks = 0; // #of passes.
            long ncopied = 0; // #of tuples copied.
            byte[] fromKey = null; // updated after each pass.
            final byte[] toKey = null; // upper bound is fixed.
            final int flags = IRangeQuery.ALL;
            final int capacity = 10000; // chunk size.
            
            while (true) {
                
                /*
                 * Figure out the upper bound on the #of tuples that could be
                 * materialized.
                 * 
                 * Note: the upper bound on the #of key-value pairs in the range is
                 * truncated to an [int].
                 */
                
                final int rangeCount = (int) src.rangeCount(fromKey, toKey);

                final int limit = (rangeCount > capacity ? capacity : rangeCount);

                /*
                 * Iterator reading from the unisolated view of the source index
                 * partition.
                 */
                final ITupleIterator itr = src.rangeIterator(fromKey, toKey, limit,
                        flags, null/*filter*/);
                
                // Build a result set from the iterator.
                final ResultSet rset = new ResultSet(src, capacity, flags, itr);

                /*
                 * Copy data in result set onto the target index partition.
                 * 
                 * Note: This task does not handle re-directs. The target index
                 * partition MUST be on the target data service.
                 */
                targetDataService.submit(ITx.UNISOLATED, targetIndexName,
                        new CopyBufferedWritesProcedure(rset)).get();

                // #of tuples copied.
                ncopied += rset.getNumTuples();

                // #of passes
                nchunks++;
                
                if (rset.isExhausted()) {

                    if(INFO)
                        log.info("Copied "+ncopied+" tuples in "+nchunks+" chunks");
                    
                    break;

                }

                /*
                 * Update to the next key to be copied.
                 * 
                 * Note: There may be a fence post here if the index uses a
                 * fixed length successor semantics for the key. Therefore it is
                 * safer to use [fromKey = rset.getLastKey()]. This has the
                 * effect that the last tuple in each result set is copied again
                 * as the first tuple in the next result set, but that is of
                 * little consequence.
                 */
//                fromKey = BytesUtil.successor(rset.getLastKey());
                fromKey = rset.getLastKey();
                
            }

            /*
             * Drop the old index partition. This action will be rolled back
             * automatically if this task fails so the source index partition
             * will remain available if the move fails.
             */
            getJournal().dropIndex(getOnlyResource());
            
            if (INFO)
                log.info("Updating metadata index: name=" + scaleOutIndexName
                        + ", oldLocator=" + moveResult.oldLocator
                        + ", newLocator=" + moveResult.newLocator);

            // atomic update on the metadata server.
            resourceManager.getFederation().getMetadataService()
                    .moveIndexPartition(scaleOutIndexName,
                            moveResult.oldLocator, moveResult.newLocator);

            // set flag indicating that clients will now see the new index partition.
            moveResult.registeredInMDS.set(true);
            
            // will notify tasks that index partition has moved.
            resourceManager.setIndexPartitionGone(getOnlyResource(),
                    StaleLocatorReason.Move);
            
            // notify successful index partition move.
            resourceManager.indexPartitionMoveCounter.incrementAndGet();

            /*
             * Note: The new index partition will not be usable until (and
             * unless) this task commits.
             */
            
            return ncopied;
            
        } finally {
            
            updateEvent.end();
            
        }
        
        }
        
    }

    /**
     * Task copies data described in a {@link ResultSet} onto the target index
     * partition. This task is invoked by the
     * {@link AtomicUpdateMoveIndexPartitionTask} while the latter holds an
     * exclusive lock on the source index partition.  The task itself runs
     * on the target {@link IDataService}.
     * <p>
     * Note: This task does not handle re-directs. The target index partition
     * MUST be on the target data service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyBufferedWritesProcedure implements IIndexProcedure,
            IDataServiceAwareProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = -7715086561289117891L;

        private final ResultSet rset;
        
        public CopyBufferedWritesProcedure(final ResultSet rset) {
        
            if (rset == null)
                throw new IllegalArgumentException();
            
            this.rset = rset;
            
        }
        
        public final boolean isReadOnly() {
            
            return false;
            
        }
        
        private DataService dataService;
        
        public void setDataService(DataService dataService) {

            this.dataService = dataService;
            
        }

        protected DataService getDataService() {
            
            if (dataService == null)
                throw new IllegalStateException();
            
            return dataService;
            
        }
        
        public Object apply(final IIndex ndx) {

            /*
             * Note: used to increment the #of index partition moves received by
             * the target data service.
             */
            final ResourceManager resourceManager = getDataService()
                    .getResourceManager();

            assert rset != null;
         
            /*
             * Get hold of the BTree that is currently absorbing writes for the
             * target index partition.
             * 
             * Note: Since the target index partition was recently create the
             * _odds_ are that it is just a BTree on the live journal for the
             * target data service. However, if the target data service has
             * undergone a concurrent overflow then the target index partition
             * COULD be a FusedView.
             * 
             * In either case, we want the mutable BTree that is absorbing
             * writes and we copy the data from the source index partition view
             * to the target index partition view.
             */
            final BTree dst = (BTree) ((ndx instanceof AbstractBTree) ? ndx
                    : ((FusedView) ndx).getSources()[0]);

            /*
             * The name of the index partition that we are receiving. 
             */
            final String name = dst.getIndexMetadata().getName();
            
            final int n = rset.getNumTuples();
            
            final boolean deleteMarkers = ndx.getIndexMetadata().getDeleteMarkers();
            
            final boolean versionTimestamps = ndx.getIndexMetadata().getVersionTimestamps();
      
            if (ndx.getIndexMetadata().getOverflowHandler() != null) {

                /*
                 * FIXME Must apply overflowHandler - see AbstractBTree#rangeCopy.
                 * 
                 * Probably the easiest way to handle this is to do an
                 * UNISOLATED build on the view, or to combine an build on the
                 * old view (from the lastCommitTime) which runs before the
                 * update task with an UNISOLATED build on the BTree buffering
                 * writes. The send the resulting index segment(s) to the target
                 * data service while holding an exclusive write lock. Once the
                 * target puts those files into place as a view we do the atomic
                 * update thing here. The "send two index segments" version will
                 * have the lowest latency since we can build and send one while
                 * we continue to absorb writes and then send another with just
                 * the buffered writes.
                 */
                throw new UnsupportedOperationException("Must apply overflowHandler");

            }
            
            for (int i = 0; i < n; i++) {
                
                final byte[] key = rset.getKey(i);
                
                if (versionTimestamps) {

                    final long timestamp = rset.getVersionTimestamps()[i];

                    if (deleteMarkers && rset.getDeleteMarkers()[i]==0?false:true) {

                        dst.insert(key, null/* value */, true/* delete */,
                                timestamp, null/* tuple */);

                    } else {

                        dst.insert(key, rset.getValue(i),
                                        false/* delete */, timestamp, null/* tuple */);

                    }

                } else {

                    if (deleteMarkers && rset.getDeleteMarkers()[i] == 0 ? false
                            : true) {

                        dst.remove(key);

                    } else {

                        dst.insert(key, rset.getValue(i));

                    }

                }
                
            }

            if(rset.isExhausted()) {
                
                /*
                 * The result set is exhausted, which means that we are done
                 * copying tuples from the source index partition. At this point
                 * we set [sourcePartitionId = -1] on the target index partition
                 * as an indication that the MOVE operation is complete.
                 * 
                 * @todo this needs to be done atomically while holding a global
                 * lock. See various other notes in this source file about
                 * making MOVE robust.
                 */
                
                final LocalPartitionMetadata oldpmd = dst.getIndexMetadata()
                        .getPartitionMetadata();
                
                if (oldpmd.getSourcePartitionId() == -1) {

                    throw new IllegalStateException(
                            "Expecting the sourcePartitionId to be set.");

                }
                
                // clone the current metadata record for the live index.
                final IndexMetadata indexMetadata = dst.getIndexMetadata().clone();

                // clear the sourcePartitionId.
                final LocalPartitionMetadata newpmd = new LocalPartitionMetadata(
                        oldpmd.getPartitionId(),//
                        -1, // Note: MOVE is complete.
                        oldpmd.getLeftSeparatorKey(),//
                        oldpmd.getRightSeparatorKey(),//
                        oldpmd.getResources(),//
                        oldpmd.getHistory()
                        );

                // set the new partition metadata on the index metadata.
                indexMetadata.setPartitionMetadata(newpmd);
                
                // set the new index metadata on the live btree.
                dst.setIndexMetadata(indexMetadata);
         
                // increment #of index partition moves received by this service (the target).
                resourceManager.indexPartitionReceiveCounter.incrementAndGet();
                
                if (INFO)
                    log.info("Move finished: cleared sourcePartitionId: name="
                            + name);
                
                /*
                 * Note in the log that we have received the index partition.
                 * Clients will not be directed to this index partition until
                 * the metadata index has been updated.
                 */
                log.warn("Received: name=" + name);
                
                // make sure that the BTree will be included in the commit.
                assert dst.needsCheckpoint() : "name=" + name;
                
            }
            
            return null;
            
        }

    }

}
