/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

package com.bigdata.journal;

import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager.ManagedJournal;

import java.util.UUID;

/**
 * Task class that can be used to handle the distributed protocol
 * by an implementation of the shard (data) service.
 */
public class DistributedCommitTask extends AbstractTask<Void> {

    // ctor arg.
    private final ResourceManager resourceManager;
    private UUID dataServiceUUID;
    private final Tx state;
    private final long revisionTime;
        
    // derived.
    private final long tx;
        
    /**
     * @param concurrencyManager
     * @param resourceManager
     * @param dataServiceUUID
     * @param localState
     * @param revisionTime
     */
    public DistributedCommitTask
               (final ConcurrencyManager concurrencyManager,
                final ResourceManager resourceManager,
                final UUID dataServiceUUID,
                final Tx localState,
                final long revisionTime)
     {
        super(concurrencyManager,
              ITx.UNISOLATED,
              localState.getDirtyResource());

        if (resourceManager == null) {
            throw new IllegalArgumentException
                          ("null resourceManager");
        }

        if (localState == null) {
            throw new IllegalArgumentException
                          ("null localState");
        }

        if (revisionTime == 0L) {
            throw new IllegalArgumentException
                          ("revisionTime == 0");
        }

        long startTime = localState.getStartTimestamp();
        if (revisionTime <= startTime) {
            throw new IllegalArgumentException
                          ("revisionTime <= startTime ["
                           +revisionTime+" <= "+startTime+"]");
        }

        this.resourceManager = resourceManager;
        this.dataServiceUUID = dataServiceUUID;
        this.state = localState;
        this.revisionTime = revisionTime;
        this.tx = localState.getStartTimestamp();
    }

    /**
     * FIXME Finish, write tests and debug.
     */
    @Override
    protected Void doTask() throws Exception {

//BTM            final ITransactionService txService = resourceManager.getLiveJournal().getLocalTransactionManager().getTransactionService();
final TransactionService txService = resourceManager.getLiveJournal().getLocalTransactionManager().getTransactionService();

        prepare();

        final long commitTime = txService.prepared(tx, dataServiceUUID);

        // obtain the exclusive write lock on journal.
        lockJournal();
        try {
            // Commit using the specified commit time.
            commit(commitTime);

            boolean success = false;
            try {
                /*
                 * Wait until the entire distributed transaction is
                 * committed.
                 */
                success = txService.committed(tx, dataServiceUUID);

            } finally {

                if (!success) {
                    // Rollback the journal.
                    rollback();
                }
            }
        } finally {
            // release the exclusive write lock on journal.
            unlockJournal();
        }
        return null;
    }

    /**
     * Prepare the transaction (validate and merge down onto the unisolated
     * indices and then checkpoints those indices).
     * <p>
     * Note: This presumes that we are already holding exclusive write locks
     * on the named indices such that the pre-conditions for validation and
     * its post-conditions can not change until we either commit or discard
     * the transaction.
     * <p>
     * Note: The indices need to be isolated as by {@link AbstractTask} or
     * they will be enrolled onto the commitList of 
     * <code>com.bigdata.journal.Name2Addr</code> when they become dirty and
     * then checkpointed and included with the NEXT commit.
     * <p>
     * For this reason, the {@link DistributedCommitTask} is an UNISOLATED
     * task so that we can reuse the existing mechanisms as much as
     * possible.
     * 
     * FIXME This will work if we can grab the write service lock from
     * within the task (which will mean changing that code to allow the lock
     * with the caller only still running or simply waiting until we are
     * signaled by the txService that all participants are either go
     * (continue execution and will commit at the next group commit, but
     * then we need a protocol to impose the correct commit time, e.g., by
     * passing it on the task and ensuring that there is no other tx ready
     * in the commit group) or abort (just throw an exception).
     */
    protected void prepare() {
        state.prepare(revisionTime);
    }

    /**
     * Obtain the exclusive lock on the write service. This will prevent any
     * other tasks using the concurrency API from writing on the journal.
     */
    protected void lockJournal() {
        throw new UnsupportedOperationException();
    }
        
    protected void unlockJournal() {
        throw new UnsupportedOperationException();
    }
        
    /**
     * Commit the transaction using the specified <i>commitTime</i>.
     * <p>
     * Note: There are no persistent side-effects unless this method returns
     * successfully.
     * 
     * @param commitTime
     *            The commit time that must be used.
     */
    protected void commit(final long commitTime) {
        /*
         * @todo enroll the named indices onto the commitList of
         * <code>com.bigdata.journal.Name2Addr</code> (this
         * basically requires breaking the isolation imposed by the
         * AbstractTask).
         */
        if (true)
            throw new UnsupportedOperationException();

        final ManagedJournal journal = resourceManager.getLiveJournal();
            
        // atomic commit.
        journal.commitNow(commitTime);
    }
        
    /**
     * Discard the last commit, restoring the journal to the previous commit
     * point.
     */
    protected void rollback() {
        final ManagedJournal journal = resourceManager.getLiveJournal();
        journal.rollback();
    }
}
