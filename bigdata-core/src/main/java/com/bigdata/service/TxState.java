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

package com.bigdata.service;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.journal.RunState;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.util.config.LogUtil;

/**
 * Class that encapsulates the transaction state as maintained by the
 * transaction service.
 * <p>
 * Note: The commitTime and revisionTime are requested by the local
 * transaction manager for single phase commits, which means that this class
 * could only know their values for a distributed transaction commit. Hence
 * they are not represented here.
 */
public class TxState {

    private static Logger logger =
        LogUtil.getLog4jLogger((TxState.class).getName());

    public static final transient String[] EMPTY = new String[0];

    /**
     * If the transaction is read-only and a write operation was requested.
     */
    public static final transient String ERR_READ_ONLY = "Read-only";

    /**
     * If the transaction is not known to this service.
     */
    public static final transient String ERR_NO_SUCH = "Unknown transaction";

    /**
     * If a transaction is no longer active.
     */
    public static final transient String ERR_NOT_ACTIVE = "Not active";

    /**
     * If the transaction service is not in a run state which permits the
     * requested operation.
     */
    public static final transient String ERR_SERVICE_NOT_AVAIL =
                                             "Service not available";

    /**
     * The transaction identifier.
     */
    public final long tx;

    /**
     * <code>true</code> iff the transaction is read-only.
     */
    public final boolean readOnly;

    /**
     * A per-transaction lock used to serialize operations on a given
     * transaction. You need to hold this lock for most of the operations on
     * this class, including any access to the {@link RunState}.
     * <p>
     * Note: DO NOT attempt to acquire the outer lock (for example,
     *       in AbstractTransactionService or EmbeddedTransactionService)
     *       if you are already holding this {@link #lock}. This is a
     *       lock ordering problem and can result in a deadlock.
     */
    final public ReentrantLock lock = new ReentrantLock();
        
    /**
     * The run state of the transaction (only accessible while you are
     * holding the {@link #lock}.
     */
    private RunState runState = RunState.Active;

    /**
     * The commit time assigned to a distributed read-write transaction
     * during the commit protocol and otherwise ZERO (0L).
     */
    private long commitTime = 0L;

    /**
     * The set of shard services on which a read-write transaction
     * has been started and <code>null</code> if this is not a read-write
     * transaction.
     * <p>
     * Note: We only track this information for a distributed database.
     */
    private final Set<UUID/* dataService */> dataServices;

    /**
     * The set of named resources that the transaction has declared
     * across all shard services on which it has written and
     * <code>null</code> if this is not a read-write transaction.
     * <p>
     * Note: We only track this information for a distributed database.
     */
    private final Set<String/* name */> resources;

    private final int hashCode;

    public TxState(final long tx) {
        if (tx == ITx.UNISOLATED)
            throw new IllegalArgumentException();
        if (tx == ITx.READ_COMMITTED)
            throw new IllegalArgumentException();

        this.tx = tx;
        this.readOnly = TimestampUtility.isReadOnly(tx);

        // pre-compute the hash code for the transaction.
        this.hashCode = Long.valueOf(tx).hashCode();

        this.dataServices = readOnly ? null : new LinkedHashSet<UUID>();
        this.resources = readOnly ? null : new LinkedHashSet<String>();
    }

    /**
     * Change the {@link RunState}.
     * 
     * @param newval
     *            The new {@link RunState}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the state transition is not allowed.
     * 
     * @see RunState#isTransitionAllowed(RunState)
     */
    public void setRunState(final RunState newval) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (newval == null)
            throw new IllegalArgumentException();
            
        if (!runState.isTransitionAllowed(newval)) {

        throw new IllegalStateException("runState=" + runState
                + ", newValue=" + newval);
        }
        this.runState = newval;
    }

    /**
     * The commit time assigned to a distributed read-write transaction
     * during the commit protocol.
     * 
     * @return The assigned commit time.
     * 
     * @throws IllegalStateException
     *             if the commit time has not been assigned.
     */
    public long getCommitTime() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalMonitorStateException();
        }
        if (commitTime == 0L) {
            throw new IllegalStateException();
        }
        return commitTime;
    }

    /**
     * Sets the assigned commit time.
     * 
     * @param commitTime
     *            The assigned commit time.
     */
    public void setCommitTime(final long commitTime) {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalMonitorStateException();
        }
        if (commitTime == 0L) {
            throw new IllegalArgumentException();
        }
        if (this.commitTime != 0L) {
            throw new IllegalStateException();
        }
        this.commitTime = commitTime;
    }

    /**
     * Return the resources declared by the transaction.
     */
    public String[] getResources() {
        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (resources == null)
            return EMPTY;
        return resources.toArray(new String[] {});
    }

    /**
     * Return <code>true</code> iff the dataService identified by the
     * {@link UUID} is one on which this transaction has been started.
     * 
     * @param dataServiceUUID
     *            The {@link UUID} identifying a shard service.
     * 
     * @return <code>true</code> if this transaction has been started
     *         on that shard service. <code>false</code> for read-only
     * transactions.
     */
    public boolean isStartedOn(final UUID dataServiceUUID) {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (dataServiceUUID == null)
            throw new IllegalArgumentException();
        if (dataServices == null)
            return false;
        return dataServices.contains(dataServiceUUID);
    }
 
    /**
     * Declares resources on a shard service instance on which the
     * transaction will write.
     * 
     * @param dataService
     *            The shard service identifier.
     * @param resource
     *            An array of named resources on the shard service on which
     *            the transaction will write (or at least for which it
     *            requires an exclusive write lock).
     * 
     * @throws IllegalStateException
     *             if the transaction is read-only.
     * @throws IllegalStateException
     *             if the transaction is not active.
     */
    final public void declareResources(final UUID dataService,
                                       final String[] resource)
    {
        if (dataService == null)
            throw new IllegalArgumentException();
            
        if (resource == null)
            throw new IllegalArgumentException();
            
        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (readOnly)
            throw new IllegalStateException(ERR_READ_ONLY);

        if (!isActive())
            throw new IllegalStateException(ERR_NOT_ACTIVE);

        dataServices.add(dataService);
            
        // Note: sufficient to prevent deadlocks when there are shared indices.
        resources.addAll(Arrays.asList(resource));
            
        if (logger.isInfoEnabled()) {
            logger.info("shardService=" + dataService + ", resource="
                        + Arrays.toString(resource));
        }
    }

    /**
     * Return the #of shard services on which a read-write
     * transaction has executed an operation.
     * 
     * @return The #of shard services.
     * 
     * @throws IllegalStateException
     *             if the transaction is read-only.
     * @throws IllegalMonitorStateException
     *             if the caller does not hold the lock.
     */
    final public int getDataServiceCount() {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if(readOnly)
            throw new IllegalStateException(ERR_READ_ONLY);

        return dataServices.size();
    }

    /**
     * The set of shard services on which the transaction has
     * written.
     * 
     * @throws IllegalStateException
     *             if not a read-write transaction.
     */
    public UUID[] getDataServiceUUIDs() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        if (dataServices == null)
            throw new IllegalStateException();
        return dataServices.toArray(new UUID[] {});
    }

    /**
     * Return <code>true</code> iff a read-write transaction has
     * started on more than one shard service.
     */
    final public boolean isDistributedTx() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return !readOnly && dataServices.size() > 1;
    }

    final public boolean isReadOnly() {
        return readOnly;
    }

    final public boolean isActive() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Active;
    }

    final public boolean isPrepared() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Prepared;
    }

    final public boolean isComplete() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Committed
                    || runState == RunState.Aborted;
    }

    final public boolean isCommitted() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Committed;
    }

    final public boolean isAborted() {
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Aborted;
    }

    /**
     * The hash code is based on the {@link #getStartTimestamp()}.
     */
    final public int hashCode() {
        return hashCode;
    }

    /**
     * True iff they are the same object or have the same start timestamp.
     * 
     * @param o
     *            Another transaction object.
     */
    final public boolean equals(ITx o) {
        return this == o || tx == o.getStartTimestamp();
    }

    /**
     * Returns a string representation of the transaction state.
     */
    final public String toString() {

        /*
         * Note: info reported here MUST be safe and MUST NOT require a
         * lock!
         */            
        return "GlobalTxState[tx="+tx+",readOnly="+readOnly
               +",runState="+runState + "]";
    }
}

