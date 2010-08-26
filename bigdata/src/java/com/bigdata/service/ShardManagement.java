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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;

/**
 * Methods implemented by both the ShardService and the ShardLocator service
 * that are used to manage the data maintained by those services.
 */
public interface ShardManagement {

    /**
     * Returns the metadata for the named index.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            A <code>long</code> value representing the identifier of
     *            the transaction. The value of this parameter may be 
     *            {@link ITx#UNISOLATED} for the unisolated index view, or
     *            {@link ITx#READ_COMMITTED}, or a valid time value for
     *            a historical view no later than the specified time value.
     *            
     * @return The metadata for the named index.
     * 
     * @throws IOException
     */
    IndexMetadata getIndexMetadata(String name, long timestamp)
            throws IOException, InterruptedException, ExecutionException;

    /**
     * Provides streaming traversal of keys and/or values in a key range.
     * <p>
     * Note: In order to visit all keys in a range, clients are expected
     *       to issue repeated calls in which the <i>fromKey</i> is
     *       incremented to the successor of the last key visited until
     *       either an empty {@link ResultSet} is returned or the
     *       {@link ResultSet#isLast()} flag is set, indicating that all
     *       keys up to (but not including) the <i>startKey</i> have been
     *       visited.
     * <p>
     * Note: If the iterator can be determined to be read-only and it is
     *       submitted as {@link ITx#UNISOLATED} then it will be run as
     *       {@link ITx#READ_COMMITTED} to improve concurrency.
     * 
     * @param tx
     *            A <code>long</code> value representing the identifier of
     *            the transaction. The value of this parameter may be 
     *            {@link ITx#UNISOLATED} IFF the operation is NOT isolated
     *            by a transaction -or- it may be a negative value; which
     *            specifies that data should be read from the most recent
     *            commit point not later than the absolute value of the
     *            value input for the <i>tx</i> parameter (a fully isolated
     *            read-only transaction using a historical start time)
     * @param name
     *            The index name (required).
     * @param fromKey
     *            The starting key for the scan (or <code>null</code> iff
     *            there is no lower bound).
     * @param toKey
     *            The first key that will not be visited (or <code>null</code>
     *            iff there is no upper bound).
     * @param capacity
     *            When non-zero, this is the maximum #of entries to process.
     * @param flags
     *            One or more flags formed by bitwise OR of zero or more of the
     *            constants defined by {@link IRangeQuery}.
     * @param filter
     *            An optional object that may be used to layer additional
     *            semantics onto the iterator. The filter will be
     *            constructed on the server and in the execution context
     *            for the iterator, so it will execute directly against the
     *            index for the maximum efficiency.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     */
    ResultSet rangeIterator(long tx,
                            String name,
                            byte[] fromKey,
                            byte[] toKey,
                            int capacity,
                            int flags,
                            IFilterConstructor filter)
                  throws InterruptedException, ExecutionException, IOException;

    /**
     * Submit a {@link Callable} and return its {@link Future}. The
     * {@link Callable} will execute on the
     * {@link IBigdataFederation#getExecutorService()}.
     * 
     * @return The {@link Future} for that task.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be accepted for execution.
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @todo change API to <T> Future<T> submit(Callable<T> proc). This will
     *       break existing code but reflects the correct use of generics.
     */
    Future<? extends Object> submit(Callable<? extends Object> proc)
                                 throws IOException;

    /**
     * Submits a procedure.
     * <p>
     * Unisolated operations SHOULD be used to achieve "auto-commit" semantics.
     * Fully isolated transactions are useful IFF multiple operations must be
     * composed into a ACID unit.
     * 
     * @param tx
     *            A <code>long</code> value representing the identifier of
     *            the transaction. The value of this parameter may be 
     *            {@link ITx#UNISOLATED} for an ACID operation NOT isolated
     *            by a transaction, or {@link ITx#READ_COMMITTED} for a
     *            read-committed operation not protected by a transaction
     *            (that is, no global read lock), or any valid commit time
     *            for a read-historical operation not protected by a
     *            transaction (again, no global read lock).
     * @param name
     *            The name of the index partition.
     * @param proc
     *            The procedure to be executed.
     * 
     * @return The {@link Future} from which the outcome of the executed
     *         procedure may be obtained.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be accepted for execution.
     * @throws IOException
     *             if there is an RMI problem.
     */
    Future submit(long tx, String name, IIndexProcedure proc)
               throws IOException;

    /**
     * Attempt to pause the service accepting {@link ITx#UNISOLATED}
     * writes and then purge any resources that are no longer required,
     * based on the {@link StoreManager.Options#MIN_RELEASE_AGE}.
     * <p>
     * Note: Resources are normally purged during synchronous overflow
     * handling. However, asynchronous overflow handling can cause resources
     * to no longer be needed as new index partition views are defined.
     * This method MAY be used to trigger a release before the next
     * overflow event.
     * 
     * @param timeout
     *            The timeout (in milliseconds) that the method will await the
     *            pause of the write service.
     * @param truncateJournal
     *            When <code>true</code>, the live journal will be truncated
     *            to its minimum extent (all writes will be preserved but there
     *            will be no free space left in the journal). This may be used
     *            to force the service to its minimum possible footprint for
     *            the configured history retention policy.
     * 
     * @return <code>true</code> if successful and <code>false</code> if the
     *         write service could not be paused after the specified timeout.
     * 
     * @param truncateJournal
     *            When <code>true</code> the live journal will be truncated
     *            such that no free space remains in the journal.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    boolean purgeOldResources(long timeout, boolean truncateJournal)
                throws IOException, InterruptedException;
}
