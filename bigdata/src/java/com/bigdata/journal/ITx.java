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

import java.util.Date;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.IsolatedFusedView;

/**
 * <p>
 * Interface for transactional reading and writing of persistent data.
 * </p>
 * 
 * @see ITransactionManager
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITx {

    /**
     * The constant that SHOULD used as the timestamp for an <em>unisolated</em>
     * read-write operation. The value of this constant is ZERO (0L) -- this
     * value corresponds to <code>Wed Dec 31 19:00:00 EST 1969</code> when
     * interpreted as a {@link Date}.
     */
    public static final long UNISOLATED = 0L;
    
    /**
     * A constant that SHOULD used as the timestamp for <em>read-committed</em>
     * (non-transactional dirty reads) operations. The value of this constant is
     * MINUS ONE (-1L) -- this value corresponds to
     * <code>Wed Dec 31 18:59:59 EST 1969</code> when interpreted as a
     * {@link Date}.
     * <p>
     * If you want a scale-out index to be read consistent over multiple
     * operations, then use {@link IIndexStore#getLastCommitTime()} when you
     * specify the timestamp for the view. The index will be as of the specified
     * commit time and more recent commit points will not become visible.
     * <p>
     * {@link AbstractTask}s that run with read-committed isolation provide a
     * read-only view onto the most recently committed state of the indices on
     * which they read. However, when a process runs a series of
     * {@link AbstractTask}s with read-committed isolation the view of the
     * index in each distinct task will change if concurrenct processes commit
     * writes on the index. Further, an index itself can appear or disappear if
     * concurrent processes drop or register that index.
     * <p>
     * A read-committed transaction imposes fewer constraints on when old
     * resources (historical journals and index segments) may be released. For
     * this reason, a read-committed transaction is a good choice when a
     * very-long running read must be performed on the database. Since a
     * read-committed transaction does not allow writes, the commit and abort
     * protocols are identical.
     * 
     * @todo define another constant for "read consistent" semantics. it would
     *       read from the last globally committed state consistently for each
     *       operation. so, for example, an iterator scanning across multiple
     *       index partitions will be read-consistent but another operation on
     *       the same index could read from a different commit point.
     */
    public static final long READ_COMMITTED = -1L;
    
    /**
     * The start time for the transaction as assigned by a centralized
     * transaction manager service. Transaction start times are unique and also
     * serve as transaction identifiers. Note that this is NOT the time at which
     * a transaction begins executing on a specific journal as the same
     * transaction may start at different moments on different journals and
     * typically will only start on some journals rather than all.
     * 
     * @return The transaction start time.
     */
    public long getStartTimestamp();

    /**
     * Return the commit timestamp assigned to this transaction by a centralized
     * transaction manager service.
     * 
     * @return The commit timestamp assigned to this transaction.
     * 
     * @exception UnsupportedOperationException
     *                unless the transaction is writable.
     * 
     * @exception IllegalStateException
     *                if the transaction is writable but has not yet prepared (
     *                the commit time is assigned when the transaction is
     *                prepared).
     */
    public long getCommitTimestamp();
    
    /**
     * Prepare the transaction for a {@link #commit()} by validating the write
     * set for each index isolated by the transaction.
     * 
     * @param commitTime
     *            The commit time assigned by a centralized transaction manager
     *            service -or- ZERO (0L) IFF the transaction is read-only.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active. If the transaction is
     *                not complete, then it will be aborted.
     * 
     * @exception ValidationError
     *                If the transaction can not be validated. If this exception
     *                is thrown, then the transaction was aborted.
     */
    public void prepare(long commitTime);

    /**
     * Commit a transaction that has already been {@link #prepare(long)}d.
     * 
     * @return The commit time assigned to the transactions -or- 0L if the
     *         transaction was read-only.
     * 
     * @exception IllegalStateException
     *                If the transaction has not {@link #prepare(long) prepared}.
     *                If the transaction is not already complete, then it is
     *                aborted.
     * 
     * @return The commit timestamp assigned by a centralized transaction
     *         manager service or <code>0L</code> if the transaction was
     *         read-only.
     */
    public long commit();

    /**
     * Abort the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction is already complete.
     */
    public void abort();

    /**
     * The type-safe isolation level for this transaction.
     */
    public IsolationEnum getIsolationLevel();
    
    /**
     * When true, the transaction will reject writes.
     */
    public boolean isReadOnly();
    
    /**
     * When true, the transaction has an empty write set.
     */
    public boolean isEmptyWriteSet();
    
    /**
     * A transaction is "active" when it is created and remains active until it
     * prepares or aborts.  An active transaction accepts READ, WRITE, DELETE,
     * PREPARE and ABORT requests.
     * 
     * @return True iff the transaction is active.
     */
    public boolean isActive();

    /**
     * A transaction is "prepared" once it has been successfully validated and
     * has fulfilled its pre-commit contract for a multi-stage commit protocol.
     * An prepared transaction accepts COMMIT and ABORT requests.
     * 
     * @return True iff the transaction is prepared to commit.
     */
    public boolean isPrepared();

    /**
     * A transaction is "complete" once has either committed or aborted. A
     * completed transaction does not accept any requests.
     * 
     * @return True iff the transaction is completed.
     */
    public boolean isComplete();

    /**
     * A transaction is "committed" iff it has successfully committed. A
     * committed transaction does not accept any requests.
     * 
     * @return True iff the transaction is committed.
     */
    public boolean isCommitted();

    /**
     * A transaction is "aborted" iff it has successfully aborted. An aborted
     * transaction does not accept any requests.
     * 
     * @return True iff the transaction is aborted.
     */
    public boolean isAborted();

    /**
     * Return an isolated view onto a named index. The index will be isolated at
     * the same level as this transaction. Changes on the index will be made
     * restart-safe iff the transaction successfully commits. Writes on the
     * returned index will be isolated in an {@link IsolatedFusedView}. Reads that
     * miss on the {@link IsolatedFusedView} will read through named index as of the
     * ground state of this transaction. If the transaction is read-only then
     * the index will not permit writes.
     * <p>
     * During {@link #prepare(long)}, the write set of each
     * {@link IsolatedFusedView} will be validated against the then current commited
     * state of the named index.
     * <p>
     * During {@link #commit()}, the validated write sets will be merged down
     * onto the then current committed state of the named index.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> if no index is registered
     *         under that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     *                
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public IIndex getIndex(String name);
    
    /**
     * Return an array of the resource(s) (the named indices) on which the
     * transaction has written (the isolated index(s) that absorbed the writes
     * for the transaction).
     */
    public String[] getDirtyResource();
    
}
