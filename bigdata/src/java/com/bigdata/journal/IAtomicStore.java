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
/*
 * Created on Feb 3, 2007
 */

package com.bigdata.journal;

import com.bigdata.rawstore.IRawStore;

/**
 * Interface for low-level operations on a store supporting an atomic commit.
 * Persistent implementations of this interface are restart-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAtomicStore extends IRawStore {

    /**
     * Abandon the current write set (synchronous).
     */
    public void abort();
    
    /**
     * Atomic commit (synchronous).
     * <p>
     * Note: if the commit fails (by throwing any kind of exception) then you
     * MUST invoke {@link #abort()} to abandon the current write set.
     * 
     * @return The timestamp assigned to the {@link ICommitRecord} -or- 0L if
     *         there were no data to commit.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the store is not writable.
     */
    public long commit();

    /**
     * Invoked when a journal is first created, re-opened, or when the
     * committers have been {@link #discardCommitters() discarded}.
     */
    public void setupCommitters();

    /**
     * This method is invoked whenever the store must discard any hard
     * references that it may be holding to objects registered as
     * {@link ICommitter}s.
     */
    public void discardCommitters();
    
    /**
     * Set a persistence capable data structure for callback during the commit
     * protocol.
     * <p>
     * Note: the committers must be reset after restart or whenever
     * 
     * @param index
     *            The slot in the root block where the address of the
     *            {@link ICommitter} will be recorded.
     * 
     * @param committer
     *            The commiter.
     */
    public void setCommitter(int index, ICommitter committer);

    /**
     * The last address stored in the specified root slot as of the last
     * committed state of the store.
     * 
     * @param index
     *            The index of the root address to be retrieved.
     * 
     * @return The address stored at that index.
     * 
     * @exception IndexOutOfBoundsException
     *                if the index is negative or too large.
     */
    public long getRootAddr(int index);

    /**
     * Return a read-only view of the current root block.
     * 
     * @return The current root block.
     */
    public IRootBlockView getRootBlockView();

    /**
     * Return the {@link ICommitRecord} for the most recent committed state
     * whose commit timestamp is less than or equal to <i>timestamp</i>. This
     * is used by a {@link Tx transaction} to locate the committed state that is
     * the basis for its operations.
     * 
     * @param timestamp
     *            The timestamp of interest.
     * 
     * @return The {@link ICommitRecord} for the most recent committed state
     *         whose commit timestamp is less than or equal to <i>timestamp</i>
     *         -or- <code>null</code> iff there are no {@link ICommitRecord}s
     *         that satisify the probe.
     */
    public ICommitRecord getCommitRecord(long timestamp);

}
