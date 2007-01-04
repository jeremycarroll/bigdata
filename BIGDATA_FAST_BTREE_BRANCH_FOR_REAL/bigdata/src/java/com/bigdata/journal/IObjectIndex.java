/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;


/**
 * <p>
 * Interface for operations on the index mapping int32 within segment persistent
 * identifiers onto slots in the journal. There are always logical levels to the
 * object index. The first is the object index for the journal outside of any
 * transactional scope. There is actually one such index per committed state on
 * the journal. The second is the per-transaction object index.
 * </p>
 * <p>
 * BEGIN obtains a read-only view onto the then-current (BEGIN time) object
 * index for the committed state that will be read by that transaction. Changes
 * to the object index are then are made within the per-transaction object index
 * as new data versions are written or deleted. During PREPARE, the object index
 * is merged with the then-current (PREPARE time, e.g., last committed) object
 * index. A COMMIT makes the merged object index accessible to new transactions.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IObjectIndex {

    /**
     * Update the object index to map the id onto the slots. When there is a
     * current version and it was written during the current transaction, then
     * its slots MUST be synchronously deallocated since they are no longer
     * accessible to any active transaction.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param slots
     *            The slots on which the current version of the identified data
     *            was written within this transaction scope.
     */
    public void put( int id, ISlotAllocation slots );
    
    /**
     * Return the slots on which the current version of the data is stored.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * 
     * @return The slots on which the data version is stored or
     *         <code>null</code> if the identifier is not mapped.
     * 
     * @exception DataDeletedException
     *                This exception is thrown if the object is logically
     *                deleted on the journal within the scope visible to the
     *                transaction. The caller MUST NOT resolve the persistent
     *                identifier against the database since the current version
     *                is deleted.
     */
    public ISlotAllocation get( int id );

//    /**
//     * Removes and returns the first slot on which a version of the deleted data
//     * version was last written.
//     * 
//     * @param id
//     *            The int32 within-segment persistent identifier.
//     * 
//     * @return The first slot.
//     * 
//     * @exception IllegalArgumentException
//     *                if the persistent identifier is bad.
//     * @exception IllegalStateException
//     *                if <i>id</id> does not identify deleted data.
//     */
//    public int removeDeleted( int id );
    
    /**
     * Mark the entry in the object index as <em>deleted</em>. If the current
     * version was written during this transaction, then this method
     * synchronously deallocates the slots allocated to the current version. In
     * either case, subsequent reads on the object index will respond with a
     * {@link DataDeletedException}.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * 
     * @exception IllegalArgumentException
     *                if the transaction identifier is bad.
     * @exception DataDeletedException
     *                if the data is already deleted.
     * 
     */
    public void delete( int id );
 
    /**
     * <p>
     * Merge the transaction scope object index onto the global scope object
     * index.
     * </p>
     * <p>
     * Note: This method is invoked by a transaction during commit processing to
     * merge the write set of its object index into the global scope. This
     * operation does NOT check for conflicts. The pre-condition is that the
     * transaction has already been validated (hence, there will be no
     * conflicts). The method exists on the object index so that we can optimize
     * the traversal of the object index in an implementation specific manner
     * (vs exposing an iterator).  This method is also responsible for incrementing
     * the {@link IObjectIndexEntry#getVersionCounter() version counter}s that are
     * used to detect write-write conflicts during validation.
     * </p>
     */
    public void mergeWithGlobalObjectIndex(Journal journal);

    /**
     * <p>
     * Validate changes made within the transaction against the last committed
     * state of the journal. In general there are two kinds of conflicts:
     * read-write conflicts and write-write conflicts. Read-write conflicts are
     * handled by NEVER overwriting an existing version (an MVCC style
     * strategy). Write-write conflicts are detected by backward validation
     * against the last committed state of the journal. A write-write conflict
     * exists IFF the version counter on the transaction index entry differs
     * from the version counter in the global index scope. Once detected, a the
     * resolution of a write-write conflict is delegated to a
     * {@link IConflictResolver conflict resolver}. If a write-write conflict
     * can not be validated, then validation will fail and the transaction will
     * abort. The version counters are incremented during commit as part of the
     * merge down of the transaction's object index onto the global object
     * index.
     * </p>
     * <p>
     * Validation occurs as part of the prepare/commit protocol. Concurrent
     * transactions MAY continue to run without limitation. A concurrent commit
     * (if permitted) would force re-validation since the transaction MUST now
     * be validated against the new baseline. (It is possible that this
     * validation could be optimized.)
     * </p>
     * 
     * @param journal
     *            The journal.
     * 
     * @param tx
     *            The transaction being validated.
     * 
     * @return True iff validation succeeds.
     */
    public boolean validate(Journal journal,IStore tx);

    /**
     * @todo document and reconcile with a clustered object index. also, if we
     *       go with a scale out design an journal snapshots where index ranges
     *       are evicted to index segments then we never need to both with GC
     *       inside of the journal and we can use a WORM style allocator
     *       (perfect fit allocation vs slots).
     * 
     * After a commit, the only entries that we expect to find in the
     * transaction's object index are those where a pre-existing version was
     * overwritten by the transaction. We just deallocate the slots for those
     * pre-existing versions.
     * 
     * @param allocationIndex
     *            The index on which slot allocations are maintained.
     */
    public void gc(ISlotAllocationIndex allocationIndex);
    
    /**
     * Indicates that the current data version for the persistent identifier was
     * not found in the journal's object index. An application should test the
     * database when this is returned since the current version MAY exist on the
     * database.
     * 
     * @todo The value of this constant was changed from -1 to
     *       {@link Integer#MIN_VALUE} to remove the possibility of confusing a
     *       deleted slot with a "not found" return.  However, this has implications
     *       for where the fencepost lies for the maximum #of addressable slots in a
     *       journal.  If we continue to use negative values to mark deleted entries
     *       then update those fencepost tests (which need review anyway).
     */
    public static final int NOTFOUND = Integer.MIN_VALUE;

}
