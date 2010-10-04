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
 * Created on Mar 15, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

import com.bigdata.btree.isolation.IConflictResolver;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ITxCommitProtocol;

//BTM
import com.bigdata.service.IService;

/**
 * <p>
 * An interface for managing transaction life cycles.
 * </p>
 * <h2>Concurrency control</h2>
 * <p>
 * The underlying concurrency control mechanism is Multi-Version Concurrency
 * Control (MVCC). There are no "write locks" per say. Instead, a transaction
 * reads from a historical commit point identified by its assigned start time
 * (abs(transactionIdentifier) is a timestamp which identifies the commit point)
 * and writes on an isolated write set visible only to that transaction. When a
 * read-write transaction commits, its write set is validated against the then
 * current committed state of the database. If validation succeeds, the isolated
 * write set is merged down onto the database. Otherwise the transaction is
 * aborted and its write set is discarded. (It is possible to register an index
 * with an {@link IConflictResolver} in order to present the application with an
 * opportunity to validate write-write conflicts using state-based techniques,
 * i.e., by looking at the records and timestamps and making an informed
 * decision).
 * </p>
 * <p>
 * A transaction imposes "read locks" on the resources required for the
 * historical state of the database from which that transaction is reading (both
 * read-only and read-write transactions impose read locks). Those resources
 * will not be released until the transaction is complete or aborted. The
 * transaction manager coordinates the release of resources by advancing the
 * global release time - the earliest commit time from which a transaction may
 * read. Under dire circumstances (disk shortage) the transaction manager MAY
 * choose to abort transactions and advance the release time in order to permit
 * the release of locked resources and the reclaimation of their space on local
 * disk.
 * </p>
 * <h2>Centralized transaction manager service</h2>
 * <p>
 * When deployed as a distributed database there will be a centralized service
 * implementing this interface and clients will discover and talk with that
 * service. The centralized service in turn will coordinate the distributed
 * transactions with the various shard services using their local
 * implementations of this same interface. The centralized transaction service
 * SHOULD invoke the corresponding methods on a shard service IFF it
 * knows that the shard service is buffering writes for the transaction.
 * </p>
 * <h2>Timestamps</h2>
 * <p>
 * Both read-only and read-write transactions assert global read locks on the
 * resources required for that historical state of the database corresponding to
 * their start time. Those read locks are released when the transaction
 * completes. Periodically the transaction manager will advance the release
 * time, rendering views of earlier states of the database unavailable.
 * </p>
 * <p>
 * The transaction identifier is the transaction <em>start time</em>. The
 * transaction start time is choosen from among those distinct timestamps
 * available between the specified commit time and the next commit time on the
 * database. Successive read-write transactions must be assigned transaction
 * identifiers whose absolute value is strictly increasing - this requirement is
 * introduced by the MVCC protocol.
 * </p>
 * <p>
 * The sign of the transaction identifier indicates whether the transaction is
 * read-only (positive) or read-write (negative). Read-only transaction
 * identifiers may be directly used as commit times when reading on a local
 * store. Read-write transaction identifiers must have their sign bit cleared in
 * order to read from their ground state (the commit point corresponding to
 * their transaction start time) and unmodified transaction identifier is used
 * to access their mutable view (the view comprised of the write set of the
 * transaction super imposed on the ground state such that writes, overwrites,
 * and deletes are visible to the view).
 * </p>
 * <p>
 * The symbolic value {@link ITx#READ_COMMITTED} and any <code>startTime</code>
 * MAY be used to perform a lightweight read-only operations either on a local
 * data service or on the distributed database without coordination with the
 * {@link ITransactionService}, but resources MAY be released at any time since
 * no read "locks" have been declared. While a read-write transaction may be
 * readily identified by the sign associated with the transaction identifier,
 * you CAN NOT differentiate between a read-only transaction (with read-locks)
 * and a lightweight read on a given commit time. In practice, it is only the
 * transaction manager which needs to recognize read-only transactions and then
 * only to constrain its assignment of distinct transaction identifiers and to
 * coordinate the advance of the release time as transactions end. There is no
 * other practical difference between read-only transactions and lightweight
 * reads from the perspective of either the client or the individual data
 * services as read-locks are managed solely through the advancement of the
 * release time by the transaction manager.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITransactionService extends TransactionService, IService {

}
