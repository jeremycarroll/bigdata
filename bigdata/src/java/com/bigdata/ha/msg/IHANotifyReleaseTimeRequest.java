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
package com.bigdata.ha.msg;

import java.util.UUID;

import com.bigdata.journal.ITransactionService;

/**
 * Message from a follower to the leader in which the follower specifies the
 * earliest commit point that is pinned on the follower by an active transaction
 * or the minReleaseAge associated with its local {@link ITransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHANotifyReleaseTimeRequest extends IHAMessage {

    /**
     * The service that provided this information.
     */
    public UUID getServiceUUID();

    /**
     * The earliest pinned commit time on the follower.
     */
    public long getCommitTime();

    /**
     * The earliest pinned commit counter on the follower.
     */
    public long getCommitCounter();

//    /**
//     * The readsOnCommitTime of the earliest active transaction on the follower.
//     */
//    public long getReadsOnCommitTimeForEarliestActiveTx();
//
//    /**
//     * The minReleaseAge on the follower (this should be the same on all
//     * services in a quorum).
//     */
//    public long getMinReleaseAge();

    /**
     * A timestamp taken during the protocol used to agree on the new release
     * time. This is used to detect problems where the clocks are not
     * synchronized on the services.
     */
    public long getTimestamp();

}
