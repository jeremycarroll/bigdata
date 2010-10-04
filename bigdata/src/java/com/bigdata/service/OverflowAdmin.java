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
import java.util.concurrent.ExecutionException;

/**
 * Methods for administratively forcing overflow processing.
 *
 * <strong>
 * Note: This interface exists primarily to provide service
 *       implementors with a mechanism -- separate from the
 *       service's primary interface -- that allows the implementor
 *       to inject behavior into the service that will support
 *       unit tests and benchmarking activities. The methods of
 *       this interface SHOULD NOT be used on a deployed
 *       federation; as the overhead associated with a compacting
 *       merge of each index partition can be significant.
 * </strong>
 */
public interface OverflowAdmin {

    /**
     * Method sets a flag that will force overflow processing during the next
     * group commit and optionally forces a group commit. Normally there is no
     * reason to invoke this method directly. Overflow processing is triggered
     * automatically on a bottom-up basis when the extent of the live journal
     * nears the {@link Options#MAXIMUM_EXTENT}.
     * 
     * @param immediate       The purpose of this argument is to permit the
     *                        caller to trigger an overflow event even though
     *                        there are no writes being made against the data
     *                        service. When <code>true</code> the method will
     *                        write a token record on the live journal in
     *                        order to provoke a group commit. In this case
     *                        synchronous overflow processing will have
     *                        occurred by the time the method returns. When
     *                        <code>false</code> a flag is set and overflow
     *                        processing will occur on the next commit.
     * @param compactingMerge The purpose of this flag is to permit the
     *                        caller to indicate that a compacting merge
     *                        should be performed for all indices on the
     *                        data service (at least, all indices whose data
     *                        are not simply copied onto the new journal)
     *                        during the next synchronous overflow. Note that
     *                        compacting merges of indices are performed
     *                        automatically from time to time so this flag
     *                        exists mainly for people who want to force a
     *                        compacting merge for some reason.
     * 
     * @throws IOException
     * @throws InterruptedException
     *             may be thrown if <i>immediate</i> is <code>true</code>.
     * @throws ExecutionException
     *             may be thrown if <i>immediate</i> is <code>true</code>.
     */
    void forceOverflow(boolean immediate, boolean compactingMerge)
            throws IOException, InterruptedException, ExecutionException;

    /**
     * The #of asynchronous overflows that have taken place on this shard
     * service (the counter is not restart safe).
     */
    long getAsynchronousOverflowCounter() throws IOException;

    /**
     * Return <code>true</code> iff the shard service is currently engaged in
     * overflow processing.
     */
    boolean isOverflowActive() throws IOException;
}
