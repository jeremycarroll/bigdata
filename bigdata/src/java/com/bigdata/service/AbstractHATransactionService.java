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
 * Created on Dec 18, 2008
 */
package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HATXSGlue;
import com.bigdata.ha.msg.IHAGatherReleaseTimeRequest;
import com.bigdata.journal.ITransactionService;

/**
 * Adds local methods to support an HA aware {@link ITransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractHATransactionService extends
        AbstractTransactionService implements HATXSGlue {

    public AbstractHATransactionService(final Properties properties) {

        super(properties);
        
    }
    
    /**
     * Factory for the Gather task on the follower.
     * 
     * @param req
     *            The request.
     * 
     * @return The task to run on the follower.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" >
     *      Native thread leak in HAJournalServer process </a>
     */
    abstract public Callable<Void> newGatherMinimumVisibleCommitTimeTask(
            final IHAGatherReleaseTimeRequest req);

    /**
     * Coordinate the update of the <i>releaseTime</i> on each service that is
     * joined with the met quorum.
     * 
     * @param timeout
     *            The timeout for the release time consensus protocol.
     * @param units
     *            The units for that timeout.
     */
    abstract public void updateReleaseTimeConsensus(final long timeout,
            final TimeUnit units) throws IOException, TimeoutException,
            InterruptedException, Exception;

    /**
     * Used to make a serviceJoin() MUTEX with the consensus protocol.
     */
    abstract public void runWithBarrierLock(Runnable r);
    
}
