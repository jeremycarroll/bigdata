/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 13, 2012
 */

package com.bigdata.bop.engine;

import java.util.HashSet;
import java.util.Set;

/**
 * Statistics summary for a work queue feeding a specific operator for a query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class QueueStats {

    /**
     * The set of shard identifiers for which there are queued
     * {@link IChunkMessage}s.
     */
    public final Set<Integer> shardSet;

    /**
     * The #of {@link IChunkMessage}s which are queued the operator across the
     * shards for which that operator has work queued.
     */
    public int chunkCount;

    /**
     * The #of solutions in those queued chunks.
     */
    public int solutionCount;

    public QueueStats() {
        this.shardSet = new HashSet<Integer>();
    }

    // static QueueStats total(final Set<QueueStats> set) {
    // final QueueStats total = new QueueStats();
    // for(QueueStats x : set) {
    // total.shardSet.addAll(x.shardSet);
    // total.chunkCount+=x.chunkCount;
    // total.solutionCount+=x.solutionCount;
    // }
    // return total;
    // }

}