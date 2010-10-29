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

import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.ndx.ClientIndexView;
import java.util.Iterator;

/**
 * Interface that specifies methods for accessing named indices
 * existing across a federation of shard services.
 */
public interface IScaleOutIndexStore extends IIndexStore {

    /**
     * Returns a client-side view of a scale-out index corresponding to
     * a desired <code>name</code> as of a desired point in time. The 
     * index view returned automatically handles the split, join, or
     * move of index partitions within the federation of shard services.
     * 
     * @param name the name of the index whose client-side view should be
     *             returned.
     *
     * @param timestamp the point in time at which the index to be returned
     *                  was captured.
     * 
     * @return a client-side view of a scale-out index corresponding to
     *         the given <code>name</code> as of the given <i>timestamp</i>;
     *         or <code>null</code> if there is no index registered with
     *         the given <code>name</code> for the given
     *         <code>timestamp</code>.
     */
    ClientIndexView getIndex(String name, long timestamp);

    /**
     * Return a read-only view onto an {@link IMetadataIndex}.
     * 
     * @param name the name of the index that should be returned.
     *
     * @param timestamp the point in time at which the index to be
     *                  returned was captured.
     */
    IMetadataIndex getMetadataIndex(String name, long timestamp);

    /**
     * Returns an iterator that will visit the {@link PartitionLocator}s for
     * the specified scale-out index key range.
     * <p>
     * The method fetches a chunk of locators at a time from the metadata index.
     * Unless the #of index partitions spanned is very large, this will be an
     * atomic read of locators from the metadata index. When the #of index
     * partitions spanned is very large, then this will allow a chunked
     * approach.
     * <p>
     * Note: It is possible that a split, join or move could occur during the
     * process of mapping the procedure across the index partitions. When the
     * view is {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} this could
     * make the set of mapped index partitions inconsistent in the sense that it
     * might double count some parts of the key range or that it might skip some
     * parts of the key range. In order to avoid this problem the caller MUST
     * use <em>read-consistent</em> semantics. If the {@link ClientIndexView}
     * is not already isolated by a transaction, then the caller MUST create a
     * read-only transaction use the global last commit time of the federation.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     *            The timestamp of the view. It is the responsibility of the
     *            caller to choose <i>timestamp</i> so as to provide
     *            read-consistent semantics for the locator scan.
     * @param fromKey
     *            The scale-out index first key that will be visited
     *            (inclusive). When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first scale-out index key that will NOT be visited
     *            (exclusive). When <code>null</code> there is no upper bound.
     * @param reverseScan
     *            <code>true</code> if you need to visit the index partitions
     *            in reverse key order (this is done when the partitioned
     *            iterator is scanning backwards).
     * 
     * @return The iterator.
     */
    Iterator<PartitionLocator> locatorScan(String name,
                                           long timestamp,
                                           byte[] fromKey,
                                           byte[] toKey,
                                           boolean reverseScan);
}
