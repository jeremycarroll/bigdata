/*

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
 * Created on Oct 24, 2007
 */

package com.bigdata.join;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

/**
 * An abstraction for a key-range scan using the index whose natural order is
 * more efficient for a given {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAccessPath<E> extends Iterable<E> {

    /**
     * The constraints on the {@link IAccessPath}.
     * 
     * @todo there should be some way to summarize the binding pattern on the
     *       predicate as an identifier of the natural order in which the
     *       elements will be visited.
     */
    public IPredicate<E> getPredicate();

    /**
     * True iff the access path is empty (there are no matches for the
     * {@link IPredicate}) This is more conclusive than {@link #rangeCount()}
     * since you MAY have a non-zero range count when the key range is in fact
     * empty (there may be "deleted" index entries within the key range).
     */
    public boolean isEmpty();
    
    /**
     * The maximum #of elements that could be returned for the
     * {@link IPredicate}.
     * <p>
     * Note: This is an upper bound since scale-out indices use delete markers
     * and therefore will report entries that have been deleted but not yet
     * purged from the index in the range count. If the index does not support
     * delete markers then this will be an exact count.
     * 
     * @todo add boolean parameter for exact range counts? Note that these can
     *       be quite expensive since you must do a full key-range scan when the
     *       index supports delete markers.
     */
    public long rangeCount();

    /**
     * The raw iterator for traversing the selected index within the key range
     * implied by {@link IPredicate}.
     * 
     * @todo for scale-out version, the optional {@link ISPOFilter} should be
     *       sent to the data service.
     */
    public ITupleIterator<E> rangeIterator();
    
    /**
     * An iterator visiting elements using the natural order of the index
     * selected for the {@link IPredicate}. This is equivalent to
     * 
     * <pre>
     * iterator(0, 0)
     * </pre>
     * 
     * since a <i>limit</i> of ZERO (0) means no limit and a <i>capacity</i>
     * of ZERO (0) means whatever is the default capacity.
     * 
     * @return The iterator.
     * 
     * @todo this could be just {@link ITuple#getObject()} for the
     *       {@link ITupleIterator} returned by {@link #rangeIterator()}.
     */
    public IChunkedIterator<E> iterator();

    /**
     * An iterator visiting elements using the natural order of the index
     * selected for the {@link IPredicate}.
     * 
     * @param limit
     *            The maximum #of {@link SPO}s that will be visited -or- ZERO
     *            (0) if there is no limit.
     * 
     * @param capacity
     *            The maximum capacity for the buffer used by the iterator. When
     *            ZERO(0), a default capacity will be used. When a <i>limit</i>
     *            is specified, the capacity will never exceed the <i>limit</i>.
     * 
     * @return The iterator.
     */
    public IChunkedIterator<E> iterator(int limit, int capacity);

    /**
     * Remove all elements selected by the {@link IPredicate} (batch, parallel,
     * chunked, NO truth maintenance).
     * 
     * @return The #of elements that were removed.
     */
    public long removeAll();
    
}
