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

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;

/**
 * Task class that can be used to run a rangeIterator operation.
 */
public class RangeIteratorTask extends AbstractTask {

    private final byte[] fromKey;
    private final byte[] toKey;
    private final int capacity;
    private final int flags;
    private final IFilterConstructor filter;
        
    public RangeIteratorTask(ConcurrencyManager concurrencyManager,
                             long startTime,
                             String name,
                             byte[] fromKey,
                             byte[] toKey,
                             int capacity,
                             int flags,
                             IFilterConstructor filter)
    {
        super(concurrencyManager, startTime, name);

        this.fromKey = fromKey;
        this.toKey = toKey;
        this.capacity = capacity;
        this.flags = flags;
        this.filter = filter; // MAY be null.
    }

    public ResultSet doTask() throws Exception {

        final IIndex ndx = getIndex(getOnlyResource());
            
        // Figure out the upper bound on the #of tuples that could be
        // materialized.
        // 
        // Note: the upper bound on the #of key-value pairs in the range is
        // truncated to an [int].
            
        final int rangeCount = (int) ndx.rangeCount(fromKey, toKey);

        final int limit = (rangeCount > capacity ? capacity : rangeCount);

        // Iterator that will visit the key range.
        // 
        // Note: We always visit the keys regardless of whether we pass them
        // on to the caller. This is necessary in order for us to set the
        // [lastKey] field on the result set and that is necessary to
        // support continuation queries.
            
        final ITupleIterator itr = 
                  ndx.rangeIterator(fromKey, toKey, limit,
                                    flags | IRangeQuery.KEYS,
                                    filter);

        //Populate the result set from the iterator.
        return new ResultSet(ndx, capacity, flags, itr);
    }
}
