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
 * Created on Sep 24, 2008
 */

package com.bigdata.relation.rule;

import java.io.Serializable;
import java.util.concurrent.DelayQueue;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.relation.rule.eval.IStepTask;
import com.bigdata.striterator.DistinctFilter;

/**
 * A collection of constraints that may be imposed on an {@link IStep} when
 * evaluated as a query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IQueryOptions extends Serializable {

    /**
     * <code>true</code> if a {@link DistinctFilter} should be applied when
     * the query is evaluated.
     */
    public boolean isDistinct();
    
    /**
     * An optional array of {@link ISortOrder}s describing the sort order that
     * will be imposed on the generated solutions when the rule is evaluated as
     * a <em>query</em>.
     * 
     * @return An array of {@link ISortOrder}s -or- <code>null</code> iff
     *         there is no "order by" constraint.
     */
    public ISortOrder[] getOrderBy();
    
    /**
     * An optional {@link ISlice} describing a constraint on the first
     * solution and the maximum #of solutions to be materialized by a
     * <em>query</em>.
     * <p>
     * Note: Using an {@link ISlice} requires that the solutions are stable
     * for queries against the same commit point of the database.
     * 
     * @todo Stable solution sets can be handled in at least two ways.
     *       <p>
     *       (1) Forcing single-threaded execution on the {@link IRule} in order
     *       to remove an indeterminacy (at least, any due to parallelism);
     *       <p>
     *       (2)Pre-generating the entire solution set and then returning a view
     *       onto a slice of the solution set. The latter approach requires some
     *       means to eventually release the solution set. One reasonable
     *       approach is to use the {@link TemporaryStoreFactory} associated
     *       with the {@link IIndexManager} on the <em>client</em> and to hold
     *       a hard reference on a {@link DelayQueue} to the
     *       {@link TemporaryStore} on which the solution set was materialized.
     *       Manging such temporary solutions sets is, in fact, the easy part
     *       since we also need to know whether or not we are seeing the same
     *       query (against the same commit point) with only the exception that
     *       a different slice was requested.
     *       <p>
     *       Another twist is that a SORT or DISTINCT needs to be applied to the
     *       solution set as a whole.
     * 
     * @return The {@link ISlice} -or- <code>null</code> if there is no
     *         constraint on the solutions that will be visited.
     */
    public ISlice getSlice();

    /**
     * Return <code>true</code> iff query evaluation must be stable. Stable
     * query evaluation requires that the same query executed against the same
     * commit point will produce the same solutions in the same order. This
     * constraint requires that (a) query execution does not use any
     * parallelism; and (b) all {@link IStepTask} are determinate. Stable
     * queries may be useful when using an {@link ISlice} to page through a
     * solution set.
     * 
     * @return <code>true</code> if query evaluation must be stable.
     */
    public boolean isStable();
    
}
