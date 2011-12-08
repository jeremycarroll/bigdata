/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Dec 8, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IPredicate;

/**
 * A type safe enumeration of the different flavors of hash index "joins".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum HashJoinEnum {

    /**
     * A normal join. The output is the combination of the left and right hand
     * solutions. Only solutions which join are output.
     */
    Normal,
    /**
     * An optional join. The output is the combination of the left and right
     * hand solutions. Solutions which join are output, plus any left solutions
     * which did not join.
     * <p>
     * Note: Various annotations pertaining to JOIN processing are ignored when
     * used as a DISTINCT filter.
     */
    Optional,
    /**
     * A join where the left solution is output iff there exists right solution.
     * For each left solution, that solution is output exactly once iff there is
     * at least one right solution which joins with the left solution. In order
     * to enforce the cardinality constraint, this winds up populating the join
     * set and then outputs the join set once all solutions which join have been
     * identified.
     */
    Exists,
    /**
     * A filter where the left solution is output iff does not exist a right
     * solution. This basically an optional join where the solutions which join
     * are not output.
     * <p>
     * Note: This is also used for "MINUS" since the only difference between
     * "NotExists" and "MINUS" deals with the scope of the variables.
     */
    NotExists,
    /**
     * A distinct filter (not a join). Only the distinct left solutions are
     * output.
     */
    Filter;

    /**
     * Return <code>true</code> iff this is a DISTINCT SOLUTIONS filter.
     * 
     * @see #Filter
     */
    public boolean isFilter() {
        return this == Filter;
    }

    /**
     * Return <code>true</code> iff this is a JOIN with OPTIONAL semantics.
     * 
     * @see #Optional
     * @see IPredicate.Annotations#OPTIONAL
     */
    public boolean isOptional() {
        return this == Optional;
    }

}
