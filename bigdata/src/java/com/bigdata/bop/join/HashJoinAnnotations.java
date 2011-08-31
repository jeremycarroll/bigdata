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
 * Created on Aug 14, 2011
 */

package com.bigdata.bop.join;


/**
 * Annotations for hash joins.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HashJoinAnnotations {

    /**
     * The {@link IVariable[]} specifying the join variables (required). The
     * order of the entries is used when forming the as-bound keys for the hash
     * table. Duplicate elements and null elements are not permitted.
     * <p>
     * Note: The source solutions presented to a hash join MUST have bindings
     * for the {@link #JOIN_VARS} in order to join (source solutions can still
     * be pass on as optionals, but they will not join unless the join variables
     * are not bound).
     * <p>
     * Note: If no join variables are specified, then the join will consider the
     * N x M cross product, filtering for solutions which join. This is very
     * expensive when compared to a hash join. Whenever possible you should
     * identify one or more variables which must be bound for the join and
     * specify those as the join variables.
     */
    String JOIN_VARS = HashJoinAnnotations.class.getName() + ".joinVars";

}
