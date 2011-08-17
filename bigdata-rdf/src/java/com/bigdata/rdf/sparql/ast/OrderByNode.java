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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * AST node models an ORDER BY clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OrderByNode extends SolutionModifierBase {

    private final List<OrderByExpr> orderBy = new LinkedList<OrderByExpr>();

    public OrderByNode() {
    }

    public void addOrderBy(final OrderByExpr orderBy) {
        
        if (this.orderBy.contains(orderBy)) {
            throw new IllegalArgumentException("duplicate");
        }
        
        this.orderBy.add(orderBy);
        
    }
    
    public void removeOrderBy(final OrderByExpr orderBy) {
        this.orderBy.remove(orderBy);
    }
    
    public boolean hasOrderBy() {
        return !orderBy.isEmpty();
    }
    
    public List<OrderByExpr> getOrderBy() {
        return Collections.unmodifiableList(orderBy);
    }

    public String toString(final int indent) {
        final StringBuilder sb = new StringBuilder(indent(indent));
        sb.append("order by ");
        for (OrderByExpr e : orderBy) {
            sb.append(" ");
            sb.append(e.toString());
        }
        return sb.toString();
    }

}
