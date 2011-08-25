/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * An {@link IValueExpressionNode} paired with a flag to indicating an ascending
 * or descending sort order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OrderByExpr extends ASTBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends ASTBase.Annotations {
        
        String ASCENDING = "ascending";

        boolean DEFAULT_ASCENDING = true;
        
    }
    
//	private final IValueExpressionNode ve;
	
//	private final boolean ascending;
	
    public OrderByExpr(final IValueExpressionNode ve, final boolean ascending) {

        super(new BOp[] { (BOp) ve }, null/* anns */);

        setAscending(ascending);

    }

    final public boolean isAscending() {

        return getProperty(Annotations.ASCENDING, Annotations.DEFAULT_ASCENDING);

    }

    final public void setAscending(boolean ascending) {

        setProperty(Annotations.ASCENDING, ascending);

    }
   
    public IValueExpressionNode getValueExpressionNode() {

        return (IValueExpressionNode) get(0);

    }

    public IValueExpression<? extends IV> getValueExpression() {

        return getValueExpressionNode().getValueExpression();

    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        final boolean ascending = isAscending();
        
        if (!ascending) {

            sb.append("desc(");
            
        }

        sb.append(getValueExpressionNode().toString());

        if (!ascending) {
            
            sb.append(")");
            
        }

        return sb.toString();

    }

//    @Override
//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof OrderByExpr))
//            return false;
//
//        final OrderByExpr t = (OrderByExpr) o;
//
//        if (ascending != t.ascending)
//            return false;
//
//        if (!ve.equals(t.ve))
//            return false;
//        
//        return true;
//
//    }

}
