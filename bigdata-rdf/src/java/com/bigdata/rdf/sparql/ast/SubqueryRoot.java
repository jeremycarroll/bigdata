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
package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSparql11SubqueryOptimizer;

/**
 * A SPARQL 1.1 style subquery.
 */
public class SubqueryRoot extends SubqueryBase implements IJoinNode {

    public interface Annotations extends SubqueryBase.Annotations {
        
        /**
         * Annotation provides a query hint indicating whether or not the
         * subquery should be transformed into a named subquery, lifting its
         * evaluation out of the main body of the query and replacing the
         * subquery with an INCLUDE. When <code>true</code>, the subquery will
         * be lifted out. When <code>false</code>, the subquery will not be
         * lifted unless other semantics require that it be lifted out
         * regardless.
         * 
         * @see ASTSparql11SubqueryOptimizer
         */
        String RUN_ONCE = "runOnce";
        
        boolean DEFAULT_RUN_ONCE = false;
        
    }
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Deep copy constructor.
     */
    public SubqueryRoot(final SubqueryRoot queryBase) {
    
        super(queryBase);
        
    }
    
    /**
     * Shallow copy constructor.
     */
    public SubqueryRoot(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
    }

     /**
     * Note: This constructor variant may be used with the implicit subquery for
     * EXISTS to specify the type of the subquery as {@link QueryType#ASK}.
     * 
     * @param queryType
     */
    public SubqueryRoot(final QueryType queryType) {

        super(queryType);

    }

    public void setRunOnce(boolean runOnce) {
        setProperty(Annotations.RUN_ONCE,runOnce);
    }

    public boolean isRunOnce() {

        return getProperty(Annotations.RUN_ONCE, Annotations.DEFAULT_RUN_ONCE);
        
    }
    
    /**
     * Returns <code>false</code>.
     */
    final public boolean isOptional() {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to also report the {@link Annotations#RUN_ONCE} annotation.
     */
    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(super.toString(indent));
        
        final boolean runOnce = isRunOnce();

        sb.append("\n");

        sb.append(indent(indent));

        sb.append("@" + Annotations.RUN_ONCE + "=" + runOnce);

        return sb.toString();

    }

}
