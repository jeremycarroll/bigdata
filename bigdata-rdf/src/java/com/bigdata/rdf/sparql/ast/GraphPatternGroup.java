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
 * Created on Sep 5, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSubGroupJoinVarOptimizer;

/**
 * Join group or union.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class GraphPatternGroup<E extends IGroupMemberNode> extends
        GroupNodeBase<E> implements IJoinNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends GroupNodeBase.Annotations {

        /**
         * An {@link IVariable}[] of the join variables will be definitely bound
         * when we begin to evaluate a sub-group. This information is used to
         * build a hash index on the join variables and to hash join the
         * sub-group's solutions back into the parent group's solutions.
         * 
         * @see ASTSubGroupJoinVarOptimizer
         */
        String JOIN_VARS = "joinVars";
        
    }
    
    /**
     * Required deep copy constructor.
     */
    public GraphPatternGroup(GraphPatternGroup<E> op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public GraphPatternGroup(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }
    
    /**
     * 
     */
    public GraphPatternGroup() {
    }

    /**
     * @param optional
     */
    public GraphPatternGroup(boolean optional) {
        super(optional);
    }

    /**
     * The join variables for the group.
     * 
     * @see Annotations#JOIN_VARS
     */
    public IVariable<?>[] getJoinVars() {
        return (IVariable[]) getProperty(Annotations.JOIN_VARS);
    }

    public void setJoinVars(final IVariable<?>[] joinVars) {
        setProperty(Annotations.JOIN_VARS, joinVars);
    }
    
}
