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
 * Created on Aug 18, 2011
 */

package com.bigdata.rdf.sparql.ast;

/**
 * An AST node which provides a reference in an {@link IGroupNode} and indicates
 * that a named solution set should be joined with the solutions in the group.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see NamedSubqueryRoot
 */
public class NamedSubqueryInclude extends
        GroupMemberNodeBase<NamedSubqueryInclude> {

    private static final long serialVersionUID = 1L;

    interface Annotations extends SubqueryRoot.Annotations {

        /**
         * The name of the temporary solution set.
         */
        String SUBQUERY_NAME = "subqueryName";
        
        /**
         * A {@link VarNode}[] specifying the join variables that will be used
         * when the named result set is join with the query. The join variables
         * MUST be bound for a solution to join.
         * 
         * TODO This can be different for each context in the query in which a
         * given named result set is included. When there are different join
         * variables for different INCLUDEs, then we need to build a hash index
         * for each set of join variable context that will be consumed within
         * the query.
         */
        String JOIN_VARS = "joinVars";
        
    }

    /**
     * @param name
     *            The name of the subquery result set.
     */
    public NamedSubqueryInclude(final String name) {
        setName(name);
    }

    /**
     * The name of the {@link NamedSubqueryRoot} to be joined.
     */
    public String getName() {
        
        return (String) getProperty(Annotations.SUBQUERY_NAME);
                
    }

    /**
     * Set the name of the {@link NamedSubqueryRoot} to be joined.
     * 
     * @param name
     */
    public void setName(String name) {
     
        if(name == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.SUBQUERY_NAME, name);
        
    }

    /**
     * The join variables to be used when the named result set is included into
     * the query.
     */
    public VarNode[] getJoinVars() {

        return (VarNode[]) getProperty(Annotations.JOIN_VARS);

    }

    /**
     * Set the join variables.
     * 
     * @param joinVars
     *            The join variables.
     */
    public void setJoinVars(final VarNode[] joinVars) {

        setProperty(Annotations.JOIN_VARS, joinVars);

    }

    @Override
    public String toString(int indent) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(indent(indent));

        sb.append("INCLUDE ").append(getName());

        final VarNode[] joinVars = getJoinVars();

        if (joinVars != null) {

            sb.append(" JOIN ON (");

            boolean first = false;

            for (VarNode var : joinVars) {

                if (!first)
                    sb.append(",");

                sb.append(var);

                first = false;

            }

            sb.append(")");

        }

        return sb.toString();

    }

}
