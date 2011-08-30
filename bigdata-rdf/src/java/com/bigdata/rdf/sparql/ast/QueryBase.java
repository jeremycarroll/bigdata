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

import com.bigdata.rdf.sail.QueryType;

/**
 * Contains the projection clause, where clause, and solution modified clauses.
 * 
 * @see IGroupNode
 * @see ProjectionNode
 * @see GroupByNode
 * @see HavingNode
 * @see OrderByNode
 * @see SliceNode
 */
abstract public class QueryBase extends QueryNodeBase {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryNodeBase.Annotations {

        /**
         * The {@link QueryType}.
         */
        String QUERY_TYPE = "queryType";

        /**
         * The {@link ConstructNode} (optional).
         */
        String CONSTRUCT = "construct";

        /**
         * The {@link ProjectionNode} (optional). This is also used for DESCRIBE
         * queries to capture the list of variables and IRIs which are then used
         * to rewrite the DESCRIBE query into what amounts to a CONSTRUCT query.
         * The resulting CONSTRUCT query will have a different
         * {@link ProjectionNode} suitable for use with the generated
         * {@link ConstructNode}.
         */
        String PROJECTION = "projection";

        /**
         * The top-level {@link IGroupNode} (optional).
         */
        String WHERE_CLAUSE = "whereClause";

        /**
         * The {@link GroupByNode} (optional).
         */
        String GROUP_BY = "groupBy";

        /**
         * The {@link HavingNode} (optional).
         */
        String HAVING = "having";

        /**
         * The {@link OrderByNode} (optional).
         */
        String ORDER_BY = "orderBy";

        /**
         * The {@link SliceNode} (optional).
         */
        String SLICE = "slice";
        
    }

    /**
     * Constructor is hidden to force people to declare the {@link QueryType}.
     */
    @SuppressWarnings("unused")
    private QueryBase() {
        
    }
    
	public QueryBase(final QueryType queryType) {
	    
        setQueryType(queryType);
	    
	}

    /**
     * Return the type of query. This provides access to information which may
     * otherwise be difficult to determine by inspecting the generated AST or
     * query plan.
     */
	public QueryType getQueryType() {
	    
        return (QueryType) getProperty(Annotations.QUERY_TYPE);

	}

    /**
     * Set the type of query.
     */
	public void setQueryType(final QueryType queryType) {
	    
        setProperty(Annotations.QUERY_TYPE, queryType);
	    
	}
	
    /**
     * Return the construction -or- <code>null</code> if there is no construction.
     */
    public ConstructNode getConstruct() {

        return (ConstructNode) getProperty(Annotations.CONSTRUCT);
        
    }
    
    /**
     * Set or clear the construction.
     * 
     * @param construction
     *            The construction (may be <code>null</code>).
     */
    public void setConstruct(final ConstructNode construct) {

        setProperty(Annotations.CONSTRUCT, construct);
        
    }
    
    /**
     * Return the projection -or- <code>null</code> if there is no projection.
     */
    public ProjectionNode getProjection() {

        return (ProjectionNode) getProperty(Annotations.PROJECTION);

    }

    /**
     * Set or clear the projection.
     * 
     * @param projection
     *            The projection (may be <code>null</code>).
     */
    public void setProjection(final ProjectionNode projection) {

        setProperty(Annotations.PROJECTION, projection);

    }

    /**
     * Return the {@link IGroupNode} (corresponds to the WHERE clause).
     */
    @SuppressWarnings("unchecked")
    public IGroupNode<IGroupMemberNode> getWhereClause() {

        return (IGroupNode<IGroupMemberNode>) getProperty(Annotations.WHERE_CLAUSE);

    }

    /**
     * Set the {@link IGroupNode} (corresponds to the WHERE clause).
     *  
     * @param whereClause
     *            The "WHERE" clause.
     */
    public void setWhereClause(final IGroupNode<IGroupMemberNode> whereClause) {
        
        setProperty(Annotations.WHERE_CLAUSE, whereClause);

        
    }
    
    /**
     * Return the {@link GroupByNode} -or- <code>null</code> if there is none.
     */
    public GroupByNode getGroupBy() {
        
        return (GroupByNode) getProperty(Annotations.GROUP_BY);
        
    }

    /**
     * Set or clear the {@link GroupByNode}.
     * 
     * @param groupBy
     *            The new value (may be <code>null</code>).
     */
    public void setGroupBy(final GroupByNode groupBy) {

        setProperty(Annotations.GROUP_BY, groupBy);
        
    }
    
    /**
     * Return the {@link HavingNode} -or- <code>null</code> if there is none.
     */
    public HavingNode getHaving() {
        
        return (HavingNode) getProperty(Annotations.HAVING);
        
    }

    /**
     * Set or clear the {@link HavingNode}.
     * 
     * @param having
     *            The new value (may be <code>null</code>).
     */
    public void setHaving(final HavingNode having) {

        setProperty(Annotations.HAVING, having);
        
    }

	/**
	 * Return the slice -or- <code>null</code> if there is no slice.
	 */
	public SliceNode getSlice() {
        
        return (SliceNode) getProperty(Annotations.SLICE);
	    
    }

    /**
     * Set or clear the slice.
     * 
     * @param slice
     *            The slice (may be <code>null</code>).
     */
    public void setSlice(final SliceNode slice) {

        setProperty(Annotations.SLICE, slice);
        
    }

    /**
     * Return the order by clause -or- <code>null</code> if there is no order
     * by.
     */
    public OrderByNode getOrderBy() {
     
        return (OrderByNode) getProperty(Annotations.ORDER_BY);

    }

    /**
     * Set or clear the {@link OrderByNode}.
     * 
     * @param orderBy
     *            The order by (may be <code>null</code>).
     */
    public void setOrderBy(final OrderByNode orderBy) {
    
        setProperty(Annotations.ORDER_BY, orderBy);
        
    }
    
	public String toString(final int indent) {
		
	    final String s = indent(indent);
	    
		final StringBuilder sb = new StringBuilder();

        final ConstructNode construct = getConstruct();
        final ProjectionNode projection = getProjection();
        final IGroupNode<IGroupMemberNode> whereClause = getWhereClause();
        final GroupByNode groupBy = getGroupBy();
        final HavingNode having = getHaving();
        final OrderByNode orderBy = getOrderBy();
        final SliceNode slice = getSlice();
        
        if (construct != null && !construct.isEmpty()) {

            sb.append(construct.toString(indent));
            
        }

        if (projection != null && !projection.isEmpty()) {

		    sb.append(projection.toString(indent));
		    
		}

        if (whereClause != null) {

            sb.append("\n");
            
            sb.append(s);

            sb.append("where\n");

            sb.append(whereClause.toString(indent));

        }

        if (groupBy != null && !groupBy.isEmpty()) {

            sb.append(groupBy.toString(indent));

        }

        if (having != null && !having.isEmpty()) {

            sb.append("\n");

            sb.append(s);

            sb.append(having.toString());

        }

        if (orderBy != null && !orderBy.isEmpty()) {

            sb.append(orderBy.toString(indent));

        }

        if (slice != null) {

            sb.append(slice.toString(indent));

        }

        return sb.toString();

    }

//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof QueryBase))
//            return false;
//
//        final QueryBase t = (QueryBase) o;
//
//        if (queryType == null) {
//            if (t.queryType != null)
//                return false;
//        } else {
//            if (!queryType.equals(t.queryType))
//                return false;
//        }
//
//        if (construct == null) {
//            if (t.construct != null)
//                return false;
//        } else {
//            if (!construct.equals(t.construct))
//                return false;
//        }
//
//        if (projection == null) {
//            if (t.projection != null)
//                return false;
//        } else {
//            if (!projection.equals(t.projection))
//                return false;
//        }
//
//        if (whereClause == null) {
//            if (t.whereClause != null)
//                return false;
//        } else {
//            if (!whereClause.equals(t.whereClause))
//                return false;
//        }
//
//        if (groupBy == null) {
//            if (t.groupBy != null)
//                return false;
//        } else {
//            if (!groupBy.equals(t.groupBy))
//                return false;
//        }
//
//        if (having == null) {
//            if (t.having != null)
//                return false;
//        } else {
//            if (!having.equals(t.having))
//                return false;
//        }
//
//        if (orderBy == null) {
//            if (t.orderBy != null)
//                return false;
//        } else {
//            if (!orderBy.equals(t.orderBy))
//                return false;
//        }
//
//        if (slice == null) {
//            if (t.slice != null)
//                return false;
//        } else {
//            if (!slice.equals(t.slice))
//                return false;
//        }
//
//        return true;
//
//    }

}
