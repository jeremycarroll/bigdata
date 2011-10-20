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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupMemberNodeBase;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Eliminate semantically empty join group nodes which are the sole child of
 * another join groups. Such nodes either do not specify a context or they
 * specify the same context as the parent.
 * 
 * <pre>
 * { { ... } } => { ... }
 * </pre>
 * 
 * and for non-graph groups:
 * 
 * <pre>
 * { ... {} } => { ... }
 * </pre>
 * 
 * This is also done when the child is a UNION.
 * 
 * <pre>
 * { UNION {...} } => UNION {...}
 * </pre>
 * 
 * Or
 * 
 * { GRAPH ?g {...} } => GRAPH ?g {...}
 * 
 * 
 * Note: An empty <code>{}</code> matches a single empty solution. Since we
 * always push in an empty solution and the join of anything with an empty
 * solution is that source solution, this is the same as not running the group,
 * so we just eliminate the empty group.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTEmptyGroupOptimizer.java 5177 2011-09-12 17:49:44Z
 *          thompsonbry $
 */
public class ASTEmptyGroupOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTEmptyGroupOptimizer.class);

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // Main WHERE clause
        {

            final GraphPatternGroup<IGroupMemberNode> whereClause = (GraphPatternGroup<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                eliminateEmptyGroups(queryRoot, whereClause);
                
//                removeEmptyChildGroups((GraphPatternGroup<?>) whereClause);
                
            }

        }

        // Named subqueries
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (int i = 0; i < namedSubqueries.size(); i++) {

                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                        .get(i);

                final GraphPatternGroup<IGroupMemberNode> whereClause = (GraphPatternGroup<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    eliminateEmptyGroups(namedSubquery, whereClause);
                    
//                    removeEmptyChildGroups((GraphPatternGroup<?>) whereClause);

                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return queryNode;

    }

    /**
     * Eliminate a parent join group whose only child is another join group by
     * lifting the child (it replaces the parent).
     * 
     * @param op
     */
    private static void eliminateEmptyGroups(final QueryBase queryBase, 
            final GraphPatternGroup<?> op) {

        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                eliminateEmptyGroups(queryBase, childGroup);
                
                /*
                 * If we pruned the child, then we need to decrement the index
                 * so that we don't skip one.
                 */
                if (op.get(i) != child) {
                	
                	i--;
                	
                }

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                eliminateEmptyGroups(subquery, childGroup);

                /*
                 * If we pruned the child, then we need to decrement the index
                 * so that we don't skip one.
                 */
                if (op.get(i) != child) {
                	
                	i--;
                	
                }

            }
            
        }

        final int arity = op.arity();

        if (arity == 0 && 
        		op.getContext() == null &&
        		op.getParent() != null) {
        	
    		/*
    		 * If this is an empty graph pattern group then we can just prune
    		 * it out entirely, unless it is the where clause.
    		 *  
             * Also, do not prune GRAPH ?g {} or GRAPH uri {}. Those 
             * constructions have special semantics.
    		 */
    		
    		final IGroupNode<IGroupMemberNode> parent = 
    			op.getParent();
    		
    		parent.removeChild(op);
    		
        	op.setParent(null);
        	
        } else if (arity == 1 && 
        		op instanceof JoinGroupNode && 
    			op.get(0) instanceof JoinGroupNode) {
            
            /*
             * We can always merge two JoinGroupNodes into one, but we have
             * to make sure we get the optionality right.
             */

            final JoinGroupNode parent = (JoinGroupNode) op;

            final JoinGroupNode child = (JoinGroupNode) op.get(0);

            	/*
1. JoinGroup1 [optional=false] { JoinGroup2 [optional=false] { � } } -> JoinGroup2 [optional=false] { � }
2. JoinGroup1 [optional=true]  { JoinGroup2 [optional=true]  { � } } -> JoinGroup2 [optional=true]  { � }
3. JoinGroup1 [optional=true]  { JoinGroup2 [optional=false] { � } } -> JoinGroup2 [optional=true]  { � }
4. JoinGroup1 [optional=false] { JoinGroup2 [optional=true]  { � } } -> JoinGroup2 [optional=true]  { � }
            	 */
        	if (parent.isOptional() && !child.isOptional()) {
        	
        		child.setOptional(true);
        		
        	}
        	
        	swap(queryBase, parent, child);
            	
        } else if (arity == 1 && 
        		op instanceof UnionNode && 
    			op.get(0) instanceof JoinGroupNode) {
            
        	/*
        	 * We can always replace a UnionNode that has a single JoinGroupNode
        	 * with the JoinGroupNode itself.
        	 */
        	
            final UnionNode parent = (UnionNode) op;

            final JoinGroupNode child = (JoinGroupNode) op.get(0);
            
            swap(queryBase, parent, child);

        } else if (arity == 1 && 
        		op instanceof JoinGroupNode &&
        		!op.isOptional() &&
    			op.get(0) instanceof UnionNode) {
            
        	/*
        	 * If a JoinGroupNode contains a single UnionNode, we can lift the
        	 * UnionNode unless the JoinGroupNode is optional.
        	 */
            final JoinGroupNode parent = (JoinGroupNode) op;

            final UnionNode child = (UnionNode) op.get(0);
            
            swap(queryBase, parent, child);

        } else if (arity == 1 &&
        		op.get(0) instanceof IBindingProducerNode &&
                op.getParent() != null &&
                !op.isOptional() &&
                !(((IGroupNode<?>) op.getParent()) instanceof UnionNode)) {

            /*
             * The child is something which produces bindings (hence,
             * not a FILTER) and is neither a JoinGroupNode nor a
             * UnionNode and the operator is neither the top level of
             * the WHERE clause nor a UnionNode.
             * 
             * Just replace the parent JoinGroupNode (op) with the
             * child.
             */

            ((ASTBase) op.getParent()).replaceWith(op, (BOp) op.get(0));

        }

//        if (op instanceof GraphPatternGroup<?>) {
//
//            removeEmptyChildGroups((GraphPatternGroup<?>) op);
//            
//        }

    }
    
    /**
     * Swap the parent with the child inside the grandparent.  If there is no
     * grandparent, assume the parent is the where clause in the query base,
     * and replace it with the child.
     */
    static private void swap(final QueryBase queryBase, 
    		final GraphPatternGroup<?> parent, 
    		final GraphPatternGroup<?> child) {
    	
    	if (parent.getParent() == null) {
    		
    		/*
    		 * If the parent has no parent, the parent must
    		 * currently be the where clause.  Set the child to
    		 * be the new where clause instead.
    		 */
    		
    		queryBase.setWhereClause(child);
    		
    	} else {
    		
    		/*
    		 * If the parent has a parent, then remove the parent
    		 * from the grandparent and replace it with the child.
    		 */
    		
    		final ASTBase grandparent = (ASTBase) parent.getParent();
    		
    		grandparent.replaceWith(parent, child);
    		
    	}
    	
    	parent.setParent(null);
    	
    }

//    /**
//     * Remove any empty (non-GRAPH) groups (normal groups and UNIONs, but not
//     * GRAPH {}).
//     */
//    static private void removeEmptyChildGroups(final GraphPatternGroup<?> op) {
//
//        int n = op.arity();
//
//        for (int i = 0; i < n; i++) {
//
//            final BOp child = op.get(i);
//
//            if (!(child instanceof GroupNodeBase<?>))
//                continue;
//
//            if (((GroupNodeBase<?>) child).getContext() != null) {
//                /*
//                 * Do not prune GRAPH ?g {} or GRAPH uri {}. Those constructions
//                 * have special semantics.
//                 */
//                continue;
//            }
//
//            if (child.arity() == 0) {
//
//                // remove an empty child group.
//                op.removeArg(child);
//
//                // one less child to visit.
//                n--;
//
//            }
//
//        }
//
//    }

}
