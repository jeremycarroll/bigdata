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
 * Created on Oct 7, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Lift unions, which are problematic, out of minus groups like this:
 * 
 * <pre>
 * (A,MINUS(UNION(B,C))) := (A,MINUS(B),MINUS(C))
 * </pre>
 * 
 * Note: This must run before the {@link ASTBottomUpOptimizer}
 * 
 */
public class ASTFlattenMinusUnionOptimizer implements IASTOptimizer {
	
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // Main WHERE clause
        {

            final GroupNodeBase<?> whereClause = (GroupNodeBase<?>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                flattenUnionInMinus(whereClause);
                
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

                final GroupNodeBase<?> whereClause = (GroupNodeBase<?>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    flattenUnionInMinus(whereClause);
                    
                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return queryNode;

    }

    /**
     * 
     * 
     * @param op
     */
    private static void flattenUnionInMinus(final GroupNodeBase<?> op) {
    	
    	maybeFlattenThisMinus(op);
          
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GroupNodeBase<?>) {

                final GroupNodeBase<?> childGroup = (GroupNodeBase<?>) child;

                flattenUnionInMinus(childGroup);
                
            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) subquery
                        .getWhereClause();

                flattenUnionInMinus(childGroup);

            }
        }
    }

    /**
     * If node matches a very specific pattern, apply the optimization.
     * @param node
     */
	private static void maybeFlattenThisMinus(GroupNodeBase<?> node) {
        if (!(node instanceof  IJoinNode)) {
        	return;
        }
        if (node.arity()!=1) {
        	return;
        }
        if (!((IJoinNode)node).isMinus()) {
        	return;
        }
        if (!(node.args().get(0) instanceof UnionNode)) {
        	return;
        }
        /*
         * We have now found that we are in the interesting case:
         * - a minus JoinGroup
         * - with precisely one union child.
         */
        GroupNodeBase p = (GroupNodeBase) node.getParent();
        // replace this node (op) with the grandchildren, marking each one of them as a minus
        int index = p.indexOf(node);
        BOp grandchildren[] = node.args().get(0).toArray();
        BOp siblings[] = p.toArray();
        BOp rewrittenSiblings[] = new BOp[siblings.length + grandchildren.length - 1];
        System.arraycopy(siblings, 0, rewrittenSiblings, 0, index);
        int insertPos = index;
        for (BOp bop:grandchildren) {
        	rewrittenSiblings[insertPos++] = bop;
        	((JoinGroupNode)bop).setMinus(true);
        }
        System.arraycopy(siblings, index+1, rewrittenSiblings, insertPos, siblings.length - index - 1);
        p.setArgs(rewrittenSiblings);
	}

}
