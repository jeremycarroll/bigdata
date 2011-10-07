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

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Flattens nested unions whenever possible.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTFlattenUnionsOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ASTFlattenUnionsOptimizer.class);
//    
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

                flattenUnions(whereClause);
                
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

                    flattenUnions(whereClause);
                    
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
    private static void flattenUnions(final GroupNodeBase<?> op) {

        if ((op instanceof UnionNode)) {
            
            /*
             * The direct children of a UnionNode are JoinGroupNodes.
             * 
             * If the direct child of a child JoinGroupNode is a UnionNode, then
             * we will lift the child UnionNode's children into this UnionNode
             * and eliminate the child JoinGroupNode for this UnionNode which
             * was the parent of the lifted UnionNode.
             */
            final UnionNode thisUnion = (UnionNode) op;

            final List<JoinGroupNode> childrenToRemove = new LinkedList<JoinGroupNode>();
            
            for (int i = 0; i < op.arity(); i++) {

                final JoinGroupNode directChild = (JoinGroupNode) op.get(i);

                if (directChild.arity() == 1
                        && directChild.get(0) instanceof UnionNode) {

                    final UnionNode childUnion = (UnionNode) directChild.get(0);

                    /*
                     * Lift union.
                     * 
                     * Note: All children MUST be JoinGroupNodes
                     */
                    
                    for (int j = 0; j < childUnion.arity(); j++) {

                        final JoinGroupNode childToLift = (JoinGroupNode) childUnion
                                .get(j);

                        thisUnion.addChild(childToLift);
                        
                        childrenToRemove.add(directChild);
                        
                    }
                    
                }

            }
            
            // Remove any join group nodes for lifted unions.
            for(JoinGroupNode directChild : childrenToRemove) {
                
                thisUnion.removeChild(directChild);
                
            }

        }

        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GroupNodeBase<?>) {

                final GroupNodeBase<?> childGroup = (GroupNodeBase<?>) child;

                flattenUnions(childGroup);
                
            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) subquery
                        .getWhereClause();

                flattenUnions(childGroup);

            }

        }

    }

}
