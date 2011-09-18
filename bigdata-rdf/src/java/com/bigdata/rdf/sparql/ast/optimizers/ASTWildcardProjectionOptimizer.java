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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;

import cutthecrap.utils.striterators.Striterator;

/**
 * Rewrites any {@link ProjectionNode} with a wild card into the set of
 * variables visible to the {@link QueryBase} having that projection. This is
 * done first for the {@link NamedSubqueriesNode} and then depth-first for the
 * WHERE clause. Only variables projected by a subquery will be projected by the
 * parent query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTWildcardProjectionOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if(!(queryNode instanceof QueryRoot))
            return queryNode;
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        /*
         * NAMED SUBQUERIES
         * 
         * Rewrite the named subquery projections before the where clause.
         */
        if (queryRoot.getNamedSubqueries() != null) {

            for (NamedSubqueryRoot subqueryRoot : queryRoot
                    .getNamedSubqueries()) {

                rewriteProjection(sa, subqueryRoot);

            }

        }
        
        /*
         * WHERE CLAUSE
         * 
         * Bottom up visitation so we can get rewrite the projections of
         * subqueries before we rewrite the projections of the parent query.
         */
        if (queryRoot.getWhereClause() != null) {
            
            @SuppressWarnings("unchecked")
            final Iterator<QueryBase> itr = (Iterator<QueryBase>) new Striterator(
                    BOpUtility.postOrderIterator((BOp) queryRoot
                            .getWhereClause())).addTypeFilter(QueryBase.class);

            while (itr.hasNext()) {

                final QueryBase queryBase = itr.next();

                rewriteProjection(sa, queryBase);

            }

        }

        // Rewrite the projection on the QueryRoot last.
        rewriteProjection(sa, queryRoot);

        return queryRoot;
    
    }

    /**
     * Rewrite the projection for the {@link QueryBase}.
     * 
     * @param sa
     *            {@link StaticAnalysis} helper.
     * @param queryBase
     *            The {@link QueryBase} whose {@link ProjectionNode} will be
     *            rewritten.
     */
    private void rewriteProjection(final StaticAnalysis sa,
            final QueryBase queryBase) {

        final ProjectionNode projection = queryBase.getProjection();

        if (projection != null && projection.isWildcard()) {

            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryBase
                    .getWhereClause();

            final Set<IVariable<?>> varSet = sa.getSpannedVariables(
                    whereClause, new LinkedHashSet<IVariable<?>>());

            final ProjectionNode p2 = new ProjectionNode();
            
            queryBase.setProjection(p2);
            
            for(IVariable<?> var : varSet) {
            
                p2.addProjectionVar(new VarNode(var.getName()));
                
            }
            
        }
        
    }

}
