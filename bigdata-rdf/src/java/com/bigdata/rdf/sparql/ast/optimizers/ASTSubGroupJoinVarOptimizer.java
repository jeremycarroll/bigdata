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
 * Created on Oct 25, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizer assigns join variables to sub-groups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTSubGroupJoinVarOptimizer implements IASTOptimizer {

    @SuppressWarnings("unchecked")
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    assignJoinVars(sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        assignJoinVars(sa, queryRoot.getWhereClause());

        return queryRoot;

    }
    
    /**
     * Assign the join variable(s) to the group. The join variables are those
     * variables which are definitely bound by the time the group runs, so this
     * depends on the order of the nodes in the parent group. There may be zero
     * or more join variables. Child groups are processed recursively.
     * 
     * @param sa
     * @param group
     */
    private void assignJoinVars(final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        if (group.getParentGraphPatternGroup() != null) {

            /*
             * The variables which will be definitely bound based on an analysis
             * of the group.
             */
            final Set<IVariable<?>> boundByGroup = sa
                    .getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */);

            /*
             * Find the set of variables which will be definitely bound by the
             * time the group is evaluated.
             */
            final Set<IVariable<?>> incomingBindings = sa
                    .getDefinitelyIncomingBindings(
                            (GraphPatternGroup<?>) group,
                            new LinkedHashSet<IVariable<?>>());

            /*
             * This is only those variables which are bound on entry into the group
             * in which the SERVICE join appears *and* which are "must" bound
             * variables projected by the SERVICE.
             */
            boundByGroup.retainAll(incomingBindings);

            @SuppressWarnings("rawtypes")
            final IVariable[] joinVars = boundByGroup.toArray(new IVariable[0]);

            group.setJoinVars(joinVars);
            
            /*
             * The variables used by the group and its children, including 
             * filters.
             */
            final Set<IVariable<?>> usedByGroup = sa
            		.getSpannedVariables(group, 
            				true /*filters*/, new LinkedHashSet<IVariable<?>>());

            /*
             * Find the set of variables which have appeared in the query and
             * may be bound by the time the group is evaluated.
             */
            final Set<IVariable<?>> maybeIncomingBindings = sa
                    .getMaybeIncomingBindings(
                            (GraphPatternGroup<?>) group,
                            new LinkedHashSet<IVariable<?>>());

            /*
             * Retain the variables used by the group that have already
             * appeared previously in the query up to this point. 
             */
            usedByGroup.retainAll(maybeIncomingBindings);
            
            @SuppressWarnings("rawtypes")
            final IVariable[] projectInVars = usedByGroup.toArray(new IVariable[0]);

            group.setProjectInVars(projectInVars);
            
        }

        /*
         * Recursion.
         */
        for (IGroupMemberNode child : group) {

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> subGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                assignJoinVars(sa, subGroup);

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> subGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                assignJoinVars(sa, subGroup);

            }

        }

    }

}
