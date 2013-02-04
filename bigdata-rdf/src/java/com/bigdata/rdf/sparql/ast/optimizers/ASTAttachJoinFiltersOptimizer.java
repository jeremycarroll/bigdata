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
 * Created on Oct 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;

/**
 * Optimizer attaches {@link FilterNode}s which will run as "join filters" to
 * {@link StatementPatternNode}s. The joins must already be in the order in
 * which they will be evaluated. Join filters which are already attached to
 * required joins will be pick up and reattached as appropriate for the current
 * join evaluation order.
 * <p>
 * Note: Even though a {@link FilterNode} is attached to a given join, the
 * {@link FilterNode} may have materialization requirements which make it
 * impossible to evaluate the constraint on the physical JOIN operator. In such
 * cases, a materialization pattern will be used to ensure that the necessary
 * variables have been materialized before the constraint runs. The
 * materialization pipeline, of necessity, runs after the join and the
 * constraint will be modeled as a {@link ConditionalRoutingOp}. However, this
 * optimizer is NOT responsible for those decisions. It just attaches filters to
 * join based on when their variables become bound, not when their variables are
 * known to satisify the materialization requirements for the filter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTAttachJoinFiltersOptimizer.java 5455 2011-10-29 19:43:53Z
 *          thompsonbry $
 */
public class ASTAttachJoinFiltersOptimizer implements IASTOptimizer {

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

                    attachJoinFilters(context, sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        attachJoinFilters(context, sa, queryRoot.getWhereClause());

        return queryRoot;
        
    }

    /**
     * Recursively process groups.
     * 
     * @param context
     * @param sa
     * @param group
     */
    @SuppressWarnings("unchecked")
    private void attachJoinFilters(final IEvaluationContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        /*
         * Recursion.
         */
        for(IGroupMemberNode child : group) {

            if (child instanceof GraphPatternGroup<?>) {
            
                attachJoinFilters(context, sa,
                        (GraphPatternGroup<IGroupMemberNode>) child);
                
            } else if (child instanceof SubqueryRoot) {
                
                attachJoinFilters(context, sa,
                        ((SubqueryRoot) child).getWhereClause());
                
            }
            
        }
        
        if (group instanceof JoinGroupNode) {
            
            /*
             * Filter attachment for this join group.
             */
            
            attachJoinFilters2(context, sa, (JoinGroupNode) group);
            
        }
        
    }
    
    /**
     * Figure out which filters will be attached to which statement patterns.
     * This only inspects the required statement patterns. Simple optionals are
     * handled by {@link ASTSimpleOptionalOptimizer}.
     * <p>
     * Note: This handles re-attach by collecting previously attached FILTERS
     * from required joins.
     */
    private void attachJoinFilters2(final IEvaluationContext context,
            final StaticAnalysis sa, final JoinGroupNode group) {

        /*
         * Collect all required joins and all join filters.
         */

        // The join path (required joins only).
        final List<IJoinNode> requiredJoins = new LinkedList<IJoinNode>();

        // The join filters.
        final List<FilterNode> joinFilters = new LinkedList<FilterNode>(
                sa.getJoinFilters(group));

        for (IGroupMemberNode child : group) {
            
            if (!(child instanceof IJoinNode)) {
                continue;
            }
            
            final IJoinNode aJoinNode = (IJoinNode) child;
            
            if (aJoinNode.isOptional()) {
                /*
                 * Note: We do not attach filters to OPTIONAL joins here.
                 * 
                 * Note: The ASTSimpleOptionalOptimizer is responsible for
                 * filter attachment to optional statement pattern nodes. It
                 * looks for optional JoinGroupNodes which it can rewrite into
                 * an optional StatementPatternNode. The join filter (if any)
                 * attached to that optional StatementPatternNode MUST NOT be
                 * detached and reassigned even if the join order changes since
                 * it is really part of an optional group which was rewritten
                 * into an optional statement pattern.
                 */
                continue;
            }

            requiredJoins.add(aJoinNode);
            
            final List<FilterNode> ownJoinFilters = aJoinNode
                    .getAttachedJoinFilters();
            
            if (ownJoinFilters != null) {
            
                // Pick up any join filters already attached to this join node.
                
                joinFilters.addAll(ownJoinFilters);
                
                aJoinNode.setAttachedJoinFilters(null);
                
            }

        }

        if (requiredJoins.isEmpty()) {

            // Nothing to do.
            return;
            
        }

        // The join filters that become attached to joins.
        final List<FilterNode> attachedFilters = new LinkedList<FilterNode>();

        /*
         * Figure out which filters are attached to which joins.
         */
        {

            final int requiredJoinCount = requiredJoins.size();

            final IJoinNode[] path = requiredJoins
                    .toArray(new IJoinNode[requiredJoinCount]);

            final Set<IVariable<?>> knownBound = sa
                    .getDefinitelyIncomingBindings(group,
                            new LinkedHashSet<IVariable<?>>());

            final FilterNode[][] assignedConstraints = sa
                    .getJoinGraphConstraints(path, joinFilters
                            .toArray(new FilterNode[joinFilters.size()]),
                            knownBound, false/* pathIsComplete */);

            /*
             * Attach the join filters.
             */
            for (int i = 0; i < requiredJoinCount; i++) {

                final IJoinNode tmp = path[i];

                final FilterNode[] filters = assignedConstraints[i];

                if (filters.length > 0) {

                    tmp.setAttachedJoinFilters(Arrays.asList(filters));
                    
                    attachedFilters.addAll(Arrays.asList(filters));
                    
                }

            }

        }

        /*
         * Remove all join filters from the group. They have all been attached
         * to the joins.
         */
        for (FilterNode joinFilter : attachedFilters) {

            group.removeArg(joinFilter);

        }

    }

}
