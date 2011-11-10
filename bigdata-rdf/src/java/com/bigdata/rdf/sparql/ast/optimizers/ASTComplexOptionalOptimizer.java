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
 * Created on Oct 19, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Rewrite a join group using two or more complex OPTIONAL groups using a hash
 * join pattern.
 * <p>
 * Note: this optimization is not required if there is only one complex optional
 * in the join group. It is only when there are multiple complex optional groups
 * that we need to lift those groups out as named subqueries (since they need to
 * feed each other). If there is only one complex optional group, then we can
 * run it as a sub-group instead.
 * <p>
 * NOte: This optimization presumes that simple optional groups were already
 * translated into optional {@link StatementPatternNode}s.
 * <p>
 * Queries with multiple complex optional groups can be rewritten into the hash
 * join of solution sets as follows.
 * 
 * <ol>
 * 
 * <li>First, create a hash index from the required joins and any simple
 * optional joins. Given the modeling abilities of the AST, this is most easily
 * achieved by converting the required joins into a named subquery. The join
 * variable(s) for that named subquery will the subset of variable(s) which are
 * shared by each of the complex OPTIONAL groups. The
 * {@link ASTNamedSubqueryOptimizer} already handles the assignment of join
 * variables, so we do not need to consider it further here.</li>
 * 
 * <li>For each complex optional group, use the solution step generated in (1)
 * and run the optional group as a named subquery producing a new solution set.
 * The WHERE clause of the named subquery should look like:
 * 
 * <pre>
 * {INCLUDE %set . OPTIONAL {...}}
 * </pre>
 * 
 * </li>
 * 
 * <li>Join all of the named solution sets in (2) back together. For example, if
 * there were two complex optional groups and the required joins resulted in
 * known bound variables for var1 and var2, then those result sets might be
 * combined as follows in the main WHERE clause of the rewritten query.
 * 
 * <pre>
 * INCLUDE %set1 .
 * INCLUDE %set2 JOIN ON (?var1, ?var2) .
 * </pre>
 * 
 * Note: The join variables for those INCLUDEs MUST be identified through static
 * analysis. Failure to use available join variables will result in an extremely
 * inefficient query plan as the full cross product of the solutions will be
 * compared to identify solutions which join.</li>
 * 
 * </ol>
 * 
 * TODO The rewrite into named subquery includes means that we wind up building
 * more hash indices than we strictly require as a hash index will also be built
 * at the start of each optional group. However, since the hash index at the
 * start of the optional group has exactly the same data as the named subquery
 * include's hash index, we should elide the step which builds the extra hash
 * index.
 * 
 * TODO This optimzer should not be strictly necessary at all. The same behavior
 * should arise from running the complex optionals as sub-groups. Based on a few
 * govtrack CI queries, it looks like we do better when the complex optional
 * groups are lifted out as named subqueries. We need to go back and investigate
 * whether or not this is true and why.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTComplexOptionalOptimizer.java 5365 2011-10-19 20:56:18Z
 *          thompsonbry $
 */
public class ASTComplexOptionalOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTComplexOptionalOptimizer.class);
    
    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> exogenousVars = sa.getExogenousVars(
                bindingSets, new LinkedHashSet<IVariable<?>>());

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    convertComplexOptionalGroups(context, sa, namedSubquery,
                            namedSubquery.getWhereClause(), exogenousVars);

                }

            }

        }
        
        // Now process the main where clause.
        convertComplexOptionalGroups(context, sa, queryRoot,
                queryRoot.getWhereClause(), exogenousVars);

        return queryNode;

    }

    /**
     * Using a depth-first recursion, examine each join group. If a join group
     * contains two or more complex optional groups (that is, an optional group
     * which was not translated into a optional statement pattern node) then
     * convert the join group having those complex optional child groups.
     */
    private void convertComplexOptionalGroups(final AST2BOpContext context,
            final StaticAnalysis sa,
            final QueryBase query,
            final GraphPatternGroup<IGroupMemberNode> group,
            final Set<IVariable<?>> exogenousVars) {

        final int arity = group.arity();

        int complexOptionalGroupCount = 0;

        for (int i = 0; i < arity; i++) {

            final BOp child = (BOp) group.get(i);

            if (child instanceof GraphPatternGroup<?>) {

                /*
                 * Note: Do recursion *before* we do the rewrite.
                 */

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                convertComplexOptionalGroups(context, sa, query, childGroup,
                        exogenousVars);

                if (childGroup.isOptional()) {
                    /*
                     * Note: This presumes that simple optionals were already
                     * translated into optional statement patterns.
                     */
                    complexOptionalGroupCount++;
                }

            } else if (child instanceof SubqueryRoot) {

                // Recursion into subqueries.

                final SubqueryRoot subqueryRoot = (SubqueryRoot) child;

                convertComplexOptionalGroups(context, sa, query,
                        subqueryRoot.getWhereClause(), exogenousVars);

            }

        }
        
        if (complexOptionalGroupCount >= 2 && (group instanceof JoinGroupNode)) {

            /*
             * Convert a join group having more than one direct child which is a
             * complex optional group.
             */
            
            convertJoinGroup(context, sa, query, (JoinGroupNode) group,
                    exogenousVars);

        }
        
    }
    
    /**
     * 1. Move the required joins (INCLUDEs, statement pattern nodes, and
     * required subqueries) plus any simple OPTIONALs (which are statement
     * pattern nodes) into a new named subquery (unless the required joins are
     * already a single INCLUDE).
     * <p>
     * 2. Lift each complex optional group into a new named subquery which
     * INCLUDEs the result from (1), replacing it with an INCLUDE of the named
     * subquery in this group.
     * <p>
     * Note: The named solution set from (1) is NOT directly INCLUDEd back into
     * this join group. Instead, it is INCLUDEd into each the named subquery for
     * each complex optional lifted out of this join group.
     * 
     * @param query
     *            The (sub-)query in which the join group appears. We need to
     *            know what is being projected out of the (sub-)query in order
     *            to compute the projections when converting sub-groups into
     *            subqueries.
     * @param group
     *            The join group to be rewritten.
     * @param exogenousVars
     *            The exogenous variables for the query.
     */
    private void convertJoinGroup(final AST2BOpContext context,
            final StaticAnalysis sa, final QueryBase query,
            final JoinGroupNode group, final Set<IVariable<?>> exogenousVars) {

        /*
         * Step 1.
         * 
         * FIXME This MUST recognize the case where everything is already in a
         * single INCLUDE, which is the post-condition for Step 1.  Test this
         * by feeding the post-condition of this step into a unit test.
         */
        
        // The name of the solution set for this join group.
        final String mainSolutionSetName = "--nsr-" + context.nextId();

        // The set of direct children which are complex optional groups.
        final List<JoinGroupNode> complexGroups = new LinkedList<JoinGroupNode>();
        
//        // The list of direct children which were moved.
//        final List<IGroupMemberNode> move = new LinkedList<IGroupMemberNode>();

        {

            final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                    QueryType.SELECT, mainSolutionSetName);

            sa.getQueryRoot().getNamedSubqueriesNotNull().add(nsr);

            final JoinGroupNode whereClause = new JoinGroupNode();
            nsr.setWhereClause(whereClause);
            
            // Move any pre-filters or join-filters to the named subquery.
            {
                for (FilterNode f : sa.getPreFilters(group)) {
                    whereClause.addChild(f);
                }
                for (FilterNode f : sa.getJoinFilters(group)) {
                    whereClause.addChild(f);
                }
            }

            // Move any required joins to the named subquery.
            final IGroupMemberNode[] members = group
                    .toArray(new IGroupMemberNode[] {});
            for(IGroupMemberNode t : members) {
                
                if (t instanceof StatementPatternNode
                        || t instanceof NamedSubqueryInclude
                        || t instanceof SubqueryRoot
                        || t instanceof ServiceNode
                        || t instanceof UnionNode) {
                    // Moved to the named subquery.
//                    move.add(t);
                    group.removeChild(t);
                    whereClause.addChild(t);
                } else if (t instanceof FilterNode) {
                    /*
                     * Leave other filters in place. They depend on something in
                     * the optional groups.
                     */
                } else if(t instanceof AssignmentNode) {
                    /*
                     * Leave assignment nodes in place. They run last.
                     */
                } else if (t instanceof JoinGroupNode) {
                    final JoinGroupNode childGroup = (JoinGroupNode) t;
                    if (childGroup.isOptional()) {
                        /*
                         * This will be moved into a different named subquery in
                         * the next step (below).
                         */
                        complexGroups.add(childGroup);
                    } else {
                        // Move non-optional child group to the named subquery.
                        group.removeChild(t);
                        whereClause.addChild(childGroup);
                    }
                } else {
                    /*
                     * This is a catch all for things which might not have been
                     * considered above.
                     */
                    throw new AssertionError("Not expecting: "+t+" in "+group);
                }

            }

            /*
             * Create the PROJECTION for the lifted named subquery.
             * 
             * Note: Everything which was lifted is no longer present in the
             * WHERE clause. Thus, when computing the projection of the lifted
             * subquery we want to project anything which appeared in the lifted
             * where clause IF it is referenced again by those things which
             * remain in the group (but paying attention to variable scoping for
             * sub-queries).
             * 
             * TODO Make this projection DISTINCT if that does not change the
             * query semantics.
             */
            {

                // All variables which are used within the WHERE clause of the lifted named subquery.
                final Set<IVariable<?>> groupVars = sa.getSpannedVariables(whereClause,
                        new LinkedHashSet<IVariable<?>>());

                // All variables still referenced in the joins or filters of
                // the group (after extracting the named subquery).
                final Set<IVariable<?>> afterVars = sa.getSpannedVariables(
                        (BOp) group, new LinkedHashSet<IVariable<?>>());

                if (query.getProjection() != null) {
                    // Include anything that we must project out of the query.
                    final ProjectionNode tmp = query.getProjection();
                    tmp.getProjectionVars(afterVars);
                }

                final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
                projectedVars.addAll(groupVars);
                projectedVars.retainAll(afterVars);

                final ProjectionNode projection = new ProjectionNode();

                for (IVariable<?> v : projectedVars) {

                    projection.addProjectionVar(new VarNode(v.getName()));

                }

                nsr.setProjection(projection);

            }
            
        }

        // Step 2 (for each direct child complex optional group).
        for (JoinGroupNode childGroup : complexGroups) {

//            log.error("Convert: " + childGroup);

            final String solutionSetName = "--nsr-" + context.nextId();

            final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                    QueryType.SELECT, solutionSetName);

            sa.getQueryRoot().getNamedSubqueriesNotNull().add(nsr);

            final JoinGroupNode whereClause = new JoinGroupNode();
            nsr.setWhereClause(whereClause);

            final NamedSubqueryInclude mainInclude = new NamedSubqueryInclude(
                    mainSolutionSetName);

            whereClause.addChild(mainInclude);

            // Replace the complex optional group with an INCLUDE of the
            // generated solution set back into the main query.

            final NamedSubqueryInclude anInclude = new NamedSubqueryInclude(
                    solutionSetName);
            
            if (group.replaceWith(childGroup, anInclude) != 1)
                throw new AssertionError();

            whereClause.addChild(childGroup);

            /*
             * Create the projection for the named subquery.
             * 
             * TODO Make this projection DISTINCT if that does not change the
             * query semantics.
             */
            {

                final Set<IVariable<?>> projectedVars = sa.getProjectedVars(
                        anInclude, whereClause, query, exogenousVars,
                        new LinkedHashSet<IVariable<?>>());

                final ProjectionNode projection = new ProjectionNode();

                for (IVariable<?> v : projectedVars) {

                    projection.addProjectionVar(new VarNode(v.getName()));

                }

                nsr.setProjection(projection);

            }

        }
        
//        /*
//         * Clean up. We need to remove anything which was moved.
//         * 
//         * Note: We can't easily do this while iterating over the group members
//         * so the things which get moved are collected up and then removed in a
//         * clean up step.
//         */
//        for (IGroupMemberNode t : move) {
//        
//            group.removeArg((BOp) t);
//            
//        }

    }

    private void liftSparql11Subquery(final AST2BOpContext context,
            final StaticAnalysis sa, final SubqueryRoot subqueryRoot) {

        final String newName = "-subSelect-" + context.nextId();

        final NamedSubqueryInclude include = new NamedSubqueryInclude(newName);

        final IGroupNode<IGroupMemberNode> parent = subqueryRoot.getParent();

        /*
         * Note: A SubqueryRoot normally starts out as the sole child of a
         * JoinGroupNode. However, other rewrites may have written out that
         * JoinGroupNode and it does not appear to be present for an ASK
         * subquery.
         * 
         * Therefore, when the parent of the SubqueryRoot is a JoinGroupNode
         * having the SubqueryRoot as its only child, we use the parent's parent
         * in order to replace the JoinGroupNode when we lift out the
         * SubqueryRoot. Otherwise we use the parent since there is no wrapping
         * JoinGroupNode (or if there is, it has some other stuff in there as
         * well).
         */
         
        if ((parent instanceof JoinGroupNode) && ((BOp) parent).arity() == 1
                && parent.getParent() != null) {
            
            final IGroupNode<IGroupMemberNode> pp = parent.getParent();

            // Replace the sub-select with the include.
            if (((ASTBase) pp).replaceWith((BOp) parent, include) == 0)
                throw new AssertionError();

        } else {

            // Replace the sub-select with the include.
            if (((ASTBase) parent).replaceWith((BOp) subqueryRoot, include) == 0)
                throw new AssertionError();
            
        }

        final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                subqueryRoot.getQueryType(), newName);

        nsr.setConstruct(subqueryRoot.getConstruct());
        nsr.setGroupBy(subqueryRoot.getGroupBy());
        nsr.setHaving(subqueryRoot.getHaving());
        nsr.setOrderBy(subqueryRoot.getOrderBy());
        nsr.setProjection(subqueryRoot.getProjection());
        nsr.setSlice(subqueryRoot.getSlice());
        nsr.setWhereClause(subqueryRoot.getWhereClause());

        sa.getQueryRoot().getNamedSubqueriesNotNull().add(nsr);

    }

}
