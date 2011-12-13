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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.constraints.SparqlTypeErrorBOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;

/**
 * Rewrites aspects of queries where bottom-up evaluation would produce
 * different results. This includes joins which are not "well designed" as
 * defined in section 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge
 * Pérez et al and also FILTERs on variables whose bindings are not in scope.
 * <p>
 * Note: The test suite for this class is a set of DAWG tests which focus on
 * bottom up evaluation semantics, including:
 * <p>
 * Nested Optionals - 1 (Query is not well designed because there are no shared
 * variables in the intermediate join group and there is an embedded OPTIONAL
 * join group. Since ?v is not present in the intermediate join group the (:x3
 * :q ?w . OPTIONAL { :x2 :p ?v }) solutions must be computed first and then
 * joined against the (:x1 :p ?v) solutions.)
 * 
 * <pre>
 * SELECT *
 * { 
 *     :x1 :p ?v .
 *     OPTIONAL
 *     {
 *       :x3 :q ?w .
 *       OPTIONAL { :x2 :p ?v }
 *     }
 * }
 * </pre>
 * 
 * Filter-scope - 1 (Query is not well designed because there are no shared
 * variables in the intermediate join group and there is an embedded OPTIONAL
 * join group. Also, ?v is used in the FILTER but is not visible in that scope.)
 * 
 * <pre>
 * SELECT *
 * { 
 *     :x :p ?v . 
 *     { :x :q ?w 
 *       OPTIONAL {  :x :p ?v2 FILTER(?v = 1) }
 *     }
 * }
 * </pre>
 * 
 * Join-scope - 1 (Query is not well designed because there are no shared
 * variables in the intermediate group and there is an embedded OPTIONAL join
 * group.)
 * 
 * <pre>
 * SELECT *
 * { 
 *   ?X  :name "paul"
 *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
 * }
 * </pre>
 * 
 * Filter-nested - 2 (Filter on variable ?v which is not in scope)
 * 
 * <pre>
 * SELECT ?v
 * { :x :p ?v . { FILTER(?v = 1) } }
 * </pre>
 * 
 * Nested groups which do not share variables with their parent can be lifted
 * out into a named subquery. This has the same effect as bottom up evaluation
 * since we will run the named subquery first and then perform the join against
 * the parent group. However, in this case an exogenous binding which causes a
 * shared variable to exist would mean that the query could run normally since
 * the value in the outer group and the inner group would now be correlated
 * through the exogenous binding. E.g., <code?X</code> in the last example
 * above.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
 * @see http://www.dcc.uchile.cl/~cgutierr/papers/sparql.pdf
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTBottomUpOptimizer.java 5189 2011-09-14 17:56:53Z thompsonbry
 *          $
 * 
 *          TODO I have been assuming that the presence of any shared variable
 *          is enough to enforce correlation between the solution sets and cause
 *          the results of bottom up evaluation to be the same as our standard
 *          evaluation model. If this is not true then we could just lift
 *          everything into a named subquery, order the named subqueries by
 *          their dependencies and just let it run.
 */
public class ASTBottomUpOptimizer implements IASTOptimizer {

    /**
     * Used for the prefix of the generated named set name.
     */
    static String NAMED_SET_PREFIX = "%-bottom-up-";
    
    /**
     * 
     */
    public ASTBottomUpOptimizer() {
    }

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // All exogenous variables (given in the source solutions).
        final Set<IVariable<?>> exogenousVars = StaticAnalysis.getExogenousVars(
                bindingSets, new LinkedHashSet<IVariable<?>>());

        /*
         * Rewrite badly designed left joins by lifting them into a named
         * subquery.
         */
        {

            /*
             * Collect optional groups.
             * 
             * Note: We can not transform graph patterns inside of SERVICE calls
             * so this explicitly visits the interesting parts of the tree.
             */

            final StaticAnalysis sa = new StaticAnalysis(queryRoot);

            // List of the inner optional groups for badly designed left joins.
            final List<JoinGroupNode> innerOptionalGroups = new LinkedList<JoinGroupNode>();

            {

                if (queryRoot.getNamedSubqueries() != null) {

                    for (NamedSubqueryRoot namedSubquery : queryRoot
                            .getNamedSubqueries()) {

                        @SuppressWarnings("unchecked")
                        final GraphPatternGroup<IGroupMemberNode> group = (GraphPatternGroup<IGroupMemberNode>) namedSubquery
                                .getWhereClause();

                        checkForBadlyDesignedLeftJoin(sa, group,
                                innerOptionalGroups);

                    }

                }

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> group = (GraphPatternGroup<IGroupMemberNode>) queryRoot
                        .getWhereClause();

                checkForBadlyDesignedLeftJoin(sa, group, innerOptionalGroups);

            }

            /*
             * Convert badly designed left joins into named subqueries. This
             * gives the join group effective "bottom-up" evaluation semantics
             * since we will run the named subqueries before we run anything
             * else.
             */

            for (JoinGroupNode group : innerOptionalGroups) {

                liftBadlyDesignedLeftJoin(context, sa, queryRoot, group);

            }

        }

        /*
         * Hide variables which would not be in scope for bottom up evaluation.
         */
        {

            final StaticAnalysis sa = new StaticAnalysis(queryRoot);

            // Map for renamed variables.
            final Map<IVariable<?>/* old */, IVariable<?>/* new */> map = new LinkedHashMap<IVariable<?>, IVariable<?>>();

            // Handle named subqueries.
            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

//                    @SuppressWarnings("unchecked")
//                    final GraphPatternGroup<IGroupMemberNode> group = (GraphPatternGroup<IGroupMemberNode>) namedSubquery
//                            .getWhereClause();
//
//                    // WHERE clause for the named subquery.
//                    handleFiltersWithVariablesNotInScope(context, sa,
//                            queryRoot, exogenousVars, map, namedSubquery, group);
                    handleFiltersWithVariablesNotInScope(context, sa, namedSubquery,
                            bindingSets);

                }

            }

//            @SuppressWarnings("unchecked")
//            final GraphPatternGroup<IGroupMemberNode> group = (GraphPatternGroup<IGroupMemberNode>) queryRoot
//                    .getWhereClause();
//
////             The main WHERE clause.
//            handleFiltersWithVariablesNotInScope(context, sa, queryRoot,
//                    exogenousVars, map, queryRoot, group);
            handleFiltersWithVariablesNotInScope(context, sa, queryRoot, bindingSets);

        }

        /*
         * Handle MINUS when it appears without shared variables.
         */
        {

            final StaticAnalysis sa = new StaticAnalysis(queryRoot);

            // Handle named subqueries.
            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    // WHERE clause for the named subquery.
                    handleMinusWithoutSharedVariables(context, sa,
                            namedSubquery.getWhereClause());

                }

            }

            handleMinusWithoutSharedVariables(context, sa,
                    queryRoot.getWhereClause());

        }

        return queryNode;
    
    }

    /**
     * We are looking for queries of the form:
     * 
     * <pre>
     * P = ((?X, name, paul) OPT ((?Y, name, george) OPT (?X, email, ?Z)))
     * </pre>
     * 
     * i.e. variables used by the right side of a left join that are not bound
     * in the parent group but are bound in groups above the parent group.
     */
    private void checkForBadlyDesignedLeftJoin(
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> whereClause,
            final List<JoinGroupNode> badlyDesignedLeftJoins) {

        // Check all join groups.
        final Iterator<JoinGroupNode> itr = BOpUtility.visitAll(
                (BOp) whereClause, JoinGroupNode.class);
        
        while(itr.hasNext()) {
            
            final JoinGroupNode group = itr.next();
            
            if(!group.isOptional()) {
                // Ignore non-optional join groups.
                continue;
            }

            /*
             * This is a candidate for an inner join group of a badly designed
             * optional join pattern, so check it in depth.
             */
            checkForBadlyDesignedLeftJoin2(sa, group, badlyDesignedLeftJoins);
   
        }
        
    }

    /**
     * Identify problem variables. These are variables appear in joins within an
     * optional <i>group</i>, but which do appear in joins in the groups's
     * parent but do appear in joins in some parent of that parent.
     * <p>
     * Under bottom up evaluation semantics, the variable become bound from the
     * inner most nested group first. This means that the optional group can
     * join with its parent, producing bindings for a variable not shared
     * transitively by its parent with its parent's parents. For example, the
     * <code>?X</code> in the inner optional will have already been joined with
     * the <code>?Y</code> and is only then joined with the access path for
     * <code>?X :name "paul"</code>. If there was an optional join for
     * <code>?X</code>, then <code>?X</code> will already be bound for that
     * solution in that access path. Under these circumstances, bottom up
     * evaluation can produce different results than left-to-right evaluation.
     * <p>
     * In the data set for this query, while there are solutions for
     * <code>?X name "paul"</code>, there is no solution for
     * <code>?X name "paul"</code> for which <code>?X :email ?Z</code> is also
     * true. Hence, this query has no solutions in the data.
     * 
     * <pre>
     * SELECT *
     * { 
     *   ?X  :name "paul"
     *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
     * }
     * </pre>
     * 
     * (This query is <code>var-scope-join-1</code> from the DAWG compliance
     * test suite.)
     * <ol>
     * <li>Add all vars used in the group (statement patterns and filters)</li>
     * <li>Remove all vars bound by the parent group (statement patterns)</li>
     * <li>Retain all vars from the grandparent groups (statement patterns)</li>
     * </ol>
     * 
     * @param sa
     * @param group
     *            A group to inspect. It is is an optional group, then we
     *            consider this as a candidate for a badly designed left join
     *            pattern. Otherwise we recursively descend into the group.
     * @param badlyDesignedLeftJoins
     *            A list of all badly designed left joins which have been
     *            identified.
     * 
     *            TODO This ignores the exogenous variables. unit test for this
     *            case and fix.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
     */
    private void checkForBadlyDesignedLeftJoin2(
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group,
            final List<JoinGroupNode> badlyDesignedLeftJoins) {

        assert group.isOptional();

        /*
         * Check to see whether this is the inner optional of a badly designed
         * left-join pattern.
         */

        final IGroupNode<? extends IGroupMemberNode> p = group
                .getParentJoinGroup();

        if (p == null) {
            // No parent.
            return;
        }

//        if(((JoinGroupNode)p).isMinus()) return;
        
        final IGroupNode<? extends IGroupMemberNode> pp = p
                .getParentJoinGroup();

        if (pp == null) {
            // No parent's parent.
            return;
        }

        /*
         * This is all definitely bound variables above the candidate optional
         * group in the hierarchy.
         * 
         * Note: [topDownVars] needs to be reset on each entry with a new Set to
         * avoid side-effects when we recursively explore sibling groups for
         * this pattern. This method was rewritten without recursion to avoid
         * that problem. It is now driven out of an iterator visiting the
         * candidate optional join groups.
         */
        final Set<IVariable<?>> topDownVars = sa.getDefinitelyIncomingBindings(
                p, new LinkedHashSet<IVariable<?>>());

        /*
         * Obtain the set of variables used in JOINs -OR- FILTERs within this
         * optional group.
         * 
         * Note: We must consider the variables used in filters as well when
         * examining a candidate inner optional group for a badly designed left
         * join. This is necessary in order to capture uncorrelated variables
         * having the same name in the FILTER and in the parent's parent.
         */
        final Set<IVariable<?>> innerGroupVars = sa
                .getDefinitelyProducedBindingsAndFilterVariables(group,
                        new LinkedHashSet<IVariable<?>>());

        /*
         * Obtain the set of variables used in joins within the parent join
         * group.
         */
        final Set<IVariable<?>> parentVars = sa.getDefinitelyProducedBindings(
                (IBindingProducerNode) p, new LinkedHashSet<IVariable<?>>(),
                false/* recursive */);

        /*
         * The inner optional is part of a badly designed left join if it uses
         * variables which are not present in the parent but which are present
         * in the group hierarchy above that parent.
         */

        // remove all variables declared by the parent.
        innerGroupVars.removeAll(parentVars);

        // retain all variables declared by the parent's parent.
        innerGroupVars.retainAll(topDownVars);

        if (!innerGroupVars.isEmpty()) {

            badlyDesignedLeftJoins.add((JoinGroupNode) group);

        }

    }

    /**
     * If the {@link JoinGroupNode} qualifies as a badly designed left join then
     * lift it into a {@link NamedSubqueryRoot} and replace it with a
     * {@link NamedSubqueryInclude}.
     * 
     * @param group
     *            The OPTIONAL join group. This group and its parent
     *            {@link JoinGroupNode} will be lifted out and replaced by a
     *            {@link NamedSubqueryInclude}.
     */
    private void liftBadlyDesignedLeftJoin(final AST2BOpContext context,
            final StaticAnalysis sa, final QueryRoot queryRoot,
            final JoinGroupNode group) {

        // The parent join group.
        final JoinGroupNode p = group.getParentJoinGroup();

        if (p == null)
            throw new AssertionError();

        // The parent's parent join group.
        final JoinGroupNode pp = p.getParentJoinGroup();

        if (pp == null)
            throw new AssertionError();

        final String namedSet = context.createVar(NAMED_SET_PREFIX);

        final NamedSubqueryRoot nsr = new NamedSubqueryRoot(QueryType.SELECT,
                namedSet);
        {
        
            {
            
                final ProjectionNode projection = new ProjectionNode();

                nsr.setProjection(projection);
                
                final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
                
                sa.getMaybeProducedBindings(p, vars, true/* recursive */);
                
                for (IVariable<?> var : vars) {
                
                    projection.addProjectionVar(new VarNode(var.getName()));
                    
                }

            }
            
            nsr.setWhereClause(p);

            queryRoot.getNamedSubqueriesNotNull().add(nsr);
            
        }
        
        final NamedSubqueryInclude nsi = new NamedSubqueryInclude(namedSet);

        if (p.isOptional()) {

            /*
             * TODO This is a hack because the INCLUDE operation (a solution
             * set hash join) does not currently support OPTIONAL. As a
             * workaround the INCLUDE is stuffed into an OPTIONAL group.
             */

            final JoinGroupNode tmp = new JoinGroupNode();
            
            tmp.setOptional(true);
            
            tmp.addChild(nsi);

            pp.replaceWith(p, tmp);

        } else if (p.isMinus()) {

            /*
             * TODO This is a hack because the INCLUDE operation does not
             * currently support MINUS. As a workaround the INCLUDE is stuffed
             * into an MINUS group.
             */

            final JoinGroupNode tmp = new JoinGroupNode();
            
            tmp.setMinus(true);

            tmp.addChild(nsi);

            pp.replaceWith(p, tmp);

        } else {

            // Replace with named subquery INCLUDE.
            pp.replaceWith(p, nsi);
        
        }
       
    }
    
    /**
     * Examine each {@link JoinGroupNode} in the query and each FILTER in each
     * {@link JoinGroupNode}. If the filter depends on a variable which is not
     * in scope then we must rewrite the AST in order to preserve bottom up
     * evaluation semantics.
     * <p>
     * Such filters and variables are identified. The variables within the
     * filters are then rewritten in a consistent manner across the filters
     * within the group, renaming the provably unbound variables in the filters
     * to anonymous variables. This provides effective bottom up evaluation
     * scope for the variables.
     * 
     * FIXME This should be an "in-scope" test.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/414 (SPARQL 1.1
     *      EXISTS, NOT EXISTS, and MINUS)
     */
//    private void handleFiltersWithVariablesNotInScope(
//            final AST2BOpContext context, final StaticAnalysis sa,
//            final QueryRoot queryRootx, final Set<IVariable<?>> exogenousVars,
//            final Map<IVariable<?>/*old*/,IVariable<?>/*new*/> map,
//            final QueryBase queryBase,
//            final GraphPatternGroup<IGroupMemberNode> group
//            ) {
//
//        /*
//         * All variables potentially bound by joins in this group or a subgroup.
//         */
//        final Set<IVariable<?>> maybeBound = sa.getMaybeProducedBindings(group,
//                new LinkedHashSet<IVariable<?>>(), true/* recursive */);
//
//        // All variables appearing in the source solutions.
//        maybeBound.addAll(exogenousVars);
//
//        if (group.isOptional()) {
//
//            /*
//             * "A FILTER inside an OPTIONAL can reference a variable bound in
//             * the required part of the OPTIONAL."
//             * 
//             * Note: This is ONLY true when the [group] is OPTIONAL. Otherwise
//             * the variables in the parent are not visible.
//             */
//
//            // The "required" part of the optional is the parent group.
//            final JoinGroupNode p = group.getParentJoinGroup();
//
//            if (p != null) {
//
//                // bindings "maybe" produced in the parent (non-recursive)
//                final Set<IVariable<?>> incomingBound = sa
//                        .getMaybeProducedBindings(p,
//                                new LinkedHashSet<IVariable<?>>(), false/* recursive */);
//
//                // add to those visible in FILTERs for this group.
//                maybeBound.addAll(incomingBound);
//
//            }
//
//        }
//
//        // For everything in this group.
//        for (IGroupMemberNode child : group) {
//
//            // Only consider the FILTERs.
//            if (!(child instanceof FilterNode))
//                continue;
//
//            final FilterNode filter = (FilterNode) child;
//
//            if (rewriteUnboundVariablesInFilter(context, maybeBound, map,
//                    null/* parent */, filter.getValueExpressionNode())) {
//
//                /*
//                 * Re-generate the IVE for this filter.
//                 */
//
//                // clear the old value expression.
//                filter.getValueExpressionNode().setValueExpression(null);
//
//                // re-generate the value expression.
//                AST2BOpUtility.toVE(context.getLexiconNamespace(),
//                        filter.getValueExpressionNode());
//
//            }
//
//        }
//        
//    }

 /* Old version.
  *
  * Note: This will see ALL join groups, including those in a SERVICE or
  * (NOT) EXISTS annotation. This approach makes it impossible for us to
  * identify when the FILTER is in a (NOT) EXISTS graph pattern since the
  * graph pattern appears as an annotation and is not back linked from
  * the FILTER in which it appears. What we need to do instead is descend
  * recursively, building up a list of the variables which are visible in
  * each scope.
  */
    private void handleFiltersWithVariablesNotInScope(
            final AST2BOpContext context,
            final StaticAnalysis sa,
            final QueryBase queryBase,
            final IBindingSet[] bindingSets) {

//        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // All exogenous variables (given in the source solutions).
        final Set<IVariable<?>> exogenous = StaticAnalysis.getExogenousVars(
                bindingSets, new LinkedHashSet<IVariable<?>>());

        // Map for renamed variables.
        final Map<IVariable<?>/* old */, IVariable<?>/* new */> map = new LinkedHashMap<IVariable<?>, IVariable<?>>();

        /*
         * Visit all join groups, which is where the filters are found.
         */
        final Iterator<JoinGroupNode> itr = BOpUtility.visitAll(
                queryBase.getWhereClause(), JoinGroupNode.class);

        while (itr.hasNext()) {

            final JoinGroupNode group = itr.next();

            if (sa.findParent(group) instanceof FilterNode) {
                /*
                 * Skip EXISTS and NOT EXISTS graph patterns when they are
                 * visited directly. These are handled when we visit the join
                 * group containing the FILTER in which they appear.
                 * 
                 * Note: The only time that findParent() will report a
                 * FilterNode is when either EXISTS or NOT EXISTS is used and
                 * the group is the graph pattern for those functions.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/414
                 * (SPARQL 1.1 EXISTS, NOT EXISTS, and MINUS)
                 */
                continue;
            }
            
            /*
             * All variables potentially bound by joins in this group or a
             * subgroup.
             */
            final Set<IVariable<?>> maybeBound = sa
                    .getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */);
            
            // All variables appearing in the source solutions.
            maybeBound.addAll(exogenous);

            if (group.isOptional()) {

                /*
                 * "A FILTER inside an OPTIONAL can reference a variable
                 * bound in the required part of the OPTIONAL."
                 * 
                 * Note: This is ONLY true when the [group] is OPTIONAL.
                 * Otherwise the variables in the parent are not visible.
                 */

                // The "required" part of the optional is the parent group.
                final JoinGroupNode p = group.getParentJoinGroup();
                
                if (p != null) {
                    
                    // bindings "maybe" produced in the parent (non-recursive)
                    final Set<IVariable<?>> incomingBound = sa
                            .getMaybeProducedBindings(p,
                                    new LinkedHashSet<IVariable<?>>(), false/* recursive */);

                    // add to those visible in FILTERs for this group.
                    maybeBound.addAll(incomingBound);
                
                }
                
            }

            // For everything in this group.
            for (IGroupMemberNode child : group) {

                // Only consider the FILTERs.
                if (!(child instanceof FilterNode))
                    continue;

                final FilterNode filter = (FilterNode) child;

                if(rewriteUnboundVariablesInFilter(context, maybeBound, map,
                        null/* parent */, filter.getValueExpressionNode())) {
                    
                    /*
                     * Re-generate the IVE for this filter.
                     */

                    // clear the old value expression.
                    filter.getValueExpressionNode().setValueExpression(null);
                    
                    // re-generate the value expression.
                    AST2BOpUtility.toVE(context.getLexiconNamespace(),
                            filter.getValueExpressionNode());
                    
                }

            }

        }
        
    }

    /**
     * If a FILTER depends on a variable which is not in scope for that filter
     * then that variable will always be unbound in that scope. However, we can
     * not fail the entire filter since it could use <code>BOUND(var)</code>.
     * This takes the approach of rewriting the FILTER to use an anonymous
     * variable for any variable which is provably not bound.
     * <p>
     * Note: The alternative approach is to replace the unbound variable with a
     * type error. However, BOUND(?x) would have to be "replaced" by setting its
     * {@link IValueExpressionNode} to [false]. Also, COALESCE(....) could use
     * an unbound variable and no type error should be thrown. We either have to
     * remove the expression the unbound variable shows up in from the
     * COALESCE() or change it to an anonymous variable. If you want to pursue
     * this approach see {@link SparqlTypeErrorBOp#INSTANCE}.
     * 
     * @param context
     *            The context is used to generate anonymous variables.
     * @param maybeBound
     *            The set of variables which are in scope in the group.
     * @param map
     *            A map used to provide consistent variable renaming in the
     *            filters of the group.
     * @param parent
     *            The parent {@link IValueExpressionNode}.
     * @param node
     *            An {@link IValueExpressionNode}. If this is a {@link VarNode}
     *            and the variable is not in scope, then the {@link VarNode} is
     *            replaced in the parent by an anonymous variable.
     * 
     * @return <code>true</code> if the expression was modified and its
     *         {@link IValueExpressionNode} needs to be rebuilt.
     * 
     * @see AST2BOpUtility#toVE(String, IValueExpressionNode)
     */
    private boolean rewriteUnboundVariablesInFilter(final AST2BOpContext context,
            final Set<IVariable<?>> maybeBound,
            final Map<IVariable<?>/* old */, IVariable<?>/* new */> map,
            final IValueExpressionNode parent, final IValueExpressionNode node) {

        boolean modified = false;
        // recursion.
        {
            
            final int arity = ((BOp) node).arity();
            
            for (int i = 0; i < arity; i++) {
            
                final BOp tmp = ((BOp) node).get(i);
                
                if(!(tmp instanceof IValueExpressionNode))
                    continue;
                
                final IValueExpressionNode child = (IValueExpressionNode) tmp;
                
                modified |= rewriteUnboundVariablesInFilter(context,
                        maybeBound, map, node, child);
                
            }
            
        }

        if (!(node instanceof VarNode)) {
            // Not a variable.
            return modified;
        }

        final VarNode varNode = (VarNode) node;

        final IVariable<?> ovar = varNode.getValueExpression();

        if (maybeBound.contains(ovar)) {
            // A variable which might be bound during evaluation.
            return modified;
        }

        /*
         * A variable which is provably not bound.
         * 
         * Note: In order to mimic the variable scope for bottom-up evaluation
         * we need to "hide" this variable.
         */
        IVariable<?> nvar = map.get(ovar);
        
        if(nvar == null) {
            
            /*
             * An anonymous variable which will never be bound by the query. The
             * map is used to share the replace an unbound variable with the
             * corresponding anonymous variable in the same manner throughout
             * the group.
             */
            map.put(ovar,
                    nvar = Var.var(context.createVar("-unbound-var-"
                            + ovar.getName() + "-")));

        }

        ((ASTBase) parent).replaceAllWith(ovar, nvar);

        return true;

    }

    /**
     * Handle MINUS when it appears without shared variables. We just get rid of
     * the MINUS group since it can not interact with the parent group without
     * shared variables (without shared variables, nothing joins and if nothing
     * joins then nothing is removed from the parent).
     * 
     * @param context
     * @param group
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void handleMinusWithoutSharedVariables(
            final AST2BOpContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<?> group) {

        int arity = group.arity();

        for (int i = 0; i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) group.get(i);

            if (!(child instanceof GraphPatternGroup)) {
                
                continue;
                
            }

            final GraphPatternGroup<?> childGroup = (GraphPatternGroup<?>)child;

            /*
             * Recursion.
             */
            handleMinusWithoutSharedVariables(context, sa,
                        childGroup);

            /*
             * Examine this child.
             */
            if(childGroup.isMinus()) {

                // FIXME This should be an "in-scope" test (not MUST|MAYBE).
                final Set<IVariable<?>> incomingBound = sa
                        .getDefinitelyIncomingBindings(childGroup,
                                new LinkedHashSet<IVariable<?>>());

                final Set<IVariable<?>> maybeProduced = sa
                        .getMaybeProducedBindings(childGroup,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */);

                final Set<IVariable<?>> intersection = new LinkedHashSet<IVariable<?>>(
                        incomingBound);

                intersection.retainAll(maybeProduced);

//                System.err.println("intersection=" + intersection + ", incoming="
//                        + incomingBound + ", produced=" + maybeProduced);
                
                if (intersection.isEmpty()) {

                    // Remove the MINUS operator. It can not have any effect.
                    
                    ((IGroupNode) group).removeChild(childGroup);
                    
                    i++;

                }
                
            }
            
        }

    }

}
