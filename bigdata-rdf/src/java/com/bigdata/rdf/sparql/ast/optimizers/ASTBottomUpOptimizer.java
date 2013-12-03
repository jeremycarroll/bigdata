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

import java.util.ArrayList;
import java.util.Collections;
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
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
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
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;

/**
 * Rewrites aspects of queries where bottom-up evaluation would produce
 * different results. This includes joins which are not "well designed" as
 * defined in section 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge
 * Perez et al and also FILTERs on variables whose bindings are not in scope.
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

        findAndLiftBadlyDesignedLeftJoins(context, queryRoot);

        findAndHideVariablesNotInScope(context, queryRoot, bindingSets);

        findAndRewriteSomeMinusGroups(context, queryRoot);

        return queryNode;
    
    }

    /**
     * MINUS groups may be obviously unnecessary, or may need to be rewritten as named sub queries.
     * Most are OK.
     * @param context
     * @param queryRoot
     */
	private void findAndRewriteSomeMinusGroups(final AST2BOpContext context, final QueryRoot queryRoot) {
		/*
         * Handle MINUS when it appears without shared variables.
         */
        {

            final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);
            final List<JoinGroupNode> badlyDesignedMinusJoins = new LinkedList<JoinGroupNode>();

            // Handle named subqueries.
            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    // WHERE clause for the named subquery.
                    analyzeMinusGroups(context, sa,
                            namedSubquery.getWhereClause(), badlyDesignedMinusJoins);

                }

            }

            analyzeMinusGroups(context, sa,
                    queryRoot.getWhereClause(), badlyDesignedMinusJoins);
            

            liftBadlyDesignedJoins(context, sa, badlyDesignedMinusJoins);

        }
	}

	private void findAndHideVariablesNotInScope(final AST2BOpContext context, final QueryRoot queryRoot,
			final IBindingSet[] bindingSets) {
		/*
         * Hide variables which would not be in scope for bottom up evaluation.
         */
        {

            final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

            // Handle named subqueries.
            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    handleFiltersWithVariablesNotInScope(context, sa,
                            namedSubquery, bindingSets);

                }

            }

            handleFiltersWithVariablesNotInScope(context, sa, queryRoot,
                    bindingSets);

        }
	}

	private void findAndLiftBadlyDesignedLeftJoins(final AST2BOpContext context, final QueryRoot queryRoot) {
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

            final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

            // List of the inner optional groups for badly designed left joins.
            final List<JoinGroupNode> innerOptionalGroups = new LinkedList<JoinGroupNode>();

            {

                if (queryRoot.getNamedSubqueries() != null) {

                    for (NamedSubqueryRoot namedSubquery : queryRoot
                            .getNamedSubqueries()) {

                        @SuppressWarnings("unchecked")
                        final GraphPatternGroup<IGroupMemberNode> group = (GraphPatternGroup<IGroupMemberNode>) namedSubquery
                                .getWhereClause();

                        checkForBadlyDesignedLeftJoin(context, sa, group,
                                innerOptionalGroups);

                    }

                }

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> group = (GraphPatternGroup<IGroupMemberNode>) queryRoot
                        .getWhereClause();

                checkForBadlyDesignedLeftJoin(context, sa, group,
                        innerOptionalGroups);

            }

            liftBadlyDesignedJoins(context, sa, innerOptionalGroups);

        }
	}

	/**
	 * The badly designed children are things like optionals or minuses
	 * which need to be run in the context of their siblings, but not their parent's siblings.
	 * @param context
	 * @param sa
	 * @param badlyDesignedJoinGroupChildren
	 */
	private void liftBadlyDesignedJoins(final AST2BOpContext context, final StaticAnalysis sa,
			final List<JoinGroupNode> badlyDesignedJoinGroupChildren) {
		/*
		 * Convert badly designed left joins into named subqueries. This
		 * gives the join group effective "bottom-up" evaluation semantics
		 * since we will run the named subqueries before we run anything
		 * else.
		 */

		for (JoinGroupNode group : badlyDesignedJoinGroupChildren) {

		    liftBadlyDesignedJoin(context, sa, group);

		}
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
            final IEvaluationContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> whereClause,
            final List<JoinGroupNode> badlyDesignedLeftJoins) {

        // Check all join groups.
        final Iterator<JoinGroupNode> itr = BOpUtility.visitAll(
                (BOp) whereClause, JoinGroupNode.class);
        
        while(itr.hasNext()) {
            
            final JoinGroupNode group = itr.next();

            if (!group.isOptional()) {
                // Ignore non-optional join groups.
                continue;
            }

            /*
             * This is a candidate for an inner join group of a badly designed
             * optional join pattern, so check it in depth.
             */
            checkForBadlyDesignedLeftJoin2(context, sa, group,
                    badlyDesignedLeftJoins);

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
     *            FIXME This ignores the exogenous variables. unit test for this
     *            case and fix. [A variable would have to be bound is all
     *            exogenous solutions in order to allow us to avoid the rewrite
     *            for bottom up semantics. E.g., it would have to be a member of
     *            {@link ISolutionSetStats#getAlwaysBound()} but not a member of
     *            {@link ISolutionSetStats#getConstants()} since it does not
     *            have to be bound to the same value in each solution].
     *            <p>
     *            I have made a partial fix. However, an exogenous variable IS
     *            NOT visible within a subquery unless it is projected into that
     *            subquery. Thus, it is incorrect to simply consult
     *            {@link ISolutionSetStats#getAlwaysBound()}
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
     */
    private void checkForBadlyDesignedLeftJoin2(
            final IEvaluationContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group,
            final List<JoinGroupNode> badlyDesignedLeftJoins) {

        assert group.isOptional();

        /*
         * Check to see whether this is the inner optional of a badly designed
         * left-join pattern.
         */

        final IGroupNode<? extends IGroupMemberNode> p =
//                sa.findParentJoinGroup(group)
                group.getParentJoinGroup()
                ;

        if (p == null) {
            // No parent.
            return;
        }
//        System.err.println("Considering: "+group);
        
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

        /*
         * Remove any variables which are bound in all of the exogenous
         * solutions. These are visible everywhere (except within a subquery if
         * they are not projected into that subquery).
         * 
         * FIXME This is not a 100% correct fix. The problem is that it ignores
         * the variable scoping rules for a subquery. Variables are only visible
         * within a subquery if they are projected into that subquery, even if
         * the binding is exogenous.  The correct fix is to lift this into
         * StaticAnalyis#getDefinitelyProducedBindings(), and which point the
         * line below can be removed as it will have been correctly handled by
         * the method on StaticAnalysis.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
         */
        innerGroupVars.removeAll(context.getSolutionSetStats().getAlwaysBound());

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
    private void liftBadlyDesignedJoin(final AST2BOpContext context,
            final StaticAnalysis sa,
            final JoinGroupNode group) {

        // The parent join group.
        final JoinGroupNode p = group.getParentJoinGroup();

        if (p == null)
            throw new AssertionError();
        
        liftBadlyDesignedJoinGroup(context, sa, p);
       
    }

	private void liftBadlyDesignedJoinGroup(final AST2BOpContext context, final StaticAnalysis sa,
			final JoinGroupNode groupWithBadlyDesignedChild) throws AssertionError {
		final QueryRoot queryRoot = sa.getQueryRoot();
        
        final IGroupNode<IGroupMemberNode> pp = groupWithBadlyDesignedChild.getParent();
        

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
                
                sa.getMaybeProducedBindings(groupWithBadlyDesignedChild, vars, true/* recursive */);
                
                for (IVariable<?> var : vars) {
                
                    projection.addProjectionVar(new VarNode(var.getName()));
                    
                }

            }
            
            nsr.setWhereClause(groupWithBadlyDesignedChild);

            queryRoot.getNamedSubqueriesNotNull().add(nsr);
            
        }
        
        final NamedSubqueryInclude nsi = new NamedSubqueryInclude(namedSet);
        final IGroupMemberNode replacement;
        final JoinGroupNode wrappedNsi = new JoinGroupNode();
        wrappedNsi.addChild(nsi);
        
        // We have to decide whether to use the nsi as is, or wrapped in a JoinGroup
        // The cases are: if we have top set Optional or Minus then we need to wrap
        // If the parent is not a JoinGroup then we need to wrap.

        if (groupWithBadlyDesignedChild.isOptional()) {
            
        	wrappedNsi.setOptional(true);
            groupWithBadlyDesignedChild.setOptional(false);
            replacement = wrappedNsi;

        } else if (groupWithBadlyDesignedChild.isMinus()) {
        	wrappedNsi.setMinus(true);
            groupWithBadlyDesignedChild.setMinus(false);
            replacement = wrappedNsi;
        } else if (pp instanceof JoinGroupNode) {
        	replacement = nsi;
        } else {
        	replacement = wrappedNsi;
        }
        pp.replaceWith(groupWithBadlyDesignedChild, replacement);
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
     * <p>
     * Note: This will see ALL join groups, including those in a SERVICE or
     * (NOT) EXISTS annotation. Therefore, we use findParent() to identify when
     * the FILTER is in a (NOT) EXISTS graph pattern since the graph pattern
     * appears as an annotation and is not back linked from the FILTER in which
     * it appears.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/414 (SPARQL 1.1
     *      EXISTS, NOT EXISTS, and MINUS)
     */
    private void handleFiltersWithVariablesNotInScope(
            final AST2BOpContext context,
            final StaticAnalysis sa,
            final QueryBase queryBase,
            final IBindingSet[] bindingSets) {

        // All exogenous variables (given in the source solutions).
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Set<IVariable<?>> exogenous = (context == null ? (Set) Collections
                .emptySet() : context.getSolutionSetStats().getUsedVars());

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
                 * TODO This could still fail on nested groups within the (NOT)
                 * EXISTS graph pattern since findParent() would report a
                 * JoinGroupNode parent rather than the eventual FilterNode
                 * parent.
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
            
            /*
             * All variables appearing in the source solutions.
             * 
             * TODO This is incorrectly considering all exogenous variables. It
             * must consider only those which are in scope. Exogenous variables
             * are NOT visible in a Sub-Select unless they are projected into
             * that Sub-Select.
             */
            maybeBound.addAll(exogenous);

            if (group.isOptional()) {

                /*
                 * "A FILTER inside an OPTIONAL can reference a variable
                 * bound in the required part of the OPTIONAL."
                 * 
                 * Note: This is ONLY true when the [group] is OPTIONAL.
                 * Otherwise the variables in the parent are not visible.
                 * 
                 * Two fairly difficult test cases articulating the scope rules
                 * are:
                 * 
                 * http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/filter-nested-2.rq
                 * 
                 * and
                 * 
                 * http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-005-not-simplified
                 * (see 
                 * http://www.w3.org/TR/2013/REC-sparql11-query-20130321/#convertGraphPattern)
                 * 
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

                    // Recursively clear the old value expression.

                	
                	// gather subexpression (avoiding CCME)
                	List<FunctionNode> subexpr = new ArrayList<FunctionNode>();
                    final Iterator<FunctionNode> veitr = BOpUtility.visitAll(filter, FunctionNode.class);
                    while (veitr.hasNext()) {
                    	subexpr.add(veitr.next());
                    }
                	
                    // clear
                    for (FunctionNode ive:subexpr) {
                    	ive.setValueExpression(null);
                    }
                    
                    
                    
                    final GlobalAnnotations globals = new GlobalAnnotations(
                    		context.getLexiconNamespace(),
                    		context.getTimestamp()
                    		);
                    
                    // re-generate the value expression.
                    AST2BOpUtility.toVE(globals,
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
                
                modified |=  rewriteUnboundVariablesInFilter(context,
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
     * Analyze minus groups in two fashions:
     * 
     * A) Handle MINUS when it appears without shared variables. We just get rid of
     * the MINUS group since it can not interact with the parent group without
     * shared variables (without shared variables, nothing joins and if nothing
     * joins then nothing is removed from the parent).
     * 
     * B) Some MINUS groups need to be rewritten in bottom up fashion
     * 
     * @param context
     * @param group
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void analyzeMinusGroups(
            final IEvaluationContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<?> group,
            final List<JoinGroupNode> badlyDesignedMinusJoins) {

        int arity = group.arity();
        
        boolean haveAlreadyLiftedGroup = false;
        
        Set<IVariable<?>> maybeIncomingParentBindings = null; // initialize lazily

        for (int i = 0; i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) group.get(i);

            if (!(child instanceof GraphPatternGroup)) {
                
                continue;
                
            }

            final GraphPatternGroup<?> childGroup = (GraphPatternGroup<?>)child;

            /*
             * Recursion.
             */
            analyzeMinusGroups(context, sa, childGroup, badlyDesignedMinusJoins);

            /*
             * Examine this child.
             */
            if(childGroup.isMinus()) {

                final Set<IVariable<?>> maybeIncomingSibling =
                		sa.getMaybeIncomingSiblingBindings(childGroup, new LinkedHashSet<IVariable<?>>());

                final Set<IVariable<?>> maybeProduced = sa
                        .getMaybeProducedBindings(childGroup,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */);
                final Set<IVariable<?>> definitelyIncomingSibling = sa
                        .getDefinitelyIncomingSiblingBindings(childGroup,
                                new LinkedHashSet<IVariable<?>>());
                
                if (!StaticAnalysis.intersect(maybeIncomingSibling,maybeProduced)) {

                    // Remove the MINUS operator. It can not have any effect.
                    
                    ((IGroupNode) group).removeChild(childGroup);
                    
                    i++;

                } else {
                	if (haveAlreadyLiftedGroup) {
                		continue;
                	}
                	
                	if ( maybeIncomingParentBindings == null ) {
                		maybeIncomingParentBindings = sa.getMaybeIncomingBindings(group, new LinkedHashSet<IVariable<?>>());
                	}
                    final Set<IVariable<?>> definitelyProduced = sa
                            .getDefinitelyProducedBindings(childGroup,
                                    new LinkedHashSet<IVariable<?>>(), true/* recursive */);
                    /* There are some cases where the top-down left-to-right execution and the bottom-up semantics
                    do not agree. Such Queries are called 'badly designed' and MUST be rewritten (by lifting out named
                   subqueries). The named subquery will correspond to the parent node here. 
                   
                   The issues are for any variable that 
                   -  MAY be an incoming binding to the parent group
                   -  is not a MUST incoming binding from the siblings
                   -  and is a MAY binding within the MINUS
                      
                   A) Such variables might have a join involving that variable from the parent with the MINUS
                   without the binding coming from the siblings (which define the bottom up semantics), this would
                   remove solutions incorrectly
                   
                   com.bigdata.rdf.sparql.ast.eval.TestUnionMinus.test_union_minus_11_bind()
                   
                   B) Such variables may prevent a join between the parent bindings and the minus bindings
                   which is possible between the sibling bindings and the minus bindings, hence incorrectly failing 
                   to remove solutions 
                   
                   com.bigdata.rdf.sparql.ast.eval.TestUnionMinus.test_union_minus_12_bind()
                   
                   
                   C)
                   A further set of issues seen in 
                   com.bigdata.rdf.sparql.ast.eval.TestUnionMinus.test_union_minus_13_bind()
                   and
                   com.bigdata.rdf.sparql.ast.eval.TestUnionMinus.test_union_minus_11_bind()
                   is:
                   A variable that:
                   - MAY be an incoming binding from the siblings
                   - is not a MUST binding within the minus
                   - but is a MAY binding within the minus
                   - and there are no variables that MUST be set on both the siblings and the minus
                   is difficult, and the minus itself needs to be executed first
                   to avoid such a variable being incorrect set in the minus and the incoming bindings
                   and hence causing the minus to subtract.
                   
                   Both of these issues may be present :( as in 
                   com.bigdata.rdf.sparql.ast.eval.TestUnionMinus.test_union_minus_11_bind()
                   
                   */
                	// Issues A and B above
                   
                    if (StaticAnalysis.intersect(StaticAnalysis.and(maybeIncomingParentBindings,maybeProduced),StaticAnalysis.andNot(definitelyIncomingSibling))) {
                    	badlyDesignedMinusJoins.add((JoinGroupNode) childGroup);
                    	haveAlreadyLiftedGroup = true;
                    }
                    // Issue C above
                    if (StaticAnalysis.intersect(StaticAnalysis.and(maybeIncomingSibling, maybeProduced), StaticAnalysis.andNot(definitelyProduced))) {
                    	if (!StaticAnalysis.intersect(definitelyIncomingSibling,definitelyProduced) ) {
                    		System.err.println("Not implemented lifting of:");
                    		System.err.println(childGroup.toString(8));
              // This did not work, even after some effort.
//                    		badlyDesignedMinusJoins.add((JoinGroupNode) childGroup);
                    	}
                    }
                    
                	
                }
                
            }
            
        }

    }

}
