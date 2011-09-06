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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;

import com.bigdata.rdf.sparql.ast.FunctionRegistry;

/**
 * Pre-populated list of the default optimizers.
 * 
 * <pre>
 * optimizerList.add(new BindingAssigner()); // yes.
 * optimizerList.add(new ConstantOptimizer(strategy));
 * optimizerList.add(new CompareOptimizer());
 * optimizerList.add(new ConjunctiveConstraintSplitter());
 * optimizerList.add(new SameTermFilterOptimizer());
 * // only need to optimize the join order this way if we are not
 * // using native joins
 * if (nativeJoins == false) {
 *     optimizerList.add(new QueryJoinOptimizer(new BigdataEvaluationStatistics(
 *             this)));
 * }
 * optimizerList.add(new FilterOptimizer());
 * optimizerList.optimize(tupleExpr, dataset, bindings);
 * replaceValues(dataset, tupleExpr, bindings); // yes.
 * </pre>
 * 
 * FIXME {@link ConstantOptimizer}. Rewrites the query replacing any aspect
 * which can be statically evaluated to a constant with that constant. The
 * implementation considers variables, functions, and constants.
 * 
 * FIXME {@link CompareOptimizer}. Replaces Compare with SameTerm whenever
 * possible. (I think that we handle this in the {@link FunctionRegistry}, but
 * that should be verified and documented here.)
 * 
 * FIXME {@link ConjunctiveConstraintSplitter}. Takes a FILTER with an AND of
 * constraints and replaces it with one filter per left or right hand side of
 * the AND. This flattens the value expression hierarchy. It also might allow us
 * to reject some solutions earlier in the pipeline since we do not have to wait
 * for all of the variables to become bound and we can potentially evaluate some
 * of the individual constraints earlier than others.
 * 
 * FIXME {@link SameTermFilterOptimizer}. Optimizes SameTerm(X,Y) by renaming
 * one of the variables to the other variable. Optimizes SameTerm(X,aURI) by
 * assigning the constant to the variable.
 * 
 * FIXME {@link FilterOptimizer}. Pushes filters as far down in the query model
 * as possible.
 * 
 * FIXME Sesame has define a bunch of IQueryOptimizers that we are not using
 * (even before this refactor). Many of them apply to their SQL backends. A few
 * might be relevant to us:
 * <ul>
 * <li>{@link DisjunctiveConstraintOptimizer} - moves SameTerm closer to joins
 * for queries involving UNIONs.</li>
 * <li>{@link IterativeEvaluationOptimizer} - ???</li>
 * <li>{@link QueryModelNormalizer} - various simplifications of their tuple
 * expression query model. I am not sure whether or not there is anything here
 * we could leverage.</li>
 * </ul>
 * 
 * TODO Optimize away empty join groups and optimize those containing just a
 * single child by lifting the child into the parent whenever possible.
 * 
 * TODO Recognize OR of constraints and rewrite as IN. We can then optimize the
 * IN operator in a variety of ways (in fact, the {@link FunctionRegistry}
 * already handles those optimizations for IN).
 * 
 * TODO Optimize IN and named graph and default graph queries with inline access
 * path.
 * 
 * TODO Optimize when you have a nested graph pattern with an eventual parent
 * graph pattern by placing a SameTerm filter to ensure that the nested graph
 * pattern only find solutions which are not allowed by the outer graph.
 * 
 * TODO StatementPatternNode should inherit the context dynamically from the
 * parent rather than requiring the context to be specified explicitly. This is
 * also true for a subquery. If it specifies a GRAPH pattern, then you MUST put
 * a FILTER on it. (An IASTOptimizer could take care of that.)
 * 
 * FIXME Either handle via AST rewrites or verify that AST2BOpUtility handles
 * this during convert().
 * <p>
 * An empty {} matches a single empty solution.
 * <p>
 * GRAPH ?g {} matches the distinct named graphs in the named graph portion of
 * the data set (special case). This should be translated into a distinct term
 * advancer on CSPO if there is no data set. If the named graphs are listed
 * explicitly, then just return that list. Third case: Anzo supports a FILTER on
 * the named graph or default graphs for ACLs.
 * <p>
 * GRAPH <uri> {} is an existence test for the graph? (Matt is not sure on this
 * one.)
 * 
 * FIXME What follows are some rules for static analysis of variable scope.
 * <p>
 * Rule: A variable bound within an OPTIONAL *MAY* be bound in the parent group.
 * <p>
 * Rule: A variable bound within a UNION *MAY* be bound in the parent group.
 * Exception: if the variable is bound on all alternatives in the UNION, then it
 * MUST be bound in the parent group.
 * <p>
 * A variable bound by a statement pattern or a let/bind MUST be bound within
 * the parent group and within all contexts which are evaluated *after* it is
 * bound. (This is the basis for propagation of bindings to the parent. Since
 * SPARQL demands bottom up evaluation semantics a variable which MUST be bound
 * in a group MUST be bound in its parent.)
 * 
 * FIXME If a subquery does not share ANY variables which MUST be bound in the
 * parent's context then rewrite the subquery into a named/include pattern so it
 * will run exactly once. {@link SubqueryRoot.Annotations#RUN_ONCE}. (If it does
 * not share any variables at all then it will produce a cross product and,
 * again, we want to run that subquery once.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DefaultOptimizerList.java 5115 2011-09-01 15:24:57Z
 *          thompsonbry$
 */
public class DefaultOptimizerList extends OptimizerList {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public DefaultOptimizerList() {

//        /**
//         * Rule: A variable within a subquery is distinct from the same name
//         * variable outside of the subquery unless the variable is projected
//         * from the subquery.
//         * <p>
//         * This rewrites variable variable names within subqueries which are not
//         * projected out of the subquery such that they do not overlap with
//         * other variables in the query.
//         * <p>
//         * Note: This must be run before the {@link ASTBindingAssigner} since
//         * otherwise we could bind a variable in a subquery which was not being
//         * projected by that subquery.
//         */
//        add(new ASTSubqueryVariableScopeRewrite());
        
        /**
         * Propagates bindings from an input solution into the query, replacing
         * variables with constants while retaining the constant / variable
         * association.
         * 
         * TODO Other optimizations are possible when the {@link IBindingSet}[]
         * has multiple solutions. In particular, the possible values which a
         * variable may take on can be written into an IN constraint and
         * associated with the query in the appropriate scope. Those are not
         * being handled yet. Also, if a variable takes on the same value in ALL
         * source solutions, then it can be replaced by a constant.
         */ 
        add(new ASTBindingAssigner());

        /**
         * Imposes a LIMIT of ONE for a non-aggregation ASK query.
         */
        add(new AskOptimizer());

        /**
         * Rewrites the projection node of a DESCRIBE query into, generating a
         * CONSTRUCT clause and extending the WHERE clause to capture the
         * semantics of the DESCRIBE query. The query type is also changed to
         * CONSTRUCT.
         */
        add(new DescribeOptimizer());

        /**
         * Creates and assigns a PROJECTION of all variables in the CONSTRUCT
         * clause.
         */
        add(new ConstructOptimizer());

        /**
         * If a {@link SubqueryRoot} appears in an otherwise empty (and
         * non-optional) {@link JoinGroupNode}, then the join group is replaced
         * by the {@link SubqueryRoot}.
         */
        add(new ASTSubqueryRootInGroupOptimizer());
        
        /**
         * Validates named subquery / include patterns, identifies the join
         * variables, and annotates the named subquery root and named subquery
         * include with those join variables.
         */
        add(new ASTNamedSubqueryOptimizer());

    }

}
