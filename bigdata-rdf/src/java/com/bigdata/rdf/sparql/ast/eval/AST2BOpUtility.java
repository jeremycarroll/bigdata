package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.CopyOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSubqueryIncludeOp;
import com.bigdata.bop.controller.NamedSubqueryOp;
import com.bigdata.bop.controller.ServiceCallJoin;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.rdf.join.InlineMaterializeOp;
import com.bigdata.bop.solutions.DistinctBindingSetOp;
import com.bigdata.bop.solutions.DistinctBindingSetsWithHTreeOp;
import com.bigdata.bop.solutions.GroupByOp;
import com.bigdata.bop.solutions.GroupByRewriter;
import com.bigdata.bop.solutions.GroupByState;
import com.bigdata.bop.solutions.IGroupByRewriteState;
import com.bigdata.bop.solutions.IGroupByState;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.bop.solutions.IVComparator;
import com.bigdata.bop.solutions.MemoryGroupByOp;
import com.bigdata.bop.solutions.MemorySortOp;
import com.bigdata.bop.solutions.PipelinedAggregationOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.BindingConstraint;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.ConditionalBind;
import com.bigdata.rdf.internal.constraints.InBOp;
import com.bigdata.rdf.internal.constraints.IsInlineBOp;
import com.bigdata.rdf.internal.constraints.IsMaterializedBOp;
import com.bigdata.rdf.internal.constraints.NeedsMaterializationBOp;
import com.bigdata.rdf.internal.constraints.ProjectedConstraint;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.TryBeforeMaterializationConstraint;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.lexicon.LexPredicate;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ASTUtil;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceCall;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTNamedSubqueryOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.spo.DistinctTermAdvancer;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;

import cutthecrap.utils.striterators.FilterBase;
import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.NOPFilter;

/**
 * Query plan generator converts an AST into a query plan made up of
 * {@link PipelineOp}s
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see <a href=
 *      "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=QueryEvaluation"
 *      >Query Evaluation</a>.
 */
public class AST2BOpUtility extends Rule2BOpUtility {

    private static final transient Logger log = Logger
            .getLogger(AST2BOpUtility.class);

    /**
     * Top-level entry point converts an AST query model into an executable
     * query plan.
     */
    public static PipelineOp convert(final AST2BOpContext ctx) {

        /*
         * FIXME Pass in zero or more binding sets to use when optimizing the
         * query. This is mainly for rewriting variables as constants (when
         * there is one source binding set) or for adding IN filters (where
         * there are several and we can constrain a variable to the known values
         * which it can take on). This could be passed through using the [ctx]
         * or directly.
         * 
         * Do we still want the ability to pass in an IBindingSet[] to query
         * evaluation or we are planning to handle this entirely via a "java"
         * service interface. If so, then we need the ability to run the
         * optimizer once we have evaluated that service request in order to do
         * things like set an IN filter based on the returned solution set.
         * 
         * If we model this with an "inline access path", then that is really
         * very close to the Htree / hash join. The IN constraint could just be
         * the evaluation of the join. We could also do column projections,
         * which would give us something like the hash-set based IN constraint.
         */
        final IBindingSet[] bindingSets = null;

        // The AST query model.
        final ASTContainer astContainer = ctx.astContainer;

        // The AST model as produced by the parser.
        final QueryRoot originalQuery = astContainer.getOriginalAST();
        
        // Run the AST query rewrites / query optimizers.
        final QueryRoot optimizedQuery = (QueryRoot) ctx.optimizers.optimize(ctx,
                originalQuery, bindingSets);
        
        // Set the optimized AST model on the container.
        astContainer.setOptimizedAST(optimizedQuery);

        // Final static analysis object for the optimized query.
        ctx.sa = new StaticAnalysis(optimizedQuery);
        
        // The executable query plan.
        PipelineOp queryPlan = convert(optimizedQuery, ctx);

        if (queryPlan == null) {

            /*
             * An empty query plan. Just copy anything from the input to the
             * output.
             */

            queryPlan = addStartOp(ctx);

        }

        /*
         * Set the queryId on the top-level of the query plan.
         * 
         * Note: The queryId is necessary to coordinate access to named subquery
         * result sets when they are consumed within a subquery rather than the
         * main query.
         */

        // TODO Attach the query plan to the ASTContainer?
        queryPlan = (PipelineOp) queryPlan.setProperty(
                QueryEngine.Annotations.QUERY_ID, ctx.queryId);

        if (log.isInfoEnabled()) {
            log.info(astContainer);
            log.info(BOpUtility.toString(queryPlan));
        }

        return queryPlan;

    }

    /**
     * Convert a query (or subquery) into a query plan (pipeline).
     * 
     * @param query
     *            Either a {@link QueryRoot}, a {@link SubqueryRoot}, or a
     *            {@link NamedSubqueryRoot}.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The query plan.
     */
    private static PipelineOp convert(final QueryBase query,
            final AST2BOpContext ctx) {

        final GraphPatternGroup<?> root = query.getWhereClause();

        if (root == null)
            throw new IllegalArgumentException("No group node");

        PipelineOp left = addStartOpOnCluster(ctx);

        /*
         * Named subqueries.
         */
        if (query instanceof QueryRoot) {

            final NamedSubqueriesNode namedSubqueries = ((QueryRoot) query)
                    .getNamedSubqueries();

            if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

                // WITH ... AS [name] ... INCLUDE style subquery declarations.
                left = addNamedSubqueries(left, namedSubqueries,
                        (QueryRoot) query, ctx);

            }

        }

        // The top-level "WHERE" clause.
        left = convertJoinGroupOrUnion(left, root, ctx);

        final ProjectionNode projection = query.getProjection() == null ? null
                : query.getProjection().isEmpty() ? null : query
                        .getProjection();

        if (projection != null) {

            /**
             * Add any evaluated projections
             * 
             * Note: If any of the projected value expressions is an an
             * aggregate then ALL must be aggregates (which includes value
             * expressions involving aggregate functions, bare variables
             * appearing on the group by clause, variables bound by the group by
             * clause, and constants).
             * 
             * Note: If GROUP BY or HAVING is used then the projected value
             * expressions MUST be aggregates. However, if these do not appear
             * and any of the projected value expressions is an aggregate then
             * we are still going to use a GROUP BY where all solutions form a
             * single implicit group.
             * 
             * Note: The combination of the IVs, the math and comparison
             * operators, and the materialization pipe MUST ensure that any IVs
             * which need to be materialized have been materialized BEFORE we
             * run ORDER BY or GROUP BY. Because those operators must act on all
             * solutions "at once", implementations of those operators CAN NOT
             * use the conditional routing logic to handle IVs which are not
             * materialized.
             */

            if (projection.isWildcard())
                throw new AssertionError(
                        "Wildcard projection was not rewritten.");

            final GroupByNode groupBy = query.getGroupBy() == null ? null
                    : query.getGroupBy().isEmpty() ? null : query.getGroupBy();

            final HavingNode having = query.getHaving() == null ? null : query
                    .getHaving().isEmpty() ? null : query.getHaving();

            // true if this is an aggregation query.
            final boolean isAggregate = isAggregate(projection, groupBy, having);

            if (isAggregate) {

                left = addAggregation(left, projection, groupBy, having, ctx);

            } else {

                left = addProjectedAssigments(left,
                        projection.getAssignmentProjections(), ctx);

            }

            /*
             * Note: REDUCED allows, but does not require, either complete or
             * partial filtering of duplicates. It is part of what openrdf does
             * for a DESCRIBE query.
             * 
             * Note: We do not currently have special operator for REDUCED. One
             * could be created using chunk wise DISTINCT. Note that REDUCED may
             * not change the order in which the solutions appear (but we are
             * evaluating it before ORDER BY so that is Ok.)
             */

            if (projection.isDistinct() || projection.isReduced()) {

                left = addDistinct(left, query, ctx);

            }

        }

        final OrderByNode orderBy = query.getOrderBy();

        if (orderBy != null && !orderBy.isEmpty()) {

            left = addOrderBy(left, orderBy, ctx);

        }

        final SliceNode slice = query.getSlice();

        if (slice != null) {

            left = addSlice(left, slice, ctx);

        }

        left = addEndOp(left, ctx);

        /*
         * Set a timeout on a query or subquery.
         */
        {

            final long timeout = query.getTimeout();

            if (timeout > 0 && timeout != Long.MAX_VALUE) {

                left = (PipelineOp) left.setProperty(BOp.Annotations.TIMEOUT,
                        timeout);

            }

        }

        /*
         * TODO Do we need to add operators for materialization of any remaining
         * variables which are being projected out of the query? Note that we do
         * not want to force materialization for subqueries. They should stay as
         * IVs. (I think that materialization is still handled by the bulk IV
         * resolution iterators).
         */

        return left;

    }

    /**
     * Add pipeline operators for named subquery solution sets. The solution
     * sets will be computed before the rest of the query plan runs. They may be
     * referenced from other parts of the query plan.
     * <p>
     * Each solution set has a name. For each {@link NamedSubqueryInclude}, we
     * also determine the join variables which are used to build a hash index
     * (this can be different for different includes of the same named solution
     * set in the same query). Together, the join variables and the name of the
     * solution set form a unique name for each hash index. That hash index (or
     * hash indices if we have more than one combination of join variables) will
     * be generated by the {@link NamedSubqueryOp}.
     * <p>
     * The main use case for WITH AS INCLUDE is a heavy subquery which should be
     * run first. However, in principle, we could run these where the INCLUDE
     * appears rather than up front as long as we are careful not to project
     * bindings into the subquery (other than those it projects out). Whether or
     * not this is more efficient depends on the subquery and how it is combined
     * into the rest of the query. (Running it first should be the default
     * policy.)
     * 
     * @param left
     * @param namedSubquerieNode
     * @param queryRoot
     *            The top-level query. This is required because we need to
     *            understand the join variables which will be used when the
     *            subquery result is joined back into the query for WITH AS
     *            INCLUDE style subqueries.
     * @param ctx
     * @return
     * 
     * @see ASTNamedSubqueryOptimizer
     */
    private static PipelineOp addNamedSubqueries(PipelineOp left,
            final NamedSubqueriesNode namedSubquerieNode,
            final QueryRoot queryRoot, final AST2BOpContext ctx) {

//        /*
//         * Run the named subqueries sequentially in the pre-determined
//         * order.
//         */
//        for (NamedSubqueryRoot subqueryRoot : namedSubquerieNode) {
//
//            left = createNamedSubquery(left, subqueryRoot, ctx);
//
//        }

        /*
         * If there is more than one named subquery whose DEPENDS_ON attribute
         * is an empty String[], then run them in parallel using STEPS (and
         * filter them out in the 2nd pass). Then run the remaining named
         * subqueries in their current sequence.
         */

        // All named subqueries with NO dependencies.
        final List<NamedSubqueryRoot> runFirst = new LinkedList<NamedSubqueryRoot>();

        // Remaining named subqueries, which MUST already be in an ordering
        // consistent with their dependencies.
        final List<NamedSubqueryRoot> remainder = new LinkedList<NamedSubqueryRoot>();

        for (NamedSubqueryRoot subqueryRoot : namedSubquerieNode) {

            final String[] dependsOn = subqueryRoot.getDependsOn();

            if (dependsOn.length == 0)
                runFirst.add(subqueryRoot);
            else
                remainder.add(subqueryRoot);

        }

        final int nfirst = runFirst.size();

        if (nfirst == 0) {

            // Can't be true. At least one of the named subqueries must not
            // depend on any of the others.

            throw new AssertionError();

        } else if (nfirst == 1) {

            left = createNamedSubquery(left, runFirst.get(0), ctx);

        } else {

            final PipelineOp[] steps = new PipelineOp[nfirst];

            int i = 0;

            for (NamedSubqueryRoot subqueryRoot : runFirst) {

                steps[i++] = createNamedSubquery(null/* left */, subqueryRoot,
                        ctx);

            }

            // Do not run the subqueries with unlimited parallelism.
            final int maxParallelSubqueries = Math.min(steps.length, 10);
            
            // Run the steps in parallel.
            left = new Steps(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(Steps.Annotations.SUBQUERIES, steps),//
                    new NV(Steps.Annotations.MAX_PARALLEL_SUBQUERIES,
                            maxParallelSubqueries)//
            );

        }

        // Run the remaining steps in sequence.
        for (NamedSubqueryRoot subqueryRoot : remainder) {

            left = createNamedSubquery(left, subqueryRoot, ctx);

        }

        return left;

    }

    static private PipelineOp createNamedSubquery(PipelineOp left,
            final NamedSubqueryRoot subqueryRoot, final AST2BOpContext ctx) {

        final PipelineOp subqueryPlan = convert(subqueryRoot, ctx);

        if (log.isInfoEnabled())
            log.info("\nsubquery: " + subqueryRoot + "\nplan=" + subqueryPlan);

        final IVariable<?>[] joinVars = ASTUtil.convert(subqueryRoot
                .getJoinVars());

        final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                ctx.queryId, subqueryRoot.getName(), joinVars);

        left = new NamedSubqueryOp(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(NamedSubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                new NV(NamedSubqueryOp.Annotations.JOIN_VARS, joinVars),//
                new NV(NamedSubqueryOp.Annotations.NAMED_SET_REF,
                        namedSolutionSet)//
        );

        return left;

    }

    /**
     * Add an operator to evaluate a {@link ServiceCall}.
     * 
     * @param left
     * @param serviceNode
     * @param ctx
     * @return
     */
    static private PipelineOp addServiceCall(PipelineOp left,
            final ServiceNode serviceNode, final AST2BOpContext ctx) {

        // TODO Pass BindingsClause from QueryRoot through to Service.
        left = new ServiceCallJoin(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(ServiceCallJoin.Annotations.SERVICE_URI,
                        serviceNode.getServiceURI()),//
                new NV(ServiceCallJoin.Annotations.GROUP_NODE,
                        serviceNode.getGroupNode()),//
                new NV(ServiceCallJoin.Annotations.NAMESPACE,
                        ctx.db.getNamespace()),//
                new NV(ServiceCallJoin.Annotations.TIMESTAMP,
                        ctx.db.getTimestamp())//
        );

        return left;

    }

    /**
     * Add a join against a pre-computed temporary solution set into a join
     * group.
     * <p>
     * Note: Since the subquery solution set has already been computed and only
     * contains bindings for solutions projected by the subquery, we do not need
     * to adjust the visibility of bindings when we execute the hash join
     * against the named solution set.
     */
    private static PipelineOp addSubqueryInclude(PipelineOp left,
            final NamedSubqueryInclude subqueryInclude, final AST2BOpContext ctx) {

        if (log.isInfoEnabled())
            log.info("include: solutionSet=" + subqueryInclude.getName());

        final VarNode[] joinvars = subqueryInclude.getJoinVars();

        if (joinvars == null) {

            /*
             * The most likely explanation is not running the
             * ASTNamedSubqueryOptimizer.
             */

            throw new AssertionError();

        }

        if (joinvars.length == 0) {

            /*
             * Note: If there are no join variables then the join will examine
             * the full N x M cross product of solutions. That is very
             * inefficient, so we are logging a warning.
             */

            log.warn("No join variables: " + subqueryInclude.getName());

        }

        final IVariable<?>[] joinVars = ASTUtil.convert(joinvars);

        final NamedSolutionSetRef namedSolutionSetRef = new NamedSolutionSetRef(
                ctx.queryId, subqueryInclude.getName(), joinVars);

        left = new NamedSubqueryIncludeOp(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(NamedSubqueryIncludeOp.Annotations.NAMED_SET_REF,
                        namedSolutionSetRef),//
                new NV(NamedSubqueryIncludeOp.Annotations.JOIN_VARS, joinVars)//
        );

        return left;

    }

    /**
     * Add an explicit SPARQL 1.1 subquery into a join group.
     * <p>
     * Rule: A variable within a subquery is distinct from the same name
     * variable outside of the subquery unless the variable is projected from
     * the subquery. This is handled by a pattern in which variables in the
     * parent context which are not projected by the subquery are hidden from
     * view during its evaluation scope.
     * <p>
     * Note: We evaluate SPARQL 1.1 style sub-queries using a pipelined subquery
     * join. For each solution which flows through the pipeline, we issue the
     * sub-query for the as-bound solution and join the subquery results against
     * that as-bound solution.
     * <p>
     * Note: Some SPARQL 1.1 subqueries may be more efficiently run once to
     * produce a temporary solution set which is then joined into the query for
     * each solution which flows through the query. Such subqueries should be
     * translated into named subqueries with an include by a query optimizer
     * step. When a subquery is rewritten like this is will no longer appear as
     * a {@link SubqueryRoot} node in the AST.
     * <p>
     * Note: This also handles ASK subqueries, which are automatically created
     * for {@link ExistsNode} and {@link NotExistsNode}.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
     */
    private static PipelineOp addSparql11Subquery(PipelineOp left,
            final SubqueryRoot subquery, final AST2BOpContext ctx) {

        final boolean aggregate = isAggregate(subquery);

        final PipelineOp subqueryPlan = convert(subquery, ctx);

        if (log.isInfoEnabled())
            log.info("\nsubquery: " + subquery + "\nplan=" + subqueryPlan);

        switch (subquery.getQueryType()) {
        case ASK: {

            /*
             * The projection should have just one variable, which is the
             * variable that will be bound to true if there is a solution to the
             * subquery and to false if there is no solution to the subquery.
             */

            final ProjectionNode projection = subquery.getProjection();

            if (projection == null)
                throw new RuntimeException("No projection for ASK subquery.");

            final IVariable<?>[] vars = projection.getProjectionVars();

            if (vars == null || vars.length != 1)
                throw new RuntimeException(
                        "Expecting ONE (1) projected variable for ASK subquery.");

            final IVariable<?> askVar = vars[0];

            left = new SubqueryOp(leftOrEmpty(left),// SUBQUERY
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(SubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(SubqueryOp.Annotations.OPTIONAL, false),//
                    new NV(SubqueryOp.Annotations.ASK_VAR, askVar),//
                    new NV(SubqueryOp.Annotations.SELECT, null),//
                    new NV(SubqueryOp.Annotations.IS_AGGREGATE, aggregate)//
            );
            break;
        }
        case SELECT: {

            final ProjectionNode projection = subquery.getProjection();

            final IVariable<?>[] vars = projection.getProjectionVars();

            left = new SubqueryOp(leftOrEmpty(left),// SUBQUERY
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(SubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(SubqueryOp.Annotations.OPTIONAL, false),//
                    new NV(SubqueryOp.Annotations.SELECT, vars),//
                    new NV(SubqueryOp.Annotations.IS_AGGREGATE, aggregate)//
            );

            break;

        }
        default:
            throw new UnsupportedOperationException();
        }

        return left;

    }

    /**
     * Return <code>true</code> if any of the {@link ProjectionNode},
     * {@link GroupByNode}, or {@link HavingNode} indicate that this is an
     * aggregation query.
     * 
     * @param query
     *            The query.
     * 
     * @return <code>true</code>if it is an aggregation query.
     */
    private static boolean isAggregate(final QueryBase query) {

        return isAggregate(query.getProjection(), query.getGroupBy(),
                query.getHaving());

    }

    /**
     * Return <code>true</code> if any of the {@link ProjectionNode},
     * {@link GroupByNode}, or {@link HavingNode} indicate that this is an
     * aggregation query. All arguments are optional.
     */
    private static boolean isAggregate(final ProjectionNode projection,
            final GroupByNode groupBy, final HavingNode having) {

        if (groupBy != null && !groupBy.isEmpty())
            return true;

        if (having != null && !having.isEmpty())
            return true;

        if (projection != null) {

            for (IValueExpressionNode exprNode : projection) {

                final IValueExpression<?> expr = exprNode.getValueExpression();

                if (isObviousAggregate(expr)) {

                    return true;

                }

            }

        }

        return false;

    }

    /**
     * Return <code>true</code> iff the {@link IValueExpression} is an obvious
     * aggregate (it uses an {@link IAggregate} somewhere within it). This is
     * used to identify projections which are aggregates when they are used
     * without an explicit GROUP BY or HAVING clause.
     * <p>
     * Note: Value expressions can be "non-obvious" aggregates when considered
     * in the context of a GROUP BY, HAVING, or even a SELECT expression where
     * at least one argument is a known aggregate. For example, a constant is an
     * aggregate when it appears in a SELECT expression for a query which has a
     * GROUP BY clause. Another example: any value expression used in a GROUP BY
     * clause is an aggregate when the same value expression appears in the
     * SELECT clause.
     * <p>
     * This method is only to find the "obvious" aggregates which signal that a
     * bare SELECT clause is in fact an aggregation.
     * 
     * @param expr
     *            The expression.
     * 
     * @return <code>true</code> iff it is an obvious aggregate.
     */
    private static boolean isObviousAggregate(final IValueExpression<?> expr) {

        if (expr instanceof IAggregate<?>)
            return true;

        final Iterator<BOp> itr = expr.argIterator();

        while (itr.hasNext()) {

            final IValueExpression<?> arg = (IValueExpression<?>) itr.next();

            if (arg != null) {

                if (isObviousAggregate(arg)) // recursion.
                    return true;

            }

        }

        return false;

    }

    /**
     * Generate the query plan for a join group or union. This is invoked for
     * the top-level "WHERE" clause and may be invoked recursively for embedded
     * join groups.
     * 
     * @param left
     * @param groupNode
     * @param ctx
     * @return
     */
    private static PipelineOp convertJoinGroupOrUnion(final PipelineOp left,
            final IGroupNode<? extends IGroupMemberNode> groupNode,
            final AST2BOpContext ctx) {

        if (groupNode instanceof UnionNode) {

            return convertUnion(left, (UnionNode) groupNode, ctx);

        } else if (groupNode instanceof JoinGroupNode) {

            return convertJoinGroup(left, (JoinGroupNode) groupNode, ctx);

        } else {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Generate the query plan for a union.
     * 
     * @param unionNode
     * @param ctx
     * @return
     * 
     *         TODO Could recognize
     * 
     *         <pre>
     * optional {
     *   { a } union { b }
     * }     *
     * </pre>
     * 
     *         as an optional union, but we are not handling the optional
     *         property for a union right now.
     */
    private static PipelineOp convertUnion(PipelineOp left,
            final UnionNode unionNode, final AST2BOpContext ctx) {

        if (unionNode.isOptional()) {
            /*
             * This in fact shows up in the TCK which is a bit weird.
             */
            log.warn("Optional union? : " + ctx.astContainer);
        }

        final int arity = unionNode.size();

        if (arity == 0) {
            /*
             * TODO We can probably just return [left]. The old evaluation
             * strategy code would translate an empty union into an empty
             * iterator. That could be captured by a rewrite of the AST which
             * recognizes and removes empty UNION nodes.
             */
            throw new IllegalArgumentException();

        }

        // The bopId for the UNION or STEP.
        final int thisId = ctx.nextId();

        final BOp[] subqueries = new BOp[arity];

        int i = 0;
        for (IGroupMemberNode child : unionNode) {

            // convert the child
            if (child instanceof JoinGroupNode) {

                subqueries[i++] = convertJoinGroup(null/* left */,
                        (JoinGroupNode) child, ctx);

            } else {

                throw new RuntimeException("Illegal child type for union: "
                        + child.getClass());

            }

        }

        final LinkedList<NV> anns = new LinkedList<NV>();
        anns.add(new NV(BOp.Annotations.BOP_ID, thisId));
        anns.add(new NV(Union.Annotations.SUBQUERIES, subqueries));

        // if (union.getParent() == null) {
        anns.add(new NV(Union.Annotations.EVALUATION_CONTEXT,
                BOpEvaluationContext.CONTROLLER));
        anns.add(new NV(Union.Annotations.CONTROLLER, true));
        // }

        final PipelineOp union = applyQueryHints(new Union(leftOrEmpty(left),
                NV.asMap(anns.toArray(new NV[anns.size()]))), ctx.queryHints);

        return union;

    }

    /**
     * Join group consists of: statement patterns, constraints, and sub-groups
     * <p>
     * Sub-groups can be either join groups (optional) or unions (non-optional)
     * <p>
     * No such thing as a non-optional sub join group (in Sparql 1.0)
     * <p>
     * No such thing as an optional statement pattern, only optional sub-groups
     * <p>
     * Optional sub-groups with at most one statement pattern can be lifted into
     * this group using an optional PipelineJoin, but only if that sub-group has
     * no constraints that need materialized variables. (why? because if we put
     * in materialization steps in between the join and the constraint, the
     * constraint won't have the same optional semantics as the join and will
     * lose its association with that join and its "optionality".)
     * 
     * <pre>
     * 1. Partition the constraints:
     *    -preConditionals: all variables already bound.
     *    -joinConditionals: variables bound by statement patterns in this group.
     *    -postConditionals: variables bound by sub-groups of this group (or not at all).
     *    
     * 2. Pipeline the preConditionals. Add materialization steps as needed.
     * 
     * 3. Non-optional joins and non-optional subqueries.
     * 
     *   3a. Join the statement patterns. Use the static optimizer to attach
     *       constraints to joins. Lots of funky stuff with materialization and
     *       named / default graph joins.
     * 
     *   3b. Pipeline the SPARQL 1.1 subquery.
     * 
     *   3c. Hash join for each named subquery include. 
     * 
     * 4. Pipeline the optional sub-groups (join groups). Lift single optional
     * statement patterns into this group (avoid the subquery op) per the
     * instructions above regarding materialization and constraints. Unions are
     * handled here because they do not always produce bindings for a given
     * variable and hence have semantics similar to optionals for the purposes
     * of attaching constraints and reordering join graphs. For union, make sure 
     * it's not an empty union.
     * 
     * 5. Pipeline the postConditionals. Add materialization steps as needed.
     * </pre>
     * 
     * TODO Think about how hash joins fit into this.
     */
    private static PipelineOp convertJoinGroup(PipelineOp left,
            final JoinGroupNode joinGroup, final AST2BOpContext ctx) {

        final StaticAnalysis sa = ctx.sa;
        
        // /*
        // * Place the StartOp at the beginning of the pipeline.
        // *
        // * TODO We only need a start op on a cluster if there is a requirement
        // * to marshall all solutions from a parent's evaluation context onto
        // the
        // * top-level query controller. I am not sure that we ever have to do
        // * this for a subquery, but there might be cases where it is required.
        // * For all other cases, we should begin with left := null. However,
        // * there are cases where getParent() is running into a null reference
        // * during query evaluation if [left := null] here. Look into and
        // resolve
        // * those issues.
        // */
        // if(left == null)
        // left = addStartOp(ctx);

        /*
         * Add the pre-conditionals to the pipeline.
         * 
         * TODO These filters should be lifted into the parent group (by a
         * rewrite rule) so we can avoid starting a subquery only to have it
         * failed by a filter. We will do less work if we fail the solution in
         * the parent group.
         */
        left = addConditionals(left, sa.getPreFilters(joinGroup), ctx);

        /*
         * FIXME We need to move away from the DataSetJoin class and replace it
         * with an IPredicate to which we have attached an inline access path.
         * That transformation needs to happen in a rewrite rule, which means
         * that we will wind up removing the IN filter and replacing it with an
         * AST node for that inline AP (something conceptually similar to a
         * statement pattern but for a column projection of the variable for the
         * IN expression). That way we do not have to magically "subtract" the
         * known "IN" filters out of the join- and post- filters.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
         * DataSetJoin with an "inline" access path.)
         * 
         * @see JoinGroupNode#getInFilters()
         */
        left = addKnownInConditionals(left, joinGroup.getInFilters(), ctx);

        /*
         * Required joins and non-optional subqueries.
         * 
         * Note: SPARQL 1.1 style subqueries are currently always pipelined.
         * Like named subquery includes, they are also never optional. However,
         * there is no a-priori reason why we should run pipelined subqueries
         * before named subquery includes and, really, no reason why we can not
         * mix these with the required joins (above). I believe that this is
         * being done solely for expediency (because the static query optimizer
         * can not handle it).
         * 
         * Also, note that named subquery includes are hash joins. We have an
         * index. If the operator supported cutoff evaluation then we could
         * easily reorder them with the other required joins using the RTO.
         * 
         * Ditto for pipelined SPARQL 1.1 subquery. If it supported cutoff
         * evaluation, then the RTO could reorder them with the required joins.
         * This is even true when the subquery uses GROUP BY or ORDER BY, which
         * imply the use of at once operators. While we must fully materialize
         * the solutions for each evaluation of the subquery, the evaluation is
         * based on the as-bound solutions flowing into the subquery. If the
         * subquery is unselective, then clearly this will be painful and it
         * might be better to lift such unselective subqueries into named
         * subqueries in order to obtain a hash index over the entire subquery
         * solution set when evaluated with an empty source binding set.
         * 
         * Note: This logic was originally constructed before we had required
         * joins other than on a statement pattern. This shaped how the FILTERs
         * were attached and how the materialization pipeline was generated in
         * order to have materialized RDF Values on hand for those FILTERs.
         * 
         * We now have several kinds of required joins: pipelined statement
         * pattern joins, SPARQL 1.1 subquery, named subquery include, subquery
         * hash joins (when the subquery is optional), service call joins, etc.
         * 
         * FIXME The code currently only handles the FILTER attachment and
         * materialization pipeline for the required statement pattern joins.
         * However, for efficiency, FILTERs MUST be attached to these joins as
         * appropriate for ALL CASES and variables MUST be materialized as
         * required for those filters to run.
         * 
         * FIXME All of these joins can be reordered by either static analysis
         * of cardinality (which has not been extended to handle this yet) or by
         * the RTO. The filter attachment decisions (and the materialization
         * pipeline generation) needs to be deferred until we actually evaluate
         * the join graph (at least for the RTO).
         */
        {

            /*
             * Run service calls first.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof ServiceNode) {
                    left = addServiceCall(left, (ServiceNode) child, ctx);
                }
            }

            /*
             * Add joins against named solution sets from WITH AS INCLUDE style
             * subqueries for which there are NO join variables. Such includes
             * will be a cross product so we want to run them as early as
             * possible.
             * 
             * Note: This corresponds to a very common use case where the named
             * subquery is used to constrain the remainder of the join group.
             * 
             * Note: If there ARE join variables the the named subquery include
             * MUST NOT be run until after the join variables have been bound.
             * Failure to observe this rule will cause the unbound variable to
             * be included when computing the hash code of a solution and the
             * join will not produce the correct solutions.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof NamedSubqueryInclude) {
                    final NamedSubqueryInclude nsi = (NamedSubqueryInclude) child;
                    final VarNode[] joinVars = nsi.getJoinVars();
                    if (joinVars == null)
                        throw new AssertionError();
                    if (joinVars.length == 0)
                        left = addSubqueryInclude(left, nsi, ctx);
                }
            }

            /*
             * Add the joins (statement patterns) and the filters on those
             * joins.
             * 
             * Note: This winds up handling materialization steps as well (it
             * calls through to Rule2BOpUtility).
             */
            left = addJoins(left, joinGroup, sa, ctx);

            /*
             * Add SPARQL 1.1 style subqueries.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof SubqueryRoot) {
                    left = addSparql11Subquery(left, (SubqueryRoot) child, ctx);
                }
            }

            /*
             * Add named subquery joins for named subqueries for which at least
             * one join variable was identified by static analysis.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof NamedSubqueryInclude) {
                    final NamedSubqueryInclude nsi = (NamedSubqueryInclude) child;
                    final VarNode[] joinVars = nsi.getJoinVars();
                    if (joinVars == null)
                        throw new AssertionError();
                    if (joinVars.length > 0)
                        left = addSubqueryInclude(left, nsi, ctx);
                }
            }

        }

        /*
         * Add the subqueries (individual optional statement patterns, optional
         * join groups, and nested union).
         */
        left = addSubqueries(left, joinGroup, ctx);

        /*
         * Add the LET assignments to the pipeline.
         * 
         * TODO Review as generated query plans: Make sure that we do not
         * reorder LET/BIND in a join group. I believe that we are supposed to
         * run them in the given order, just not in the given location (they
         * run last).
         */
        left = addAssignments(left, joinGroup.getAssignments(), ctx, false/* projection */);

        /*
         * Add the post-conditionals to the pipeline.
         */
        left = addConditionals(left, sa.getPostFilters(joinGroup), ctx);

        /*
         * Add the end operator if necessary.
         */
        if (joinGroup.getParent() != null) {
            left = addEndOp(left, ctx);
        }

        return left;

    }

	/**
	 * 
	 * @param ctx
	 * @return The {@link StartOp} iff this query will run on a cluster and
	 *         otherwise <code>null</code>.
	 * 
	 *         TODO This is just experimental.
	 */
	private static final PipelineOp addStartOpOnCluster(final AST2BOpContext ctx) {
	
		if (false && ctx.isCluster())
			return addStartOp(ctx);
		
    	return null;
    	
    }
    
    /**
     * Adds a {@link StartOp}.
     * <p>
     * Note: {@link StartOp} is not necessary in query plans. It is just a
     * convenient concept. When not using {@link StartOp}, the first operator in
     * the plan just needs to have an empty <code>args[]</code>. The only time
     * this is really necessary is when the top-level query plan would otherwise
     * be empty (a <code>null</code>). In this case, the {@link StartOp} just
     * copies its inputs to its outputs (which is all it ever does).
     * 
     * @return The {@link StartOp}.
     */
    private static final PipelineOp addStartOp(final AST2BOpContext ctx) {

        final PipelineOp start = applyQueryHints(
                new StartOp(BOp.NOARGS, NV.asMap(new NV[] {//
                                new NV(Predicate.Annotations.BOP_ID, ctx
                                        .nextId()),
                                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER), })),
                ctx.queryHints);

        return start;

    }

    /**
     * Project select expressions (non-aggregation case).
     * 
     * FIXME https://sourceforge.net/apps/trac/bigdata/ticket/368 (Prune
     * variable bindings during query evaluation). This is basically the last
     * place where we can prune out variables which are not being projected. We
     * need to do that here, but also incrementally.
     * <p>
     * Note: Sesame allows externally given bindings to flow through the query
     * even if they are projected by the query. This means that the "projection"
     * can not remove all bindings except those which are projected. Either we
     * need to also not prune exogenous bindings or we need to strip out
     * bindings introduced by anonymous variables which are part of query
     * rewrites. The former seems easier.
     */
    private static final PipelineOp addProjectedAssigments(PipelineOp left,
            final List<AssignmentNode> assignments, final AST2BOpContext ctx) {

        left = addAssignments(left, assignments, ctx, true/* projection */);

        return left;

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addAssignments(PipelineOp left,
            final List<AssignmentNode> assignments, final AST2BOpContext ctx,
            final boolean projection) {

        final Set<IVariable<IV>> done = new LinkedHashSet<IVariable<IV>>();

        for (AssignmentNode assignmentNode : assignments) {

            final IValueExpression ve = assignmentNode.getValueExpression();

            final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

            /*
             * Get the vars this filter needs materialized.
             */
            vars.addAll(assignmentNode.getMaterializationRequirement()
                    .getVarsToMaterialize());

            /*
             * Remove the ones we've already done.
             */
            vars.removeAll(done);

            final int bopId = ctx.nextId();

            final ConditionalBind b = new ConditionalBind(
                    assignmentNode.getVar(),
                    assignmentNode.getValueExpression(), projection);

            IConstraint c = projection ? new ProjectedConstraint(b)
                    : new BindingConstraint(b);

            /*
             * We might have already materialized everything we need for this
             * filter.
             */
            if (vars.size() > 0) {

                left = addMaterializationSteps(left, bopId, ve, vars, ctx);

                /*
                 * Add all the newly materialized variables to the set we've
                 * already done.
                 */
                done.addAll(vars);

                c = new TryBeforeMaterializationConstraint(c);

            }

            left = applyQueryHints(
                    new ConditionalRoutingOp(
                            leftOrEmpty(left),
                            NV.asMap(new NV[] {//
                                    new NV(BOp.Annotations.BOP_ID, bopId),
                                    new NV(
                                            ConditionalRoutingOp.Annotations.CONDITION,
                                            c), })), ctx.queryHints);

        }

        return left;

    }

    @SuppressWarnings("rawtypes")
    private static final PipelineOp addConditionals(PipelineOp left,
            final Collection<FilterNode> filters, final AST2BOpContext ctx) {

        final Set<IVariable<IV>> done = new LinkedHashSet<IVariable<IV>>();

        for (FilterNode filter : filters) {

            @SuppressWarnings("unchecked")
            final IValueExpression<IV> ve = (IValueExpression<IV>) filter
                    .getValueExpression();

            final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

            /*
             * Get the variables that this filter needs materialized.
             */
            vars.addAll(filter.getMaterializationRequirement()
                    .getVarsToMaterialize());

            /*
             * Remove the ones we've already done.
             */
            vars.removeAll(done);

            final int bopId = ctx.nextId();

            /*
             * We might have already materialized everything we need for this
             * filter.
             */
            if (vars.size() > 0) {

                left = addMaterializationSteps(left, bopId, ve, vars, ctx);

                /*
                 * All the newly materialized vars to the set we've already
                 * done.
                 */
                done.addAll(vars);

            }

            final IConstraint c = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    ve);

            left = applyQueryHints(
                    new ConditionalRoutingOp(
                            leftOrEmpty(left),
                            NV.asMap(new NV[] {//
                                    new NV(BOp.Annotations.BOP_ID, bopId),
                                    new NV(
                                            ConditionalRoutingOp.Annotations.CONDITION,
                                            c), })), ctx.queryHints);

        }

        return left;

    }

    /**
     * Convert an IN filter to a join with an inline access path.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
     *      DataSetJoin with an "inline" access path.)
     */
    @SuppressWarnings("rawtypes")
    private static final PipelineOp addKnownInConditionals(PipelineOp left,
            final Collection<FilterNode> filters, final AST2BOpContext ctx) {

        for (FilterNode filter : filters) {

            final IValueExpression ve = filter.getValueExpression();

            final InBOp bop = (InBOp) ve;
            final IConstant<IV>[] set = bop.getSet();
            final LinkedHashSet<IV> ivs = new LinkedHashSet<IV>();
            for (IConstant<IV> iv : set) {
                ivs.add(iv.get());
            }
            final IVariable var = (IVariable) bop.getValueExpression();
            left = new DataSetJoin(leftOrEmpty(left), NV.asMap(new NV[] {//
                            new NV(DataSetJoin.Annotations.VAR, var),//
                            new NV(DataSetJoin.Annotations.BOP_ID,
                                    ctx.idFactory.incrementAndGet()),//
                            new NV(DataSetJoin.Annotations.GRAPHS, ivs) //
                    }));
        }

        return left;

    }
    
    /**
     * Adds required statement pattern joins to the query plan for the join
     * group.
     * 
     * @param left
     * @param joinGroup
     * @param sa
     * @param ctx
     * @return
     */
    private static final PipelineOp addJoins(PipelineOp left,
            final JoinGroupNode joinGroup, final StaticAnalysis sa,
            final AST2BOpContext ctx) {

        @SuppressWarnings("rawtypes")
        final List<IPredicate> preds = new LinkedList<IPredicate>();

        for (StatementPatternNode sp : joinGroup.getStatementPatterns()) {
            if(!sp.isSimpleOptional()) {
                // Only required statement patterns.
                preds.add(toPredicate(sp, ctx));
            }
        }

        if (preds.isEmpty()) {
            return left;
        }

        final List<IConstraint> constraints = new LinkedList<IConstraint>();

        for (FilterNode filter : sa.getJoinFilters(joinGroup)) {

            constraints.add(new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    filter.getValueExpression()));

        }

        @SuppressWarnings("rawtypes")
        final IRule rule = new Rule("null", // name
                null, // head
                preds.toArray(new IPredicate[preds.size()]), // tails
                constraints.size() > 0 // constraints
                ? constraints.toArray(new IConstraint[constraints.size()])
                        : null);

        final Set<IVariable<?>> knownBound = sa.getIncomingBindings(
                joinGroup, new LinkedHashSet<IVariable<?>>());

        /*
         * TODO Pull code up for this. (note that Rule2BOpUtility is also
         * handling materialization steps for joins.
         */

        left = Rule2BOpUtility.convert(rule, left, knownBound, ctx.idFactory,
                ctx.db, ctx.queryEngine, ctx.queryHints);

        return left;

    }

    /**
     * Adds an optional join (prepared by the caller) in place of an optional
     * group having a join as its sole group member.
     * 
     * @param left
     * @param pred
     *            The {@link Predicate} for some optional statement pattern
     *            node.
     * @param filters
     *            Any filters which MUST be run with that statement pattern
     *            node.
     * @param ctx
     * @return
     */
    private static final PipelineOp addOptionalJoinForSingletonOptionalSubquery(
            PipelineOp left, final Predicate<?> pred,
            final Collection<FilterNode> filters, final AST2BOpContext ctx) {

        final Collection<IConstraint> constraints = new LinkedList<IConstraint>();

        for (FilterNode filter : filters) {

            constraints.add(new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    filter.getValueExpression()));

        }

        // just for now
        left = Rule2BOpUtility.join(ctx.db, ctx.queryEngine, left, pred,
                constraints, ctx.idFactory, ctx.queryHints);

        return left;

    }

    /**
     * Adds subqueries for join groups ({@link JoinGroupNode}) and unions (
     * {@link UnionNode}) to the pipeline (this is NOT SPARQL 1.1 subquery).
     * Non-optional group groups and unions are handled first, then optionals.
     * 
     * @param left
     * @param joinGroup
     * @param ctx
     * @return
     */
    private static final PipelineOp addSubqueries(PipelineOp left,
            final JoinGroupNode joinGroup, final AST2BOpContext ctx) {

        /*
         * First do the non-optional subgroups
         */
        for (IGroupMemberNode child : joinGroup) {

            if (!(child instanceof GraphPatternGroup<?>)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            final IGroupNode<IGroupMemberNode> subgroup = (IGroupNode<IGroupMemberNode>) child;

            if (subgroup.isOptional()) {
                continue;
            }

			left = _addSubquery(left, subgroup, ctx);

        }

        /*
         * Next do the optional sub-groups.
         */
        for (IGroupMemberNode child : joinGroup) {

            if (child instanceof StatementPatternNode) {

                final StatementPatternNode sp = (StatementPatternNode) child;

                if (sp.isSimpleOptional()) {

                    /*
                     * ASTSimpleOptionalOptimizer will recognize and lift out
                     * simple optionals into the parent join group. A simple
                     * optional is basically a single a statement pattern in an
                     * optional join group. If there were any FILTERs in the
                     * simple optional join group, then they were lifted out as
                     * well and attached to this StatementPatternNode. Such
                     * FILTER(s) MUST NOT have materialization requirements for
                     * variables which were not already bound before the
                     * optional JOIN on this statement pattern.
                     * 
                     * TODO Move logic to set OPTIONAL on the Predicate into
                     * toPredicate. It can already see the isSimpleOptional
                     * annotation on the StatementPatternNode.
                     */

                    final Predicate<?> pred = (Predicate<?>) toPredicate(sp,
                            ctx).setProperty(IPredicate.Annotations.OPTIONAL,
                            Boolean.TRUE);

                    final List<FilterNode> filters = sp.getFilters();

                    left = addOptionalJoinForSingletonOptionalSubquery(left,
                            pred, filters, ctx);

                }

            }
            
            if (!(child instanceof GraphPatternGroup<?>)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            final IGroupNode<IGroupMemberNode> subgroup = (IGroupNode<IGroupMemberNode>) child;

            if (!subgroup.isOptional()) {
                continue;
            }

            if (subgroup instanceof JoinGroupNode
                    && ((JoinGroupNode) subgroup).isSimpleOptional()) {

                /*
                 * Optimize a simple join group as an optional join rather than
                 * a subquery.
                 * 
                 * FIXME Deprecated by ASTSimpleOptionalOptimizer. This code
                 * block should go away.
                 */

                final JoinGroupNode subJoinGroup = (JoinGroupNode) subgroup;

                final StatementPatternNode sp = subJoinGroup
                        .getSimpleOptional();

                final Collection<FilterNode> filters = subJoinGroup
                        .getFilters();

                final Predicate<?> pred = (Predicate<?>) toPredicate(sp, ctx)
                        .setProperty(IPredicate.Annotations.OPTIONAL,
                                Boolean.TRUE);

                left = addOptionalJoinForSingletonOptionalSubquery(left, pred,
                        filters, ctx);

            } else {

            	left = _addSubquery(left, subgroup, ctx);
            	
            }

        }

        return left;

    }

	private static PipelineOp _addSubquery(//
			PipelineOp left,//
			final IGroupNode<IGroupMemberNode> subgroup,//
			final AST2BOpContext ctx//
			) {

		final boolean optional = subgroup.isOptional();
		
        final PipelineOp subquery = convertJoinGroupOrUnion(null/* left */,
                subgroup, ctx);

		if (ctx.isCluster()
				&& BOpUtility.visitAll((BOp) subgroup,
						NamedSubqueryInclude.class).hasNext()) {
			/*
			 * Since something in the subgroup (or recursively in some
			 * sub-subgroup) will require access to a named solution set, we
			 * add a CopyOp() to force the solutions to be materialized on
			 * the top-level query controller before issuing the subquery.
			 * 
			 * @see https://sourceforge.net/apps/trac/bigdata/ticket/379#comment:1
			 */
			left = new CopyOp(leftOrEmpty(left), NV.asMap(new NV[] {//
				new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
				new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
						BOpEvaluationContext.CONTROLLER),//
				}));

        }
        
        left = new SubqueryOp(leftOrEmpty(left), //
                new NV(Predicate.Annotations.BOP_ID, ctx.nextId()), //
                new NV(SubqueryOp.Annotations.SUBQUERY, subquery), //
                new NV(SubqueryOp.Annotations.OPTIONAL, optional)//
        );

        return left;
        
    }
    
    /**
     * Wrap with an operator which will be evaluated on the query controller so
     * the results will be streamed back to the query controller in scale-out.
     * <p>
     * Note: For scale-out, we need to stream the results back to the node from
     * which the subquery was issued. If the subquery is issued against the
     * local query engine where the {@link IBindingSet} was produced, then the
     * that query engine is the query controller for the subquery and an
     * {@link EndOp} on the subquery would bring the results for the subquery
     * back to that query controller.
     * <p>
     * Note: This should be conditional based on whether or not we are running
     * on a cluster, but also see
     * https://sourceforge.net/apps/trac/bigdata/ticket/227.
     */
    private static final PipelineOp addEndOp(PipelineOp left,
            final AST2BOpContext ctx) {

		if (left != null
				&& ctx.isCluster()
                && !left.getEvaluationContext().equals(
                        BOpEvaluationContext.CONTROLLER)) {

            left = new EndOp(leftOrEmpty(left),//
                    NV.asMap(
                            //
                            new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER)//
                    ));

        }

        return left;

    }

    /**
     * Add DISTINCT semantics to the pipeline.
     * 
     * TODO If the expected cardinality is large then prefer
     * {@link DistinctBindingSetsWithHTreeOp} rather than
     * {@link DistinctBindingSetOp}.
     * 
     * TODO Support parallel decomposition of distinct on a cluster (DISTINCT
     * can be run on each node if we hash partition the DISTINCT operator based
     * on the variables on which DISTINCT will be imposed and the results when
     * streamed back to the controller will still be distinct.)
     */
    private static final PipelineOp addDistinct(PipelineOp left,
            final QueryBase query, final AST2BOpContext ctx) {

        final int bopId = ctx.nextId();

        final ProjectionNode projection = query.getProjection();

        if (projection.isWildcard())
            throw new AssertionError("Wildcard projection was not rewritten.");
        final IVariable<?>[] vars = projection.getProjectionVars();

        final PipelineOp op;
        if (ctx.nativeDistinct) {
            /*
             * DISTINCT on the JVM heap.
             */
            op = new DistinctBindingSetOp(//
                    leftOrEmpty(left),//
                    NV.asMap(new NV[] {//
                            new NV(DistinctBindingSetOp.Annotations.BOP_ID,
                                    bopId),
                            new NV(DistinctBindingSetOp.Annotations.VARIABLES,
                                    vars),
                            new NV(
                                    DistinctBindingSetOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),
                            new NV(
                                    DistinctBindingSetOp.Annotations.SHARED_STATE,
                                    true), }));
        } else {
            /*
             * DISTINCT on the native heap.
             */
            op = new DistinctBindingSetsWithHTreeOp(//
                    leftOrEmpty(left),//
                    NV.asMap(new NV[] {//
                            new NV(
                                    DistinctBindingSetsWithHTreeOp.Annotations.BOP_ID,
                                    bopId),//
                            new NV(
                                    DistinctBindingSetsWithHTreeOp.Annotations.VARIABLES,
                                    vars),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    }));
        }

        left = applyQueryHints(op, ctx.queryHints);

        return left;

    }

    /**
     * Add an aggregation operator. It will handle the grouping (if any),
     * optional having filter, and projected select expressions. A generalized
     * aggregation operator will be used unless the aggregation corresponds to
     * some special case.
     * 
     * @param left
     *            The previous operator in the pipeline.
     * @param projection
     *            The projected select expressions (MUST be aggregates when
     *            interpreted in context with groupBy and having).
     * @param groupBy
     *            The group by clause (optional).
     * @param having
     *            The having clause (optional).
     * @param ctx
     * 
     * @return The left-most operator in the pipeline.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addAggregation(PipelineOp left,
            final ProjectionNode projection, final GroupByNode groupBy,
            final HavingNode having, final AST2BOpContext ctx) {

        final IValueExpression<?>[] projectExprs = projection
                .getValueExpressions();

        final IValueExpression<?>[] groupByExprs = groupBy == null ? null
                : groupBy.getValueExpressions();

        final IConstraint[] havingExprs = having == null ? null : having
                .getConstraints();

        final IGroupByState groupByState = new GroupByState(projectExprs,
                groupByExprs, havingExprs);

        final IGroupByRewriteState groupByRewrite = new GroupByRewriter(
                groupByState);

        final int bopId = ctx.nextId();

        final GroupByOp op;

        /*
         * TODO Review. I believe that AssignmentNode.getValueExpression()
         * should always return the Bind().
         */
        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        if (projectExprs != null) {

            for (IValueExpression expr : projectExprs) {

                if (expr instanceof Bind) {

                    vars.add(((Bind) expr).getVar());

                    // Note: side-effect on expr!
                    expr = ((Bind) expr).getExpr();

                }

                if (expr instanceof IVariable<?>) {

                    vars.add((IVariable<IV>) expr);

                } else {

                    StaticAnalysis.gatherVarsToMaterialize(
                            expr, vars);

                }

            }

        }

        if (groupByExprs != null) {

            for (IValueExpression expr : groupByExprs) {

                if (expr instanceof Bind) {

                    vars.add(((Bind) expr).getVar());

                    // Note: side-effect on expr.
                    expr = ((Bind) expr).getExpr();

                }

                if (expr instanceof IVariable<?>) {

                    vars.add((IVariable<IV>) expr);

                } else {

                    StaticAnalysis.gatherVarsToMaterialize(
                            expr, vars);

                }

            }

        }

        left = addMaterializationSteps(left, bopId, vars, ctx);

        if (!groupByState.isAnyDistinct() && !groupByState.isSelectDependency()
                && !groupByState.isNestedAggregates()) {

            /*
             * Extremely efficient pipelined aggregation operator.
             * 
             * TODO This operation can be parallelized on a cluster either if it
             * does not use any operators which can not be decomposed (such as
             * AVERAGE) or if we rewrite such operators into AVG(X) :=
             * SUM(X)/COUNT(X). When it is parallelized, we need to run it on
             * each node and add another instance of this operator on the query
             * controller which aggregates the aggregates from the nodes in the
             * cluster. If we have done rewrites of things like AVERAGE, then we
             * can not compute the AVERAGE until after we have computed the
             * aggregate of aggregates. The simplest way to do that is using a
             * LET operator after the aggregates have been combined.
             */

            op = new PipelinedAggregationOp(leftOrEmpty(left),//
                    NV.asMap(new NV[] {//
                            new NV(BOp.Annotations.BOP_ID, bopId),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(PipelineOp.Annotations.PIPELINED, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(GroupByOp.Annotations.GROUP_BY_STATE,
                                    groupByState), //
                            new NV(GroupByOp.Annotations.GROUP_BY_REWRITE,
                                    groupByRewrite), //
                            new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    }));

        } else {

            /*
             * General aggregation operator on the JVM heap.
             * 
             * TODO There is a sketch of a generalized aggregation operator for
             * the native heap but it needs to be reworked (the control
             * structure is incorrect). This generalized aggregation operator
             * for the native heap would be a better only when the #of solutions
             * to be grouped is large and we can not pipeline the aggregation.
             * (The code for this is either on the CI machine or the
             * workstation). BBT 8/17/2011.
             */

            op = new MemoryGroupByOp(leftOrEmpty(left), NV.asMap(new NV[] {//
                            new NV(BOp.Annotations.BOP_ID, bopId),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                            new NV(GroupByOp.Annotations.GROUP_BY_STATE,
                                    groupByState), //
                            new NV(GroupByOp.Annotations.GROUP_BY_REWRITE,
                                    groupByRewrite), //
                    }));

        }

        left = applyQueryHints(op, ctx.queryHints);

        return left;

    }

    /**
     * Add an ORDER BY operator.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addOrderBy(PipelineOp left,
            final OrderByNode orderBy, final AST2BOpContext ctx) {

        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        final ISortOrder<IV>[] sortOrders = new ISortOrder[orderBy.size()];

        final Iterator<OrderByExpr> it = orderBy.iterator();

        for (int i = 0; it.hasNext(); i++) {

            final OrderByExpr orderByExpr = it.next();

            IValueExpression<?> expr = orderByExpr.getValueExpression();

            if (!(expr instanceof IVariableOrConstant<?> && !(expr instanceof IBind))) {

                /*
                 * Wrap the expression with a BIND of an anonymous variable.
                 */

                expr = new Bind(Var.var(), expr);

            }

            if (expr instanceof IVariable<?>) {

                vars.add((IVariable<IV>) expr);

            } else {

                StaticAnalysis.gatherVarsToMaterialize(
                        expr, vars);

            }

            sortOrders[i] = new SortOrder(expr, orderByExpr.isAscending());

        }

        final int sortId = ctx.nextId();

        left = addMaterializationSteps(left, sortId, vars, ctx);

        left = applyQueryHints(
                new MemorySortOp(
                        leftOrEmpty(left),
                        NV.asMap(new NV[] {//
                                new NV(MemorySortOp.Annotations.BOP_ID, sortId),//
                                new NV(MemorySortOp.Annotations.SORT_ORDER,
                                        sortOrders),//
                                new NV(
                                        MemorySortOp.Annotations.VALUE_COMPARATOR,
                                        new IVComparator()),//
                                new NV(
                                        MemorySortOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER),//
                                new NV(MemorySortOp.Annotations.PIPELINED, true),//
                                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
                                new NV(MemorySortOp.Annotations.SHARED_STATE,
                                        true),//
                                new NV(MemorySortOp.Annotations.LAST_PASS, true),//
                        })), ctx.queryHints);

        return left;

    }

    /**
     * Impose an OFFSET and/or LIMIT on a query.
     */
    private static final PipelineOp addSlice(PipelineOp left,
            final SliceNode slice, final AST2BOpContext ctx) {

        final int bopId = ctx.nextId();

        left = applyQueryHints(
                new SliceOp(leftOrEmpty(left), NV.asMap(new NV[] {
                        new NV(SliceOp.Annotations.BOP_ID, bopId),
                        new NV(SliceOp.Annotations.OFFSET, slice.getOffset()),
                        new NV(SliceOp.Annotations.LIMIT, slice.getLimit()),
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(SliceOp.Annotations.PIPELINED, false),//
                        new NV(SliceOp.Annotations.MAX_PARALLEL, 1),//
                        new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
                })), ctx.queryHints);

        return left;

    }

    /**
     * If the value expression that needs the materialized variables can run
     * without a {@link NotMaterializedException} then just route to the
     * <i>rightId</i> (around the rest of the materialization pipeline steps).
     * This happens in the case of a value expression that only "sometimes"
     * needs materialized values, but not always (i.e. materialization
     * requirement depends on the data flowing through). A good example of this
     * is {@link CompareBOp}, which can sometimes work on internal values and
     * sometimes can't.
     * 
     * TODO Consider the efficiency of the steps which are being taken. Should
     * we test for the most common cases first, or for those with the least
     * latency to "fix"?
     * 
     * @see TryBeforeMaterializationConstraint
     */
    @SuppressWarnings("rawtypes")
    public static PipelineOp addMaterializationSteps(PipelineOp left,
            final int rightId, final IValueExpression<IV> ve,
            final Collection<IVariable<IV>> vars, final AST2BOpContext ctx) {

        /*
         * If the constraint "c" can run without a NotMaterializedException then
         * bypass the rest of the pipeline by routing the solutions to rightId.
         */
        final IConstraint c2 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                new NeedsMaterializationBOp(ve));

        left = applyQueryHints(
                new ConditionalRoutingOp(
                        leftOrEmpty(left),
                        NV.asMap(new NV[] {//
                                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),
                                new NV(
                                        ConditionalRoutingOp.Annotations.CONDITION,
                                        c2),
                                new NV(PipelineOp.Annotations.ALT_SINK_REF,
                                        rightId), })), ctx.queryHints);

        return addMaterializationSteps(left, rightId, vars, ctx);

    }

    /**
     * Adds a series of materialization steps to materialize terms needed
     * downstream. To materialize the variable <code>?term</code>, the pipeline
     * looks as follows:
     * 
     * <pre>
     * (A) leftId      : The upstream operator
     * (B) condId1     : if !materialized then condId2 (fall through) else rightId 
     * (C) condId2     : if inline then inlineMatId (fall through) else lexJoinId
     * (D) inlineMatId : InlineMaterializeOp; goto rightId.
     * (E) lexJoinId   : LexJoin (fall through).
     * (F) rightId     : the downstream operator.
     * </pre>
     * 
     * <pre>
     * left 
     * ->
     * ConditionalRoutingOp1 (condition=!IsMaterialized(?term), alt=right)
     * ->
     * ConditionalRoutingOp2 (condition=IsInline(?term), alt=PipelineJoin)
     * ->
     * InlineMaterializeOp (predicate=LexPredicate(?term), sink=right)
     * ->
     * PipelineJoin (predicate=LexPredicate(?term))
     * ->
     * right
     * </pre>
     * 
     * @param left
     *            the left (upstream) operator that immediately proceeds the
     *            materialization steps
     * @param rightId
     *            the id of the right (downstream) operator that immediately
     *            follows the materialization steps
     * @param vars
     *            the terms to materialize
     * 
     * @return the final bop added to the pipeline by this method
     * 
     * @see TryBeforeMaterializationConstraint
     * 
     * TODO make [vars] a Set.
     */
    @SuppressWarnings("rawtypes")
    private static PipelineOp addMaterializationSteps(PipelineOp left,
            final int rightId, final Collection<IVariable<IV>> vars,
            final AST2BOpContext ctx) {

        final Iterator<IVariable<IV>> it = vars.iterator();

        int firstId = ctx.nextId();

        while (it.hasNext()) {

            final IVariable<IV> v = it.next();

            // Generate bopIds for the routing targets for this variable.
            final int condId1 = firstId;
            final int condId2 = ctx.nextId();
            final int inlineMaterializeId = ctx.nextId();
            final int lexJoinId = ctx.nextId();

            final int endId;

            if (!it.hasNext()) {

                /*
                 * If there are no more terms to materialize, the terminus of
                 * this materialization pipeline is the "right" (downstream)
                 * operator that was passed in.
                 */
                endId = rightId;

            } else {

                /*
                 * If there are more terms, the terminus of this materialization
                 * pipeline is the 1st operator of the next materialization
                 * pipeline.
                 */
                endId = firstId = ctx.nextId();

            }

            final IConstraint c1 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    new IsMaterializedBOp(v, false/* materialized */));

            final PipelineOp condOp1 = applyQueryHints(
                    new ConditionalRoutingOp(
                            leftOrEmpty(left),
                            NV.asMap(new NV[] {//
                                    new NV(BOp.Annotations.BOP_ID, condId1),
                                    new NV(
                                            ConditionalRoutingOp.Annotations.CONDITION,
                                            c1),
                                    new NV(PipelineOp.Annotations.SINK_REF,
                                            condId2),
                                    new NV(PipelineOp.Annotations.ALT_SINK_REF,
                                            endId), })), ctx.queryHints);

            if (log.isDebugEnabled()) {
                log.debug("adding 1st conditional routing op: " + condOp1);
            }

            final IConstraint c2 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    new IsInlineBOp(v, true/* inline */));

            final PipelineOp condOp2 = applyQueryHints(
                    new ConditionalRoutingOp(
                            leftOrEmpty(condOp1),
                            NV.asMap(new NV[] {//
                                    new NV(BOp.Annotations.BOP_ID, condId2),
                                    new NV(
                                            ConditionalRoutingOp.Annotations.CONDITION,
                                            c2),
                                    new NV(PipelineOp.Annotations.SINK_REF,
                                            inlineMaterializeId),
                                    new NV(PipelineOp.Annotations.ALT_SINK_REF,
                                            lexJoinId), })), ctx.queryHints);

            if (log.isDebugEnabled()) {
                log.debug("adding 2nd conditional routing op: " + condOp2);
            }

            final Predicate lexPred;
            {

                /*
                 * Note: Use the timestamp of the triple store view unless this
                 * is a read/write transaction, in which case we need to use the
                 * unisolated view in order to see any writes which it may have
                 * performed (lexicon writes are always unisolated).
                 */

                long timestamp = ctx.db.getTimestamp();
                if (TimestampUtility.isReadWriteTx(timestamp))
                    timestamp = ITx.UNISOLATED;

                final String ns = ctx.db.getLexiconRelation().getNamespace();

                lexPred = LexPredicate.reverseInstance(ns, timestamp, v);

            }

            if (log.isDebugEnabled()) {
                log.debug("lex pred: " + lexPred);
            }

            final PipelineOp inlineMaterializeOp = applyQueryHints(
                    new InlineMaterializeOp(
                            leftOrEmpty(condOp2),
                            NV.asMap(new NV[] {//
                                    new NV(BOp.Annotations.BOP_ID,
                                            inlineMaterializeId),
                                    new NV(
                                            InlineMaterializeOp.Annotations.PREDICATE,
                                            lexPred.clone()),
                                    new NV(PipelineOp.Annotations.SINK_REF,
                                            endId), })), ctx.queryHints);

            if (log.isDebugEnabled()) {
                log.debug("adding inline materialization op: "
                        + inlineMaterializeOp);
            }

            {
                // annotations for this join.
                final List<NV> anns = new LinkedList<NV>();

                // TODO Why is lexPred being cloned?
                Predicate pred = (Predicate) lexPred.clone();
                
                anns.add(new NV(BOp.Annotations.BOP_ID, lexJoinId));
                anns.add(new NV(PipelineOp.Annotations.SINK_REF, endId));
                
                if (ctx.isCluster() && !Rule2BOpUtility.forceRemoteAPs) {
                    // use a partitioned join.
                    anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.SHARDED));
                    pred = (Predicate) pred.setProperty(
                            Predicate.Annotations.REMOTE_ACCESS_PATH, false);
                }

                anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

                // Join against the lexicon to materialize the Value.
                final PipelineOp lexJoinOp = applyQueryHints(
                        //
                        new PipelineJoin(leftOrEmpty(inlineMaterializeOp), //
                                anns.toArray(new NV[anns.size()])),
                        ctx.queryHints);

                if (log.isDebugEnabled()) {
                    log.debug("adding lex join op: " + lexJoinOp);
                }

                left = lexJoinOp;

            }
            
        }

        return left;

    }

    /**
     * Method produces a {@link Predicate} which captures the semantics of the
     * {@link StatementPatternNode}.
     * <p>
     * Note: This method is responsible for layering in the default graph and
     * named graph semantics on the access path associated with a statement
     * pattern.
     * <p>
     * Note: This method is NOT responsible for selecting the statement index to
     * use for the access path. That is decided when we determine the join
     * ordering.
     * 
     * @param sp
     *            The statement pattern.
     * @param ctx
     * 
     * @return The {@link Predicate} which models the access path constraints
     *         for that statement pattern.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
     *      DataSetJoin with an "inline" access path.)
     */
    @SuppressWarnings("rawtypes")
    private static final Predicate toPredicate(final StatementPatternNode sp,
            final AST2BOpContext ctx) {

        final QueryRoot query = ctx.astContainer.getOptimizedAST();
        
        final AbstractTripleStore database = ctx.db;

        final DatasetNode dataset = query.getDataset();

        final Value predValue = sp.p().getValue();
        if (log.isDebugEnabled()) {
            log.debug(predValue);
        }

        final IVariableOrConstant<IV> s = sp.s().getValueExpression();
        final IVariableOrConstant<IV> p = sp.p().getValueExpression();
        final IVariableOrConstant<IV> o = sp.o().getValueExpression();

        // The graph term/variable iff specified by the query.
        final TermNode cvar = sp.c();
        final IVariableOrConstant<IV> c = cvar == null ? null : cvar
                .getValueExpression();

        // The annotations for the predicate.
        final List<NV> anns = new LinkedList<NV>();

        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
                new String[] { database.getSPORelation().getNamespace() }));

        // timestamp
        anns.add(new NV(IPredicate.Annotations.TIMESTAMP, database
                .getSPORelation().getTimestamp()));

        if (!database.isQuads()) {
            /*
             * Either triple store mode or provenance mode.
             */
            if (cvar != null && database.isStatementIdentifiers()
                    && cvar.getValue() != null) {
                /*
                 * Note: The context position is used as a statement identifier
                 * (SID). SIDs may be used to retrieve provenance statements
                 * (statements about statement) using high-level query. SIDs are
                 * represented as blank nodes and is not possible to have them
                 * bound in the original query. They only become bound during
                 * query evaluation.
                 */
                throw new IllegalArgumentException(
                        "Context position is a statement identifier and may not be bound in the original query: "
                                + sp);
            }
        } else {
            /*
             * Quad store mode.
             */

            // quads mode.
            anns.add(new NV(Rule2BOpUtility.Annotations.QUADS, true));
            // attach the Scope.
            anns.add(new NV(Rule2BOpUtility.Annotations.SCOPE, sp.getScope()));

            /*
             * Note: For a default graph access path, [cvar] can not become
             * bound since the context will is stripped from the visited ISPOs.
             */
            if (dataset != null) {
                // attach the DataSet.
                anns.add(new NV(Rule2BOpUtility.Annotations.DATASET, dataset));
                switch (sp.getScope()) {
                case DEFAULT_CONTEXTS: {
                    if (dataset.getDefaultGraphFilter() != null) {
                        anns.add(new NV(
                                IPredicate.Annotations.INDEX_LOCAL_FILTER,
                                ElementFilter.newInstance(dataset
                                        .getDefaultGraphFilter())));
                    }
                    break;
                }
                case NAMED_CONTEXTS: {
                    if (dataset.getNamedGraphFilter() != null) {
                        anns.add(new NV(
                                IPredicate.Annotations.INDEX_LOCAL_FILTER,
                                ElementFilter.newInstance(dataset
                                        .getNamedGraphFilter())));
                    }
                    break;
                }
                default:
                    throw new AssertionError();
                }
            }
        } // quads

        /*
         * Layer on filters which can run on the local access path (close to the
         * data).
         * 
         * Note: We can now stack filters so there may be other things which can
         * be leveraged here.
         * 
         * FIXME Handle StatementPatternNode#EXISTS here (This turns into an
         * iterator with a limit of ONE (1) on the {@link SPOAccessPath}. This
         * should turn into a LIMIT annotation on the access path (which does
         * not currently recognize an offset/limit annotation; instead you use
         * an alternative iterator call).
         */
        {

            final List<IFilter> filters = new LinkedList<IFilter>();

            if (Boolean.valueOf(sp.getProperty(
                    StatementPatternNode.Annotations.DISTINCT,
                    StatementPatternNode.Annotations.DEFAULT_DISTINCT))) {

                /*
                 * Visit only the distinct values for the first key component.
                 * 
                 * FIXME We MUST pin the choice of the index for this filter. In
                 * order to do that we need to interact with the join ordering
                 * since we typically want to run this first in a join group.
                 * 
                 * TODO GRAPH ?g {} can use this to visit the distinct named
                 * graphs for which there exists a statement in the database.
                 * 
                 * FIXME One exception where we can not use PARALLEL without
                 * also imposing DISTINCT is the @DISTINCT annotation in
                 * scale-out. This is because a given IV in the first key
                 * component could span more than one shard.
                 */
                filters.add(new DistinctTermAdvancer(database.isQuads() ? 4 : 3));
                
            }

            if (!query.getIncludeInferred()) {
                /*
                 * Filter out anything which is not an Explicit statement.
                 */
                filters.add(ElementFilter
                        .newInstance(ExplicitSPOFilter.INSTANCE));
            }

            final int nfilters = filters.size();
            final IFilter filter;
            if (nfilters > 0) {
                if (nfilters == 1) {
                    // Just the one filter.
                    filter = filters.get(0);
                } else {
                    // Stack multiple filters.
                    final FilterBase tmp = new NOPFilter();
                    for (IFilter f : filters) {
                        tmp.addFilter(f);
                    }
                    filter = tmp;
                }
                /*
                 * Attach a filter on elements visited by the access path.
                 */
                anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
                        filter));
            }

        }
        
        /*
         * Explicitly set the access path / iterator flags.
         * 
         * Note: High level query generally permits iterator level parallelism.
         * We set the PARALLEL flag here so it can be used if a global index
         * view is chosen for the access path.
         * 
         * Note: High level query for SPARQL always uses read-only access paths.
         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
         * semantics then you will need to remote the READONLY flag for the
         * mutable access paths.
         */
        {
         
            final int flags = IRangeQuery.DEFAULT | IRangeQuery.PARALLEL
                    | IRangeQuery.READONLY;
            
            anns.add(new NV(IPredicate.Annotations.FLAGS, flags));
            
        }

        {
        
            // Decide on the correct arity for the predicate.
            final BOp[] vars;
            if (!database.isQuads() && !database.isStatementIdentifiers()) {
                vars = new BOp[] { s, p, o };
            } else if (c == null) {
                vars = new BOp[] { s, p, o, com.bigdata.bop.Var.var() };
            } else {
                vars = new BOp[] { s, p, o, c };
            }

            return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));

        }

    }

    /**
     * Convert an {@link IValueExpressionNode} (recursively) to an
     * {@link IValueExpression}.
     * 
     * @param lex
     *            The lexicon namespace.
     * @param node
     *            The expression to convert.
     * 
     * @return The converted expression.
     * 
     * @see ASTSetValueExpressionsOptimizer
     */
    public static final IValueExpression<? extends IV> toVE(final String lex,
            final IValueExpressionNode node) {

        /*
         * Check to see if value expression has already been created and cached
         * on node.
         */
        if (node.getValueExpression() != null) {

            return node.getValueExpression();

        }

        if (node instanceof VarNode) {

            return node.getValueExpression();

        } else if (node instanceof ConstantNode) {

            return node.getValueExpression();

        } else if (node instanceof AssignmentNode) {

            final AssignmentNode assignment = (AssignmentNode) node;

            final IValueExpressionNode valueExpr = assignment
                    .getValueExpressionNode();

            @SuppressWarnings("rawtypes")
            final IValueExpression<? extends IV> ve = toVE(lex, valueExpr);

            return ve;

        } else if (node instanceof FunctionNode) {

            final FunctionNode functionNode = (FunctionNode) node;

            final URI functionURI = functionNode.getFunctionURI();

            // TODO More efficient if the factory replaces a [null] reference
            // for scalar values with Collections.emptyMap().  Modify the 
            // parse tree => AST conversion to leave [scalarValues:=null] unless
            // we need to set something explicitly.
            final Map<String, Object> scalarValues = functionNode.getScalarValues();

            // TODO More efficient to pass in the FunctionNode than creating a
            // new VEN[] each time.
            final ValueExpressionNode[] args = functionNode.args().toArray(
                    new ValueExpressionNode[functionNode.arity()]);

            @SuppressWarnings("rawtypes")
            final IValueExpression<? extends IV> ve = FunctionRegistry.toVE(
                    lex, functionURI, scalarValues, args);

            functionNode.setValueExpression(ve);

            return ve;

        } else {

            throw new IllegalArgumentException(node.toString());

        }

    }

    /*
     * TODO Integrate code to attach RangeBOps to predicates. (This code is from
     * the old BigdataEvaluationStrategyImpl3 class. It should be moved into an
     * IASTOptimizer for recognizing range constraints.)
     * 
     * TODO Ranges can be just upper or lower. They do not need to be both. They
     * can even be an excluded middle.
     * 
     * TODO The big thing about handling range constraints is make sure that we
     * query each part of the OS(C)P index which corresponds to a datatype which
     * could have a value legally promotable within the context in which the
     * LT/LTE/GT/GTE filter(s) occur. For example, x>5 && x<10 needs to do a
     * key-range scan for xsd:int, xsd:integer, .... The big win comes when we
     * can recognize a datatype constraint at the same time such that we only
     * touch one part of the index for that key range of values.
     */
//  private void attachRangeBOps(final SOpGroup g) {
//
//      final Map<IVariable,Collection<IValueExpression>> lowerBounds =
//          new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//      final Map<IVariable,Collection<IValueExpression>> upperBounds =
//          new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//      
//      for (SOp sop : g) {
//          final BOp bop = sop.getBOp();
//          if (!(bop instanceof SPARQLConstraint)) {
//              continue;
//          }
//          final SPARQLConstraint c = (SPARQLConstraint) bop;
//          if (!(c.getValueExpression() instanceof CompareBOp)) {
//              continue;
//          }
//          final CompareBOp compare = (CompareBOp) c.getValueExpression();
//          final IValueExpression left = compare.get(0);
//          final IValueExpression right = compare.get(1);
//          final CompareOp op = compare.op();
//          if (left instanceof IVariable) {
//              final IVariable var = (IVariable) left;
//              final IValueExpression ve = right;
//              if (op == CompareOp.GE || op == CompareOp.GT) {
//                  // ve is a lower bound
//                  Collection bounds = lowerBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      lowerBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              } else if (op == CompareOp.LE || op == CompareOp.LT) {
//                  // ve is an upper bound
//                  Collection bounds = upperBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      upperBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              }
//          } 
//          if (right instanceof IVariable) {
//              final IVariable var = (IVariable) right;
//              final IValueExpression ve = left;
//              if (op == CompareOp.LE || op == CompareOp.LT) {
//                  // ve is a lower bound
//                  Collection bounds = lowerBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      lowerBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              } else if (op == CompareOp.GE || op == CompareOp.GT) {
//                  // ve is an upper bound
//                  Collection bounds = upperBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      upperBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              }
//          }
//      }
//      
//      final Map<IVariable,RangeBOp> rangeBOps = 
//          new LinkedHashMap<IVariable,RangeBOp>();
//      
//      for (IVariable v : lowerBounds.keySet()) {
//          if (!upperBounds.containsKey(v))
//              continue;
//          
//          IValueExpression from = null;
//          for (IValueExpression ve : lowerBounds.get(v)) {
//              if (from == null)
//                  from = ve;
//              else
//                  from = new MathBOp(ve, from, MathOp.MAX,this.tripleSource.getDatabase().getNamespace());
//          }
//
//          IValueExpression to = null;
//          for (IValueExpression ve : upperBounds.get(v)) {
//              if (to == null)
//                  to = ve;
//              else
//                  to = new MathBOp(ve, to, MathOp.MIN,this.tripleSource.getDatabase().getNamespace());
//          }
//          
//          final RangeBOp rangeBOp = new RangeBOp(v, from, to); 
//          
//          if (log.isInfoEnabled()) {
//              log.info("found a range bop: " + rangeBOp);
//          }
//          
//          rangeBOps.put(v, rangeBOp);
//      }
//      
//      for (SOp sop : g) {
//          final BOp bop = sop.getBOp();
//          if (!(bop instanceof IPredicate)) {
//              continue;
//          }
//          final IPredicate pred = (IPredicate) bop;
//          final IVariableOrConstant o = pred.get(2);
//          if (o.isVar()) {
//              final IVariable v = (IVariable) o;
//              if (!rangeBOps.containsKey(v)) {
//                  continue;
//              }
//              final RangeBOp rangeBOp = rangeBOps.get(v);
//              final IPredicate rangePred = (IPredicate)
//                  pred.setProperty(SPOPredicate.Annotations.RANGE, rangeBOp);
//              if (log.isInfoEnabled())
//                  log.info("range pred: " + rangePred);
//              sop.setBOp(rangePred);
//          }
//      }
//  }
  
}
