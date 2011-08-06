package com.bigdata.bop.solutions;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableFactory;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.util.InnerCause;

/**
 * A pipelined aggregation operator based on an in memory hash table associating
 * with per-group state for each aggregate expression (it can also handle the
 * degenerate case where all solutions form a single implicit group). This
 * operator is highly efficient, but may only be used if (a) DISTINCT is NOT
 * specified for any aggregate and (b) aggregates do not embed other aggregates.
 * <p>
 * Note: This implementation is a pipelined operator which inspects each chunk
 * of solutions as they arrive. The state is shared across invocations of the
 * operator for each source chunk. The operator waits until the last chunk has
 * been consumed before writing the output solutions. In order to observe the
 * lastInvocation signal, the operator MUST be single threaded (
 * {@link PipelineOp.Annotations#MAX_PARALLEL}:=1) and running on the query
 * controller.
 * <p>
 * Note: Since this operator evaluates {@link IAggregate}s incrementally (one
 * input solution at a time), it relies on {@link IAggregate}'s contract for
 * "sticky" errors. See {@link IAggregate#get(IBindingSet)} and
 * {@link IAggregate#done()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class PipelinedAggregationOp extends GroupByOp implements
        IVariableFactory {

	private final static transient Logger log = Logger
			.getLogger(PipelinedAggregationOp.class);
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            HashMapAnnotations, GroupByOp.Annotations {

    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>true</code>. This is a pipelined aggregation operator and
     * MAY NOT be used to evaluate aggregation requests which use DISTINCT or
     * which nest {@link IAggregate}s in other {@link IAggregate}s.
     */
    public boolean isPipelinedAggregationOp() {

        return true;
        
    }

    /**
     * Required deep copy constructor.
     */
    public PipelinedAggregationOp(final PipelinedAggregationOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public PipelinedAggregationOp(final BOp[] args,
            final Map<String, Object> annotations) {

		super(args, annotations);

        switch (getEvaluationContext()) {
		case CONTROLLER:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

		if (!isSharedState()) {
            /*
             * Note: shared state is used to share the hash table across
             * invocations.
             */
			throw new UnsupportedOperationException(Annotations.SHARED_STATE
					+ "=" + isSharedState());
		}
		
        if (getMaxParallel() != 1) {
            /*
             * Note: The operator MUST be single threaded in order to receive
             * the isLastInvocation notice.
             */
            throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
                    + "=" + getMaxParallel());
        }

    }

    /**
     * @see Annotations#INITIAL_CAPACITY
     */
    public int getInitialCapacity() {

        return getProperty(Annotations.INITIAL_CAPACITY,
                Annotations.DEFAULT_INITIAL_CAPACITY);

    }

    /**
     * @see Annotations#LOAD_FACTOR
     */
    public float getLoadFactor() {

        return getProperty(Annotations.LOAD_FACTOR,
                Annotations.DEFAULT_LOAD_FACTOR);

    }
    
    public BOpStats newStats(final IQueryContext queryContext) {
    	
    	return new AggregateStats(this);
    	
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));
        
    }

    /**
     * Return a new anonymous variable (this is overridden by some unit tests in
     * order to have predictable variable names).
     */
    public IVariable<?> var() {

        return Var.var();

    }

    /**
     * Wrapper used for the solution groups.
     */
    private static class SolutionGroup {

        /** The hash code for {@link #vals}. */
        private final int hash;

        /**
         * The computed values for the groupBy value expressions in the order in
         * which they were declared.
         */
        private final IConstant<?>[] vals;

        public String toString() {
            return super.toString() + //
                    "{group=" + Arrays.toString(vals) + //
                    "}";
        }

        /**
         * Return a new {@link SolutionGroup} given the value expressions and
         * the binding set.
         * 
         * @param groupBy
         *            The value expressions to be computed.
         *            The binding set.
         * 
         * @return The new {@link SolutionGroup} -or- <code>null</code> if any
         *         of the value expressions evaluates or a <code>null</code>
         *         -OR- throws a {@link SparqlTypeErrorException}.
         */
        static SolutionGroup newInstance(final IValueExpression<?>[] groupBy,
                final IBindingSet bset) {

            final IConstant<?>[] r = new IConstant<?>[groupBy.length];

            for (int i = 0; i < groupBy.length; i++) {

                final Object asBound;
                try {
                    /*
                     * Note: This has a side-effect on the solution and causes
                     * the evaluated GROUP_BY value expressions to become bound
                     * on the solution. This is necessary in order for us to
                     * compute the aggregates incrementally.
                     */
                    asBound = groupBy[i].get(bset);
                } catch (SparqlTypeErrorException ex) {
                    // Drop solution.
                    return null;
                }
                if (asBound == null) {
                    // Drop solution.
                    return null;
                }
                @SuppressWarnings({ "rawtypes", "unchecked" })
                final IConstant<?> x = new Constant(asBound);
                r[i] = x;

            }

            return new SolutionGroup(r);
            
        }

        private SolutionGroup(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof SolutionGroup)) {
                return false;
            }
            final SolutionGroup t = (SolutionGroup) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }

    } // SolutionGroup
    
    /**
     * State associated with each {@link SolutionGroup} (this is not used if all
     * solutions belong to a single implicit group).
     */
    private static class SolutionGroupState {
        
        /**
         * The aggregate expressions to be evaluated. The {@link IAggregate}s
         * MUST have been cloned to avoid side-effect across groups.
         */
        private final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr;

        /**
         * The intermediate solution with all bindings produced when evaluating
         * this solution group.  Any bare variables and any variables declared
         * by the GROUP_BY clause are projected onto {@link #aggregates} by 
         * the constructor.
         */
        private final IBindingSet aggregates;

        /**
         * 
         * @param groupBy
         *            The (rewritten) GROUP_BY clause.
         * @param aggExpr
         *            The aggregates to be computed for each group. The
         *            {@link IAggregate}s will be *cloned* in order to avoid
         *            side-effects across groups.
         * @param bset
         *            The first input solution encountered for the group (the
         *            one which led to the group becoming defined).
         */
        SolutionGroupState(final IValueExpression<?>[] groupBy,
                final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr,
                final IBindingSet bset) {

            this.aggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

            for (Map.Entry<IAggregate<?>, IVariable<?>> e : aggExpr.entrySet()) {

                // Note: IAggregates MUST be cloned to avoid side-effects.
                this.aggExpr.put((IAggregate<?>) e.getKey().clone(),
                        e.getValue());

            }

            /**
             * Propagate GROUP_BY expression onto [aggregates].
             */

            this.aggregates = new ListBindingSet();

            final IBindingSet aSolution = bset;
            
            for (IValueExpression<?> expr : groupBy) {

                if (expr instanceof IVariable<?>) {

                    /**
                     * Propagate bare variable used in GROUP_BY clause to
                     * [aggregates].
                     * 
                     * <pre>
                     * GROUP BY ?x
                     * </pre>
                     */

                    final IVariable<?> var = (IVariable<?>) expr;

                    // Note: MUST be a binding for each groupBy var.
                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    final Constant<?> val = new Constant(var.get(aSolution));

                    // Bind on [aggregates].
                    aggregates.set(var, val);

                } else if (expr instanceof IBind<?>) {

                    /**
                     * Propagate BIND declared by GROUP_BY clause to
                     * [aggregates].
                     * 
                     * <pre>
                     * GROUP BY (2*?y as ?x)
                     * </pre>
                     */

                    final IBind<?> bindExpr = (IBind<?>) expr;

                    // Compute value expression.
                    // Note: MUST be valid since group exists.
                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    final Constant<?> val = new Constant(
                            bindExpr.get(aSolution));

                    // Variable to be projected out by SELECT.
                    final IVariable<?> ovar = ((IBind<?>) expr).getVar();

                    // Bind on [aggregates].
                    aggregates.set(ovar, val);

                }

            } // next GROUP_BY value expression

        }
        
    } // class SolutionGroupState

    /**
     * Extends {@link BOpStats} to provide the shared state for the aggregation
     * operator across invocations for different source chunks.
     * <p>
     * Note: mutable fields on instances of this class are guarded by the
     * monitor for the instance.
     */
    private static class AggregateStats extends BOpStats {

        /**
		 * 
		 */
        private static final long serialVersionUID = 1L;

        /**
         * <code>true</code> until we initialize the shared start during the
         * first invocation of the {@link ChunkTask}.
         */
        private boolean first = true;
        
        /**
         * A map whose keys are the bindings on the specified variables and
         * whose values are the per-group state.
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         * <p>
         * Note: This is only iff an explicit GROUP_BY clause is used.
         */
        private LinkedHashMap<SolutionGroup, SolutionGroupState> map;

        /**
         * The aggregates to be computed (they have internal state).
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         * <p>
         * Note: This is bound iff all solutions will be collected within a
         * single implicit group.
         */
        private LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr;
        
        public AggregateStats(final PipelinedAggregationOp op) {
        }

        /**
         * Discard the shared state (this can not be discarded until the last
         * invocation).
         */
        private void discardState() {
            map = null;
            aggExpr = null;
        }

    }

    /**
     * Task executing on the node.
     */
    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * A map whose keys are the bindings on the specified variables (the
         * keys and the values are the same since the map implementation does
         * not allow <code>null</code> values).
         * <p>
         * Note: This is bound iff an explicit GROUP_BY clause was used.
         */
        private final LinkedHashMap<SolutionGroup, SolutionGroupState> map;

        /**
         * The aggregates to be computed (they have internal state).
         * <p>
         * Note: This is bound iff all solutions will be collected within a
         * single implicit group.
         */
        private final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr;

        private final IGroupByState groupByState;

        private final IGroupByRewriteState rewrite;
        
        private final IValueExpression<?>[] groupBy;
        
        ChunkTask(final PipelinedAggregationOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.groupByState = new GroupByState(//
                    (IValueExpression<?>[]) op.getRequiredProperty(GroupByOp.Annotations.SELECT), //
                    (IValueExpression<?>[]) op.getProperty(GroupByOp.Annotations.GROUP_BY), //
                    (IConstraint[]) op.getProperty(GroupByOp.Annotations.HAVING)//
            );

            this.rewrite = new GroupByRewriter(groupByState) {
                @Override
                public IVariable<?> var() {
                    return op.var();
                }
            };

            if (groupByState.isAnyDistinct()) {
                // Pipelined aggregation does not support DISTINCT.
                throw new UnsupportedOperationException(
                        "DISTINCT not allowed with pipelined aggregation.");
            }

            if (groupByState.isNestedAggregates()) {
                /*
                 * Pipelined aggregation does not support aggregates which embed
                 * other aggregates.
                 */
                throw new UnsupportedOperationException(
                        "Nested aggregates not allowed with pipelined aggregation.");
            }

            this.groupBy = groupByState.getGroupByClause();

//            this.select = groupByState.getSelectClause();
            
//            this.having = groupByState.getHavingClause();

            final AggregateStats stats = (AggregateStats)context.getStats();
            synchronized (stats) {
                if (stats.first) {
                    /*
                     * Setup the shared state
                     */
                    stats.first = false;
                    if (groupBy == null) {
                        stats.map = null;
                        stats.aggExpr = rewrite.getAggExpr();
                    } else {
                        // The map is only defined if a GROUP_BY clause was
                        // used.
                        stats.map = new LinkedHashMap<SolutionGroup, SolutionGroupState>(
                                op.getInitialCapacity(), op.getLoadFactor());
                        stats.aggExpr = null;
                    }
                } // if(stats.first)
                /*
                 * Initialize from the shared state.
                 */
                this.map = stats.map;
                this.aggExpr = stats.aggExpr;
            } // synchronized(stats)
        }

        /**
         * Update the state of the {@link IAggregate}s for the appropriate
         * group.
         * 
         * @param bset
         *            The solution.
         */
        private void accept(final IBindingSet bset) {

            if (groupBy == null || groupBy.length == 0)
                throw new IllegalArgumentException();

            if (bset == null)
                throw new IllegalArgumentException();

            final SolutionGroup s = SolutionGroup.newInstance(groupBy, bset);

            if (s == null) {

                // Drop the solution.

                if (log.isDebugEnabled())
                    log.debug("Dropping solution: " + bset);

                return;

            }

            SolutionGroupState m = map.get(s);

            if (m == null) {

                map.put(s,
                        m = new SolutionGroupState(groupBy, rewrite
                                .getAggExpr(), bset));

            }

            // Accept the solution.
            if (log.isTraceEnabled())
                log.trace("Accepting solution: " + bset);

            // Update the aggregates.
            doAggregate(m.aggExpr, bset);
            
        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                while (itr.hasNext()) {
                    
                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    for (IBindingSet bset : a) {

                        if (groupBy == null) {

                            /*
                             * A single implicit group.
                             */
                            doAggregate(aggExpr, bset);

                        } else {

                            /*
                             * Explicit GROUP_BY.
                             */
                            accept(bset);

                        }
                        
                    }

                }

                if(context.isLastInvocation()) {

                    // The solutions to be written onto the sink.
                    final List<IBindingSet> outList = new LinkedList<IBindingSet>();
                    
                    if(groupBy == null) {

                        /*
                         * A single implicit group.
                         * 
                         * Output solution for the implicit group IFF the HAVING
                         * constraints are satisfied.
                         */

                        /**
                         * The intermediate solution with all bindings produced
                         * when evaluating this solution group.
                         * 
                         * Note: There is no GROUP_BY so we do not need to
                         * propagate any bindings declared by that clause.
                         * 
                         * This evaluates the (rewritten) SELECT expressions.
                         * The rewritten HAVING clause (if any) is then
                         * evaluated. If the solution is not dropped, then only
                         * the SELECTed variables are projected out.
                         */
                        final IBindingSet aggregates = new ListBindingSet();

                        // Finalize and bind on [aggregates].
                        finalizeAggregates(aggExpr, aggregates);
                        
                        // Evaluate SELECT expressions.
                        for (IValueExpression<?> expr : rewrite.getSelect2()) {

                            /*
                             * Note: This a hack turns an
                             * IllegalArgumentException which we presume is
                             * coming out of new Constant(null) into an
                             * (implicit) SPARQL type error so we can drop the
                             * binding for this SELECT expression. (Note that we
                             * are not trying to drop the entire group!)
                             */
                            try {
                                expr.get(aggregates);
                            } catch (IllegalArgumentException ex) {
                                if (log.isInfoEnabled())
                                    log.info("will not bind solution for aggregate due to error: expr="
                                            + expr + ", cause=" + ex);
                                continue;
                            }

                        }

                        // Verify optional HAVING constraint(s)
                        final boolean drop;
                        final IConstraint[] having2 = rewrite.getHaving2();
                        if (having2 != null
                                && !BOpUtility
                                        .isConsistent(having2, aggregates)) {
                            // drop this solution.
                            drop = true;
                        } else {
                            drop = false;
                        }

                        if (log.isInfoEnabled())
                            log.info((drop ? "drop" : "keep") + " : "
                                    + aggregates);

                        if (!drop) {

                            // project out only selected variables.
                            final IBindingSet out = aggregates
                                    .copy(groupByState.getSelectVars().toArray(
                                            new IVariable[0]));

                            outList.add(out);
                            
                        }

                    } else {

                        /*
                         * Explicit GROUP_BY.
                         * 
                         * Output solutions for the observed groups which pass
                         * the optional HAVING constraint(s).
                         */
                        for (SolutionGroupState groupState : map.values()) {

                            final IBindingSet aggregates = groupState.aggregates;

                            // Finalize and bind on [aggregates].
                            finalizeAggregates(groupState.aggExpr, aggregates);

                            // Evaluate SELECT expressions.
                            for (IValueExpression<?> expr : rewrite
                                    .getSelect2()) {

                                /*
                                 * Note: This hack turns an
                                 * IllegalArgumentException which we presume is
                                 * coming out of new Constant(null) into an
                                 * (implicit) SPARQL type error so we can drop
                                 * the binding for this SELECT expression. (Note
                                 * that we are not trying to drop the entire
                                 * group!)
                                 */
                                try {
                                    expr.get(aggregates);
                                } catch (IllegalArgumentException ex) {
                                    if (log.isInfoEnabled())
                                        log.info("will not bind solution for aggregate due to error: expr="
                                                + expr + ", cause=" + ex);
                                    continue;
                                }

                            }

                            // Verify optional HAVING constraint(s)
                            final boolean drop;
                            final IConstraint[] having2 = rewrite.getHaving2();
                            if (having2 != null
                                    && !BOpUtility.isConsistent(having2,
                                            aggregates)) {
                                // drop this solution.
                                drop = true;
                            } else {
                                drop = false;
                            }

                            if (log.isInfoEnabled())
                                log.info((drop ? "drop" : "keep") + " : "
                                        + aggregates);

                            if (!drop) {

                                // project out only selected variables.
                                final IBindingSet out = aggregates
                                        .copy(groupByState.getSelectVars()
                                                .toArray(new IVariable[0]));

                                outList.add(out);

                            }

                        }

                    }

                    if (!outList.isEmpty()) {

                        // Write the solutions onto the sink.
                        sink.add(outList.toArray(new IBindingSet[0]));

                        sink.flush();

                    }

                    // Discard the shared state.
                    ((AggregateStats) stats).discardState();

                }
                
                // done.
                return null;
                
            } finally {

                sink.close();

            }

        }

    }

    /**
     * Update the {@link IAggregate}s for the given binding set.
     * <p>
     * Note: The {@link IAggregate} instances MUST be distinct within each group
     * to avoid side-effects across groups.
     * 
     * @param aggExpr
     *            The aggregate expressions to be evaluated.
     * @param bset
     *            The binding set.
     */
    static private void doAggregate(
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr,
            final IBindingSet bset) {

        for (IAggregate<?> a : aggExpr.keySet()) {

            try {

                a.get(bset);

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                    /*
                     * Trap the type error. The group will be reported, but this
                     * aggregate will not bind a value for that group (the
                     * aggregate will track its error state internally.)
                     */
                    if (log.isInfoEnabled())
                        log.info("type error: expr=" + a + " : " + t);

                }

            }

        }

    }

    /**
     * Finalize the {@link IAggregate}s for a solution group (or for the
     * implicit group formed from all solutions when no GROUP_BY was given).
     * This invokes {@link IAggregate#done()} on each {@link IAggregate} in turn
     * and binds any non-<code>null</code> results onto <i>aggregates</i>.
     * 
     * @param aggExpr
     *            The aggregate expressions to be evaluated.
     * @param aggregates
     *            The binding set where the aggregates will become bound.
     */
    static private void finalizeAggregates(
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr,
            final IBindingSet aggregates) {

        for (Map.Entry<IAggregate<?>, IVariable<?>> e : aggExpr.entrySet()) {

            final IAggregate<?> expr = e.getKey();

            final Object val;
            
            try {

                val = expr.done();
                
            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                    // trap the type error and filter out the
                    // solution
                    if (log.isInfoEnabled())
                        log.info("aggregate will not bind due type error: expr="
                                + expr + " : " + t);

                    // No binding.
                    continue;

                } else {
                    
                    throw new RuntimeException(t);
                    
                }
                
            }

            if (val != null) {
                
                // bind the result.
                aggregates.set(e.getValue(), new Constant(val));
                
            }

        }

    }

}
