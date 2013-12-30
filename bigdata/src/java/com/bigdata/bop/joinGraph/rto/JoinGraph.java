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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.joinGraph.rto;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpIdFactory;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.controller.AbstractSubqueryOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.util.concurrent.Haltable;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of
 * relations and joins which connect those relations. This boils down to a
 * collection of {@link IPredicate}s (selects on relations), shared variables
 * (which identify joins), and {@link IConstraint}s (which limit solutions).
 * Operators other than standard joins (including optional joins, sort, order
 * by, etc.) must be handled downstream from the join graph in a "tail plan".
 * 
 * @see http://arxiv.org/PS_cache/arxiv/pdf/0810/0810.4809v1.pdf, XQuery Join
 *      Graph Isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see JGraph
 */
public class JoinGraph extends PipelineOp {

//	private static final transient Logger log = Logger
//			.getLogger(JoinGraph.class);

	private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger
            .getLogger(JoinGraph.class);

    /**
	 * Known annotations.
	 */
	public interface Annotations extends PipelineOp.Annotations {

		/**
		 * The variables which are projected out of the join graph.
		 */
		String SELECTED = JoinGraph.class.getName() + ".selected";
		
        /**
         * The vertices of the join graph, expressed an an {@link IPredicate}[]
         * (required).
         */
        String VERTICES = JoinGraph.class.getName() + ".vertices";

        /**
         * The constraints on the join graph, expressed an an
         * {@link IConstraint}[] (optional, defaults to no constraints).
         */
        String CONSTRAINTS = JoinGraph.class.getName() + ".constraints";

        /**
         * The initial limit for cutoff sampling (default
         * {@value #DEFAULT_LIMIT}).
         */
        String LIMIT = JoinGraph.class.getName() + ".limit";

		int DEFAULT_LIMIT = 100;

        /**
         * The <i>nedges</i> edges of the join graph having the lowest
         * cardinality will be used to generate the initial join paths (default
         * {@value #DEFAULT_NEDGES}). This must be a positive integer. The edges
         * in the join graph are sorted in order of increasing cardinality and
         * up to <i>nedges</i> of those edges having the lowest cardinality are
         * used to form the initial set of join paths. For each edge selected to
         * form a join path, the starting vertex will be the vertex of that edge
         * having the lower cardinality.
         */
		String NEDGES = JoinGraph.class.getName() + ".nedges";

		int DEFAULT_NEDGES = 2;
		
        /**
         * The type of sample to take (default {@value #DEFAULT_SAMPLE_TYPE)}.
         * 
         * @see SampleIndex.SampleType
         */
        String SAMPLE_TYPE = JoinGraph.class.getName() + ".sampleType";
        
        String DEFAULT_SAMPLE_TYPE = SampleType.RANDOM.name();

	}

    /**
     * Query attribute names for the {@link JoinGraph}. The fully qualified name
     * of the attribute is formed by appending the attribute name to the
     * "bopId-", where <code>bopId</code> is the value returned by
     * {@link BOp#getId()}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface Attributes {

        /**
         * The join path selected by the RTO (output).
         */
        String PATH = JoinGraph.class.getName() + ".path";

        /**
         * The samples associated with join path selected by the RTO (output).
         */
        String SAMPLES = JoinGraph.class.getName() + ".samples";

        /**
         * The physical query plan generated from the RTO determined best join
         * ordering (output). This is used to specify the query plan to be
         * executed by a downstream operator.
         */
        String QUERY_PLAN = JoinGraph.class.getName() + ".queryPlan";

	}
	
	/**
	 * @see Annotations#SELECTED
	 */
	public IVariable<?>[] getSelected() {

		return (IVariable[]) getRequiredProperty(Annotations.SELECTED);

	}

	/**
	 * @see Annotations#VERTICES
	 */
	public IPredicate<?>[] getVertices() {

		return (IPredicate[]) getRequiredProperty(Annotations.VERTICES);

	}

    /**
     * @see Annotations#CONSTRAINTS
     */
    public IConstraint[] getConstraints() {

        return (IConstraint[]) getProperty(Annotations.CONSTRAINTS, null/* none */);

    }

	/**
	 * @see Annotations#LIMIT
	 */
	public int getLimit() {

		return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);

	}

	/**
	 * @see Annotations#NEDGES
	 */
	public int getNEdges() {

		return getProperty(Annotations.NEDGES, Annotations.DEFAULT_NEDGES);

	}

	/**
	 * @see Annotations#SAMPLE_TYPE
	 */
	public SampleType getSampleType() {

	    return SampleType.valueOf(getProperty(Annotations.SAMPLE_TYPE,
                Annotations.DEFAULT_SAMPLE_TYPE));
	    
	}

	/**
	 * Return the computed join path.
	 * 
	 * @see Attributes#PATH
	 */
    public Path getPath(final IRunningQuery q) {

        return (Path) q.getAttributes().get(getId() + "-" + Attributes.PATH);

    }

    /**
     * Return the samples associated with the computed join path.
     * 
     * @see Annotations#SAMPLES
     */
    @SuppressWarnings("unchecked")
    public Map<PathIds, EdgeSample> getSamples(final IRunningQuery q) {

        return (Map<PathIds, EdgeSample>) q.getAttributes().get(
                getId() + "-" + Attributes.SAMPLES);

    }    

    private void setPath(final IRunningQuery q, final Path p) {

        q.getAttributes().put(getId() + "-" + Attributes.PATH, p);
        
    }

    private void setSamples(final IRunningQuery q,
            final Map<PathIds, EdgeSample> samples) {
        
        q.getAttributes().put(getId() + "-" + Attributes.SAMPLES, samples);
        
    }

    /**
     * Return the query plan to be executed based on the RTO determined join
     * ordering.
     * 
     * @see Attributes#QUERY_PLAN
     */
    public PipelineOp getQueryPlan(final IRunningQuery q) {

        return (PipelineOp) q.getAttributes().get(
                getId() + "-" + Attributes.QUERY_PLAN);

    }

    private void setQueryPlan(final IRunningQuery q,
            final PipelineOp queryPlan) {
        
        q.getAttributes().put(getId() + "-" + Attributes.QUERY_PLAN, queryPlan);
        
    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public JoinGraph(final JoinGraph op) {
    
        super(op);
        
    }

    public JoinGraph(final BOp[] args, final NV... anns) {

        this(args, NV.asMap(anns));

	}

    public JoinGraph(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        // required property.
        final IVariable<?>[] selected = (IVariable[]) getProperty(Annotations.SELECTED);

        if (selected == null)
            throw new IllegalArgumentException(Annotations.SELECTED);

        if (selected.length == 0)
            throw new IllegalArgumentException(Annotations.SELECTED);

        // required property.
        final IPredicate<?>[] vertices = (IPredicate[]) getProperty(Annotations.VERTICES);

        if (vertices == null)
            throw new IllegalArgumentException(Annotations.VERTICES);

        if (vertices.length == 0)
            throw new IllegalArgumentException(Annotations.VERTICES);

        if (getLimit() <= 0)
            throw new IllegalArgumentException(Annotations.LIMIT);

        if (getNEdges() <= 0)
            throw new IllegalArgumentException(Annotations.NEDGES);

        if (!isController())
            throw new IllegalArgumentException();

        switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new IllegalArgumentException(Annotations.EVALUATION_CONTEXT
                    + "=" + getEvaluationContext());
        }

	}

    @Override
	public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

		return new FutureTask<Void>(new JoinGraphTask(context));

	}


	/**
	 * Evaluation of a {@link JoinGraph}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	private class JoinGraphTask implements Callable<Void> {

	    private final BOpContext<IBindingSet> context;

	    JoinGraphTask(final BOpContext<IBindingSet> context) {

	        if (context == null)
	            throw new IllegalArgumentException();

	        this.context = context;

	    }

        /**
         * {@inheritDoc}
         * 
         * FIXME When run as sub-query, we need to fix point the upstream
         * solutions and then flood them into the join graph. Samples of the
         * known bound variables can be pulled from those initial solutions.
         */
	    @Override
	    public Void call() throws Exception {
	        
	        final long begin = System.nanoTime();
	        
            // Create the join graph.
            final JGraph g = new JGraph(getVertices(), getConstraints(),
                    getSampleType());

            /*
             * This map is used to associate join path segments (expressed as an
             * ordered array of bopIds) with edge sample to avoid redundant effort.
             * 
             * FIXME RTO: HEAP MANAGMENT : This map holds references to the cutoff
             * join samples. To ensure that the map has the minimum heap footprint,
             * it must be scanned each time we prune the set of active paths and any
             * entry which is not a prefix of an active path should be removed.
             * 
             * TODO RTO: MEMORY MANAGER : When an entry is cleared from this map,
             * the corresponding allocation in the memory manager (if any) must be
             * released. The life cycle of the map needs to be bracketed by a
             * try/finally in order to ensure that all allocations associated with
             * the map are released no later than when we leave the lexicon scope of
             * that clause.
             */
            final Map<PathIds, EdgeSample> edgeSamples = new LinkedHashMap<PathIds, EdgeSample>();

            // Find the best join path.
            final Path p = g.runtimeOptimizer(context.getRunningQuery()
                    .getQueryEngine(), getLimit(), getNEdges(), edgeSamples);

            // Set attribute for the join path result.
            setPath(context.getRunningQuery(), p);

            // Set attribute for the join path samples.
            setSamples(context.getRunningQuery(), edgeSamples);

	        final long mark = System.nanoTime();
	        
	        final long elapsed_queryOptimizer = mark - begin;
	        
	        // Factory avoids reuse of bopIds assigned to the predicates.
	        final BOpIdFactory idFactory = new BOpIdFactory();

            /*
             * Generate the query from the join path.
             * 
             * FIXME Update this using StaticAnalysis logic. Also, both this and
             * the JGraph need to handle triples versus named graph versus
             * default graph APs. Further, JGraph should handle filters that
             * require conditional materialization.
             */
			final PipelineOp queryOp = PartitionedJoinGroup.getQuery(idFactory,
					false/* distinct */, getSelected(), p.getPredicates(),
					getConstraints());

            // Set attribute for the join path samples.
            setQueryPlan(context.getRunningQuery(), queryOp);

            // Run the query, blocking until it is done.
	        JoinGraph.runSubquery(context, queryOp);

	        final long elapsed_queryExecution = System.nanoTime() - mark;
	        
            if (log.isInfoEnabled())
                log.info("RTO: queryOptimizer="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed_queryOptimizer)
                        + ", queryExecution="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed_queryExecution));

	        return null;

	    }

	} // class JoinGraphTask

    /**
     * Execute the selected join path.
     * <p>
     * Note: When executing the query, it is actually being executed as a
     * subquery. Therefore we have to take appropriate care to ensure that the
     * results are copied out of the subquery and into the parent query. See
     * {@link AbstractSubqueryOp} for how this is done.
     * 
     * @throws Exception
     * 
     * @todo When we execute the query, we should clear the references to the
     *       samples (unless they are exact, in which case they can be used as
     *       is) in order to release memory associated with those samples if the
     *       query is long running. Samples must be held until we have
     *       identified the final join path since each vertex will be used by
     *       each maximum length join path and we use the samples from the
     *       vertices to re-sample the surviving join paths in each round. [In
     *       fact, the samples are not being provided to this evaluation context
     *       right now.]
     * 
     * @todo If there are source binding sets then they need to be applied above
     *       (when we are sampling) and below (when we evaluate the selected
     *       join path).
     */
    static private void runSubquery(
            final BOpContext<IBindingSet> parentContext,
            final PipelineOp queryOp) throws Exception {

        final QueryEngine queryEngine = parentContext.getRunningQuery()
                .getQueryEngine();

        /*
         * Run the query.
         * 
         * @todo pass in the source binding sets here and also when sampling the
         * vertices.
         */

        ICloseableIterator<IBindingSet[]> subquerySolutionItr = null;

        final IRunningQuery runningSubquery = queryEngine.eval(queryOp);

        try {

            // Declare the child query to the parent.
            ((AbstractRunningQuery) parentContext.getRunningQuery())
                    .addChild(runningSubquery);

            // Iterator visiting the subquery solutions.
            subquerySolutionItr = runningSubquery.iterator();

            // Copy solutions from the subquery to the query.
            final long nout = BOpUtility.copy(subquerySolutionItr,
                    parentContext.getSink(), null/* sink2 */,
                    null/* mergeSolution */, null/* selectVars */,
                    null/* constraints */, null/* stats */);

//            System.out.println("nout=" + nout);

            // verify no problems.
            runningSubquery.get();

//            System.out.println("Future Ok");

        } catch (Throwable t) {

            if (Haltable.isTerminationByInterrupt(t)) {

                // normal termination.
                return;

            }

            // log.error(t,t);

            /*
             * Propagate the error to the parent and rethrow the first cause
             * error out of the subquery.
             */
            throw new RuntimeException(parentContext.getRunningQuery().halt(t));

        } finally {

            runningSubquery.cancel(true/* mayInterruptIfRunning */);

            if (subquerySolutionItr != null)
                subquerySolutionItr.close();

        }

    }

}
