/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.bop.controller;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.rule.Rule;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of
 * relations and joins which connect those relations. This boils down to a
 * collection of {@link IPredicate}s (selects on relations) and shared variables
 * (which identify joins). Operators other than standard joins (including
 * optional joins, sort, order by, etc.) must be handled downstream from the
 * join graph in a "tail plan".
 * 
 * @see http://arxiv.org/PS_cache/arxiv/pdf/0810/0810.4809v1.pdf, XQuery Join
 *      Graph Isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Examine the overhead of the runtime optimizer. Look at ways to prune
 *       its costs. For example, by pruning the search, by recognizing when the
 *       query is simple enough to execute directly, by recognizing when we have
 *       already materialized the answer to the query, etc.
 */
public class JoinGraph extends PipelineOp {

	private static final transient Logger log = Logger
			.getLogger(JoinGraph.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Known annotations.
	 */
	public interface Annotations extends PipelineOp.Annotations {

		/**
		 * The vertices of the join graph expressed an an {@link IPredicate}[].
		 */
		String VERTICES = JoinGraph.class.getName() + ".vertices";

		/**
		 * The initial limit for cutoff sampling (default
		 * {@value #DEFAULT_LIMIT}).
		 */
		String LIMIT = JoinGraph.class.getName() + ".limit";

		int DEFAULT_LIMIT = 100;
	}

	/**
	 * @see Annotations#VERTICES
	 */
	public IPredicate[] getVertices() {

		return (IPredicate[]) getRequiredProperty(Annotations.VERTICES);

	}

	/**
	 * @see Annotations#LIMIT
	 */
	public int getLimit() {

		return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);

	}

	public JoinGraph(final NV... anns) {

		this(BOpBase.NOARGS, NV.asMap(anns));

	}

	/**
	 * 
	 * TODO How can join constraints be moved around? Just attach them where
	 * ever a variable becomes bound? And when do we filter out variables which
	 * are not required downstream? Once we decide on a join path and execute it
	 * fully (rather than sampling that join path).
	 */
	public JoinGraph(final BOp[] args, final Map<String, Object> anns) {

		super(args, anns);

		switch (getEvaluationContext()) {
		case CONTROLLER:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

	}

	public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

		return new FutureTask<Void>(new JoinGraphTask(context));

	}

	/**
	 * Used to assign row identifiers.
	 */
	static private final IVariable<Integer> ROWID = Var.var("__rowid");

	/**
	 * A sample of a {@link Vertex} (an access path).
	 */
	public static class VertexSample {

		/**
		 * Fast range count. This will be the same for each sample taken
		 * (assuming a read historical view or even a time scale of query which
		 * is significantly faster than update).
		 */
		public final long rangeCount;

		/**
		 * The limit used to produce the {@link #sample}.
		 */
		public final int limit;

		/**
		 * When <code>true</code>, the result is not a sample but the
		 * materialized access path.
		 * 
		 * TODO When <code>true</code>, we could run the join against the sample
		 * rather than the disk. This would require wrapping the sample as an
		 * access path. Since all exact samples will be pretty small, this is
		 * not likely to have any great performance benefit.
		 */
		public final boolean exact;

		/**
		 * Sample.
		 */
		final Object[] sample;

		/**
		 * 
		 * @param rangeCount
		 * @param limit
		 * @param exact
		 * @param sample
		 */
		public VertexSample(final long rangeCount, final int limit,
				final boolean exact, final Object[] sample) {

			if (rangeCount < 0L)
				throw new IllegalArgumentException();

			if (limit <= 0)
				throw new IllegalArgumentException();

			if (sample == null)
				throw new IllegalArgumentException();

			this.rangeCount = rangeCount;

			this.limit = limit;

			this.exact = exact;

			this.sample = sample;

		}

		public String toString() {
			return "VertexSample{rangeCount=" + rangeCount + ",limit=" + limit
					+ ",exact=" + exact + ", sampleSize=" + sample.length + "}";
		}

	}

	/**
	 * A vertex of the join graph is an annotated relation (this corresponds to
	 * an {@link IPredicate} with additional annotations to support the adaptive
	 * query optimization algorithm).
	 * <p>
	 * The unique identifier for a {@link Vertex} (within a given join graph) is
	 * the {@link BOp.Annotations#BOP_ID} decorating its {@link IPredicate}.
	 * {@link #hashCode()} is defined in terms of this unique identifier so we
	 * can readily detect when a {@link Set} already contains a given
	 * {@link Vertex}.
	 */
	public static class Vertex implements Serializable {

		/**
         * 
         */
		private static final long serialVersionUID = 1L;

		public final IPredicate<?> pred;

		/**
		 * The most recently taken sample of the {@link Vertex}.
		 */
		VertexSample sample = null;

		Vertex(final IPredicate<?> pred) {

			if (pred == null)
				throw new IllegalArgumentException();

			this.pred = pred;

		}

		public String toString() {

			return "Vertex{pred=" + pred + ",sample=" + sample + "}";

		}

		/**
		 * Equals is based on a reference test.
		 */
		public boolean equals(Object o) {
			return this == o;
		}

		/**
		 * The hash code is just the {@link BOp.Annotations#BOP_ID} of the
		 * associated {@link IPredicate}.
		 */
		public int hashCode() {
			return pred.getId();
		}

		/**
		 * Take a sample of the vertex. If the sample is already exact, then
		 * this is a NOP.
		 * 
		 * @param limit
		 *            The sample cutoff.
		 */
		public void sample(final QueryEngine queryEngine, final int limit) {

			if (queryEngine == null)
				throw new IllegalArgumentException();

			if (limit <= 0)
				throw new IllegalArgumentException();

			final VertexSample oldSample = this.sample;

			if (oldSample != null && oldSample.exact) {

				/*
				 * The old sample is already the full materialization of the
				 * vertex.
				 */

				return;

			}

			final BOpContextBase context = new BOpContextBase(queryEngine);
			
			final IRelation r = context.getRelation(pred);

			final IAccessPath ap = context.getAccessPath(r, pred);

			final long rangeCount = oldSample == null ? ap
					.rangeCount(false/* exact */) : oldSample.rangeCount;

			if (rangeCount <= limit) {

				/*
				 * Materialize the access path.
				 * 
				 * TODO This could be more efficient if we raised it onto the AP
				 * or if we overrode CHUNK_CAPACITY and the fully buffered
				 * iterator threshold such that everything was materialized as a
				 * single chunk.
				 */

				final List<Object> tmp = new ArrayList<Object>((int) rangeCount);

				final IChunkedIterator<Object> itr = ap.iterator();

				try {

					while (itr.hasNext()) {

						tmp.add(itr.next());

					}

				} finally {

					itr.close();
				}

				sample = new VertexSample(rangeCount, limit, true/* exact */,
						tmp.toArray(new Object[0]));

			} else {

				/*
				 * Materialize a random sample from the access path.
				 */

				final SampleIndex sampleOp = new SampleIndex(
						new BOp[] {}, //
						NV.asMap(//
							new NV(SampleIndex.Annotations.PREDICATE, pred),//
							new NV(SampleIndex.Annotations.LIMIT, limit)));

				sample = new VertexSample(rangeCount, limit, false/* exact */,
						sampleOp.eval(context));

			}

			if (log.isInfoEnabled())
				log.info("Sampled: " + sample);

			return;
			
		}

	}

	/**
	 * A sample of an {@link Edge} (a join).
	 */
	public static class EdgeSample {

		/**
		 * The fast range count (aka cardinality) for the source vertex of the
		 * edge (whichever vertex has the lower cardinality).
		 */
		public final long rangeCount;

		/**
		 * The limit used to sample the edge (this is the limit on the #of
		 * solutions generated by the cutoff join used when this sample was
		 * taken).
		 */
		public final int limit;

		/**
		 * The #of binding sets out of the source sample vertex sample which
		 * were consumed.
		 */
		public final int inputCount;

		/**
		 * The #of binding sets generated before the join was cutoff.
		 * <p>
		 * Note: If the outputCount is zero then this is a good indicator that
		 * there is an error in the query such that the join will not select
		 * anything. This is not 100%, merely indicative.
		 */
		public final int outputCount;

		/**
		 * The ratio of the #of input samples consumed to the #of output samples
		 * generated (the join hit ratio or scale factor).
		 */
		public final double f;

		/**
		 * The estimated cardinality of the join.
		 */
		public final long estimatedCardinality;

		/**
		 * Flag is set when the estimate is likely to be a lower bound for the
		 * cardinality of the edge.
		 * <p>
		 * If the {@link #inputCount} is ONE (1) and the {@link #outputCount} is
		 * the {@link #limit} then the {@link #estimatedCardinality} is a lower
		 * bound as more than {@link #outputCount} solutions could have been
		 * produced by the join against a single input solution.
		 */
		public final boolean estimateIsLowerBound;

		/**
		 * Flag indicates that the {@link #estimatedCardinality} underflowed.
		 * <p>
		 * Note: When the source vertex sample was not exact, then it is
		 * possible for the cardinality estimate to underflow. When, in
		 * addition, {@link #outputCount} is LT {@link #limit}, then feeding the
		 * sample of source tuples in is not sufficient to generated the desired
		 * #of output tuples. In this case, {@link #f join hit ratio} will be
		 * low. It may even be that zero output tuples were generated, in which
		 * case the join hit ratio will appear to be zero. However, the join hit
		 * ratio actually underflowed and an apparent join hit ratio of zero
		 * does not imply that the join will be empty unless the source vertex
		 * sample is actually the fully materialized access path - see
		 * {@link VertexSample#exact} and {@link #exact}.
		 */
		public final boolean estimateIsUpperBound;

		/**
		 * <code>true</code> if the sample is the exact solution for the join
		 * path.
		 * <p>
		 * Note: If the entire source vertex is being feed into the sample,
		 * {@link VertexSample#exact} flags this condition, and outputCount is
		 * also LT the limit, then the edge sample is the actual result of the
		 * join. That is, feeding all source tuples into the join gives fewer
		 * than the desired number of output tuples.
		 * 
		 * TODO This field marks this condition and should be used to avoid
		 * needless re-computation of a join whose exact solution is already
		 * known.
		 */
		public final boolean exact;

		/**
		 * The sample of the solutions for the join path.
		 */
		private final IBindingSet[] sample;

		/**
		 * Create an object which encapsulates a sample of an edge.
		 * 
		 * @param limit
		 *            The limit used to sample the edge (this is the limit on
		 *            the #of solutions generated by the cutoff join used when
		 *            this sample was taken).
		 * @param sourceVertexSample
		 *            The sample for source vertex of the edge (whichever vertex
		 *            has the lower cardinality).
		 * @param inputCount
		 *            The #of binding sets out of the source sample vertex
		 *            sample which were consumed.
		 * @param outputCount
		 *            The #of binding sets generated before the join was cutoff.
		 */
		EdgeSample(
				// final VertexSample sourceVertexSample,
				final long sourceSampleRangeCount,
				final boolean sourceSampleExact, final int limit,
				final int inputCount, final int outputCount,
				final IBindingSet[] sample) {

			if (sample == null)
				throw new IllegalArgumentException();

			// this.rangeCount = sourceVertexSample.rangeCount;
			this.rangeCount = sourceSampleRangeCount;

			this.limit = limit;

			this.inputCount = inputCount;

			this.outputCount = outputCount;

			f = outputCount == 0 ? 0 : (outputCount / (double) inputCount);

			estimatedCardinality = (long) (rangeCount * f);

			estimateIsLowerBound = inputCount == 1 && outputCount == limit;

			// final boolean sourceSampleExact = sourceVertexSample.exact;
			estimateIsUpperBound = !sourceSampleExact && outputCount < limit;

			this.exact = sourceSampleExact && outputCount < limit;

			this.sample = sample;
		}

		public String toString() {
			return getClass().getName() + "{inputRangeCount=" + rangeCount
					+ ", limit=" + limit + ", inputCount=" + inputCount
					+ ", outputCount=" + outputCount + ", f=" + f
					+ ", estimatedCardinality=" + estimatedCardinality
					+ ", estimateIsLowerBound=" + estimateIsLowerBound
					+ ", estimateIsUpperBound=" + estimateIsUpperBound
					+ ", sampleIsExactSolution=" + exact + "}";
		}

	};

	/**
	 * An edge of the join graph is an annotated join operator. The edges of the
	 * join graph are undirected. Edges exist when the vertices share at least
	 * one variable.
	 * <p>
	 * {@link #hashCode()} is defined in terms of the unordered hash codes of
	 * the individual vertices.
	 */
	public static class Edge implements Serializable {

		/**
         * 
         */
		private static final long serialVersionUID = 1L;

		/**
		 * The vertices connected by that edge.
		 */
		public final Vertex v1, v2;

		/**
		 * The set of shared variables.
		 */
		public final Set<IVariable<?>> shared;

		/**
		 * The last sample for this edge and <code>null</code> if the edge has
		 * not been sampled.
		 */
		public EdgeSample sample = null;

		public Edge(final Vertex v1, final Vertex v2,
				final Set<IVariable<?>> shared) {
			if (v1 == null)
				throw new IllegalArgumentException();
			if (v2 == null)
				throw new IllegalArgumentException();
			if (shared == null)
				throw new IllegalArgumentException();
			if (shared.isEmpty())
				throw new IllegalArgumentException();
			this.v1 = v1;
			this.v2 = v2;
			this.shared = shared;
		}

		/**
		 * Note: The vertices of the edge are labeled using the
		 * {@link BOp.Annotations#BOP_ID} associated with the {@link IPredicate}
		 * for each vertex.
		 */
		public String toString() {

			return "Edge{ (V" + v1.pred.getId() + ",V" + v2.pred.getId()
					+ "), estCard="
					+ (sample == null ? "N/A" : sample.estimatedCardinality)
					+ ", shared=" + shared.toString() + ", sample=" + sample
					+ "}";

		}

		/**
		 * Equality is determined by reference testing.
		 */
		public boolean equals(final Object o) {

			return this == o;

		}

		/**
		 * The hash code of an edge is the hash code of the vertex with the
		 * smaller hash code X 31 plus the hash code of the vertex with the
		 * larger hash code. This definition compensates for the arbitrary order
		 * in which the vertices may be expressed and also recognizes that the
		 * vertex hash codes are based on the bop ids, which are often small
		 * integers.
		 */
		public int hashCode() {

			if (hash == 0) {

				final int h1 = v1.hashCode();
				final int h2 = v2.hashCode();

				final int h;
				if (h1 < h2) {

					h = h1 * 31 + h2;

				} else {

					h = h2 * 31 + h1;

				}

				hash = h;

			}
			return hash;

		}

		private int hash;

		/**
		 * Return the vertex with the smaller estimated cardinality.
		 * 
		 * @throws IllegalStateException
		 *             if either vertex has not been sampled.
		 */
		public Vertex getMinimumCardinalityVertex() {

			if (v1.sample == null) // vertex not sampled.
				throw new IllegalStateException();

			if (v2.sample == null) // vertex not sampled.
				throw new IllegalStateException();

			return (v1.sample.rangeCount < v2.sample.rangeCount) ? v1 : v2;

		}

		/**
		 * Return the vertex with the larger estimated cardinality (the vertex
		 * not returned by {@link #getMinimumCardinalityVertex()}).
		 * 
		 * @throws IllegalStateException
		 *             if either vertex has not been sampled.
		 */
		public Vertex getMaximumCardinalityVertex() {

			// The vertex with the minimum cardinality.
			final Vertex o = getMinimumCardinalityVertex();

			// Return the other vertex.
			return (v1 == o) ? v2 : v1;

		}

		/**
		 * Estimate the cardinality of the edge.
		 * 
		 * @param context
		 * 
		 * @return The estimated cardinality of the edge.
		 * 
		 * @throws Exception
		 */
		public long estimateCardinality(final QueryEngine queryEngine,
				final int limit) throws Exception {

			if (limit <= 0)
				throw new IllegalArgumentException();

			/*
			 * Figure out which vertex has the smaller cardinality. The sample
			 * of that vertex is used since it is more representative than the
			 * sample of the other vertex.
			 */
			// vertex v, vprime
			final Vertex v, vp;
			if (v1.sample == null) // vertex not sampled.
				throw new IllegalStateException();
			if (v2.sample == null) // vertex not sampled.
				throw new IllegalStateException();
			if (v1.sample.rangeCount < v2.sample.rangeCount) {
				v = v1;
				vp = v2;
			} else {
				v = v2;
				vp = v1;
			}

			/*
			 * TODO This is difficult to setup because we do not have a concept
			 * (or class) corresponding to a fly weight relation and we do not
			 * have a general purpose relation, just arrays or sequences of
			 * IBindingSets. Also, all relations are persistent. Temporary
			 * relations are on a temporary store and are locatable by their
			 * namespace rather than being Objects.
			 * 
			 * The algorithm presupposes fly weight / temporary relations this
			 * both to wrap the sample and to store the computed intermediate
			 * results.
			 * 
			 * Note: The PipelineJoin does not have a means to halt after a
			 * limit is satisfied. In order to achieve this, we have to wrap it
			 * with a SliceOp.
			 * 
			 * Together, this means that we are dealing with IBindingSet[]s for
			 * both the input and the output of the cutoff evaluation of the
			 * edge rather than rows of the materialized relation.
			 * 
			 * TODO On subsequent iterations we would probably re-sample [v] and
			 * we would run against the materialized intermediate result for
			 * [v'].
			 */

			/*
			 * Convert the source sample into an IBindingSet[].
			 * 
			 * TODO We might as well do this when we sample the vertex.
			 */
			final IBindingSet[] sourceSample = new IBindingSet[v.sample.sample.length];
			{
				for (int i = 0; i < sourceSample.length; i++) {
					final IBindingSet bset = new HashBindingSet();
					BOpContext.copyValues((IElement) v.sample.sample[i],
							v.pred, bset);
					sourceSample[i] = bset;
				}
			}

			// Sample the edge and save the sample on the edge as a side-effect.
			this.sample = estimateCardinality(queryEngine, limit, v, vp,
					v.sample.rangeCount, v.sample.exact, sourceSample);

			return sample.estimatedCardinality;

		}

		/**
		 * Estimate the cardinality of the edge.
		 * 
		 * @param queryEngine
		 * @param limit
		 * @param vSource
		 *            The source vertex.
		 * @param vTarget
		 *            The target vertex
		 * @param sourceSample
		 *            The sample for the source vertex. When this is a one-step
		 *            estimation of the cardinality of the edge, then this
		 *            sample is taken from the {@link VertexSample}. When the
		 *            edge (vSource,vTarget) extends some {@link Path}, then
		 *            this is taken from the {@link EdgeSample} for that
		 *            {@link Path}.
		 * 
		 * @return The result of sampling that edge.
		 * 
		 * @throws Exception
		 */
		public EdgeSample estimateCardinality(final QueryEngine queryEngine,
				final int limit, final Vertex vSource, final Vertex vTarget,
				final long sourceSampleRangeCount,
				final boolean sourceSampleExact, IBindingSet[] sourceSample)
				throws Exception {

			if (limit <= 0)
				throw new IllegalArgumentException();

			// Inject a rowId column.
			sourceSample = BOpUtility.injectRowIdColumn(ROWID, 1/* start */,
					sourceSample);

			/*
			 * TODO Any constraints on the edge (other than those implied by
			 * shared variables) need to be annotated on the join. Constraints
			 * (other than range constraints which are directly coded by the
			 * predicate) will not reduce the effort to compute the join, but
			 * they can reduce the cardinality of the join and that is what we
			 * are trying to estimate here.
			 */
			final PipelineJoin joinOp = new PipelineJoin(new BOp[] {}, //
					new NV(BOp.Annotations.BOP_ID, 1),//
					new NV(PipelineJoin.Annotations.PREDICATE, vTarget.pred
							.setBOpId(3)));

			final SliceOp sliceOp = new SliceOp(new BOp[] { joinOp },//
					NV.asMap(//
							new NV(BOp.Annotations.BOP_ID, 2), //
							new NV(SliceOp.Annotations.LIMIT, (long) limit), //
							new NV(BOp.Annotations.EVALUATION_CONTEXT,
									BOpEvaluationContext.CONTROLLER)));

			// run the cutoff sampling of the edge.
			final UUID queryId = UUID.randomUUID();
			final RunningQuery runningQuery = queryEngine.eval(queryId,
					sliceOp, new LocalChunkMessage<IBindingSet>(queryEngine,
							queryId, joinOp.getId()/* startId */,
							-1 /* partitionId */,
							new ThickAsynchronousIterator<IBindingSet[]>(
									new IBindingSet[][] { sourceSample })));

			// #of source samples consumed.
			int inputCount = 0;
			// #of output samples generated.
			int outputCount = 0;
			final List<IBindingSet> result = new LinkedList<IBindingSet>();
			try {
				try {
					IBindingSet bset = null;
					// Figure out the #of source samples consumed.
					final Iterator<IBindingSet> itr = new Dechunkerator<IBindingSet>(
							runningQuery.iterator());
					while (itr.hasNext()) {
						bset = itr.next();
						result.add(bset);
						outputCount++;
					}
					// #of input rows consumed.
					inputCount = bset == null ? 0 : ((Integer) bset.get(ROWID)
							.get());
				} finally {
					// verify no problems. FIXME Restore test of the query.
					// runningQuery.get();
				}
			} finally {
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}

			/*
			 * TODO Improve comments here. See if it is possible to isolate a
			 * common base class which would simplify the setup of the cutoff
			 * join and the computation of the sample stats.
			 */

			final EdgeSample edgeSample = new EdgeSample(
					sourceSampleRangeCount, sourceSampleExact, limit,
					inputCount, outputCount, result
							.toArray(new IBindingSet[result.size()]));

			if (log.isInfoEnabled())
				log.info("edge=" + this + ", sample=" + edgeSample);

			return edgeSample;

		}

	}

	/**
	 * A sequence of {@link Edge}s (aka join steps).
	 */
	public static class Path {

		/**
		 * An immutable ordered list of the edges in the (aka the sequence of
		 * joins represented by this path).
		 */
		public final List<Edge> edges;

		/**
		 * The sample obtained by the step-wise cutoff evaluation of the ordered
		 * edges of the path. This sample is generated one edge at a time rather
		 * than by attempting the cutoff evaluation of the entire join path (the
		 * latter approach does allow us to limit the amount of work to be done
		 * to satisfy the cutoff).
		 */
		final public EdgeSample sample;

		/**
		 * The cumulative estimated cardinality of the path. This is zero for an
		 * empty path. For a path consisting of a single edge, this is the
		 * estimated cardinality of that edge. When creating a new path adding
		 * an edge to an existing path, the cumulative cardinality of the new
		 * path is the cumulative cardinality of the existing path plus the
		 * estimated cardinality of the cutoff join of the new edge given the
		 * input sample of the existing path.
		 */
		final public long cumulativeEstimatedCardinality;

		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("Path{");
			boolean first = true;
			for (Edge e : edges) {
				if (!first)
					sb.append(",");
				sb.append("(" + e.v1.pred.getId() + "," + e.v2.pred.getId()
						+ ")");
				first = false;
			}
			sb.append(",cumEstCard=" + cumulativeEstimatedCardinality
					+ ",sample=" + sample + "}");
			return sb.toString();
		}

		/**
		 * Create an empty path.
		 */
		public Path() {
			this.edges = Collections.emptyList();
			this.cumulativeEstimatedCardinality = 0;
			this.sample = null;
		}

		/**
		 * Create a path from a single edge.
		 * 
		 * @param e
		 *            The edge.
		 */
		public Path(final Edge e) {

			if (e == null)
				throw new IllegalArgumentException();

			if (e.sample == null)
				throw new IllegalArgumentException("Not sampled: " + e);

			this.edges = Collections.singletonList(e);

			this.sample = e.sample;

			this.cumulativeEstimatedCardinality = e.sample.estimatedCardinality;

		}

		/**
		 * Constructor used by {@link #addEdge(QueryEngine, int, Edge)}
		 * 
		 * @param edges
		 *            The edges in the new path.
		 * @param cumulativeEstimatedCardinality
		 *            The cumulative estimated cardinality of the new path.
		 * @param sample
		 *            The sample from the last
		 */
		private Path(final List<Edge> edges,
				final long cumulativeEstimatedCardinality,
				final EdgeSample sample) {

			if (edges == null)
				throw new IllegalArgumentException();

			if (cumulativeEstimatedCardinality < 0)
				throw new IllegalArgumentException();

			if (sample == null)
				throw new IllegalArgumentException();

			this.edges = Collections.unmodifiableList(edges);

			this.cumulativeEstimatedCardinality = cumulativeEstimatedCardinality;

			this.sample = sample;

		}

		/**
		 * Return <code>true</code> iff the {@link Path} contains at least one
		 * {@link Edge} for that {@link Vertex}.
		 * 
		 * @param v
		 *            The vertex
		 * 
		 * @return true if the vertex is already part of the path.
		 */
		public boolean contains(final Vertex v) {

			if (v == null)
				throw new IllegalArgumentException();

			for (Edge e : edges) {

				if (e.v1 == v || e.v2 == v)
					return true;

			}

			return false;
		}

		/**
		 * Return <code>true</code> if this path is an unordered super set of
		 * the given path. In the case where both paths have the same vertices
		 * this will also return <code>true</code>.
		 * 
		 * @param p
		 *            Another path.
		 * 
		 * @return <code>true</code> if this path is an unordered super set of
		 *         the given path.
		 */
		public boolean isUnorderedSuperSet(final Path p) {

			if (p == null)
				throw new IllegalArgumentException();

			if (edges.size() < p.edges.size()) {
				/*
				 * Fast rejection. This assumes that each edge after the first
				 * adds one distinct vertex to the path. That assumption is
				 * enforced by #addEdge().
				 */
				return false;
			}

			final Vertex[] v1 = getVertices();
			final Vertex[] v2 = p.getVertices();

			if (v1.length < v2.length) {
				// Proven false since the other set is larger.
				return false;
			}

			/*
			 * Scan the vertices of the caller's path. If any of those vertices
			 * are NOT found in this path then the caller's path can not be a
			 * subset of this path.
			 */
			for (int i = 0; i < v2.length; i++) {

				final Vertex tmp = v2[i];

				boolean found = false;
				for (int j = 0; j < v1.length; j++) {

					if (v1[j] == tmp) {
						found = true;
						break;
					}

				}

				if (!found) {
					return false;
				}

			}

			return true;

		}

		/**
		 * Return the vertices in this path (in path order).
		 * 
		 * @return The vertices (in path order).
		 * 
		 *         TODO This could be rewritten without the toArray() using a
		 *         method which visits the vertices of a path in any order.
		 */
		public Vertex[] getVertices() {
			final Set<Vertex> tmp = new LinkedHashSet<Vertex>();
			for (Edge e : edges) {
				tmp.add(e.v1);
				tmp.add(e.v2);
			}
			final Vertex[] a = tmp.toArray(new Vertex[tmp.size()]);
			return a;
		}

		/**
		 * Return <code>true</code> if this path begins with the given path.
		 * 
		 * @param p
		 *            The given path.
		 * 
		 * @return <code>true</code> if this path begins with the given path.
		 */
		public boolean beginsWith(final Path p) {

			if (p == null)
				throw new IllegalArgumentException();

			if (p.edges.size() > edges.size()) {
				// Proven false since the caller's path is longer.
				return false;
			}

			for (int i = 0; i < p.edges.size(); i++) {
				final Edge eSelf = edges.get(i);
				final Edge eOther = p.edges.get(i);
				if (eSelf != eOther) {
					return false;
				}
			}

			return true;
		}

		/**
		 * Add an edge to a path, computing the estimated cardinality of the new
		 * path, and returning the new path.
		 * 
		 * @param queryEngine
		 * @param limit
		 * @param e
		 *            The edge.
		 * 
		 * @return The new path.
		 * 
		 * @throws Exception
		 */
		public Path addEdge(final QueryEngine queryEngine, final int limit,
				final Edge e) throws Exception {

			if (e == null)
				throw new IllegalArgumentException();

			// Figure out which vertices are already part of this path.
			final boolean v1Found = contains(e.v1);
			final boolean v2Found = contains(e.v2);

			if (!v1Found && !v2Found)
				throw new IllegalArgumentException(
						"Edge does not extend path: edge=" + e + ", path="
								+ this);

			if (v1Found && v2Found)
				throw new IllegalArgumentException(
						"Edge already present in path: edge=" + e + ", path="
								+ this);

			// The vertex which is already part of this path.
			final Vertex sourceVertex = v1Found ? e.v1 : e.v2;

			// The new vertex, which is not part of this path.
			final Vertex targetVertex = v1Found ? e.v2 : e.v1;

			/*
			 * Chain sample the edge.
			 * 
			 * Note: ROX uses the intermediate result I(p) for the existing path
			 * as the input when sampling the edge. The corresponding concept
			 * for us is the sample for this Path, which will have all variable
			 * bindings produced so far. In order to estimate the cardinality of
			 * the new join path we have to do a one step cutoff evaluation of
			 * the new Edge, given the sample available on the current Path.
			 * 
			 * TODO It is possible for the path sample to be empty. Unless the
			 * sample also happens to be exact, this is an indication that the
			 * estimated cardinality has underflowed. How are we going to deal
			 * with this situation?!? What would appear to matter is the amount
			 * of work being performed by the join in achieving that low
			 * cardinality. If we have to do a lot of work to get a small
			 * cardinality then we should prefer join paths which achieve the
			 * same reduction in cardinality with less 'intermediate
			 * cardinality' - that is, by examining fewer possible solutions.
			 * [In fact, the estimated (cumulative) cardinality might not be a
			 * good reflection of the IOs to be done -- this needs more
			 * thought.]
			 */

			final EdgeSample edgeSample = e.estimateCardinality(queryEngine,
					limit, sourceVertex, targetVertex,
					this.sample.estimatedCardinality, this.sample.exact,
					this.sample.sample);

			{

				final List<Edge> edges = new ArrayList<Edge>(
						this.edges.size() + 1);

				edges.addAll(this.edges);

				edges.add(e);

				final long cumulativeEstimatedCardinality = this.cumulativeEstimatedCardinality
						+ edgeSample.estimatedCardinality;

				// Extend the path.
				final Path tmp = new Path(edges,
						cumulativeEstimatedCardinality, edgeSample);

				// tmp.stopVertex = e.getMaximumCardinalityVertex();

				return tmp;

			}

		}

		// /**
		// * Equality is defined by comparison of the unordered set of edges.
		// */
		// public boolean equals(final Object o) {
		// if (this == o)
		// return true;
		// if (!(o instanceof Path))
		// return false;
		// final Path t = (Path) o;
		// if (edges.length != t.edges.length)
		// return false;
		// for (Edge e : edges) {
		// boolean found = false;
		// for (Edge x : t.edges) {
		// if (x.equals(e)) {
		// found = true;
		// break;
		// }
		// }
		// if (!found)
		// return false;
		// }
		// return true;
		// }
		//		
		// /**
		// * The hash code of path is defined as the bit-wise XOR of the hash
		// * codes of the edges in that path.
		// */
		// public int hashCode() {
		//
		// if (hash == 0) {
		//
		// int result = 0;
		//
		// for(Edge e : edges) {
		//
		// result ^= e.hashCode();
		//
		// }
		//
		// hash = result;
		//
		// }
		// return hash;
		//
		// }
		// private int hash;

	}

	/**
	 * Comma delimited table showing the estimated join hit ratio, the estimated
	 * cardinality, and the set of vertices for each of the specified join
	 * paths.
	 * 
	 * @param a
	 *            An array of join paths.
	 * 
	 * @return A table with that data.
	 */
	static public String showTable(final Path[] a) {
		final StringBuilder sb = new StringBuilder();
		final Formatter f = new Formatter(sb);
		for (int i = 0; i < a.length; i++) {
			final Path x = a[i];
			if (x.sample == null) {
				f.format("p[%2d] %7s, %10s %10s", "N/A", "N/A", "N/A", i);
			} else {
				f.format("p[%2d] % 7.2f, % 10d % 10d", i, x.sample.f,
						x.sample.estimatedCardinality,
						x.cumulativeEstimatedCardinality);
			}
			sb.append(", [");
			final Vertex[] vertices = x.getVertices();
			for (Vertex v : vertices) {
				f.format("%2d ", v.pred.getId());
			}
			sb.append("]");
			// for (Edge e : x.edges)
			// sb.append(" (" + e.v1.pred.getId() + " " + e.v2.pred.getId()
			// + ")");
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * A join graph (data structure and methods only).
	 * 
	 * Note: ROX was stated in terms of materialization of intermediate results.
	 * Bigdata was originally designed to support pipelined join evaluation in
	 * which the zero investment property is true (there exists an index for the
	 * join). While support is being developed for operator-at-once joins (e.g.,
	 * hash joins), that support is aimed at more efficient evaluation of high
	 * cardinality joins using multi-block IO. Therefore, unlike ROX, the
	 * runtime query optimizer does not materialize the intermediate results
	 * when chain sampling. Instead, it feeds a sample into a cutoff pipeline
	 * evaluation for the join path. Since some join paths can eliminate a lot
	 * of intermediate solutions and hence take a long time to satisfy the
	 * cutoff, we also specify a timeout for the cutoff evaluation of a join
	 * path. Given the zero investment property (an index exists for the join),
	 * if the cutoff is not satisfied within the timeout, then the join has a
	 * low correlation. If no solutions are generated within the timeout, then
	 * the estimate of the correlation "underflows".
	 * 
	 * Note: timeouts are a bit tricky when you are not running on a real-time
	 * platform. In particular, heavy swapping or heavy GC workloads could both
	 * cause a timeout to expire because no work was done on sampling the join
	 * path rather than because there was a lot of work to be done. Therefore,
	 * the timeout should be used to protect against join paths which take a
	 * long time to materialize <i>cutoff</i> solutions rather than to fine tune
	 * the running time of the query optimizer.
	 * 
	 * TODO Runtime query optimization is probably useless (or else should rely
	 * on materialization of intermediate results) when the cardinality of the
	 * vertices and edges for the query is small. This would let us balance the
	 * design characteristics of MonetDB and bigdata. For this purpose, we need
	 * to flag when a {@link VertexSample} is complete (e.g., the cutoff is GTE
	 * the actual range count). This also needs to be done for each join path so
	 * we can decide when the sample for the path is in fact the exact solution
	 * rather than an estimate of the cardinality of the solution together with
	 * a sample of the solution.
	 */
	public static class JGraph {

		/**
		 * Vertices of the join graph.
		 */
		private final Vertex[] V;

		/**
		 * Edges of the join graph.
		 */
		private final Edge[] E;

		public List<Vertex> getVertices() {
			return Collections.unmodifiableList(Arrays.asList(V));
		}

		public List<Edge> getEdges() {
			return Collections.unmodifiableList(Arrays.asList(E));
		}

		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("JoinGraph");
			sb.append("{V=[");
			for (Vertex v : V) {
				sb.append("\nV[" + v.pred.getId() + "]=" + v);
			}
			sb.append("],E=[");
			for (Edge e : E) {
				sb.append("\n" + e);
			}
			sb.append("\n]}");
			return sb.toString();

			// return super.toString() + "{V=" + Arrays.toString(V) + ",E="
			// + Arrays.toString(E) +
			// ", executedVertices="+executedVertices+"}";
		}

		public JGraph(final IPredicate[] v) {

			if (v == null)
				throw new IllegalArgumentException();

			if (v.length < 2)
				throw new IllegalArgumentException();

			V = new Vertex[v.length];

			for (int i = 0; i < v.length; i++) {

				V[i] = new Vertex(v[i]);

			}

			/*
			 * Identify the edges by looking for shared variables among the
			 * predicates.
			 */
			{

				final List<Edge> tmp = new LinkedList<Edge>();

				for (int i = 0; i < v.length; i++) {

					final IPredicate<?> p1 = v[i];

					for (int j = i + 1; j < v.length; j++) {

						final IPredicate<?> p2 = v[j];

						final Set<IVariable<?>> shared = Rule.getSharedVars(p1,
								p2);

						if (shared != null && !shared.isEmpty()) {

							tmp.add(new Edge(V[i], V[j], shared));

						}

					}

				}

				E = tmp.toArray(new Edge[0]);

			}

		}

		/**
		 * 
		 * @param queryEngine
		 * @param limit
		 *            The limit for sampling a vertex and the initial limit for
		 *            cutoff join evaluation. A reasonable value is
		 *            <code>100</code>.
		 * 
		 * @throws Exception
		 */
		public void runtimeOptimizer(final QueryEngine queryEngine,
				final int limit) throws Exception {

			// Setup the join graph.
			Path[] paths = round0(queryEngine, limit, 2/* nedges */);

			/*
			 * The input paths for the first round have two vertices (one edge
			 * is two vertices). Each round adds one more vertex, so we have
			 * three vertices by the end of round 1. We are done once we have
			 * generated paths which include all vertices.
			 * 
			 * This occurs at round := nvertices - 1
			 */

			final int nvertices = V.length;

			int round = 1;

			while (round < nvertices - 1) {

				paths = expand(queryEngine, limit, round++, paths);

			}

			/*
			 * FIXME Choose the best join path and execute it (or return the
			 * evaluation order to the caller).
			 * 
			 * FIXME This must either recognize each time a join path is known
			 * to dominate all other join paths and then execute it or iterator
			 * until the total join path is decided and then execute the
			 * original query using that join path.
			 * 
			 * @todo When executing the query, it is actually being executed as
			 * a subquery. Therefore we have to take appropriate care to ensure
			 * that the results are copied out of the subquery and into the
			 * parent query.
			 * 
			 * @todo When we execute the query, we should clear the references
			 * to the sample (unless they are exact, in which case they can be
			 * used as is) in order to release memory associated with those
			 * samples if the query is long running.
			 */
			
		}

		/**
		 * Choose the starting vertices.
		 * 
		 * @param nedges
		 *            The maximum #of edges to choose.
		 */
		public Path[] choseStartingPaths(final int nedges) {

			final List<Path> tmp = new LinkedList<Path>();

			// All edges in the graph.
			final Edge[] edges = getEdges().toArray(new Edge[0]);

			// Sort them by ascending expected cardinality.
			Arrays.sort(edges, 0, edges.length,
					EstimatedEdgeCardinalityComparator.INSTANCE);

			// Choose the top-N edges (those with the least cardinality).
			for (int i = 0; i < edges.length && i < nedges; i++) {

				tmp.add(new Path(edges[i]));

			}

			final Path[] a = tmp.toArray(new Path[tmp.size()]);

			return a;

		}

		/**
		 * Choose up to <i>nedges</i> edges to be the starting point.
		 * 
		 * @param queryEngine
		 *            The query engine.
		 * @param limit
		 *            The cutoff used when sampling the vertices and when
		 *            sampling the edges.
		 * @param nedges
		 *            The maximum #of edges to choose. Those having the smallest
		 *            expected cardinality will be chosen.
		 * 
		 * @throws Exception
		 */
		public Path[] round0(final QueryEngine queryEngine, final int limit,
				final int nedges) throws Exception {

			/*
			 * Sample the vertices.
			 */
			sampleVertices(queryEngine, limit);

			if (log.isInfoEnabled()) {
				final StringBuilder sb = new StringBuilder();
				sb.append("Vertices:\n");
				for (Vertex v : V) {
					sb.append(v.toString());
					sb.append("\n");
				}
				log.info(sb.toString());
			}

			/*
			 * Estimate the cardinality for each edge.
			 * 
			 * TODO It would be very interesting to see the variety and/or
			 * distribution of the values bound when the edge is sampled. This
			 * can be easily done using a hash map with a counter. That could
			 * tell us a lot about the cardinality of the next join path
			 * (sampling the join path also tells us a lot, but it does not
			 * explain it as much as seeing the histogram of the bound values).
			 * I believe that there are some interesting online algorithms for
			 * computing the N most frequent observations and the like which
			 * could be used here.
			 */
			estimateEdgeWeights(queryEngine, limit);

			if (log.isInfoEnabled()) {
				final StringBuilder sb = new StringBuilder();
				sb.append("Edges:\n");
				for (Edge e : E) {
					sb.append(e.toString());
					sb.append("\n");
				}
				log.info(sb.toString());
			}

			/*
			 * Choose the initial set of paths.
			 */
			final Path[] paths_t0 = choseStartingPaths(nedges);

			if (log.isInfoEnabled())
				log.info("\n*** Paths @ t0\n" + JoinGraph.showTable(paths_t0));

			return paths_t0;

		}

		/**
		 * Do one breadth first expansion.
		 * 
		 * @param queryEngine
		 *            The query engine.
		 * @param limit
		 *            The limit (this is automatically multiplied by the round
		 *            to increase the sample size in each round).
		 * @param round
		 *            The round number in [1:n].
		 * @param a
		 *            The set of paths from the previous round. For the first
		 *            round, this is formed from the initial set of edges to
		 *            consider.
		 * 
		 * @return The set of paths which survived pruning in this round.
		 * 
		 * @throws Exception
		 */
		public Path[] expand(final QueryEngine queryEngine, int limit,
				final int round, final Path[] a) throws Exception {

			if (queryEngine == null)
				throw new IllegalArgumentException();
			if (limit <= 0)
				throw new IllegalArgumentException();
			if (round <= 0)
				throw new IllegalArgumentException();
			if (a == null)
				throw new IllegalArgumentException();
			if (a.length == 0)
				throw new IllegalArgumentException();
			
			// increment the limit by itself in each round.
			limit *= round;
			
			final List<Path> tmp = new LinkedList<Path>();

			// First, copy all existing paths.
			for (Path x : a) {
				tmp.add(x);
			}

			// Vertices are inserted into this collection when they are resampled.
			final Set<Vertex> resampled = new LinkedHashSet<Vertex>();
			
			// Then expand each path.
			for (Path x : a) {

				if (x.edges.size() < round) {
					// Path is from a previous round.
					continue;
				}

				// The set of vertices used to expand this path in this round.
				final Set<Vertex> used = new LinkedHashSet<Vertex>();

				// Check all edges in the graph.
				for (Edge edgeInGraph : E) {

					// Figure out which vertices are already part of this path.
					final boolean v1Found = x.contains(edgeInGraph.v1);
					final boolean v2Found = x.contains(edgeInGraph.v2);

					if (!v1Found && !v2Found) {
						// Edge is not connected to this path.
						continue;
					}

					if (v1Found && v2Found) {
						// Edge is already present in this path.
						continue;
					}

					final Vertex newVertex = v1Found ? edgeInGraph.v2
							: edgeInGraph.v1;

					if (used.contains(newVertex)) {
						// Vertex already used to extend this path.
						continue;
					}

					// add the new vertex to the set of used vertices.
					used.add(newVertex);

					if (!resampled.add(newVertex)&&round>1) {
						/*
						 * Resample this vertex before we sample a new edge
						 * which targets this vertex.
						 */
						newVertex.sample(queryEngine, limit);
					}
					
					// Extend the path to the new vertex.
					final Path p = x.addEdge(queryEngine, limit,
							edgeInGraph);

					// Add to the set of paths for this round.
					tmp.add(p);

				}

			}

			final Path[] paths_tp1 = tmp.toArray(new Path[tmp.size()]);

			if (log.isDebugEnabled())
				log.debug("\n*** round=" + round + " : generated paths\n"
						+ JoinGraph.showTable(paths_tp1));

			final Path[] paths_tp1_pruned = pruneJoinPaths(paths_tp1);

			if (log.isInfoEnabled())
				log.info("\n*** round=" + round + ": paths{in=" + a.length
						+ ",considered=" + paths_tp1.length + ",out="
						+ paths_tp1_pruned.length + "}\n"
						+ JoinGraph.showTable(paths_tp1_pruned));

			return paths_tp1_pruned;

		}

		/**
		 * Return the {@link Vertex} whose {@link IPredicate} is associated with
		 * the given {@link BOp.Annotations#BOP_ID}.
		 * 
		 * @param bopId
		 *            The bop identifier.
		 * @return The {@link Vertex} -or- <code>null</code> if there is no such
		 *         vertex in the join graph.
		 */
		public Vertex getVertex(int bopId) {
			for (Vertex v : V) {
				if (v.pred.getId() == bopId)
					return v;
			}
			return null;
		}

		/**
		 * Return the {@link Edge} associated with the given vertices. The
		 * vertices may appear in any order.
		 * 
		 * @param v1
		 *            One vertex.
		 * @param v2
		 *            Another vertex.
		 * 
		 * @return The edge -or- <code>null</code> if there is no such edge in
		 *         the join graph.
		 */
		public Edge getEdge(Vertex v1, Vertex v2) {
			for (Edge e : E) {
				if (e.v1 == v1 && e.v2 == v2)
					return e;
				if (e.v1 == v2 && e.v2 == v1)
					return e;
			}
			return null;
		}

		/**
		 * Obtain a sample and estimated cardinality (fast range count) for each
		 * vertex.
		 * 
		 * @param queryEngine
		 * @param limit
		 *            The sample size.
		 */
		public void sampleVertices(final QueryEngine queryEngine, final int limit) {

			for (Vertex v : V) {

				v.sample(queryEngine, limit);

			}

		}

		/**
		 * Estimate the cardinality of each edge.
		 * 
		 * @param context
		 * 
		 * @throws Exception
		 */
		public void estimateEdgeWeights(final QueryEngine queryEngine,
				final int limit) throws Exception {

			for (Edge e : E) {

				if (e.v1.sample == null || e.v2.sample == null) {

					/*
					 * We can only estimate the cardinality of edges connecting
					 * vertices for which samples were obtained.
					 */
					continue;

				}

				e.estimateCardinality(queryEngine, limit);

			}

		}

		/**
		 * Return the {@link Edge} having the minimum estimated cardinality out
		 * of those edges whose cardinality has been estimated.
		 * 
		 * @param A
		 *            set of vertices to be excluded from consideration
		 *            (optional).
		 * 
		 * @return The minimum cardinality edge -or- <code>null</code> if there
		 *         are no {@link Edge}s having an estimated cardinality.
		 */
		public Edge getMinimumCardinalityEdge(final Set<Vertex> visited) {

			long minCard = Long.MIN_VALUE;
			Edge minEdge = null;

			for (Edge e : E) {

				if (e.sample == null) {

					// Edge has not been sampled.
					continue;

				}

				if (visited != null
						&& (visited.contains(e.v1) || visited.contains(e.v2))) {

					// A vertex of that edge has already been consumed.
					continue;

				}

				final long estimatedCardinality = e.sample.estimatedCardinality;

				if (minEdge == null || estimatedCardinality < minCard) {

					minEdge = e;

					minCard = estimatedCardinality;

				}

			}

			return minEdge;

		}

		// /**
		// * Return the {@link Edge} having the minimum estimated cardinality
		// out
		// * of those edges whose cardinality has been estimated.
		// *
		// * @return The minimum cardinality edge -or- <code>null</code> if
		// there
		// * are no {@link Edge}s having an estimated cardinality.
		// */
		// public Edge getMinimumCardinalityEdge() {
		//		
		// return getMinimumCardinalityEdge(null);
		//			
		// }

		/**
		 * Return the #of edges in which the given vertex appears where the
		 * other vertex of the edge does not appear in the set of visited
		 * vertices.
		 * 
		 * @param v
		 *            The vertex.
		 * @param visited
		 *            A set of vertices to be excluded from consideration.
		 * 
		 * @return The #of such edges.
		 */
		public int getEdgeCount(final Vertex v, final Set<Vertex> visited) {

			return getEdges(v, visited).size();

		}

		/**
		 * Return the edges in which the given vertex appears where the other
		 * vertex of the edge does not appear in the set of visited vertices.
		 * 
		 * @param v
		 *            The vertex.
		 * @param visited
		 *            A set of vertices to be excluded from consideration
		 *            (optional).
		 * 
		 * @return Those edges.
		 */
		public List<Edge> getEdges(final Vertex v, final Set<Vertex> visited) {

			if (v == null)
				throw new IllegalArgumentException();

			if (visited != null && visited.contains(v))
				return Collections.emptyList();

			final List<Edge> tmp = new LinkedList<Edge>();

			for (Edge e : E) {

				if (v.equals(e.v1) || v.equals(e.v2)) {

					if (visited != null) {

						if (visited.contains(e.v1))
							continue;

						if (visited.contains(e.v2))
							continue;

					}

					tmp.add(e);

				}

			}

			return tmp;

		}

		/**
		 * Prune paths which are dominated by other paths. Start the algorithm
		 * by passing in all edges which have the minimum cardinality (when
		 * comparing their expected cardinality after rounding to 2 significant
		 * digits).
		 * <p>
		 * If there is a path [p] whose total cost is LTE the cost of executing
		 * just its last edge [e], then the path [p] dominates all paths
		 * beginning with edge [e]. The dominated paths should be pruned. [This
		 * is a degenerate case of the next rule.]
		 * <p>
		 * If there is a path, [p] != [p1], where [p] is an unordered superset
		 * of [p1] (that is the vertices of p are a superset of the vertices of
		 * p1, but allowing the special case where the set of vertices are the
		 * same), and the cumulative cost of [p] is LTE the cumulative cost of
		 * [p1], then [p] dominates (or is equivalent to) [p1] and p1 should be
		 * pruned.
		 * <p>
		 * If there is a path, [p], which has the same vertices as a path [p1]
		 * and the cumulative cost of [p] is LTE the cumulative cost of [p1],
		 * then [p] dominates (or is equivalent to) [p1]. The path [p1] should
		 * be pruned. [This is a degenerate case of the prior rule.]
		 * 
		 * @param a
		 *            A set of paths.
		 * 
		 * @return The set of paths with all dominated paths removed.
		 * 
		 *         FIXME This does not give us a stopping condition unless the
		 *         set of paths becomes empty. I think it will tend to search
		 *         too far for a best path, running the risk of increasing
		 *         inaccuracy introduced by propagation of samples. Resampling
		 *         the vertices and increasing the vertex and edge cutoff at
		 *         each iteration of the search could compensate for that.
		 * 
		 *         TODO Cumulative estimated cardinality is an estimate of the
		 *         work to be done. However, the actual cost of a join depends
		 *         on whether we will use nested index subquery or a hash join
		 *         and the cost of that operation on the database. There could
		 *         be counter examples where the cost of the hash join with a
		 *         range scan using the unbound variable is LT the nested index
		 *         subquery. For those cases, we will do the same amount of IO
		 *         on the hash join but there will still be a lower cardinality
		 *         to the join path since we are feeding in fewer solutions to
		 *         be joined.
		 */
		public Path[] pruneJoinPaths(final Path[] a) {
			final StringBuilder sb = new StringBuilder();
			final Formatter f = new Formatter(sb);
			final Set<Path> pruned = new LinkedHashSet<Path>();
			for (int i = 0; i < a.length; i++) {
				final Path Pi = a[i];
				if (Pi.sample == null)
					throw new RuntimeException("Not sampled: " + Pi);
				if (pruned.contains(Pi))
					continue;
				for (int j = 0; j < a.length; j++) {
					if (i == j)
						continue;
					final Path Pj = a[j];
					if (Pj.sample == null)
						throw new RuntimeException("Not sampled: " + Pj);
					if (pruned.contains(Pj))
						continue;
					final boolean isPiSuperSet = Pi.isUnorderedSuperSet(Pj);
					if (!isPiSuperSet) {
						// Can not directly compare these join paths.
						continue;
					}
					final long costPi = Pi.cumulativeEstimatedCardinality;
					final long costPj = Pj.cumulativeEstimatedCardinality;
					final boolean lte = costPi <= costPj;
					List<Integer> prunedByThisPath = null;
					if (lte) {
						prunedByThisPath = new LinkedList<Integer>();
						if (pruned.add(Pj))
							prunedByThisPath.add(j);
						for (int k = 0; k < a.length; k++) {
							final Path x = a[k];
							if (x.beginsWith(Pj)) {
								if (pruned.add(x))
									prunedByThisPath.add(k);
							}
						}
					}
					if (log.isDebugEnabled()) {
						f
								.format(
										"Comparing: P[%2d] with P[%2d] : %10d %2s %10d %s",
										i, j, costPi, (lte ? "<=" : ">"),
										costPj, lte ? " *** pruned "
												+ prunedByThisPath : "");
						log.debug(sb);
						sb.setLength(0);
					}
				} // Pj
			} // Pi
			final Set<Path> keep = new LinkedHashSet<Path>();
			for (Path p : a) {
				if (pruned.contains(p))
					continue;
				keep.add(p);
			}
			final Path[] b = keep.toArray(new Path[keep.size()]);
			return b;
		}

	} // class JGraph

	/**
	 * Evaluation of a {@link JoinGraph}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	private class JoinGraphTask implements Callable<Void> {

		private final BOpContext<IBindingSet> context;

		private final JGraph g;

		private int limit;

		JoinGraphTask(final BOpContext<IBindingSet> context) {

			if (context == null)
				throw new IllegalArgumentException();

			this.context = context;

			limit = getLimit();

			if (limit <= 0)
				throw new IllegalArgumentException();

			final IPredicate[] v = getVertices();

			g = new JGraph(v);

		}

		public Void call() throws Exception {

			// Create the join graph.
			final JGraph g = new JGraph(getVertices());

			// Run it.
			g.runtimeOptimizer(context.getRunningQuery().getQueryEngine(), limit);

			return null;

		}

		// /**
		// * Return <code>true</code> iff there exists at least one {@link Edge}
		// * branching from a vertex NOT found in the set of vertices which have
		// * visited.
		// *
		// * @param visited
		// * A set of vertices.
		// *
		// * @return <code>true</code> if there are more edges to explore.
		// */
		// private boolean moreEdgesToVisit(final Set<Vertex> visited) {
		//			
		// // Consider all edges.
		// for(Edge e : E) {
		//				
		// if (visited.contains(e.v1) && visited.contains(e.v2)) {
		// /*
		// * Since both vertices for this edge have been executed the
		// * edge is now redundant. Either it was explicitly executed
		// * or another join path was used which implies the edge by
		// * transitivity in the join graph.
		// */
		// continue;
		// }
		//
		// /*
		// * We found a counter example (an edge which has not been
		// * explored).
		// */
		// if (log.isTraceEnabled())
		// log.trace("Edge has not been explored: " + e);
		//
		// return true;
		//
		// }
		//
		// // No more edges to explore.
		// return false;
		//			
		// }

	}

	private static double roundToSignificantFigures(final double num,
			final int n) {
		if (num == 0) {
			return 0;
		}

		final double d = Math.ceil(Math.log10(num < 0 ? -num : num));
		final int power = n - (int) d;

		final double magnitude = Math.pow(10, power);
		final long shifted = Math.round(num * magnitude);
		return shifted / magnitude;
	}

	/**
	 * Places vertices into order by the {@link BOp#getId()} associated with
	 * their {@link IPredicate}.
	 */
	private static class BOpIdComparator implements Comparator<Vertex> {

		private static final transient Comparator<Vertex> INSTANCE = new BOpIdComparator();

		@Override
		public int compare(final Vertex o1, final Vertex o2) {
			final int id1 = o1.pred.getId();
			final int id2 = o2.pred.getId();
			if (id1 < id2)
				return -1;
			if (id2 > id1)
				return 1;
			return 0;
		}

	}

	/**
	 * Places edges into order by ascending estimated cardinality. Edges which
	 * are not weighted are ordered to the end.
	 * 
	 * TODO unit tests, including with unweighted edges.
	 */
	private static class EstimatedEdgeCardinalityComparator implements
			Comparator<Edge> {

		public static final transient Comparator<Edge> INSTANCE = new EstimatedEdgeCardinalityComparator();

		@Override
		public int compare(final Edge o1, final Edge o2) {
			if (o1.sample == null && o2.sample == null) {
				// Neither edge is weighted.
				return 0;
			}
			if (o1.sample == null) {
				// o1 is not weighted, but o2 is. sort o1 to the end.
				return 1;
			}
			if (o2.sample == null) {
				// o2 is not weighted. sort o2 to the end.
				return -1;
			}
			final long id1 = o1.sample.estimatedCardinality;
			final long id2 = o2.sample.estimatedCardinality;
			if (id1 < id2)
				return -1;
			if (id1 > id2)
				return 1;
			return 0;
		}

	}

}
