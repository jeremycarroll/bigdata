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
 * Created on Feb 22, 2011
 */

package com.bigdata.bop.joinGraph.rto;

import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.NoSolutionsException;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.bop.rdf.join.DataSetJoin;

/**
 * A runtime optimizer for a join graph. The {@link JoinGraph} bears some
 * similarity to ROX (Runtime Optimizer for XQuery), but has several significant
 * differences:
 * <ol>
 * <li>
 * 1. ROX starts from the minimum cardinality edge of the minimum cardinality
 * vertex. The {@link JoinGraph} starts with one or more low cardinality
 * vertices.</li>
 * <li>
 * 2. ROX always extends the last vertex added to a given join path. The
 * {@link JoinGraph} extends all vertices having unexplored edges in each
 * breadth first expansion.</li>
 * <li>
 * 3. ROX is designed to interleave operator-at-once evaluation of join path
 * segments which dominate other join path segments. The {@link JoinGraph} is
 * designed to prune all join paths which are known to be dominated by other
 * join paths for the same set of vertices in each round and iterates until a
 * join path is identified which uses all vertices and has the minimum expected
 * cumulative estimated cardinality. Join paths which survive pruning are
 * re-sampled as necessary in order to obtain better information about edges in
 * join paths which have a low estimated cardinality in order to address a
 * problem with underflow of the cardinality estimates.</li>
 * </ol>
 * 
 * TODO For join graphs with a large number of vertices we may need to constrain
 * the #of vertices which are explored in parallel. This could be done by only
 * branching the N lowest cardinality vertices from the already connected edges.
 * Since fewer vertices are being explored in parallel, paths are more likely to
 * converge onto the same set of vertices at which point we can prune the
 * dominated paths.
 * 
 * TODO Compare the cumulative expected cardinality of a join path with the
 * expected cost of a join path. The latter allows us to also explore
 * alternative join strategies, such as the parallel subquery versus scan and
 * filter decision for named graph and default graph SPARQL queries.
 * 
 * TODO Coalescing duplicate access paths can dramatically reduce the work
 * performed by a pipelined nested index subquery. (A hash join eliminates all
 * duplicate access paths using a scan and filter approach.) If we will run a
 * pipeline nested index subquery join, then should the runtime query optimizer
 * prefer paths with duplicate access paths?
 * 
 * TODO How can we handle things like lexicon joins. A lexicon join is is only
 * evaluated when the dynamic type of a variable binding indicates that the RDF
 * Value must be materialized by a join against the ID2T index. Binding sets
 * having inlined values can simply be routed around the join against the ID2T
 * index. Routing around saves network IO in scale-out where otherwise we would
 * route binding sets having identifiers which do not need to be materialized to
 * the ID2T shards.
 * 
 * @todo Examine the overhead of the runtime optimizer. Look at ways to prune
 *       its costs. For example, by pruning the search, by recognizing when the
 *       query is simple enough to execute directly, by recognizing when we have
 *       already materialized the answer to the query, etc.
 * 
 * @todo Cumulative estimated cardinality is an estimate of the work to be done.
 *       However, the actual cost of a join depends on whether we will use
 *       nested index subquery or a hash join and the cost of that operation on
 *       the database. There could be counter examples where the cost of the
 *       hash join with a range scan using the unbound variable is LT the nested
 *       index subquery. For those cases, we will do the same amount of IO on
 *       the hash join but there will still be a lower cardinality to the join
 *       path since we are feeding in fewer solutions to be joined.
 * 
 * @todo Look at the integration with the SAIL. We decorate the joins with some
 *       annotations. Those will have to be correctly propagated to the "edges"
 *       in order for edge sampling and incremental evaluation (or final
 *       evaluation) to work. The {@link DataSetJoin} essentially inlines one of
 *       its access paths. That should really be changed into an inline access
 *       path and a normal join operator so we can defer some of the details
 *       concerning the join operator annotations until we decide on the join
 *       path to be executed. An inline AP really implies an inline relation,
 *       which in turn implies that the query is a searchable context for
 *       query-local resources.
 *       <p>
 *       For s/o, when the AP is remote, the join evaluation context must be ANY
 *       and otherwise (for s/o) it must be SHARDED.
 *       <p>
 *       Since the join graph is fed the vertices (APs), it does not have access
 *       to the annotated joins so we need to generated appropriately annotated
 *       joins when sampling an edge and when evaluation a subquery.
 *       <p>
 *       One solution would be to always use the unpartitioned views of the
 *       indices for the runtime query optimizer, which is how we are estimating
 *       the range counts of the access paths right now. [Note that the static
 *       query optimizer ignores named and default graphs, while the runtime
 *       query optimizer SHOULD pay attention to these things and exploit their
 *       conditional selectivity for the query plan.]
 * 
 * @todo Handle optional join graphs by first applying the runtime optimizer to
 *       the main join graph and obtaining a sample for the selected join path.
 *       That sample will then be feed into the the optional join graph in order
 *       to optimize the join order within the optional join graph (a join order
 *       which is selective in the optional join graph is better since it build
 *       up the #of intermediate results more slowly and hence do less work).
 *       <p>
 *       This is very much related to accepting a collection of non-empty
 *       binding sets when running the join graph. However, optional join graph
 *       should be presented in combination with the original join graph and the
 *       starting paths must be constrained to have the selected join path for
 *       the original join graph as a prefix. With this setup, the original join
 *       graph has been locked in to a specific join path and the sampling of
 *       edges and vertices for the optional join graph can proceed normally.
 *       <p>
 *       True optionals will always be appended as part of the "tail plan" for
 *       any join graph and can not be optimized as each optional join must run
 *       regardless (as long as the intermediate solution survives the
 *       non-optional joins).
 * 
 * @todo There are two cases where a join graph must be optimized against a
 *       specific set of inputs. In one case, it is a sample (this is how
 *       optimization of an optional join group proceeds per above). In the
 *       other case, the set of inputs is fixed and is provided instead of a
 *       single empty binding set as the starting condition. This second case is
 *       actually a bit more complicated since we can not use a random sample of
 *       vertices unless the do not share any variables with the initial binding
 *       sets. When there is a shared variable, we need to do a cutoff join of
 *       the edge with the initial binding sets. When there is not a shared
 *       variable, we can sample the vertex and then do a cutoff join.
 * 
 * @todo When we run into a cardinality estimation underflow (the expected
 *       cardinality goes to zero) we could double the sample size for just
 *       those join paths which hit a zero estimated cardinality and re-run them
 *       within the round. This would imply that we keep per join path limits.
 *       The vertex and edge samples are already aware of the limit at which
 *       they were last sampled so this should not cause any problems there.
 *       <p>
 *       A related option would be to deepen the samples only when we are in
 *       danger of cardinality estimation underflow. E.g., a per-path limit.
 *       Resampling vertices may only make sense when we increase the limit
 *       since otherwise we may find a different correlation with the new sample
 *       but the comparison of paths using one sample base with paths using a
 *       different sample base in a different round does not carry forward the
 *       cardinality estimates from the prior round (unless we do something like
 *       a weighted moving average).
 * 
 * @todo When comparing choices among join paths having fully bound tails where
 *       the estimated cardinality has also gone to zero, we should prefer to
 *       evaluate vertices in the tail with better index locality first. For
 *       example, if one vertex had one variable in the original plan while
 *       another had two variables, then solutions which reach the 2-var vertex
 *       could be spread out over a much wider range of the selected index than
 *       those which reach the 1-var vertex. [In order to support this, we would
 *       need a means to indicate that a fully bound access path should use an
 *       index specified by the query optimizer rather than the primary index
 *       for the relation. In addition, this suggests that we should keep bloom
 *       filters for more than just the SPO(C) index in scale-out.]
 * 
 * @todo Examine behavior when we do not have perfect covering indices. This
 *       will mean that some vertices can not be sampled using an index and that
 *       estimation of their cardinality will have to await the estimation of
 *       the cardinality of the edge(s) leading to that vertex. Still, the
 *       approach should be able to handle queries without perfect / covering
 *       automatically. Then experiment with carrying fewer statement indices
 *       for quads.
 * 
 * @todo Unit test when there are no solutions to the query. In this case there
 *       will be no paths identified by the optimizer and the final path length
 *       becomes zero.
 * 
 * @see <a
 *      href="http://www-db.informatik.uni-tuebingen.de/files/research/pathfinder/publications/rox-demo.pdf">
 *      ROX </a>
 */
public class JGraph {

    private static final transient Logger log = Logger.getLogger(JGraph.class);

    /**
     * Vertices of the join graph.
     */
    private final Vertex[] V;

    /**
     * Constraints on the join graph. A constraint is applied once all
     * variables referenced by a constraint are known to be bound.
     */
    private final IConstraint[] C;

    /**
     * The kind of samples that will be taken when we sample a {@link Vertex}.
     */
    private final SampleType sampleType;
    
    /**
     * The edges of the join graph as determined by static analysis. Each
     * edge is basically a possible join. This array is comprised of just
     * those edges which are determined by static analysis. Additional edges
     * MAY be identified dynamically. Altogether there are three categories
     * of edges:
     * <ol>
     * <li>The vertices directly share variables (join with shared
     * variable(s)). These edges are identified by static analysis in the
     * constructor are are represented in {@link #E}.</li>
     * <li>The vertices indirectly share variables via a constraint (join
     * with indirectly shared variable(s)). These edges are identified by
     * dynamic analysis. Each time we expand the set of join paths under
     * consideration, we examine {@link #unshared} vertices. Given the join
     * path under consideration and the set of constraints, it may be that
     * the vertex will indirectly share a variable via a constraint and can
     * therefore participate in a constrained join.</li>
     * <li>The vertices do not share variables, either directly or
     * indirectly (unconstrained join). All vertices can join. However, if
     * there are no shared variables then the join is unconstrained (full
     * cross product). These edges are identified dynamically. In each round
     * for which a constrained edge could not be identified, we consider all
     * vertices in {@link #unshared} and choose an unconstrained edge.</li>
     * </ol>
     * 
     * @see BOpUtility#canJoinUsingConstraints(IPredicate[], IPredicate, IConstraint[])
     */
    private final Edge[] E;

    /**
     * An unordered collection of vertices which do not share any variables
     * with the other vertices in the join graph. These vertices will
     * produce full cross product joins unless a constraint causes indirect
     * sharing of variables with a join path.
     */
    private final Vertex[] unshared;

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
        sb.append("{C=[");
        for (IConstraint c : C) {
            sb.append("\nC[" + c.getId() + "]=" + c);
        }
        sb.append("\n],unshared=[");
        for (Vertex v : unshared) {
            sb.append("\n" + v);
        }
        sb.append("],E=[");
        for (Edge e : E) {
            sb.append("\n" + e);
        }
        sb.append("\n]}");
        return sb.toString();
    }

    /**
     * 
     * @param v
     *            The vertices of the join graph. These are
     *            {@link IPredicate}s associated with required joins.
     * @param constraints
     *            The constraints of the join graph (optional). Since all
     *            joins in the join graph are required, constraints are
     *            dynamically attached to the first join in which all of
     *            their variables are bound.
     * 
     * @throws IllegalArgumentException
     *             if the vertices is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the vertices is an empty array.
     * @throws IllegalArgumentException
     *             if any element of the vertices is <code>null</code>.
     * @throws IllegalArgumentException
     *             if any constraint uses a variable which is never bound by
     *             the given predicates.
     * @throws IllegalArgumentException
     *             if <i>sampleType</i> is <code>null</code>.
     * 
     * @todo unit test for a constraint using a variable which is never
     *       bound.
     */
    public JGraph(final IPredicate<?>[] v, final IConstraint[] constraints,
            final SampleType sampleType) {

        if (v == null)
            throw new IllegalArgumentException();

        if (v.length < 2)
            throw new IllegalArgumentException();

        V = new Vertex[v.length];

        for (int i = 0; i < v.length; i++) {

            if (v[i] == null)
                throw new IllegalArgumentException();

            V[i] = new Vertex(v[i]);

        }

        if (constraints != null) {
            C = new IConstraint[constraints.length];
            for (int i = 0; i < constraints.length; i++) {
                if (constraints[i] == null)
                    throw new IllegalArgumentException();
                C[i] = constraints[i];
            }
        } else {
            // No constraints.
            C = null;
        }

        if (sampleType == null)
            throw new IllegalArgumentException();
        
        this.sampleType = sampleType;

        /*
         * Create edges to represent possible joins between predicates based
         * on directly shared variables - these joins can be identified by a
         * static analysis of the join graph.
         * 
         * Note: There are really three classes of joins to be considered.
         * 
         * (1) The target predicate directly shares a variable with the
         * source predicate. These joins are always constrained since the
         * source predicate will have bound that variable. This is the
         * static analysis of the join graph.
         * 
         * (2) When the source predicate shares a variable with a constraint
         * which also shares a variable with the target predicate. While the
         * predicates do not directly share a variable, these joins are
         * constrained by the shared variable in the constraint on the
         * target predicate. BSBM Q5 is an example of this case. We do not
         * create edges for such joins here. Instead, we dynamically
         * determine when a constrained join is possible when extending the
         * join path in each round. Hence, this is part of the dynamic
         * analysis of the join graph.
         * 
         * (3) Any two predicates may always be joined. However, joins which
         * do not share variables either directly or indirectly will be full
         * cross products. Therefore such joins are run last and we do not
         * create edges for them here. Again, this is part of the dynamic
         * analysis of the join graph.
         */
        {

            // The set of identified edges for vertices which share vars.
            final List<Edge> tmp = new LinkedList<Edge>();

            // The set of vertices which share variables.
            final Set<Vertex> sharedEdgeVertices = new LinkedHashSet<Vertex>();

            // The set of vertices which do not share variables.
            final List<Vertex> unsharedEdgeVertices = new LinkedList<Vertex>();

            for (int i = 0; i < v.length; i++) {

                // consider a source vertex.
                final IPredicate<?> p1 = v[i];

                // #of vertices which share a variable with source vertex.
                int nmatched = 0;
                
                for (int j = i + 1; j < v.length; j++) {

                    // consider a possible target vertex.
                    final IPredicate<?> p2 = v[j];

//                    final Set<IVariable<?>> shared = BOpUtility
//                            .getSharedVars(p1, p2);

                    if(BOpUtility.canJoin(p1, p2)) {
//                  if (!shared.isEmpty()) {

                        /*
                         * The source and target vertices can join based on
                         * one or more shared variable(s).
                         */
                        
                        if (log.isDebugEnabled())
                            log.debug("vertices shared variable(s): vars="
                                    + BOpUtility.getSharedVars(p1, p2)
                                    + ", v1=" + p1 + ", v2=" + p2);
                        
                        tmp.add(new Edge(V[i], V[j]));//, shared));
                        
                        sharedEdgeVertices.add(V[i]);
                        
                        sharedEdgeVertices.add(V[j]);
                        
                        nmatched++;

                    } else if (constraints != null) {

                        /*
                         * The source and target vertices do not directly
                         * share any variable(s). However, there may be a
                         * constraint which shares a variable with both the
                         * source and target vertex. If such a constraint is
                         * found, then we add an edge now as that join is
                         * potentially constrained (less than the full
                         * Cartesian cross product).
                         * 
                         * Note: While this identifies possible joins via a
                         * constraint, such joins are only legal when all
                         * variables used by the constraint are known to be
                         * bound.
                         * 
                         * FIXME Edges should be identified dynamically, not
                         * statically. Probably all edges (aka possible
                         * joins) should be identified dynamically given the
                         * history represented by a given join path and the
                         * set of constraints declared for the join graph.
                         */

                        for(IConstraint c : constraints) {
                    
                            if(BOpUtility.getSharedVars(p1, c).isEmpty())
                                continue;
                            
                            if(BOpUtility.getSharedVars(p2, c).isEmpty())
                                continue;
                            
                            if (log.isDebugEnabled())
                                log
                                        .debug("vertices shared variable(s) via constraint: v1="
                                                + p1
                                                + ", v2="
                                                + p2
                                                + ", c=" + c);
                            
                            tmp.add(new Edge(V[i], V[j]));//, shared));
                            
                            sharedEdgeVertices.add(V[i]);
                            
                            sharedEdgeVertices.add(V[j]);
                            
                            nmatched++;

                        }

                    }

                }

                if (nmatched == 0 && !sharedEdgeVertices.contains(V[i])) {

                    /*
                     * The source vertex does not share any variables.
                     */

                    log
                            .warn("Vertex does not share any variables: "
                                    + V[i]);
                    
                    unsharedEdgeVertices.add(V[i]);

                }
                
            } // create edges

            E = tmp.toArray(new Edge[0]);
            
            this.unshared = unsharedEdgeVertices.toArray(new Vertex[0]); 

//          if(!unsharedEdgeVertices.isEmpty()) {
//
//                /*
//                 * FIXME NO SHARED VARS : RUN LAST. This needs to be
//                 * supported. When vertices that do not share variables
//                 * either directly or via a constraint then they should run
//                 * last as they can not constrain the query. In this case,
//                 * they are not considered by the runtime optimizer when
//                 * building up the join path until all vertices which share
//                 * variables have been exhausted. At that point, the
//                 * remaining vertices are just appended to whatever join
//                 * path was selected as having the lowest cumulative
//                 * estimated cardinality.
//                 * 
//                 * However, if there exists for a vertex which otherwise
//                 * does not share variables a constraint which should be
//                 * evaluated against that vertex, then that constraint
//                 * provides the basis for a edge (aka join). In this case,
//                 * an edge must be created for the vertex based on the
//                 * shared variable in the constraint and its position in the
//                 * join path will be decided by the runtime optimizer.
//                 */
//
//                throw new UnsupportedOperationException(
//                        "Some predicates do not share any variables with other predicates: unshared="
//                                + unsharedEdgeVertices);
//              
//          }
            
        }

    }

    /**
     * Find a good join path in the data given the join graph. The join path is
     * not guaranteed to be the best join path (the search performed by the
     * runtime optimizer is not exhaustive) but it should always be a "good"
     * join path and may often be the "best" join path.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The limit for sampling a vertex and the initial limit for
     *            cutoff join evaluation.
     * @param nedges
     *            The edges in the join graph are sorted in order of increasing
     *            cardinality and up to <i>nedges</i> of the edges having the
     *            lowest cardinality are used to form the initial set of join
     *            paths. For each edge selected to form a join path, the
     *            starting vertex will be the vertex of that edge having the
     *            lower cardinality.
     * @param sampleType
     *            Type safe enumeration indicating the algorithm which will be
     *            used to sample the initial vertices.
     * 
     * @return The join path identified by the runtime query optimizer as the
     *         best path given the join graph and the data.
     * 
     * @throws NoSolutionsException
     *             If there are no solutions for the join graph in the data (the
     *             query does not have any results).
     * @throws IllegalArgumentException
     *             if <i>queryEngine</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>limit</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>nedges</i> is non-positive.
     * @throws Exception
     * 
     * @todo It is possible that this could throw a {@link NoSolutionsException}
     *       if the cutoff joins do not use a large enough sample to find a join
     *       path which produces at least one solution (except that no solutions
     *       for an optional join do not cause the total to fail, nor do no
     *       solutions for some part of a UNION).
     * 
     *       TODO We need to automatically increase the depth of search for
     *       queries where we have cardinality estimation underflows or punt to
     *       another method to decide the join order.
     */
    public Path runtimeOptimizer(final QueryEngine queryEngine,
            final int limit, final int nedges)
            throws Exception, NoSolutionsException {

        if (queryEngine == null)
            throw new IllegalArgumentException();
        if (limit <= 0)
            throw new IllegalArgumentException();
        if (nedges <= 0)
            throw new IllegalArgumentException();

        // Setup the join graph.
        Path[] paths = round0(queryEngine, limit, nedges);

        /*
         * The initial paths all have one edge, and hence two vertices. Each
         * round adds one more vertex to each path. We are done once we have
         * generated paths which include all vertices.
         * 
         * This occurs at round := nvertices - 1
         * 
         * Note: There are a few edge cases, such as when sampling can not
         * find any solutions, even with an increased sampling limit.
         * Eventually we wind up proving that there are no solutions for the
         * query.
         */

        final int nvertices = V.length;

        int round = 1;

        while (paths.length > 0 && round < nvertices - 1) {

            paths = expand(queryEngine, limit, round++, paths);

        }

        if (paths.length == 0) {

            // There are no solutions for the join graph in the data.
            throw new NoSolutionsException();
            
        }
        
        // Should be one winner.
        assert paths.length == 1;

        if (log.isInfoEnabled()) {

            /*
             * @todo It would be nice to show the plan with the filters
             * attached, but that might be something that the caller does.
             */
            log.info("\n*** Selected join path: "
                    + Arrays.toString(paths[0].getVertexIds()) + "\n"
                    + showPath(paths[0]));

        }
        
        return paths[0];

    }

    /**
     * Return a permutation vector which may be used to reorder the given
     * {@link IPredicate}[] into the evaluation order selected by the
     * runtime query optimizer.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the given {@link Path} does not cover all vertices in
     *             the join graph.
     */
    public int[] getOrder(final Path p) {

        if(p == null)
            throw new IllegalArgumentException();
        
        final IPredicate[] path = p.getPredicates();

        if (path.length != V.length) {
            throw new IllegalArgumentException(
                    "Wrong path length: #vertices=" + V.length
                            + ", but path.length=" + path.length);
        }

        final int[] order = new int[V.length];

        for (int i = 0; i < order.length; i++) {

            boolean found = false;
            for (int j = 0; j < order.length; j++) {

                if (path[i].getId() == V[j].pred.getId()) {
                    order[i] = j;
                    found = true;
                    break;
                }

            }

            if (!found)
                throw new RuntimeException("No such vertex: id="
                        + path[i].getId());

        }
        
        return order;
        
    }

    /**
     * Choose the starting vertices.
     * 
     * @param nedges
     *            The maximum #of edges to choose.
     */
    public Path[] chooseStartingPaths(final int nedges) {

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
     * Choose up to <i>nedges</i> edges to be the starting point. For each
     * of the <i>nedges</i> lowest cardinality edges, the starting vertex
     * will be the vertex with the lowest cardinality for that edge.
     * <p>
     * Note: An edge can not serve as a starting point for exploration if it
     * uses variables (for example, in a CONSTRAINT) which are not bound by
     * either vertex (since the variable(s) are not bound, the constraint
     * would always fail).
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
     * @return An initial set of paths starting from at most <i>nedges</i>.
     * 
     * @throws Exception
     */
    public Path[] round0(final QueryEngine queryEngine, final int limit,
            final int nedges) throws Exception {

        /*
         * Sample the vertices.
         */
        sampleAllVertices(queryEngine, limit);

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
        estimateInitialEdgeWeights(queryEngine, limit);

        if (log.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Edges:\n");
            for (Edge e : E) {
                sb.append(e.toString());
                sb.append("\n");
            }
            log.debug(sb.toString());
        }

        /*
         * Choose the initial set of paths.
         */
        final Path[] paths_t0 = chooseStartingPaths(nedges);

        if (log.isInfoEnabled())
            log.info("\n*** Paths @ t0\n" + JGraph.showTable(paths_t0));

        /*
         * FIXME Discard samples which will not be reused? If yes, then do in
         * each round since with more than one starting vertex we have have
         * starting vertices which will not be re-sampled at any round (assuming
         * that all paths beginning with a given starting vertex get pruned).
         * 
         * See Edge#estimateCardinality(...) which actually requires both the
         * initial vertex and the 2nd vertex to decide which is the left hand
         * side and which is the right hand side of the join.  If we get rid of
         * Edge, then this problem will be resolved at the same time.
         */
//        /*
//         * Discard samples for vertices which were not chosen as starting points
//         * for join paths.
//         * 
//         * Note: We do not need the samples of the other vertices once we decide
//         * on the initial vertices from which the join paths will be grown so
//         * they could even be discarded at this point.
//         */
//        {
//            
//            final Set<Vertex> initialVertexSet = new LinkedHashSet<Vertex>();
//         
//            for (Path x : paths_t0) {
//
//                initialVertexSet.add(x.vertices[0]);
//                
//            }
//            
//            for(Vertex v : V) {
//                
//                if(!initialVertexSet.contains(v)) {
//                    
//                    // Discard sample.
//                    v.sample = null;
//                    
//                }
//                
//            }
//            
//        }
        
        return paths_t0;

    }

    /**
     * Do one breadth first expansion.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limitIn
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
    public Path[] expand(final QueryEngine queryEngine, int limitIn,
            final int round, final Path[] a) throws Exception {

        if (queryEngine == null)
            throw new IllegalArgumentException();
        if (limitIn <= 0)
            throw new IllegalArgumentException();
        if (round <= 0)
            throw new IllegalArgumentException();
        if (a == null)
            throw new IllegalArgumentException();
        if (a.length == 0)
            throw new IllegalArgumentException();
        
        // increment the limit by itself in each round.
        final int limit = (round + 1) * limitIn;

        if (log.isDebugEnabled())
            log.debug("round=" + round + ", limit=" + limit
                    + ", #paths(in)=" + a.length);

        /*
         * Re-sample the vertices which are the initial vertex of any of the
         * existing paths.
         * 
         * Note: We do not need to resample vertices unless they are the first
         * vertex in some path. E.g., the initial vertices from which we start.
         * The inputs to an EdgeSample are always either the sample of an
         * initial vertex or the sample of a prior cutoff join in the join
         * path's own history.
         * 
         * Note: A request to re-sample a vertex is a NOP unless the limit has
         * been increased since the last time the vertex was sampled. It is also
         * a NOP if the vertex has been fully materialized.
         */
        if (log.isDebugEnabled())
            log.debug("Re-sampling in-use vertices: limit=" + limit);

        for (Path x : a) {

            x.vertices[0].sample(queryEngine, limit, sampleType);

//            for(Edge e : x.edges) {
//                
//                e.v1.sample(queryEngine, limit);
//                e.v2.sample(queryEngine, limit);
//                
//            }
            
        }

        /*
         * Re-sample the cutoff join for each edge in each of the existing
         * paths using the newly re-sampled vertices.
         * 
         * Note: The only way to increase the accuracy of our estimates for
         * edges as we extend the join paths is to re-sample each edge in
         * the join path in path order.
         * 
         * Note: An edge must be sampled for each distinct join path prefix
         * in which it appears within each round. However, it is common for
         * surviving paths to share a join path prefix, so do not re-sample
         * a given path prefix more than once per round. 
         */
        if (log.isDebugEnabled())
            log.debug("Re-sampling in-use path segments: limit=" + limit);

        // This map is used to associate join path segments (expressed as an
        // ordered array of bopIds) with edge sample to avoid redundant effort.
        final Map<int[], EdgeSample> edgePaths = new LinkedHashMap<int[], EdgeSample>();
        
        for (Path x : a) {

            // The edges which we have visited in this path.
            final List<Edge> edges = new LinkedList<Edge>();

            /*
             * The vertices which we have visited in this path.
             * 
             * TODO This happens to be the ordered list of vertices as well
             * since it is a linked hash map. We may be able to exploit that to
             * drop the [edges] list, but [edges] is being built up first while
             * [vertices] is being built up at a different point within each
             * iteration of this loop.
             */
            final Set<Vertex> vertices = new LinkedHashSet<Vertex>();
            
            EdgeSample priorEdgeSample = null;
            
            for(Edge e : x.edges) {
                
                // Add edge to the visited set for this join path.
                edges.add(e);

                // Generate unique key for this join path segment.
                final int[] ids = Path.getVertexIds(edges);

                if (priorEdgeSample == null) {

                    /*
                     * This is the first edge in the path.
                     * 
                     * Test our local table of join path segment estimates
                     * to see if we have already re-sampled that edge. If
                     * not, then re-sample it now.
                     */

                    // Test sample cache.
                    EdgeSample edgeSample = edgePaths.get(ids);

                    if (edgeSample == null) {
                        if (e.sample != null && e.sample.limit >= limit) {

                            // The existing sample for that edge is fine.
                            edgeSample = e.sample;
                            
                        } else {

                            /*
                             * Re-sample the edge, updating the sample on
                             * the edge as a side-effect. The cutoff sample
                             * is based on the vertex sample for the minimum
                             * cardinality vertex.
                             * 
                             * FIXME This is actually using the source vertex
                             * as the source sample which is WRONG.
                             */

                            edgeSample = e.estimateCardinality(queryEngine,
                                    limit);

                        }

                        // Cache the sample.
                        if (edgePaths.put(ids, edgeSample) != null)
                            throw new AssertionError();

                    }

                    // Add both vertices to the visited set.
                    vertices.add(e.v1);
                    vertices.add(e.v2);

                    // Save sample. It will be used to re-sample the next edge.
                    priorEdgeSample = edgeSample;

                    continue;
                    
                }

                final boolean v1Found = vertices.contains(e.v1);

                // The source vertex for the new edge.
                final Vertex sVertex = v1Found ? e.v1 : e.v2;

                // The target vertex for the new edge.
                final Vertex tVertex = v1Found ? e.v2 : e.v1;

                // Look for sample for this path in our cache.
                EdgeSample edgeSample = edgePaths.get(ids);

                if (edgeSample == null) {
                    
                    /*
                     * This is some N-step edge in the path, where N is
                     * greater than ONE (1). The source vertex is the vertex
                     * which already appears in the prior edges of this join
                     * path. The target vertex is the next vertex which is
                     * visited by the join path. The sample pass in is the
                     * prior edge sample - that is, the sample from the path
                     * segment less the target vertex. This is the sample
                     * that we just updated when we visited the prior edge
                     * of the path.
                     */

                    edgeSample = e
                            .estimateCardinality(
                                    queryEngine,//
                                    limit,//
                                    sVertex,//
                                    tVertex,//
                                    priorEdgeSample//
//                                    priorEdgeSample.estimatedCardinality,//
//                                    priorEdgeSample.estimateEnum == EstimateEnum.Exact,
//                                    priorEdgeSample.limit,//
//                                    priorEdgeSample.sample//
                            );

                    if (log.isDebugEnabled())
                        log.debug("Resampled: " + Arrays.toString(ids)
                                + " : " + edgeSample);
                    
                    if (edgePaths.put(ids, edgeSample) != null)
                        throw new AssertionError();

                }

                // Save sample. It will be used to re-sample the next edge.
                priorEdgeSample = edgeSample;

                // Add target vertex to the visited set.
                vertices.add(tVertex);

            } // next Edge [e] in Path [x]

            // Save the result on the path.
            x.sample = priorEdgeSample;
            
        } // next Path [x].

        /*
         * Expand each path one step from each vertex which branches to an
         * unused vertex.
         * 
         * FIXME If we allow "runFirst" predicates into the join graph, then
         * an initial non-empty join path needs to be constructed from those
         * vertices. The operators for those APs would also have to be
         * modified to support cutoff evaluation or to be fully
         * materialized. This change should be introduced into round0() of
         * the algorithm.
         * 
         * FIXME VERTICES WHICH SHARE VARS THROUGH A CONSTRAINT : Some
         * vertices can participate in constrained joins even though they do
         * not directly share variables because a constraint may be
         * evaluated which shares variables with both the existing join path
         * and the new vertex. Such extensions can only be determined
         * dynamically using canJoinWithConstraints() or perhaps
         * PartitionedJoinGraph. The termination conditions here need to be
         * revised to permit such dynamic extensions, e.g., by terminating
         * the expansion loop only after no _unconstrained_ extension can be
         * made to the join path. At that point, we simply append in all of
         * the vertices which had not yet been incorporated (those are the
         * remaining vertices w/o shared variables). However, this should
         * also be done incrementally within the same iteration as it is
         * possible that there are two (or more) partitions to the join
         * graph (partitions which do not have shared variables) while
         * within each partition there may be shared variables.
         */

        if (log.isDebugEnabled())
            log.debug("Expanding paths: limit=" + limit + ", #paths(in)="
                    + a.length);

        final List<Path> tmp = new LinkedList<Path>();
        
        for (Path x : a) {

//          final int nedges = x.edges.size();
//
//          if (nedges < round) {
//
//              // Path is from a previous round.
//              continue;
//              
//          }

            /*
             * The set of vertices used to expand this path in this round.
             */
            final Set<Vertex> used = new LinkedHashSet<Vertex>();

            /*
             * First, consider the edges identified by static analysis. This
             * will create zero or more paths from the current join path.
             * 
             * FIXME Change this to use canJoin() and get rid of E[] and
             * of Edges in general.
             */
            // Check all edges in the graph.
            for (Edge edgeInGraph : E) {

                // Figure out which vertices are already part of this path.
                final boolean v1Found = x.contains(edgeInGraph.v1);
                final boolean v2Found = x.contains(edgeInGraph.v2);

                if (log.isTraceEnabled())
                    log.trace("Edge: " + edgeInGraph + ", v1Found="
                            + v1Found + ", v2Found=" + v2Found);
                
                if (!v1Found && !v2Found) {
                    // Edge is not connected to this path.
                    continue;
                }

                if (v1Found && v2Found) {
                    // Edge is already present in this path.
                    continue;
                }

                // the target vertex for the new edge.
                final Vertex tVertex = v1Found ? edgeInGraph.v2
                        : edgeInGraph.v1;

//              // the source vertex for the new edge.
//              final Vertex sVertex = v1Found ? edgeInGraph.v1
//                      : edgeInGraph.v2;

                if (used.contains(tVertex)) {
                    // Vertex already used to extend this path.
                    if (log.isTraceEnabled())
                        log.trace("Edge: " + edgeInGraph
                                + " - already used to extend this path.");
                    continue;
                }

                // add the new vertex to the set of used vertices.
                used.add(tVertex);

                // (Re-)sample vertex before we sample a new edge
                tVertex.sample(queryEngine, limit, sampleType);
                
                // Extend the path to the new vertex.
                final Path p = x.addEdge(queryEngine, limit, edgeInGraph);

                // Add to the set of paths for this round.
                tmp.add(p);

                if (log.isTraceEnabled())
                    log.trace("Extended path with static edge: "
                            + edgeInGraph + ", new path=" + p);

            }

            /*
             * Next, consider the edges which are identified by dynamic
             * analysis. We only consider target vertices which do not share
             * any variables and we ignore any target vertex which is
             * already part of the path.
             */

            if (unshared.length > 0) {

                /*
                 * Any vertex which (a) does not appear in the path to be
                 * extended; (b) has not already been used to extend the
                 * path; and (c) does not share any variables indirectly via
                 * constraints is added to this collection.
                 * 
                 * If we are not able to extend the path at least once by an
                 * edge identified by static analysis OR dynamic analysis,
                 * then we will use this collection as the source of
                 * unconnected edges which need to be used to extend the
                 * path.
                 */
                final Set<Vertex> nothingShared = new LinkedHashSet<Vertex>();
                
                // Consider all vertices not sharing any variables.
                for (Vertex tVertex : unshared) {

                    // Figure out which vertices are already part of this
                    // path.
                    final boolean vFound = x.contains(tVertex);

                    if (vFound) {
                        // Vertex is already part of this path.
                        if (log.isTraceEnabled())
                            log.trace("Vertex: " + tVertex
                                    + " - already part of this path.");
                        continue;
                    }

                    if (used.contains(tVertex)) {
                        // Vertex already used to extend this path.
                        if (log.isTraceEnabled())
                            log
                                    .trace("Vertex: "
                                            + tVertex
                                            + " - already used to extend this path.");
                        continue;
                    }

                    if (!BOpUtility.canJoinUsingConstraints(//
                            x.getPredicates(),// path
                            tVertex.pred,// vertex
                            C// constraints
                            )) {
                        /*
                         * Vertex does not share variables either directly
                         * or indirectly.
                         */
                        if (log.isTraceEnabled())
                            log
                                    .trace("Vertex: "
                                            + tVertex
                                            + " - does not share variables indirectly with this path.");
                        continue;
                    }

                    // add the new vertex to the set of used vertices.
                    used.add(tVertex);

                    // (Re-)sample vertex before we sample a new edge
                    tVertex.sample(queryEngine, limit, sampleType);

                    // Analyze the join path and constraints for the
                    // proposed path.
                    final PartitionedJoinGroup g;
                    {
                        // extract ordered preds from path to be extended
                        final List<IPredicate<?>> preds = new LinkedList<IPredicate<?>>(
                                Arrays.asList(x.getPredicates()));
                        // append the target vertex.
                        preds.add(tVertex.pred);
                        // convert to an array.
                        final IPredicate<?>[] newPath = preds
                                .toArray(new IPredicate[preds.size()]);
                        g = new PartitionedJoinGroup(newPath, C);
                    }
                    
                    // The set of constraints which will run for this
                    // vertex.
                    final IConstraint[] c = g
                            .getTailPlanConstraints(tVertex.pred.getId());

                    if (c == null || c.length == 0) {
                        /*
                         * Must not be null since the constraint(s) are what
                         * license this as a constrained join.
                         */
                        throw new AssertionError();
                    }

                    /*
                     * Find any vertex in the path which we are extending
                     * which shares at least one variable with one of the
                     * constraints which will run with this edge. That will
                     * be the "source" vertex for the purposes of this path
                     * extension.
                     */
                    Vertex vSource = null;
                    {

                        // The set of variables used by the constraints.
                        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();

                        for (IConstraint aConstraint : c) {

                            final Iterator<IVariable<?>> vitr = BOpUtility
                                    .getSpannedVariables(aConstraint);

                            while (vitr.hasNext()) {

                                vars.add(vitr.next());

                            }

                        }
                        
                        // Find a vertex using any of those variables.
                        for (Vertex aVertex : x.vertices) {

                            if (vSource != null) {
                                // Done.
                                break;
                            }
                            
                            final IPredicate<?> aPred = aVertex.pred;

                            final Iterator<IVariable<?>> vitr = BOpUtility
                                    .getArgumentVariables(aPred);

                            while (vSource == null && vitr.hasNext()) {

                                final IVariable<?> aVar = vitr.next();

                                if (vars.contains(aVar)) {

                                    // Done.
                                    vSource = aVertex;

                                }

                            } // while

                        } // for

                        if (vSource == null)
                            throw new AssertionError("No shared variables?");

                    } // end block

                    final Edge dynamicEdge = new Edge(/* x, */vSource,
                            tVertex);

                    // Extend the path to the new vertex.
                    final Path p = x.addEdge(queryEngine, limit,
                            dynamicEdge);

                    // Add to the set of paths for this round.
                    tmp.add(p);

                    if (log.isTraceEnabled())
                        log.trace("Extended path with dynamic edge: "
                                + dynamicEdge + ", new path=" + p);

                } // next [unshared] vertex.

                if (tmp.isEmpty()) {

                    /*
                     * NO edges were identified by static and dynamic
                     * analysis so we must consider edges which represent fully
                     * unconstrained joins.
                     */
                    
                    assert unshared.length != 0;

                    assert !nothingShared.isEmpty();

                    /*
                     * Choose any vertex from the set of those which do
                     * not share any variables with the join path. Since
                     * all of these are fully unconstrained joins we do
                     * not want to expand the join path along multiple
                     * edges in this iterator, just along a single
                     * unconstrained edge.
                     */
                    final Vertex tVertex = nothingShared.iterator().next();
                    
                    /*
                     * Since there are no shared variables, any vertex already
                     * in the path may be used as the source for this edge.
                     */
                    final Vertex vSource = x.vertices[0];
                    
                    final Edge dynamicEdge = new Edge(vSource, tVertex);

                    // Extend the path to the new vertex.
                    final Path p = x.addEdge(queryEngine, limit,
                            dynamicEdge);

                    // Add to the set of paths for this round.
                    tmp.add(p);

                    if (log.isTraceEnabled())
                        log.trace("Extended path with dynamic edge: "
                                + dynamicEdge + ", new path=" + p);
                    
                }

            } 

        } // next path

        final Path[] paths_tp1 = tmp.toArray(new Path[tmp.size()]);

        final Path[] paths_tp1_pruned = pruneJoinPaths(paths_tp1);

        if (log.isDebugEnabled())
            log.debug("\n*** round=" + round + ", limit=" + limit
                    + " : generated paths\n"
                    + JGraph.showTable(paths_tp1, paths_tp1_pruned));

        if (log.isInfoEnabled())
            log.info("\n*** round=" + round + ", limit=" + limit
                    + ": paths{in=" + a.length + ",considered="
                    + paths_tp1.length + ",out=" + paths_tp1_pruned.length
                    + "}\n" + JGraph.showTable(paths_tp1_pruned));

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
     *            The query engine.
     * @param limit
     *            The sample size.
     */
    public void sampleAllVertices(final QueryEngine queryEngine, final int limit) {

        for (Vertex v : V) {

            v.sample(queryEngine, limit, sampleType);

        }

    }

    /**
     * Estimate the cardinality of each edge. This is only invoked by
     * {@link #round0(QueryEngine, int, int)} when it is trying to select the
     * minimum cardinality edges which it will use to create the initial set of
     * join paths from which the exploration will begin.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The sample size.
     * 
     * @throws Exception
     */
    private void estimateInitialEdgeWeights(final QueryEngine queryEngine,
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
     * Return the {@link Edge} having the minimum estimated cardinality out of
     * those edges whose cardinality has been estimated.
     * 
     * @param visited
     *            A set of vertices to be excluded from consideration
     *            (optional).
     * 
     * @return The minimum cardinality edge -or- <code>null</code> if there are
     *         no {@link Edge}s having an estimated cardinality.
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
     * Prune paths which are dominated by other paths. Paths are extended in
     * each round. Paths from previous rounds are always pruned. Of the new
     * paths in each round, the following rule is applied to prune the
     * search to just those paths which are known to dominate the other
     * paths covering the same set of vertices:
     * <p>
     * If there is a path, [p] != [p1], where [p] is an unordered variant of
     * [p1] (that is the vertices of p are the same as the vertices of p1),
     * and the cumulative cost of [p] is LTE the cumulative cost of [p1],
     * then [p] dominates (or is equivalent to) [p1] and p1 should be
     * pruned.
     * 
     * @param a
     *            A set of paths.
     * 
     * @return The set of paths with all dominated paths removed.
     */
    public Path[] pruneJoinPaths(final Path[] a) {
        /*
         * Find the length of the longest path(s). All shorter paths are
         * dropped in each round.
         */
        int maxPathLen = 0;
        for (Path p : a) {
            if (p.vertices.length > maxPathLen) {
                maxPathLen = p.vertices.length;
            }
//            if (p.edges.size() > maxPathLen) {
//                maxPathLen = p.edges.size();
//            }
        }
        final StringBuilder sb = new StringBuilder();
        final Formatter f = new Formatter(sb);
        final Set<Path> pruned = new LinkedHashSet<Path>();
        for (int i = 0; i < a.length; i++) {
            final Path Pi = a[i];
            if (Pi.sample == null)
                throw new RuntimeException("Not sampled: " + Pi);
//          if (Pi.edges.size() < maxPathLen) {
            if (Pi.vertices.length < maxPathLen) {
                /*
                 * Only the most recently generated set of paths survive to
                 * the next round.
                 */
                pruned.add(Pi);
                continue;
            }
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
                final boolean isPiSuperSet = Pi.isUnorderedVariant(Pj);
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

///**
//* Places vertices into order by the {@link BOp#getId()} associated with
//* their {@link IPredicate}.
//*/
//private static class BOpIdComparator implements Comparator<Vertex> {
//
//  private static final transient Comparator<Vertex> INSTANCE = new BOpIdComparator();
//
////    @Override
//  public int compare(final Vertex o1, final Vertex o2) {
//      final int id1 = o1.pred.getId();
//      final int id2 = o2.pred.getId();
//      if (id1 < id2)
//          return -1;
//      if (id2 > id1)
//          return 1;
//      return 0;
//  }
//
//}

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

        return showTable(a, null/* pruned */);
        
    }

    /**
     * Comma delimited table showing the estimated join hit ratio, the estimated
     * cardinality, and the set of vertices for each of the specified join
     * paths.
     * 
     * @param a
     *            A set of paths (typically those before pruning).
     * @param pruned
     *            The set of paths after pruning (those which were retained)
     *            (optional). When given, the paths which were pruned are marked
     *            in the table.
     * 
     * @return A table with that data.
     */
    static public String showTable(final Path[] a,final Path[] pruned) {
        final StringBuilder sb = new StringBuilder();
        final Formatter f = new Formatter(sb);
        f.format("%5s %10s%1s * %10s (%6s/%6s) = %10s%1s : %10s %10s",
                "path",//
                "rangeCount",//
                "",// sourceSampleExact
                "f",//
                "out",//
                "in",//
                "estCard",//
                "",// estimateIs(Exact|LowerBound|UpperBound)
                "sumEstCard",//
                "joinPath\n"
                );
        for (int i = 0; i < a.length; i++) {
            final Path x = a[i];
            // true iff the path survived pruning.
            Boolean prune = null;
            if (pruned != null) {
                prune = Boolean.TRUE;
                for (Path y : pruned) {
                    if (y == x) {
                        prune = Boolean.FALSE;
                        break;
                    }
                }
            }
            if (x.sample == null) {
                f.format("p[%2d] %10s%1s * %10s (%6s/%6s) = %10s%1s : %10s", i, "N/A", "", "N/A", "N/A", "N/A", "N/A", "", "N/A");
            } else {
                f.format("p[%2d] %10d%1s * % 10.2f (%6d/%6d) = % 10d%1s : % 10d", i,
                        x.sample.sourceSample.estimatedCardinality,//
                        x.sample.sourceSample.isExact()?"E":"",//
                        x.sample.f,//
                        x.sample.outputCount,//
                        x.sample.inputCount,//
                        x.sample.estimatedCardinality,//
                        x.sample.estimateEnum.getCode(),//
                        x.cumulativeEstimatedCardinality//
                        );
            }
            sb.append("  [");
            for (Vertex v : x.getVertices()) {
                f.format("%2d ", v.pred.getId());
            }
            sb.append("]");
            if (pruned != null) {
                if (prune)
                    sb.append(" pruned");
            }
            // for (Edge e : x.edges)
            // sb.append(" (" + e.v1.pred.getId() + " " + e.v2.pred.getId()
            // + ")");
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Show the details of a join path, including the estimated cardinality and
     * join hit ratio for each step in the path.
     * 
     * @param p
     *            The join path.
     */
    public static String showPath(final Path x) {
        if(x == null)
            throw new IllegalArgumentException();
        final StringBuilder sb = new StringBuilder();
        final Formatter f = new Formatter(sb);
        {
            /*
             * @todo show sumEstCard for each step of the path. Only the
             * estimate for the current path length is currently preserved. We
             * would need to preserve the estimate for each step in the path to
             * show it here.
             * 
             * @todo show limit on EdgeSample?
             */
            f.format("%5s %10s%1s * %10s (%6s/%6s) = %10s%1s",// : %10s",//
                    "edge",
                    "rangeCount",//
                    "",// sourceSampleExact
                    "f",//
                    "out",//
                    "in",//
                    "estCard",//
                    ""// estimateIs(Exact|LowerBound|UpperBound)
//                  "sumEstCard",//
                    );
            int i = 0;
            /*
             * TODO Modify to access the per "edge" sample for the join path,
             * and to show the vertex sample for the initial edge as well.
             */
            for (Edge e : x.edges) {
                sb.append("\n");
                if (e.sample == null) {
                    f.format("%5s %10s%1s * %10s (%6s/%6s) = %10s%1s",//
                            e.getLabel(),//
                            "N/A", "", "N/A", "N/A", "N/A", "N/A", "");//, "N/A");
                } else {
                    f.format("%5s %10d%1s * % 10.2f (%6d/%6d) = % 10d%1s",//
                            e.getLabel(),//
                            e.sample.sourceSample.estimatedCardinality,//
                            e.sample.sourceSample.isExact() ? "E" : "",//
                            e.sample.f,//
                            e.sample.outputCount,//
                            e.sample.inputCount,//
                            e.sample.estimatedCardinality,//
                            e.sample.estimateEnum.getCode()//
//                          e.cumulativeEstimatedCardinality//
                            );
                }
//              sb.append("\nv[" + vertexIds[i] + "] " + e.toString());
                i++;
            }
        }
        sb.append("\n");
        return sb.toString();
    }
    
} // class JGraph
