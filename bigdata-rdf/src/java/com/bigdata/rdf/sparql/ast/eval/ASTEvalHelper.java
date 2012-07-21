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

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.rdf.join.ChunkedMaterializationIterator;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataValueReplacer;
import com.bigdata.rdf.sail.RunningQueryCloseableIterator;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.IDataSetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Helper class for evaluating SPARQL queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEvalHelper {

    /**
     * A logger whose sole purpose is to log the SPARQL queries which are being
     * evaluated.
     */
    private static final Logger log = Logger.getLogger(ASTEvalHelper.class);
    
    /**
     * Batch resolve {@link Value}s to {@link IV}s.
     * 
     * @param store
     *            The KB instance.
     * @param bs
     *            The binding set (may be empty or <code>null</code>).
     * 
     * @return A binding set having resolved {@link IV}s.
     * 
     * @throws QueryEvaluationException
     */
    static private IBindingSet batchResolveIVs(final AbstractTripleStore store,
            final BindingSet bs) throws QueryEvaluationException {

        if (bs == null || bs.size() == 0) {

            // Use an empty binding set.
            return new ListBindingSet();
            
        }

        final Object[] tmp = new BigdataValueReplacer(store).replaceValues(
                null/* dataset */, new BindingSet[] { bs });

        final BindingSet[] a = (BindingSet[]) tmp[1];

        final BindingSet tmp2 = a[0];
        
        return toBindingSet(tmp2);

    }
    
    /**
     * Evaluate a boolean query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param astContainer
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     *            
     * @return <code>true</code> if there are any solutions to the query.
     * 
     * @throws QueryEvaluationException
     */
    static public boolean evaluateBooleanQuery(
            final AbstractTripleStore store,
            final ASTContainer astContainer, final BindingSet bs)
            throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] bindingSets = mergeBindingSets(astContainer,
                batchResolveIVs(store, bs));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bindingSets);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();

        // Note: We do not need to materialize anything for ASK.
        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        CloseableIteration<BindingSet, QueryEvaluationException> itr = null;
        try {
            itr = ASTEvalHelper.evaluateQuery(
                    astContainer,
                    context,
                    bindingSets
                    , materializeProjectionInQuery//
                    , new IVariable[0]// required
                    );
            return itr.hasNext();
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
    }
        
    /**
     * Evaluate a SELECT query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     *            
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public TupleQueryResult evaluateTupleQuery(
            final AbstractTripleStore store, final ASTContainer astContainer,
            final QueryBindingSet bs) throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] bindingSets = mergeBindingSets(astContainer,
                batchResolveIVs(store, bs));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bindingSets);

        // Get the projection for the query.
        final IVariable<?>[] projected = astContainer.getOptimizedAST()
                .getProjection().getProjectionVars();

        final List<String> projectedSet = new LinkedList<String>();

        for (IVariable<?> var : projected)
            projectedSet.add(var.getName());

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();

        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        return new TupleQueryResultImpl(projectedSet,
                ASTEvalHelper.evaluateQuery(
                        astContainer,
                        context,
                        bindingSets
                        ,materializeProjectionInQuery//
                        , projected//
                        ));

    }

    /**
     * Evaluate a SELECT query without converting the results into openrdf
     * solutions.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     * @param materialize
     *            When <code>true</code>, {@link IV}s will be materialized
     *            (their {@link IVCache} association will be set to the
     *            corresponding RDF {@link Value}). When <code>false</code>,
     *            this materialization step will be skipped. However, it is
     *            possible that {@link IV}s in the query plan will be
     *            materialized anyway (for example, materialization might be
     *            required to support FILTERs in the query).
     * 
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public ICloseableIterator<IBindingSet[]> evaluateTupleQuery2(
            final AbstractTripleStore store, final ASTContainer astContainer,
            final QueryBindingSet bs, final boolean materialize)
            throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] bindingSets = mergeBindingSets(astContainer,
                batchResolveIVs(store, bs));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bindingSets);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();

        // true iff we can materialize the projection inside of the query plan.
        final boolean materializeProjectionInQuery = materialize && context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        final List<String> projectedSet;

        if (materialize) {

            /*
             * Add a materialization step.
             */
            
            // Get the projection for the query.
            final IVariable<?>[] projected = astContainer.getOptimizedAST()
                    .getProjection().getProjectionVars();

            projectedSet = new LinkedList<String>();

            for (IVariable<?> var : projected)
                projectedSet.add(var.getName());

        } else {
        
            /*
             * Do not add a materialization step.
             */

            projectedSet = null;
            
        }

        doSparqlLogging(context);
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        
        IRunningQuery runningQuery = null;
        try {

            // Submit query for evaluation.
            runningQuery = context.queryEngine.eval(queryPlan, bindingSets);

            // The iterator draining the query solutions.
            final ICloseableIterator<IBindingSet[]> it1 = runningQuery
                    .iterator();

            final ICloseableIterator<IBindingSet[]> it2;

            if (materialize && !materializeProjectionInQuery
                    && !projectedSet.isEmpty()) {

                /*
                 * Materialize IVs as RDF Values.
                 * 
                 * Note: This is the code path when we want to materialize the
                 * IVs and we can not do so within the query plan because the
                 * query uses a SLICE. If we want to materialize IVs and there
                 * is no slice, then the materialization step is done inside of
                 * the query plan.
                 * 
                 * Note: This does not materialize the IVCache for inline IVs.
                 * The assumption is that the consumer is bigdata aware and can
                 * use inline IVs directly.
                 */
                
                // The variables to be materialized.
                final IVariable<?>[] vars = projectedSet
                        .toArray(new IVariable[projectedSet.size()]);

                // Wrap with chunked materialization logic.
                it2 = new ChunkedMaterializationIterator(vars,
                        context.db.getLexiconRelation(),
                        false/* materializeInlineIVs */, it1);

            } else {
                
                it2 = it1;
                
            }
            
            return it2;

        } catch (Throwable t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            throw new QueryEvaluationException(t);
        }

    }

    /**
     * Evaluate a CONSTRUCT/DESCRIBE query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param astContainer
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     * 
     * @throws QueryEvaluationException
     */
    public static GraphQueryResult evaluateGraphQuery(
            final AbstractTripleStore store, final ASTContainer astContainer,
            final QueryBindingSet bs) throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();
        
        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] bindingSets = mergeBindingSets(astContainer,
                batchResolveIVs(store, bs));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bindingSets);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();
        
        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        return new GraphQueryResultImpl(//
                optimizedQuery.getPrefixDecls(), //
                new ASTConstructIterator(store, //
                        optimizedQuery.getConstruct(), //
                        optimizedQuery.getWhereClause(),//
                        ASTEvalHelper.evaluateQuery(
                                astContainer,
                                context,
                                bindingSets//
                                ,materializeProjectionInQuery//
                                ,optimizedQuery.getProjection()
                                        .getProjectionVars()//
                                )));

    }
    
    /**
     * Evaluate a query plan (core method).
     * 
     * @param astContainer
     *            The query model.
     * @param ctx
     *            The evaluation context.
     * @param bindingSets
     *            The source solution set(s).
     * @param materializeProjectionInQuery
     *            When <code>true</code>, the projection was materialized within
     *            query plan. When <code>false</code>, this method will take
     *            responsibility for that materialization step.
     * @param required
     *            The variables which must be materialized. Only materialized
     *            variables will be reported in the output solutions. This MAY
     *            be <code>null</code> to materialize all variables in the
     *            solutions. If MAY be empty to materialize NONE of the
     *            variables in the solutions (in which case all solutions will
     *            be empty).
     * 
     * @return An iteration which may be used to read Sesame {@link BindingSet}s
     *         containing the solutions for the query.
     * 
     * @throws QueryEvaluationException
     */
    static private CloseableIteration<BindingSet, QueryEvaluationException> evaluateQuery(
            final ASTContainer astContainer,
            final AST2BOpContext ctx,            
            final IBindingSet[] bindingSets, 
            final boolean materializeProjectionInQuery,
            final IVariable<?>[] required) throws QueryEvaluationException {

        doSparqlLogging(ctx);
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        
        IRunningQuery runningQuery = null;
        try {

            // Submit query for evaluation.
            runningQuery = ctx.queryEngine.eval(queryPlan, bindingSets);

            /*
             * Wrap up the native bigdata query solution iterator as Sesame
             * compatible iteration with materialized RDF Values.
             */
            return iterator(runningQuery, ctx.db,
                    materializeProjectionInQuery, required);

        } catch (Throwable t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            throw new QueryEvaluationException(t);
        }

    }

    /**
     * Convert a Sesame {@link BindingSet} into a bigdata {@link IBindingSet}
     * and merges it with the BINDINGS clause (if any) attached to the
     * {@link QueryRoot}.
     * 
     * @param astContainer
     *            The query container.
     * @param src
     *            The {@link BindingSet}.
     * 
     * @return The {@link IBindingSet}.
     */
    private static IBindingSet[] mergeBindingSets(
            final ASTContainer astContainer, final IBindingSet src) {

        if (astContainer == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();
        
        final List<IBindingSet> bindingsClause;
        {
            
            final BindingsClause x = astContainer.getOriginalAST()
                    .getBindingsClause();
         
            if (x == null) {
            
                bindingsClause = null;
                
            } else {
                
                bindingsClause = x.getBindingSets();
                
            }

        }

        if (bindingsClause == null || bindingsClause.isEmpty()) {

            // Just the solution provided through the API.
            return new IBindingSet[] { src };
            
        }
        
        if (src.isEmpty()) {

            /*
             * No source solution, so just use the BINDINGS clause solutions.
             */
            
            return bindingsClause
                    .toArray(new IBindingSet[bindingsClause.size()]);
            
        }

        /*
         * We have to merge the source solution given through the openrdf API
         * with the solutions in the BINDINGS clause. This "merge" is a join. If
         * the join fails for any solution, then that solution is dropped. Since
         * there is only one solution from the API, the cardinality of the join
         * is at most [1 x |BINDINGS|].
         */
        final List<IBindingSet> out = new LinkedList<IBindingSet>();
        {
        
            final Iterator<IBindingSet> itr = bindingsClause.iterator();

            while (itr.hasNext()) {

                final IBindingSet right = itr.next();

                final IBindingSet tmp = BOpContext.bind(src/* left */, right,
                        null/* constraints */, null/* varsToKeep */);

                if (tmp != null) {

                    out.add(tmp);

                }

            }

        }

        // Return the results of that join.
        return out.toArray(new IBindingSet[bindingsClause.size()]);
        
    }
    
    /**
     * Convert a Sesame {@link BindingSet} into a bigdata {@link IBindingSet}.
     * 
     * @param src
     *            The {@link BindingSet}.
     *            
     * @return The {@link IBindingSet}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static IBindingSet toBindingSet(final BindingSet src) {
        
        if (src == null)
            throw new IllegalArgumentException();

        final ListBindingSet bindingSet = new ListBindingSet();

        final Iterator<Binding> itr = src.iterator();

        while(itr.hasNext()) {

            final Binding binding = itr.next();

            final IVariable<IV> var = com.bigdata.bop.Var.var(binding.getName());
            
            final IV iv = ((BigdataValue) binding.getValue()).getIV();
            
            final IConstant<IV> val = new Constant<IV>(iv);
            
            bindingSet.set(var, val);
            
        }
        
        return bindingSet;

    }
    
    
    /**
     * Wrap {@link IRunningQuery} with the logic to materialize {@link IV}s as
     * RDF {@link Value}s.
     * 
     * @param runningQuery
     *            The {@link IRunningQuery}.
     * @param db
     *            The view of the {@link AbstractTripleStore} against which the
     *            query is running.
     * @param materializeProjectionInQuery
     *            When <code>true</code>, the projection was materialized within
     *            query plan. When <code>false</code>, this method will take
     *            responsibility for that materialization step.
     * @param required
     *            The variables which must be materialized (optional).
     * 
     * @return A Sesame {@link CloseableIteration} which will drain
     *         {@link BindingSet}s of materialized RDF {@link Value}s.
     */
    private static CloseableIteration<BindingSet, QueryEvaluationException> iterator(
            final IRunningQuery runningQuery, final AbstractTripleStore db,
            final boolean materializeProjectionInQuery,
            final IVariable<?>[] required) {
    
        /*
         * FIXME We should not dechunk just to rechunk here. This is not very
         * efficient.
         * 
         * The basic API alignment problem is that the IRunningQuery#iterator()
         * visits IBindingSet[] chunks while the BigdataBindingSetResolverator
         * and Bigdata2SesameBindingSetIterator are IChunked(Ordered)Iterators.
         * That is, they implement #nextChunk(). A very simple class could be
         * used to align an IBindingSet[] returned by next() with nextChunk(). I
         * would be suprised if this class did not already exist (in fact, the
         * class is ChunkedArraysIterator).
         * 
         * The other issue is that RunningQueryCloseableIterator would APPEAR to
         * be redundant with QueryResultIterator. However, correct termination
         * is a tricky business and the current layering obviously works. The
         * differences in those two classes appear to be (a) whether or not we
         * invoke cancel() on the IRunningQuery when the iterator is closed and
         * (b) whether or not we are buffering the last element visited. It is
         * quite possible that RunningQueryCloseableIterator simply layers on
         * one or two fixes which SHOULD be incorporated into the
         * QueryResultIterator.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/483 (Eliminate
         * unnecessary chunking and dechunking)
         */
        
        // Dechunkify the running query and monitor the Sesame iterator.
        final ICloseableIterator<IBindingSet> it1 = iterator(runningQuery);
        
        final BOp query = runningQuery.getQuery();
        
        final int chunkCapacity = query.getProperty(
                PipelineOp.Annotations.CHUNK_CAPACITY,
                PipelineOp.Annotations.DEFAULT_CHUNK_CAPACITY);

        // Wrap in an IChunkedOrderedIterator
        final IChunkedOrderedIterator<IBindingSet> it2 = new ChunkedWrappedIterator<IBindingSet>(
                it1, chunkCapacity, IBindingSet.class);

        final CloseableIteration<BindingSet, QueryEvaluationException> it3; 

        if(materializeProjectionInQuery) {
        
            /*
             * The projection of the query is being materialized by the query
             * plan. All we have to do here is convert bigdata IBindingSets
             * consisting of IVs having cached BigdataValues to Sesame
             * BindingSets.
             */
            
            // Convert IVs in IBindingSets to Sesame BindingSets with Values.
            it3 = new Bigdata2Sesame2BindingSetIterator(it2);

        } else {
        
            /*
             * The projection of the query was not materialized by the query
             * plan. We need to layer in a chunked iterator which handles that
             * materialization step and then do the conversion into Sesame
             * BindingSet objects.
             */
            
            /*
             * Note: This captures the historical behavior, which was based on
             * the AbstractTripleStore's configuration properties for
             * chunkCapacity, chunkOfChunksCapacity, and the chunkTimeout. Those
             * properties still affect the rules engine but do not otherwise
             * effect query performance. Most query operators use the PipelineOp
             * annotations to control these properties. RDF Value
             * materialization is the exception. To correct this, I have lifted
             * out all these parameters here so we can override it based on
             * query annotations.
             * 
             * There are two basic code paths for RDF Value materialization: One
             * is the ChunkedMateralizationOp (it handles the "chunk" you feed
             * it as a "chunk" and is used for materialization for FILTERs). The
             * other is the BigdataBindingSetResolverator. Both call through to
             * LexiconRelation#getTerms().
             * 
             * Regarding [termsChunkSize] and [blobsChunkSize], on a cluster,
             * the operation is parallelized (by the ClientIndexView) on a
             * cluster regardless so there is no reason to ever run materialized
             * with more than one thread. Shard local resolution can be enabled
             * by setting [materializeProjectionInQuery:=true], but at the cost
             * of doing the materialization after a SLICE (if there is one in
             * the query). However, when running through the
             * BigdataBindingSetResolverator, there will be exactly one thread
             * materializing RDF values (because the iterator pattern is single
             * threaded) unless the chunkSize exceeds this threshold.
             */
            
            // Historical values.
//            final int chunkCapacity = db.getChunkCapacity();
//            final int chunkOfChunksCapacity = db.getChunkOfChunksCapacity();
//            final long chunkTimeout = db.getChunkTimeout();
//            final int termsChunkSize = 4000;
//            final int blobsChunkSize = 4000;
            
            // Values set based on query hints.
//            final BOp query = runningQuery.getQuery();
//            final int chunkCapacity = query.getProperty(
//                    PipelineOp.Annotations.CHUNK_CAPACITY,
//                    PipelineOp.Annotations.DEFAULT_CHUNK_CAPACITY);
            final int chunkOfChunksCapacity = query.getProperty(
                    PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                    PipelineOp.Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);
            final long chunkTimeout = query.getProperty(
                    PipelineOp.Annotations.CHUNK_TIMEOUT,
                    PipelineOp.Annotations.DEFAULT_CHUNK_TIMEOUT);
            final int termsChunkSize = chunkCapacity;
            final int blobsChunkSize = chunkCapacity;
            
            // Convert bigdata binding sets to Sesame binding sets.
            it3 = new Bigdata2Sesame2BindingSetIterator(
                    // Materialize IVs as RDF Values.
                    new BigdataBindingSetResolverator(db, it2,
                            runningQuery.getQueryId(), required, chunkCapacity,
                            chunkOfChunksCapacity, chunkTimeout,
                            termsChunkSize, blobsChunkSize).start(db
                            .getExecutorService()));

        }
     
        return it3;
        
    }

    /**
     * Dechunkify the running query and monitor the Sesame iterator.
     * 
     * @param runningQuery
     *            The {@link IRunningQuery}.
     *            
     * @return An {@link ICloseableIterator} which has been dechunkified.
     */
    private static ICloseableIterator<IBindingSet> iterator(
            final IRunningQuery runningQuery) {

        // The iterator draining the query solutions.
        final ICloseableIterator<IBindingSet[]> it1 = runningQuery
                .iterator();

        // Dechunkify the original iterator
        final ICloseableIterator<IBindingSet> it2 = 
            new Dechunkerator<IBindingSet>(it1);

        // Monitor IRunningQuery and cancel if Sesame iterator is closed.
        final ICloseableIterator<IBindingSet> it3 =
            new RunningQueryCloseableIterator<IBindingSet>(runningQuery, it2);

        return it3;
        
    }

    /**
     * Evaluate a SPARQL UPDATE request (core method).
     * 
     * @param astContainer
     *            The query model.
     * @param ctx
     *            The evaluation context.
     * @param dataset
     *            A dataset which will override the data set declaration for
     *            each {@link DeleteInsertGraph} operation in the update
     *            sequence (optional).
     * @param includeInferred
     *            if inferences should be included in various operations.
     *
     * @return The timestamp of the commit point.
     *            
     * @throws SailException
     * 
     * TODO timeout for update?
     */
    static public long executeUpdate(//
            final BigdataSailRepositoryConnection conn,//
            final ASTContainer astContainer,//
            final Dataset dataset,
            final boolean includeInferred//
            ) throws UpdateExecutionException {

        if(conn == null)
            throw new IllegalArgumentException();

        if(astContainer == null)
            throw new IllegalArgumentException();
        
        try {

            if (dataset != null) {

                /*
                 * Apply the optional data set override.
                 */

                applyDataSet(conn.getTripleStore(), astContainer, dataset);
                
            }

            final AST2BOpUpdateContext ctx = new AST2BOpUpdateContext(
                    astContainer, conn);

            doSparqlLogging(ctx);

            // Propagate attribute.
            ctx.setIncludeInferred(includeInferred);

            /*
             * Convert the query (generates an optimized AST as a side-effect).
             */
            AST2BOpUpdate.optimizeUpdateRoot(ctx);

            /*
             * Generate and execute physical plans for the update operations.
             */
            AST2BOpUpdate.convertUpdate(ctx);

            return ctx.getCommitTime();
            
        } catch (Exception ex) {

            throw new UpdateExecutionException(ex);

        }

    }

    /**
     * Apply the {@link Dataset} to each {@link DeleteInsertGraph} in the UPDATE
     * request.
     * <p>
     * The openrdf API here is somewhat at odds with the current LCWD for SPARQL
     * UPDATE. In order to align them, setting the {@link Dataset} here causes
     * it to be applied to each {@link DeleteInsertGraph} operation in the
     * {@link UpdateRoot}. Note that the {@link Dataset} has no effect exception
     * for the {@link DeleteInsertGraph} operation in SPARQL 1.1 UPDATE (that is
     * the only operation which has a WHERE clause and which implements the
     * {@link IDataSetNode} interface).
     * 
     * @param tripleStore
     * @param astContainer
     * @param dataset
     * 
     * @see <a href="http://www.openrdf.org/issues/browse/SES-963"> Dataset
     *      assignment in update sequences not properly scoped </a>
     */
    static private void applyDataSet(final AbstractTripleStore tripleStore,
            final ASTContainer astContainer, final Dataset dataset) {

        if (tripleStore == null)
            throw new IllegalArgumentException();
        
        if (astContainer == null)
            throw new IllegalArgumentException();

        if (dataset == null)
            throw new IllegalArgumentException();
        
        /*
         * Batch resolve RDF Values to IVs and then set on the query model.
         */

        final Object[] tmp = new BigdataValueReplacer(tripleStore)
                .replaceValues(dataset, null/* bindings */);

        /*
         * Set the data set on the original AST.
         */
        
        final Dataset resolvedDataset = (Dataset) tmp[0];

        final UpdateRoot updateRoot = astContainer.getOriginalUpdateAST();
        
        for (Update op : updateRoot) {
        
            if (op instanceof IDataSetNode) {
            
                final IDataSetNode node = ((IDataSetNode) op);

                node.setDataset(new DatasetNode(resolvedDataset, true/* update */));
                
            }
            
        }

    }

    /**
     * Log SPARQL Query and SPARQL UPDATE requests.
     * <p>
     * Note: The SPARQL syntax is logged whenever possible. However, we
     * sometimes generate the AST directly, in which case the SPARQL syntax is
     * not available and the AST is logged instead.
     * 
     * @param ctx
     */
    private static void doSparqlLogging(final AST2BOpContext ctx) {

        if (!log.isInfoEnabled())
            return;

        /*
         * Log timestamp of the view and the SPARQL query string.
         */

        setupLoggingContext(ctx);

        final ASTContainer astContainer = ctx.astContainer;

        final String queryString = astContainer.getQueryString();

        if (queryString != null) {

            /*
             * Log the query string when it is available.
             * 
             * Note: We sometimes generate the AST directly, in which case there
             * is no query string.
             */
            
            log.info(queryString);
            
        } else {

            /*
             * If there is no query string, then log the AST instead.
             */
            
            if (astContainer.isQuery()) {
            
                log.info(astContainer.getOriginalAST());
                
            } else {
                
                log.info(astContainer.getOriginalUpdateAST());
                
            }
            
        }

        clearLoggingContext();

    }

    private static void setupLoggingContext(final IEvaluationContext context) {

        MDC.put("tx", TimestampUtility.toString(context.getTimestamp()));

    }
    
    private static void clearLoggingContext() {
        
        MDC.remove("tx");
    }
    
}
