package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IdFactory;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.join.HTreeSolutionSetHashJoinOp;
import com.bigdata.bop.join.JVMSolutionSetHashJoinOp;
import com.bigdata.bop.rdf.join.ChunkedMaterializationOp;
import com.bigdata.htree.HTree;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.EmptySolutionSetStats;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.cache.ISparqlCache;
import com.bigdata.rdf.sparql.ast.cache.SparqlCache;
import com.bigdata.rdf.sparql.ast.cache.SparqlCacheFactory;
import com.bigdata.rdf.sparql.ast.optimizers.ASTOptimizerList;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.IBigdataFederation;

/**
 * Convenience class for passing around the various pieces of context necessary
 * to construct the bop pipeline.
 */
public class AST2BOpContext implements IdFactory, IEvaluationContext {

    /**
     * The {@link ASTContainer}
     */
	public final ASTContainer astContainer;
	
	/**
	 * Factory for assigning unique within query identifiers to {@link BOp}s.
	 * 
	 * @see #nextId()
	 */
	private final AtomicInteger idFactory;
	
	/**
	 * The KB instance.
	 */
	protected final AbstractTripleStore db;
	
	/**
	 * The {@link QueryEngine}. 
	 */
	public final QueryEngine queryEngine;
	
    /**
     * The {@link SparqlCache}.
     */
    public final ISparqlCache sparqlCache;

	/**
	 * The query hints from the original {@link #query}.
	 * 
	 * @see QueryHints
	 * @see ASTQueryHintOptimizer
	 */
    public final Properties queryHints;
    
    /**
     * The unique identifier assigned to this query.
     * 
     * @see QueryHints#QUERYID
     * @see QueryEngine.Annotations#QUERY_ID
     */
    public final UUID queryId;

    /**
     * The query optimizers (this includes both query rewrites for correct
     * semantics, such as a rewrite of a DESCRIBE query into a CONSTRUCT query,
     * and query rewrites for performance optimizations).
     */
    public final ASTOptimizerList optimizers;

    /**
     * Factory for resolving relations and access paths used primarily by
     * {@link AST2BOpJoins}s.
     */
    final BOpContextBase context;
    
    /**
     * When <code>true</code>, may use the version of DISTINCT which operates on
     * the native heap (this is only used when we are doing a hash join against
     * a default graph access path and the predicate for that access path has a
     * large cardinality).
     */
    public boolean nativeDistinctSPO = QueryHints.DEFAULT_NATIVE_DISTINCT_SPO;

    /**
     * The threshold at which we will use a native hash set rather than a
     * default hash set for a default graph access path.
     */
    public long nativeDistinctSPOThreshold = QueryHints.DEFAULT_NATIVE_DISTINCT_SPO_THRESHOLD;

    /**
     * When <code>true</code>, will use the version of the DISTINCT SOLUTIONS
     * operator which uses the {@link HTree} against the native heap.
     * 
     * @see QueryHints#NATIVE_DISTINCT_SOLUTIONS
     */
    public boolean nativeDistinctSolutions = QueryHints.DEFAULT_NATIVE_DISTINCT_SOLUTIONS;

    /**
     * 
     * When <code>true</code>, use hash index operations based on the
     * {@link HTree}. Otherwise use hash index operations based on the Java
     * collection classes. The {@link HTree} is more scalable but has higher
     * overhead for small cardinality hash joins.
     * 
     * @see QueryHints#NATIVE_HASH_JOINS
     */
    public boolean nativeHashJoins = QueryHints.DEFAULT_NATIVE_HASH_JOINS;
    
    /**
     * When <code>true</code>, a merge-join pattern will be recognized if it
     * appears in a join group. When <code>false</code>, this can still be
     * selectively enabled using a query hint.
     * 
     * @see QueryHints#MERGE_JOIN
     */
    public boolean mergeJoin = QueryHints.DEFAULT_MERGE_JOIN;
    
    /**
     * The maximum parallelism for a solution set hash join when the join is
     * used in a context that does permit parallelism, such as sub-group and
     * sub-query evaluation (default {@value #maxParallelForSolutionSetHashJoin}
     * ). The historical value for bigdata releases through 1.2.1 was ONE (1).
     * While this join can be evaluated concurrently for multiple input chunks,
     * assessment done with r6411 on BSBM (explore) and govtrack failed to
     * demonstrate any performance advantage when using maxParallel = 5 (the
     * default maximum parallelism as specified by
     * {@link PipelineOp.Annotations#DEFAULT_MAX_PARALLEL}. This parameter makes
     * it easier to re-test the effect of parallelism in these joins in the
     * future.
     * 
     * @see AST2BOpUtility
     * @see PipelineOp.Annotations#MAX_PARALLEL
     * @see HTreeSolutionSetHashJoinOp
     * @see JVMSolutionSetHashJoinOp
     */
    public int maxParallelForSolutionSetHashJoin = 1;
    
    /**
     * When <code>true</code>, the projection of the query will be materialized
     * by an {@link ChunkedMaterializationOp} within the query plan unless a
     * LIMIT and/or OFFSET was specified. When <code>false</code>, the
     * projection is always materialized outside of the query plan.
     * <p>
     * Note: It is only safe and efficient to materialize the projection from
     * within the query plan when the query does not specify an OFFSET / LIMIT.
     * Materialization done from within the query plan needs to occur before the
     * SLICE operator since the SLICE will interrupt the query evaluation when
     * it is satisfied, which means that downstream operators will be canceled.
     * Therefore a materialization operator can not appear after a SLICE.
     * However, doing materialization within the query plan is not efficient
     * when the query uses a SLICE since we will materialize too many solutions.
     * This is true whether the OFFSET and/or the LIMIT was specified.
     * <p>
     * Note: It is advantegous to handle the materialization step inside of the
     * query plan since that provides transparency into the materialization
     * costs. When we handle materialization outside of the query plan, those
     * materialization costs are not reflected in the query plan statistics.
     * 
     * FIXME This can not be enabled until we fix a bug where variables in
     * optional groups (or simple optional statement patterns) are added to the
     * doneSet when we attempt to materialize them for a filter. The bug is
     * demonstrated by TestOptionals#test_complex_optional_01() as well as by
     * some of the TCK optional tests (TestTCK). (This bug is normally masked if
     * we do materialization outside of the query plan, but a query could be
     * constructed which would demonstrate the problem even then since the
     * variable appears within the query plan generator as if it is
     * "known materialized" when it is only in fact materialized within the
     * scope of the optional group.)
     * 
     * @see <a
     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/489">Optimize
     *      RDF Value materialization performance on cluster </a>
     */
    boolean materializeProjectionInQuery = false;
    
    /**
     * When <code>true</code>, force the use of REMOTE access paths in scale-out
     * joins.
     */
    public boolean remoteAPs = QueryHints.DEFAULT_REMOTE_APS;

    /**
     * The #of samples to take when comparing the cost of a SCAN with an IN
     * filter to as-bound evaluation for each graph in the data set. The samples
     * are taken from the data set. Each sample is a graph (aka context) in the
     * data set. The range counts and estimated cost to visit the AP for each of
     * the sampled contexts are combined to estimate the total cost of visiting
     * all of the contexts in the NG or DG access path.
     * <p>
     * When ZERO (0), no cost estimation will be performed and the named graph
     * or default graph join will always use the SCAN + FILTER approach.
     */
    public int accessPathSampleLimit = QueryHints.DEFAULT_ACCESS_PATH_SAMPLE_LIMIT;

    /**
     * For named and default graph access paths where access path cost
     * estimation is disabled by setting the {@link #accessPathSampleLimit} to
     * ZERO (0), this determines whether a SCAN + FILTER or PARALLEL SUBQUERY
     * (aka as-bound data set join) approach.
     */
    public boolean accessPathScanAndFilter = QueryHints.DEFAULT_ACCESS_PATH_SCAN_AND_FILTER;

    private int varIdFactory = 0;

    /**
     * Some summary statistics about the exogenous solution sets. These are
     * computed by {@link AST2BOpUtility#convert(AST2BOpContext, IBindingSet[])}
     * before it begins to run the {@link #optimizers}.
     */
    private ISolutionSetStats sss = null;
    
    @Override
    public ISolutionSetStats getSolutionSetStats() {
    
        if(sss == null)
            return EmptySolutionSetStats.INSTANCE;
        
        return sss;
        
    }
    
    /**
     * Set the statistics summary for the exogenous solution sets.
     * 
     * @param stats
     *            The summary statistics.
     *            
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>
     * @throws IllegalStateException
     *             if the property has already been set.
     */
    public void setSolutionSetStats(final ISolutionSetStats stats) {
        
        if (stats == null)
            throw new IllegalArgumentException();
        
        if(sss != null)
            throw new IllegalStateException();
        
        this.sss = stats;
        
    }
    
    /**
     * Static analysis object initialized once we apply the AST optimizers and
     * used by {@link AST2BOpUtility}.
     */
    StaticAnalysis sa = null;

    /**
     * An optional map of key-value pairs that will be attached to the
     * {@link IRunningQuery} on the query controller node.
     */
    final private AtomicReference<Map<Object, Object>> queryAttributes = new AtomicReference<Map<Object, Object>>();

    public void addQueryAttribute(final Object key, final Object val) {

        if (queryAttributes.get() == null) {

            // Lazy initialization.
            queryAttributes.compareAndSet(null/* expect */,
                    new LinkedHashMap<Object, Object>()/* update */);

        }

        queryAttributes.get().put(key, val);

    }
    
    /**
     * Return an optional (and immutable) map of key-value pairs that will be
     * attached to the {@link IRunningQuery} on the query controller node.
     * 
     * @return The map -or- <code>null</code> if no query attributes have been
     *         declared.
     */
    public Map<Object, Object> getQueryAttributes() {

        final Map<Object, Object> tmp = queryAttributes.get();

        if (tmp == null)
            return null;

        return Collections.unmodifiableMap(tmp);

    }

    /**
     * 
     * @param queryRoot
     *            The root of the query.
     * @param db
     *            The KB instance.
     * 
     *            TODO We should be passing in the {@link IIndexManager} rather
     *            than the {@link AbstractTripleStore} in order to support cross
     *            kb queries. The AST can be annotated with the namespace of the
     *            default KB instance, which can then be resolved from the
     *            {@link IIndexManager}. This would also allow us to clear up
     *            the [lex] namespace parameter to the {@link FunctionNode} and
     *            {@link FunctionRegistry}.
     */
    public AST2BOpContext(final ASTContainer astContainer,
                final AbstractTripleStore db) {

        if (astContainer == null)
            throw new IllegalArgumentException();

        if (db == null)
            throw new IllegalArgumentException();

        this.astContainer = astContainer;

        this.db = db;

        this.optimizers = new DefaultOptimizerList();

        /*
         * Note: The ids are assigned using incrementAndGet() so ONE (1) is the
         * first id that will be assigned when we pass in ZERO (0) as the
         * initial state of the AtomicInteger.
         */
        this.idFactory = new AtomicInteger(0);
        
        this.queryEngine = QueryEngineFactory.getQueryController(db
                .getIndexManager());

        /*
         * Figure out the query UUID that will be used. This will be bound onto
         * the query plan when it is generated. We figure out what it will be up
         * front so we can refer to its UUID in parts of the query plan. For
         * example, this is used to coordinate access to bits of state on a
         * parent IRunningQuery.
         * 
         * Note: The queryHints:Properties object on the ASTContainer is used
         * for the QueryID. The QueryId is somewhat special since it must
         * defined at this point in the flow of the query through the system.
         * Other things which might be special in a similar manner would include
         * authentication information, etc.
         */
        
        this.queryHints = astContainer./*getOriginalAST().*/getQueryHints();

        // Use either the caller's UUID or a random UUID.
        final String queryIdStr = queryHints == null ? null : queryHints
                .getProperty(QueryHints.QUERYID);

        final UUID queryId = queryIdStr == null ? UUID.randomUUID() : UUID
                .fromString(queryIdStr);

        this.queryId = queryId;
        
        /*
         * Cache for SPARQL named solution sets.
         * 
         * TODO Define a query hint for enabling or disabling the SPARQL cache
         * for an operation.
         */
        {
            final boolean enable = queryHints == null ? QueryHints.DEFAULT_SOLUTION_SET_CACHE
                    : Boolean
                            .valueOf(queryHints
                                    .getProperty(
                                            QueryHints.SOLUTION_SET_CACHE,
                                            QueryHints.DEFAULT_SOLUTION_SET_CACHE ? Boolean.TRUE
                                                    .toString() : Boolean.FALSE
                                                    .toString()));
            if (enable) {
                this.sparqlCache = SparqlCacheFactory
                        .getSparqlCache(queryEngine);
            } else {
                this.sparqlCache = null;
            }
        }
        
        this.context = new BOpContextBase(queryEngine);

    }

    @Override
    public long getTimestamp() {

        return db.getTimestamp();

    }
    
    public int nextId() {

        return idFactory.incrementAndGet();

    }

    @Override
    public boolean isCluster() {

        return db.getIndexManager() instanceof IBigdataFederation<?>;

    }

    @Override
    public boolean isQuads() {

        return db.isQuads();
        
    }
    
    @Override
    public boolean isSIDs() {

        return db.isStatementIdentifiers();
        
    }

    @Override
    public boolean isTriples() {

        return !db.isQuads() && !db.isStatementIdentifiers();
        
    }
    
    @Override
    public String getNamespace() {
    
        return db.getNamespace();
        
    }

    @Override
    public String getSPONamespace() {

        return db.getSPORelation().getNamespace();

    }

    @Override
    public String getLexiconNamespace() {

        return db.getLexiconRelation().getNamespace();

    }

    /**
     * Create a new variable name which is unique within the scope of this
     * {@link AST2BOpContext}.
     * 
     * @param prefix
     *            The prefix.  The general pattern for a prefix is "-foo-".
     *            
     * @return The unique name.
     */
    public final String createVar(final String prefix) {

        return prefix + (varIdFactory++);

    }

    @Override
    public long getLexiconReadTimestamp() {
    
        long timestamp = db.getTimestamp();
    
        if (TimestampUtility.isReadWriteTx(timestamp)) {
    
            timestamp = ITx.UNISOLATED;
            
        }
        
        return timestamp;
        
    }

    public AbstractTripleStore getAbstractTripleStore() {
        
        return db;
        
    }

    public ISparqlCache getSparqlCache() {
    	
    		return sparqlCache;
    	
    }
    
}
