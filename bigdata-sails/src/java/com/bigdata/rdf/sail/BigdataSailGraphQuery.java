package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.FilterIteration;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataSailGraphQuery extends SailGraphQuery implements
        BigdataSailQuery {
    
	private static Logger log = Logger.getLogger(BigdataSailGraphQuery.class);
	
    /**
     * Query hints are embedded in query strings as namespaces.  
     * See {@link QueryHints#PREFIX} for more information.
     */
	@Deprecated
    private final Properties queryHints;

    /** Set when the query is evaluated. */
    private volatile QueryRoot optimizedQuery;
    
    private final QueryRoot queryRoot;
    
    public QueryRoot getQueryRoot() {
        
        return queryRoot;
        
    }

    @Override
    public void setDataset(final Dataset dataset) {
        
        if(queryRoot == null) {
            
            super.setDataset(dataset);
            
        } else {

            /*
             * Batch resolve RDF Values to IVs and then set on the query model.
             */
            
            try {
                
                final Object[] tmp = new BigdataValueReplacer(getTripleStore())
                        .replaceValues(dataset, null/* tupleExpr */, null/* bindings */);
                
                queryRoot.setDataset(new DatasetNode((Dataset) tmp[0]));
                
            } catch (SailException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }
        
    }
    
    @Override
    public String toString() {

        if (queryRoot == null)
            return super.toString();

        QueryRoot tmp = optimizedQuery;

        if (tmp == null)
            tmp = queryRoot;

        return BOpUtility.toString2(tmp);

    }

    /**
     * Allow clients to bypass the native construct iterator, which resolves
     * binding sets into SPOs into BigdataStatements.
     */
    @Deprecated
    private boolean useNativeConstruct = false;
    
    public Properties getQueryHints() {
    	
    	return queryHints;
    	
    }

    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

    public BigdataSailGraphQuery(final QueryRoot queryRoot,
            final BigdataSailRepositoryConnection con) {

        super(null/*tupleQuery*/, con); // TODO Might have to fake the TupleExpr with a Nop.
        
        this.queryHints = queryRoot.getQueryHints();
        
        this.queryRoot = queryRoot;
        
    }
    
    @Deprecated
    public BigdataSailGraphQuery(final ParsedGraphQuery tupleQuery,
            final SailRepositoryConnection con, final Properties queryHints, 
            final boolean describe) {
        super(tupleQuery, con);
        this.queryHints = queryHints;
        this.queryRoot = null;
        if (describe) {
            optimizeDescribe();
        }
    }
    
    @Deprecated
    protected void optimizeDescribe() {
        try {
            ParsedQuery parsedQuery = getParsedQuery();
            TupleExpr node = parsedQuery.getTupleExpr();
            if (log.isInfoEnabled())
            	log.info(node);
            node = ((Reduced) node).getArg();
            node = ((Projection) node).getArg();
            ValueExpr ve = ((Filter) node).getCondition();
            node = ((Filter) node).getArg();
            if (node instanceof Join) {
                node = ((Join) node).getLeftArg();
                final Set<Var> vars = new HashSet<Var>();
                ve.visitChildren(new QueryModelVisitorBase() {
                    @Override
                    public void meet(SameTerm same) throws Exception {
                        Var var = (Var) same.getRightArg();
                        vars.add(var);
                    }
                });
                Collection<StatementPattern> sps = new LinkedList<StatementPattern>();
                Collection<ProjectionElemList> projElemLists = 
                    new LinkedList<ProjectionElemList>();
                for (Var v : vars) {
                    {
                        Var p = createAnonVar("-p" + v.getName() + "-1");
                        Var o = createAnonVar("-o" + v.getName());
                        StatementPattern sp = new StatementPattern(v, p, o);
                        sps.add(sp);
                        ProjectionElemList projElemList = new ProjectionElemList();
                        projElemList.addElement(new ProjectionElem(v.getName(), "subject"));
                        projElemList.addElement(new ProjectionElem(p.getName(), "predicate"));
                        projElemList.addElement(new ProjectionElem(o.getName(), "object"));
                        projElemLists.add(projElemList);
                    }
                    {
                        Var s = createAnonVar("-s" + v.getName());
                        Var p = createAnonVar("-p" + v.getName() + "-2");
                        StatementPattern sp = new StatementPattern(s, p, v);
                        sps.add(sp);
                        ProjectionElemList projElemList = new ProjectionElemList();
                        projElemList.addElement(new ProjectionElem(s.getName(), "subject"));
                        projElemList.addElement(new ProjectionElem(p.getName(), "predicate"));
                        projElemList.addElement(new ProjectionElem(v.getName(), "object"));
                        projElemLists.add(projElemList);
                    }
                }
                Iterator<StatementPattern> it = sps.iterator();
                Union union = new Union(it.next(), it.next());
                while (it.hasNext()) {
                    union = new Union(union, it.next());
                }
                node = new Join(node, union);
                node = new MultiProjection(node, projElemLists);
                node = new Reduced(node);
                parsedQuery.setTupleExpr(node);
            } else {
                final Set<ValueConstant> vals = new HashSet<ValueConstant>();
                ve.visitChildren(new QueryModelVisitorBase() {
                    @Override
                    public void meet(SameTerm same) throws Exception {
                        ValueConstant val = (ValueConstant) same.getRightArg();
                        vals.add(val);
                    }
                });
                Collection<StatementPattern> joins = new LinkedList<StatementPattern>();
                Collection<ProjectionElemList> projElemLists = 
                    new LinkedList<ProjectionElemList>();
                Collection<ExtensionElem> extElems = new LinkedList<ExtensionElem>();
                int i = 0;
                int constVarID = 1;
                for (ValueConstant v : vals) {
                    {
                        Var s = createConstVar(v.getValue(), constVarID++);
                        Var p = createAnonVar("-p" + i + "-1");
                        Var o = createAnonVar("-o" + i);
                        StatementPattern sp = new StatementPattern(s, p, o);
                        joins.add(sp);
                        ProjectionElemList projElemList = new ProjectionElemList();
                        projElemList.addElement(new ProjectionElem(s.getName(), "subject"));
                        projElemList.addElement(new ProjectionElem(p.getName(), "predicate"));
                        projElemList.addElement(new ProjectionElem(o.getName(), "object"));
                        projElemLists.add(projElemList);
                        extElems.add(new ExtensionElem(v, s.getName()));
                    }
                    {
                        Var s = createAnonVar("-s" + i);
                        Var p = createAnonVar("-p" + i + "-2");
                        Var o = createConstVar(v.getValue(), constVarID++);
                        StatementPattern sp = new StatementPattern(s, p, o);
                        joins.add(sp);
                        ProjectionElemList projElemList = new ProjectionElemList();
                        projElemList.addElement(new ProjectionElem(s.getName(), "subject"));
                        projElemList.addElement(new ProjectionElem(p.getName(), "predicate"));
                        projElemList.addElement(new ProjectionElem(o.getName(), "object"));
                        projElemLists.add(projElemList);
                        extElems.add(new ExtensionElem(v, o.getName()));
                    }
                    i++;
                }
                Iterator<StatementPattern> it = joins.iterator();
                node = it.next();
                while (it.hasNext()) {
                    StatementPattern j = it.next();
                    node = new Union(j, node);
                }
                node = new Extension(node, extElems);
                node = new MultiProjection(node, projElemLists);
                node = new Reduced(node);
                parsedQuery.setTupleExpr(node);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Deprecated
    private Var createConstVar(Value value, int constantVarID) {
        Var var = createAnonVar("-const-" + constantVarID);
        var.setValue(value);
        return var;
    }

    @Deprecated
    private Var createAnonVar(String varName) {
        Var var = new Var(varName);
        var.setAnonymous(true);
        return var;
    }
    
//    /**
//     * Allow clients to bypass the native construct iterator, which resolves
//     * binding sets into SPOs into BigdataStatements.  Sometimes this can
//     * cause problems, especially when construct graphs contain values not
//     * in the database's lexicon. 
//     */
//    public void setUseNativeConstruct(boolean useNativeConstruct) {
//        this.useNativeConstruct = useNativeConstruct;
//    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use query hints from SPARQL queries. Query hints are
     * embedded in query strings as namespaces.
     * 
     * @see QueryHints
     */
    @Override
    public GraphQueryResult evaluate() throws QueryEvaluationException {
    
        if (queryRoot != null) {

            if (getMaxQueryTime() > 0)
                queryRoot.setTimeout(TimeUnit.SECONDS
                        .toMillis(getMaxQueryTime()));

            queryRoot.setIncludeInferred(getIncludeInferred());

            final AbstractTripleStore store = getTripleStore();

            final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

            if (log.isInfoEnabled())
                log.info("queryRoot:\n" + queryRoot);

            /*
             * Run the query optimizer first so we have access to the rewritten
             * query plan.
             */
            this.optimizedQuery = context.optimizedQuery = (QueryRoot) context.optimizers
                    .optimize(context, queryRoot, null/* bindingSet[] */);

            if (log.isInfoEnabled())
                log.info("optimizedQuery:\n" + optimizedQuery);

            final PipelineOp queryPlan = AST2BOpUtility.convert(context);

            if (log.isInfoEnabled())
                log.info("queryPlan:\n" + queryPlan);

            final GraphQueryResult queryResult = ASTEvalHelper
                    .evaluateGraphQuery(
                    store, //
                    queryPlan, //
                    new QueryBindingSet(getBindings()),//
                    context.queryEngine, //
                    queryRoot.getProjection().getProjectionVars(),
                    queryRoot.getPrefixDecls(), //
                    queryRoot.getConstruct()//
                    );

            return queryResult;
            
        }
        try {
            
        	final TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

        	final BigdataSailConnection sailCon =
                    (BigdataSailConnection) getConnection().getSailConnection();
            
            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter = sailCon
                    .evaluate(tupleExpr, getActiveDataset(), getBindings(),
                            getIncludeInferred(),
                            queryHints);
            
            // Filters out all partial and invalid matches
            bindingsIter =
                    new FilterIteration<BindingSet, QueryEvaluationException>(
                            bindingsIter) {
                        @Override
                        protected boolean accept(BindingSet bindingSet) {
                            final Value context = bindingSet.getValue("context");
                            return bindingSet.getValue("subject") instanceof Resource
                                    && bindingSet.getValue("predicate") instanceof URI
                                    && bindingSet.getValue("object") instanceof Value
                                    && (context == null || context instanceof Resource);
                        }
                    };
                    
            if (!useNativeConstruct) {

                // Convert the BindingSet objects to actual RDF statements

                final ValueFactory vf = getConnection().getRepository().getValueFactory();

                final CloseableIteration<Statement, QueryEvaluationException> stIter;

                stIter = new ConvertingIteration<BindingSet, Statement, QueryEvaluationException>(
                        bindingsIter) {

                    @Override
                    protected Statement convert(BindingSet bindingSet) {
                    	final Resource subject = (Resource)bindingSet.getValue("subject");
                        final URI predicate = (URI)bindingSet.getValue("predicate");
                        final Value object = bindingSet.getValue("object");
                        final Resource context = (Resource)bindingSet.getValue("context");

                        if (context == null) {
                            return vf.createStatement(subject, predicate, object);
                        } else {
                            return vf.createStatement(subject, predicate, object, context);
                        }
                    }

                };

                return new GraphQueryResultImpl(getParsedQuery()
                        .getQueryNamespaces(), stIter);

            } else {
                
                /*
                 * Native construct.
                 */
                
                // Convert the BindingSet objects to actual RDF statements
                final ValueFactory vf = getConnection().getRepository()
                        .getValueFactory();

                final CloseableIteration<? extends Statement, QueryEvaluationException> stIter;

                stIter = new BigdataConstructIterator(sailCon.getTripleStore(),
                        bindingsIter, vf);

                return new GraphQueryResultImpl(getParsedQuery()
                        .getQueryNamespaces(), stIter);

            }
            
        } catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
        
    }
    
    /**
     * Return the same optimized operator tree as what would be executed.
     */
    @Deprecated
    public TupleExpr getTupleExpr() throws QueryEvaluationException {
        
        if (getParsedQuery() == null) {
            // native sparql evaluation.
            return null;
        }
        
        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();
        
        try {
       
            final BigdataSailConnection sailCon =
                (BigdataSailConnection) getConnection().getSailConnection();
            
            tupleExpr = sailCon.optimize(tupleExpr, getActiveDataset(), 
                    getBindings(), getIncludeInferred(), queryHints);
            
            return tupleExpr;
            
        } catch (SailException e) {
            
            throw new QueryEvaluationException(e.getMessage(), e);
            
        }

    }

}
