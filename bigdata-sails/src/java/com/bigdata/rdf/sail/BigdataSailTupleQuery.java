package com.bigdata.rdf.sail;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

public class BigdataSailTupleQuery extends SailTupleQuery 
        implements BigdataSailQuery {

//    private static final Logger log = Logger.getLogger(BigdataSailTupleQuery.class);
    
    private final ASTContainer astContainer;
    
    public ASTContainer getASTContainer() {
        
        return astContainer;
        
    }

    @Override
    public void setDataset(final Dataset dataset) {

        /*
         * Batch resolve RDF Values to IVs and then set on the query model.
         */

        try {

            final Object[] tmp = new BigdataValueReplacer(getTripleStore())
                    .replaceValues(dataset, null/* tupleExpr */, null/* bindings */);

            astContainer.getOriginalAST().setDataset(
                    new DatasetNode((Dataset) tmp[0]));

        } catch (SailException e) {

            throw new RuntimeException(e);

        }

    }

    @Override
    public String toString() {

        return astContainer.toString();
        
    }
    
    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

    public BigdataSailTupleQuery(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/* tupleQuery */, con);

        if (astContainer == null)
            throw new IllegalArgumentException();

        this.astContainer = astContainer;

    }

    @Override
    public TupleQueryResult evaluate() throws QueryEvaluationException {

        final QueryRoot originalQuery = astContainer.getOriginalAST();

        if (getMaxQueryTime() > 0)
            originalQuery.setTimeout(TimeUnit.SECONDS
                    .toMillis(getMaxQueryTime()));

        originalQuery.setIncludeInferred(getIncludeInferred());

        final TupleQueryResult queryResult = ASTEvalHelper.evaluateTupleQuery(
                getTripleStore(), astContainer, new QueryBindingSet(
                        getBindings()));

        return queryResult;

    }
    
    public TupleQueryResult evaluate(final IAsynchronousIterator<IBindingSet[]> bSetsItr) 
    		throws QueryEvaluationException {
    	
        final QueryRoot originalQuery = astContainer.getOriginalAST();

        if (getMaxQueryTime() > 0)
            originalQuery.setTimeout(TimeUnit.SECONDS
                    .toMillis(getMaxQueryTime()));

        originalQuery.setIncludeInferred(getIncludeInferred());

        final TupleQueryResult queryResult = ASTEvalHelper.evaluateTupleQuery(
                getTripleStore(), astContainer, bSetsItr);

        return queryResult;
    	
    }

}
