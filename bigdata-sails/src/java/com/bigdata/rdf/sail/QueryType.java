package com.bigdata.rdf.sail;

import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;

/**
 * Helper class to figure out the type of a query.
 */
public enum QueryType {

    ASK, DESCRIBE, CONSTRUCT, SELECT;

    private QueryType() {
        
    }

    /**
     * Hack returns the query type based on the first occurrence of the
     * keyword for any known query type in the query.
     * 
     * @param queryStr
     *            The query.
     * 
     * @return The query type.
     */
    static public QueryType fromQuery(final String queryStr) {
        
        try {
            final ASTQueryContainer queryContainer = SyntaxTreeBuilder
                    .parseQuery(queryStr);
            final ASTQuery query = queryContainer.getQuery();
            if(query instanceof ASTSelectQuery) return QueryType.SELECT;
            if(query instanceof ASTDescribeQuery) return QueryType.DESCRIBE;
            if(query instanceof ASTConstructQuery) return QueryType.CONSTRUCT;
            if(query instanceof ASTAskQuery) return QueryType.ASK;
            throw new RuntimeException(queryContainer.toString());
        } catch (TokenMgrError ex) {
            throw new RuntimeException(ex);
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }

    }
    
}
