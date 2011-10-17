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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import org.openrdf.query.parser.sparql.ast.SimpleNode;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * A super container for the AST.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTContainer extends ASTBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryBase.Annotations {

        /**
         * The original query from which this AST was generated.
         */
        String QUERY_STRING = "queryString";

        /**
         * The parse tree generated from the query string (optional). For the
         * default integration, this is the parse tree assembled by the Sesame
         * <code>sparql.jjt</code> grammar. Other integrations may produce
         * different parse trees using different object models.
         * <p>
         * Note: There is no guarantee that the parse tree is a serializable
         * object. It may not need to be stripped off of the {@link QueryRoot}
         * if the {@link QueryRoot} is persisted or shipped to another node in a
         * cluster.
         */
        String PARSE_TREE = "parseTree";

        /**
         * The AST as received from the parser.
         */
        String ORIGINAL_AST = "originalAST";

        /**
         * The AST as rewritten by the {@link IASTOptimizer}s.
         */
        String OPTIMIZED_AST = "optimizedAST";
        
    }

    /**
     * Deep copy constructor.
     */
    public ASTContainer(ASTContainer op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     */
    public ASTContainer(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public ASTContainer(final QueryRoot queryRoot) {
        
        super(BOp.NOARGS, null/*anns*/);
        
        setOriginalAST(queryRoot);
        
    }
    
    /**
     * Return the original query from which this AST model was generated.
     */
    public String getQueryString() {

        return (String) getProperty(Annotations.QUERY_STRING);

    }

    /**
     * Set the query string used to generate the AST model.
     * @param queryString The query string.
     */
    public void setQueryString(String queryString) {
        
        setProperty(Annotations.QUERY_STRING, queryString);

    }

    /**
     * Return the parse tree generated from the query string. 
     */
    public Object getParseTree() {

        return getProperty(Annotations.PARSE_TREE);
        
    }

    /**
     * Set the parse tree generated from the query string.
     * 
     * @param parseTree
     *            The parse tree (may be <code>null</code>).
     */
    public void setParseTree(final Object parseTree) {
        
        setProperty(Annotations.PARSE_TREE, parseTree);
        
    }
    
    /**
     * Return the original AST model (before any optimization).
     */
    public QueryRoot getOriginalAST() {

        return (QueryRoot) getProperty(Annotations.ORIGINAL_AST);

    }

    /**
     * Set the original AST model (before any optimizations).
     */
    public void setOriginalAST(final QueryRoot queryRoot) {
        
        setProperty(Annotations.ORIGINAL_AST, queryRoot);

    }

    /**
     * Return the optimized AST model.
     */
    public QueryRoot getOptimizedAST() {

        return (QueryRoot) getProperty(Annotations.OPTIMIZED_AST);

    }

    /**
     * Set the optimized AST model.
     * <p>
     * Note: You MUST deep copy the original AST to avoid destructive side
     * effects when the {@link IASTOptimizer}s are run.
     */
    public void setOptimizedAST(final QueryRoot queryRoot) {
        
        setProperty(Annotations.OPTIMIZED_AST, queryRoot);

    }

    /**
     * Clears the optimized AST model (necessary when something on which it
     * depends has been changed in the original AST model, for example, if you
     * replace the {@link DatasetNode}).
     */
    public void clearOptimizedAST() {

        clearProperty(Annotations.OPTIMIZED_AST);
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();

        final String queryString = getQueryString();
        
        final Object parseTree = getParseTree();
        
        final QueryRoot originalAST = getOriginalAST();
        
        final QueryRoot optimizedAST = getOptimizedAST();
        
        if (queryString != null) {

            sb.append("\n");
            sb.append(Annotations.QUERY_STRING);
            sb.append("\n");
            sb.append(queryString);
            sb.append("\n");

        }
        
        if (parseTree != null) {

            sb.append("\n");
            sb.append(Annotations.PARSE_TREE);
            sb.append("\n");

            if(parseTree instanceof SimpleNode) {

                // Dump parse tree for sparql.jjt grammar.
                sb.append(((SimpleNode)parseTree).dump(""));
                
            } else {
            
                /*
                 * Dump some other parse tree, assuming it implements toString()
                 * as pretty print.
                 */
                sb.append(parseTree.toString());
                sb.append("\n");
                
            }

        }

        if (originalAST != null) {

            sb.append("\n");
            sb.append(Annotations.ORIGINAL_AST);
            sb.append(originalAST);

        }

        if (optimizedAST != null) {

            sb.append("\n");
            sb.append(Annotations.OPTIMIZED_AST);
            sb.append(optimizedAST);

        }

        return sb.toString();

    }

}