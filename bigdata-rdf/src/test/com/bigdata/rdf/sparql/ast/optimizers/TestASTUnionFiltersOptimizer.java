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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for {@link ASTEmptyGroupOptimizer}.
 * 
 * TODO Test for removal of empty UNIONs. Empty UNIONs can arise through pruning
 * based on unknown IVs, or filters which are provably false. However, I would
 * expect the pruning logic to eliminate the empty group in such cases.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestASTEmptyGroupOptimizer.java 5302 2011-10-07 14:28:03Z
 *          thompsonbry $
 */
public class TestASTUnionFiltersOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTUnionFiltersOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTUnionFiltersOptimizer(String name) {
        super(name);
    }

    /**
     * https://sourceforge.net/apps/trac/bigdata/ticket/416
     */
    public void test_ticket416() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};
        
        final IV type = makeIV(RDF.TYPE);
        
        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV t1 = makeIV(new URIImpl("http://example/t1"));

        final IV t2 = makeIV(new URIImpl("http://example/t2"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("p"));
            projection.addProjectionVar(new VarNode("r"));
            projection.addProjectionVar(new VarNode("l"));
            projection.setDistinct(true);
            
            final UnionNode union = new UnionNode(); // outer
            
            final JoinGroupNode left = new JoinGroupNode();
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new VarNode("p"),
            		new VarNode("r")));
            left.addChild(new StatementPatternNode(
            		new VarNode("r"),
            		new ConstantNode(type),
            		new VarNode("type")));
            
            final JoinGroupNode right = new JoinGroupNode();
            right.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new VarNode("p"),
            		new VarNode("s")));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new ConstantNode(type),
            		new VarNode("type")));

            union.addChild(left);
            union.addChild(right);
            
            final JoinGroupNode where = new JoinGroupNode();
            
            where.addChild(union);
            
            where.addChild(new FilterNode(new FilterNode(new FunctionNode(
            		FunctionRegistry.OR, 
            		null/*scalarValues*/, 
            		new ValueExpressionNode[] { 
            				new FunctionNode(
            	            		FunctionRegistry.SAME_TERM, 
            	            		null/*scalarValues*/, 
            	            		new ValueExpressionNode[] { 
            	            				new VarNode("type"),
            	            				new ConstantNode(t1)
            	            		}),
    	            		new FunctionNode(
    	                    		FunctionRegistry.SAME_TERM, 
    	                    		null/*scalarValues*/, 
    	                    		new ValueExpressionNode[] { 
    	                    				new VarNode("type"),
    	                    				new ConstantNode(t2)
    	                    		})
            		}))));

            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("p"));
            projection.addProjectionVar(new VarNode("r"));
            projection.addProjectionVar(new VarNode("l"));
            projection.setDistinct(true);
            
            final UnionNode union = new UnionNode(); // outer
            
            final JoinGroupNode left = new JoinGroupNode();
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new VarNode("p"),
            		new VarNode("r")));
            left.addChild(new StatementPatternNode(
            		new VarNode("r"),
            		new ConstantNode(type),
            		new VarNode("type")));
            left.addChild(new FilterNode(new FunctionNode(
            		FunctionRegistry.OR, 
            		null/*scalarValues*/, 
            		new ValueExpressionNode[] { 
            				new FunctionNode(
            	            		FunctionRegistry.SAME_TERM, 
            	            		null/*scalarValues*/, 
            	            		new ValueExpressionNode[] { 
            	            				new VarNode("type"),
            	            				new ConstantNode(t1)
            	            		}),
    	            		new FunctionNode(
    	                    		FunctionRegistry.SAME_TERM, 
    	                    		null/*scalarValues*/, 
    	                    		new ValueExpressionNode[] { 
    	                    				new VarNode("type"),
    	                    				new ConstantNode(t2)
    	                    		})
            		})));
            
            final JoinGroupNode right = new JoinGroupNode();
            right.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new VarNode("p"),
            		new VarNode("s")));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new ConstantNode(type),
            		new VarNode("type")));
            right.addChild(new FilterNode(new FunctionNode(
            		FunctionRegistry.OR, 
            		null/*scalarValues*/, 
            		new ValueExpressionNode[] { 
            				new FunctionNode(
            	            		FunctionRegistry.SAME_TERM, 
            	            		null/*scalarValues*/, 
            	            		new ValueExpressionNode[] { 
            	            				new VarNode("type"),
            	            				new ConstantNode(t1)
            	            		}),
    	            		new FunctionNode(
    	                    		FunctionRegistry.SAME_TERM, 
    	                    		null/*scalarValues*/, 
    	                    		new ValueExpressionNode[] { 
    	                    				new VarNode("type"),
    	                    				new ConstantNode(t2)
    	                    		})
            		})));

            union.addChild(left);
            union.addChild(right);
            
            expected.setProjection(projection);
            expected.setWhereClause(union);

        }

        final IASTOptimizer opt1 = new ASTUnionFiltersOptimizer();
        
        final IASTOptimizer opt2 = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = opt2.optimize(
        		null/* AST2BOpContext */,
        		opt1.optimize(null/* AST2BOpContext */, given/* queryNode */, bsets),
        		bsets
        		);

        assertSameAST(expected, actual);

    }

}
