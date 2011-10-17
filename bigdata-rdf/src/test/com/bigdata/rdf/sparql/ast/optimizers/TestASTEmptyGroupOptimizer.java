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

import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.BD;

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
public class TestASTEmptyGroupOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTEmptyGroupOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTEmptyGroupOptimizer(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     JoinGroupNode {
     *       JoinGroupNode [context=VarNode(g)] {
     *         StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), NAMED_CONTEXTS)
     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
     *       }
     *     }
     * </pre>
     * 
     * Replace the outer {@link JoinGroupNode} with the inner
     * {@link JoinGroupNode}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BD.SEARCH.toString()));

        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final JoinGroupNode graphGroup = new JoinGroupNode();
            whereClause.addChild(graphGroup);
            
            graphGroup.setContext(new VarNode("g"));
            
            graphGroup.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            graphGroup.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            whereClause.setContext(new VarNode("g"));
            
            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Unit test for
     * 
     * <pre>
     * { {} } => {}
     * </pre>
     */
    public void test_eliminateJoinGroup02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // { {} }
            final JoinGroupNode whereClause = new JoinGroupNode(new JoinGroupNode());
            given.setWhereClause(whereClause);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // {}
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

        }
        
        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Unit test for
     * 
     * <pre>
     * { { {} } } => {}
     * </pre>
     */
    public void test_eliminateJoinGroup03() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // { { {} } }
            final JoinGroupNode whereClause = new JoinGroupNode(
                    new JoinGroupNode(new JoinGroupNode()));
            given.setWhereClause(whereClause);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

        }
        
        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Unit test verifies that the rewrite of an embedded sole GRAPH pattern is
     * the lifting of that GRAPH pattern (rather than replacing it with an empty
     * non-GRAPH group).
     * 
     * <pre>
     * { GRAPH foo {} } => GRAPH foo {}
     * </pre>
     */
    public void test_eliminateJoinGroup04() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // { GRAPH ?g {} }
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final JoinGroupNode graphGroup = new JoinGroupNode(new JoinGroupNode());
            graphGroup.setContext(new VarNode("g"));
            whereClause.addChild(graphGroup);

            System.err.println("given:"+given);
        }
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // GRAPH ?g {}
            final JoinGroupNode graphGroup = new JoinGroupNode(new JoinGroupNode());
            graphGroup.setContext(new VarNode("g"));
            expected.setWhereClause(graphGroup);

            System.err.println("expected:"+expected);
        }
        
        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * For this AST we need to replace the root {@link JoinGroupNode} with its
     * child {@link UnionNode}. This means setting the child {@link UnionNode}
     * as the whereClause on the {@link QueryBase}.
     * 
     * <pre>
     * SELECT DISTINCT VarNode(s) VarNode(o)
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_eliminateJoinGroup05() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        /*
         * FIXME The ASTEmptyGroupOptimizer needs to be refactored to handle
         * this.  We need to replace the parent with the child and also handle
         * the case when the parent is the top-level of the WHERE clause.
         */
        assertSameAST(expected, actual);

    }
    
}
