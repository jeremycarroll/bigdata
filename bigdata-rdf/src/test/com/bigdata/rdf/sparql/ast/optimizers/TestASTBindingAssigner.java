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

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTBindingAssigner}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTBindingAssigner extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTBindingAssigner() {
    }

    /**
     * @param name
     */
    public TestASTBindingAssigner(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o}
     * </pre>
     * 
     * and a binding for <code>?p</code> in an input solution, verify that the
     * AST is rewritten as:
     * 
     * <pre>
     * SELECT ?s where {?s CONST ?o}
     * </pre>
     * 
     * where CONST is the binding for <code>?p</code> in the input solution.
     */
    public void test_astBindingAssigner() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
//        final VarNode s = new VarNode("s");
//        final VarNode p = new VarNode("p");
//        final VarNode o = new VarNode("o");
//        
//        final IConstant const1 = new Constant<IV>(TermId.mockIV(VTE.URI));

        final IBindingSet[] bsets = new IBindingSet[] { //
        new ListBindingSet(//
                new IVariable[] { Var.var("p") },//
                new IConstant[] { new Constant<IV>(TermId.mockIV(VTE.URI)) }) //
        };

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("s"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);

            projection.addProjectionVar(new VarNode("s"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(new Constant((IVariable) Var.var("p"),
                            TermId.mockIV(VTE.URI))),
                    new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);

        }

        final IASTOptimizer rewriter = new ASTBindingAssigner();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        /**
         * FIXME This was broken by mroycsi's change to TermId#equals(). The
         * test is probably at fault.
         */
        assertSameAST(expected, actual);

    }

    /**
     * Given
     * 
     * <pre>
     * SELECT ?p where {?s ?p ?s}
     * </pre>
     * 
     * and a binding for <code>?s</code> in an input solution, verify that the
     * AST is rewritten as:
     * 
     * <pre>
     * SELECT ?p where {CONST ?p CONST}
     * </pre>
     * 
     * where CONST is the binding for <code>?s</code> in the input solution.
     * <p>
     * Note: For this unit test, a variable is replaced in more than one
     * location in the AST.
     */
    public void test_astBindingAssigner2() {

        /*
         * Note: DO NOT SHARE STRUCTURES IN THIS TEST.
         */
//        final VarNode s = new VarNode("s");
//        final VarNode p = new VarNode("p");
//        final VarNode o = new VarNode("o");
//        
//        final IConstant const1 = new Constant<IV>(TermId.mockIV(VTE.URI));

        final IBindingSet[] bsets = new IBindingSet[] { //
        new ListBindingSet(//
                new IVariable[] { Var.var("s") },//
                new IConstant[] { new Constant<IV>(TermId.mockIV(VTE.URI))}) //
        };

        // The source AST.
        final QueryRoot input = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("p"));
            input.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("s"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            input.setWhereClause(whereClause);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("p"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(//
                    new ConstantNode(new Constant((IVariable) Var.var("s"),
                            TermId.mockIV(VTE.URI))), //
                    new VarNode("p"),//
                    new ConstantNode(new Constant((IVariable) Var.var("s"),
                            TermId.mockIV(VTE.URI))), //
                    null/* c */, Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);

        }

        final IASTOptimizer rewriter = new ASTBindingAssigner();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                expected/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

}
