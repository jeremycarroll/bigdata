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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.Map;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTExistsOptimizer;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Test suite for SPARQL negation (EXISTS and MINUS).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNegation extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestNegation() {
    }

    /**
     * @param name
     */
    public TestNegation(String name) {
        super(name);
    }

    /**
     * Unit test for an query with an EXISTS filter. The EXISTS filter is
     * modeled as an ASK sub-query which projects an anonymous variable and a
     * simple test of the truth state of that anonymous variable.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT DISTINCT ?x
     * WHERE {
     *   ?x ?p ?o .
     *   FILTER ( EXISTS {?x rdf:type foaf:Person} ) 
     * }
     * </pre>
     */
    public void test_exists_1() throws Exception {

        new TestHelper(
                "exists-1", // testURI,
                "exists-1.rq",// queryFileURL
                "exists-1.trig",// dataFileURL
                "exists-1.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * Sesame Unit <code>sparql11-exists-05</code>.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?n .
     *     }
     * }
     * </pre>
     */
    public void test_sparql11_exists_05() throws Exception {

        new TestHelper(
                "sparql11-exists-05", // testURI,
                "sparql11-exists-05.rq",// queryFileURL
                "sparql11-exists-05.ttl",// dataFileURL
                "sparql11-exists-05.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }
    
    /**
     * Sesame Unit <code>sparql11-exists-06</code>, which appears to be the same
     * as an example in the LCWD.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?m .
     *         FILTER(?n = ?m)
     *     }
     * }
     * </pre>
     * 
     * <pre>
     * @prefix : <http://example/> .
     * 
     * :a :p 1 ; :q 1, 2 .
     * :b :p 3.0 ; :q 4.0, 5.0 .
     * </pre>
     * 
     * <pre>
     * { a = b, n = 3 }
     * </pre>
     * 
     * Note: There are several problems which are related to this test failure
     * <p>
     * First, the {@link ASTBottomUpOptimizer} is incorrectly deciding that
     * <code>?n</code> is not in scope within the inner FILTER. This causes the
     * variable to be renamed in that context in order to model bottom up
     * evaluation semantics which is why the inner FILTER always fails.
     * <p>
     * Second, the ASK subquery is not projecting in all variables which are in
     * scope. This is simply how that subquery was put together by the
     * {@link ASTExistsOptimizer}.
     * <p>
     * Finally, neither EXISTS not NOT EXISTS may cause any new bindings to be
     * made. Therefore, we need to filter out all bindings (including on the
     * anonymous variable) which are made in order to help us answer a (NOT)
     * EXISTS FILTER.
     * <p>
     * This issue was resolved by a change to {@link ASTBottomUpOptimizer} to
     * test when a visited {@link JoinGroupNode} was actually a graph pattern
     * inside of a {@link FilterNode}. In this case, the group is simply skipped
     * over as it will be properly handled when we visit the
     * {@link JoinGroupNode} to which the {@link FilterNode} is attached.
     */
    public void test_sparql11_exists_06() throws Exception {

        new TestHelper(
                "sparql11-exists-06", // testURI,
                "sparql11-exists-06.rq",// queryFileURL
                "sparql11-exists-06.ttl",// dataFileURL
                "sparql11-exists-06.srx" // resultFileURL,
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_01() throws Exception {

        new TestHelper(
                "sparql11-minus-01", // testURI,
                "sparql11-minus-01.rq",// queryFileURL
                "sparql11-minus-01.ttl",// dataFileURL
                "sparql11-minus-01.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test based on the SPARQL 1.1 LCWD.
     * 
     * <pre>
     * SELECT *
     * WHERE { ?s ?p ?o 
     *         MINUS { ?x ?y ?z } 
     * }
     * </pre>
     * 
     * There is only one solution to the first statement pattern. Since the
     * MINUS group binds different variables, no solutions are removed and the
     * sole solution to the <code>?s ?p ?o</code> statement pattern should be
     * reported.
     * <p>
     * Note: Since there are no shared variables, we have to lift out the MINUS
     * group into a named subquery in order to have bottom up evaluation
     * semantics.
     */
    public void test_sparql11_minus_02() throws Exception {

        new TestHelper(
                "sparql11-minus-02", // testURI,
                "sparql11-minus-02.rq",// queryFileURL
                "sparql11-minus-02.ttl",// dataFileURL
                "sparql11-minus-02.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?n .
     *     }
     * }     *
     * </pre>
     */
    public void test_sparql11_minus_05() throws Exception {

        new TestHelper(
                "sparql11-minus-05", // testURI,
                "sparql11-minus-05.rq",// queryFileURL
                "sparql11-minus-05.ttl",// dataFileURL
                "sparql11-minus-05.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?m .
     *         FILTER(?n = ?m)
     *     }
     * }
     * </pre>
     * 
     * The variable <code>?n</code> is not bound inside of the FILTER (unless it
     * is an exogenous variable) because the right hand side of MINUS does not
     * have visibility into the variables on the left hand side of MINUS.
     */
    public void test_sparql11_minus_06() throws Exception {

        new TestHelper(
                "sparql11-minus-06", // testURI,
                "sparql11-minus-06.rq",// queryFileURL
                "sparql11-minus-06.ttl",// dataFileURL
                "sparql11-minus-06.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?m .
     *         OPTIONAL {?a :r ?n} 
     *         FILTER(?n = ?m)
     *    } 
     * }
     * </pre>
     * 
     * <pre>
     * @prefix : <http://example/> .
     * 
     * :a :p 1 ; :q 1, 2 .
     * :b :p 3.0 ; :q 4.0, 5.0 .
     * </pre>
     * 
     * <pre>
     * {?a=:a, ?n=1}
     * {?a=:b, ?n=3.0}
     * </pre>
     * 
     * In this case the FILTER in the MINUS group COULD have a binding for
     * <code>?n</code> from the OPTIONAL group.
     * 
     * <p>
     * 
     * Note: This is actually a badly formed left-join pattern. The MINUS group
     * members get lifted into a named subquery which is then INCLUDEd into the
     * MINUS group.
     */
    public void test_sparql11_minus_07() throws Exception {

        new TestHelper(
                "sparql11-minus-07", // testURI,
                "sparql11-minus-07.rq",// queryFileURL
                "sparql11-minus-07.ttl",// dataFileURL
                "sparql11-minus-07.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }
    
    /**
     * <pre>
     * SELECT ?ar
     * WHERE {
     *     ?ar a <os:class/AnalysisResults>.
     *     FILTER NOT EXISTS {
     *         ?ar <os:prop/analysis/refEntity> <os:elem/loc/Artis>.
     *     }.
     *     FILTER NOT EXISTS {
     *         ?ar <os:prop/analysis/refEntity> <os:elem/loc/Kriterion>.
     *     }.
     * }
     * </pre>
     * 
     * This query demonstrated a problem where the anonymous variables created
     * for the evaluation of "exists" were not being taken into account when
     * computing the variables to be projected out of a hash join.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/515">
     *      Query with two "FILTER NOT EXISTS" expressions returns no
     *      results</a>
     */
    public void test_filter_not_exists() throws Exception {
        new TestHelper(
                "filter-not-exists", // testURI,
                "filter-not-exists.rq",// queryFileURL
                "filter-not-exists.ttl",// dataFileURL
                "filter-not-exists.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();
        }
    
    /**
     * A varient on {@link #test_filter_not_exists()} where the first SP and
     * FILTER NOT EXISTS are moved into a child join group. openrdf translates
     * this into the same operator model for execution.
     * <p>
     * We wind up with a bad evaluation plan which does not produce any results.
     * It winds up running the 2nd FILTER NOT EXISTS before the child join
     * group. However, if we disable the join order optimizer, then the child
     * join group runs first, but we still get the wrong answer. This suggests
     * that there is a problem with (a) filter attachment; and (b) nested
     * evaluation.
     * 
     * <pre>
     * SELECT DISTINCT ?ar
     * WHERE {
     *     {
     *         ?ar a <os:class/AnalysisResults>.
     *         FILTER NOT EXISTS {
     *             ?ar <os:prop/analysis/refEntity> <os:elem/loc/Artis>.
     *         }
     *     } FILTER NOT EXISTS {
     *         ?ar <os:prop/analysis/refEntity> <os:elem/loc/Kriterion>.
     *     }
     * }
     * </pre>
     * 
     * If we remove the 2nd FILTER NOT EXISTS, then the SP and the first FILTER
     * NOT EXIST are lifted out of the child graph group pattern and we get the
     * correct result (assuming that this does not change the correct query
     * result!).
     * <p>
     * If we move the 2nd FILTER NOT EXISTS into the child join group, then the
     * child group is once again lifted and we get the right result.
     * <p>
     * If we disable the join order optimizer.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/515">
     *      Query with two "FILTER NOT EXISTS" expressions returns no
     *      results</a>
     */
    public void test_filter_not_exists2() throws Exception {
        
        final TestHelper h = new TestHelper(
                "filter-not-exists", // testURI,
                "filter-not-exists2.rq",// queryFileURL
                "filter-not-exists.ttl",// dataFileURL
                "filter-not-exists.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                );
        
        /**
         * Build the expected AST.
         * 
         * <pre>
         * QueryType: SELECT
         * SELECT DISTINCT ( VarNode(ar) AS VarNode(ar) )
         * 
         *   JoinGroupNode {
         *   
         *     QueryType: ASK
         *     SELECT VarNode(ar) VarNode(-exists-2)[anonymous]
         *       JoinGroupNode {
         *         StatementPatternNode(VarNode(ar), ConstantNode(TermId(8U)[os:prop/analysis/refEntity]), ConstantNode(TermId(7U)[os:elem/loc/Kriterion]), DEFAULT_CONTEXTS)
         *           com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=1
         *           com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
         *       }
         *     @askVar=-exists-2
         *     
         *     JoinGroupNode {
         *       
         *       StatementPatternNode(VarNode(ar), ConstantNode(Vocab(14)[http://www.w3.org/1999/02/22-rdf-syntax-ns#type]), ConstantNode(TermId(5U)[os:class/AnalysisResults]), DEFAULT_CONTEXTS)
         *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=3
         *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
         *       
         *       QueryType: ASK
         *       SELECT VarNode(ar) VarNode(-exists-1)[anonymous]
         *         JoinGroupNode {
         *           StatementPatternNode(VarNode(ar), ConstantNode(TermId(8U)[os:prop/analysis/refEntity]), ConstantNode(TermId(6U)[os:elem/loc/Artis]), DEFAULT_CONTEXTS)
         *             com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=2
         *             com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
         *         }
         *       @askVar=-exists-1
         *       
         *       FILTER( com.bigdata.rdf.sparql.ast.NotExistsNode(VarNode(-exists-1))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.bigdata.com/sparql-1.1-undefined-functionsnot-exists, graphPattern=JoinGroupNode, valueExpr=com.bigdata.rdf.internal.constraints.NotBOp(com.bigdata.rdf.internal.constraints.EBVBOp(-exists-1))] )
         * 
         *     } JOIN ON (ar)
         *     
         *     FILTER( com.bigdata.rdf.sparql.ast.NotExistsNode(VarNode(-exists-2))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.bigdata.com/sparql-1.1-undefined-functionsnot-exists, graphPattern=JoinGroupNode, valueExpr=com.bigdata.rdf.internal.constraints.NotBOp(com.bigdata.rdf.internal.constraints.EBVBOp(-exists-2))] )
         *     
         *   }
         * </pre>
         * 
         * TODO First build the AST which is actually being produced. Then
         * modify that AST to the correct target AST. Finally, backtrack on the
         * filter attachment and optimizers until we fix the problem (I think
         * that it is a filter attachment/ordering problem). We might be able to
         * extract a simpler unit test at that point.
         */
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final BigdataValueFactory f = h.getTripleStore().getValueFactory();
            
            final BigdataURI refEntity = f
                    .createURI("os:prop/analysis/refEntity");

            final BigdataURI Kriterion = f.createURI("os:elem/loc/Kriterion");
            
            final BigdataURI AnalysisResults = f
                    .createURI("os:class/AnalysisResults");
            
            final BigdataURI Artis = f.createURI("os:elem/loc/Artis");
            
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            
            final BigdataValue[] values = new BigdataValue[] { refEntity,
                    Kriterion, AnalysisResults, Artis, rdfType };

            // Resolve IVs (declared when the data were loaded)
            h.getTripleStore().getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);

            final GlobalAnnotations globals = new GlobalAnnotations(store
                    .getLexiconRelation().getNamespace(), store.getTimestamp());

            final VarNode askVar1 = new VarNode("-exists-1");
            final VarNode askVar2 = new VarNode("-exists-2");
            {
                askVar1.setAnonymous(true);
                askVar2.setAnonymous(true);
            }
            
            // Prefix declarations.
            {
                expected.setPrefixDecls((Map)Collections.emptyMap());
            }
            
            // Top-level projection.
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("ar"), new VarNode("ar")));
            }

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            /**
             * <pre>
             *     QueryType: ASK
             *     SELECT VarNode(ar) VarNode(-exists-2)[anonymous]
             *       JoinGroupNode {
             *         StatementPatternNode(VarNode(ar), ConstantNode(TermId(8U)[os:prop/analysis/refEntity]), ConstantNode(TermId(7U)[os:elem/loc/Kriterion]), DEFAULT_CONTEXTS)
             *           com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=1
             *           com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
             *       }
             *     @askVar=-exists-2
             * </pre>
             */
            final SubqueryRoot notExistsSubquery2;
            {
                
                notExistsSubquery2 = new SubqueryRoot(QueryType.ASK);
                whereClause.addChild(notExistsSubquery2);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    notExistsSubquery2.setProjection(projection);
                    projection.addProjectionExpression(new AssignmentNode(
                            new VarNode("ar"), new VarNode("ar")));
                    projection.addProjectionExpression(new AssignmentNode(
                            askVar2, askVar2));
                }
                
                {
                    final JoinGroupNode whereClause2 = new JoinGroupNode();
                    notExistsSubquery2.setWhereClause(whereClause2);
                    
                    final StatementPatternNode sp2 = new StatementPatternNode(new VarNode(
                            "ar"), new ConstantNode(refEntity.getIV()),
                            new ConstantNode(Kriterion.getIV()));

                    whereClause2.addChild(sp2);
                    
                    sp2.setProperty(
                            "com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality",
                            1L);
                    sp2.setProperty(
                            "com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex",
                            SPOKeyOrder.POCS);

                }
                
                notExistsSubquery2.setAskVar(askVar2.getValueExpression());

            } // not-exists-2

            /**
             * <pre>
             *     JoinGroupNode {
             *       
             *       StatementPatternNode(VarNode(ar), ConstantNode(Vocab(14)[http://www.w3.org/1999/02/22-rdf-syntax-ns#type]), ConstantNode(TermId(5U)[os:class/AnalysisResults]), DEFAULT_CONTEXTS)
             *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=3
             *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
             *       
             *       QueryType: ASK
             *       SELECT VarNode(ar) VarNode(-exists-1)[anonymous]
             *         JoinGroupNode {
             *           StatementPatternNode(VarNode(ar), ConstantNode(TermId(8U)[os:prop/analysis/refEntity]), ConstantNode(TermId(6U)[os:elem/loc/Artis]), DEFAULT_CONTEXTS)
             *             com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=2
             *             com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
             *         }
             *       @askVar=-exists-1
             *       
             *       FILTER( com.bigdata.rdf.sparql.ast.NotExistsNode(VarNode(-exists-1))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.bigdata.com/sparql-1.1-undefined-functionsnot-exists, graphPattern=JoinGroupNode, valueExpr=com.bigdata.rdf.internal.constraints.NotBOp(com.bigdata.rdf.internal.constraints.EBVBOp(-exists-1))] )
             * 
             *     } JOIN ON (ar)
             * </pre>
             */
            final SubqueryRoot notExistsSubquery1;
            {

                final JoinGroupNode group = new JoinGroupNode();
                whereClause.addChild(group);

                final StatementPatternNode sp1 = new StatementPatternNode(
                        new VarNode("ar"), new ConstantNode(rdfType.getIV()),
                        new ConstantNode(AnalysisResults.getIV()));
                group.addChild(sp1);
                
                sp1.setProperty(
                        "com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality",
                        3L);
                sp1.setProperty(
                        "com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex",
                        SPOKeyOrder.POCS);

                /**
                 * <pre>
                 *       QueryType: ASK
                 *       SELECT VarNode(ar) VarNode(-exists-1)[anonymous]
                 *         JoinGroupNode {
                 *           StatementPatternNode(VarNode(ar), ConstantNode(TermId(8U)[os:prop/analysis/refEntity]), ConstantNode(TermId(6U)[os:elem/loc/Artis]), DEFAULT_CONTEXTS)
                 *             com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=2
                 *             com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=POCS
                 *         }
                 *       @askVar=-exists-1
                 * </pre>
                 */
                {
                    
                    notExistsSubquery1 = new SubqueryRoot(QueryType.ASK);
                    group.addChild(notExistsSubquery1);

                    {
                        final ProjectionNode projection = new ProjectionNode();
                        notExistsSubquery1.setProjection(projection);
                        projection.addProjectionExpression(new AssignmentNode(
                                new VarNode("ar"), new VarNode("ar")));
                        projection.addProjectionExpression(new AssignmentNode(
                                askVar1, askVar1));
                    }
                    
                    {
                        final JoinGroupNode whereClause1 = new JoinGroupNode();
                        notExistsSubquery1.setWhereClause(whereClause1);
                        
                        final StatementPatternNode sp3 = new StatementPatternNode(new VarNode(
                                "ar"), new ConstantNode(refEntity.getIV()),
                                new ConstantNode(Artis.getIV()));

                        whereClause1.addChild(sp3);
                        
                        sp3.setProperty(
                                "com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality",
                                2L);
                        sp3.setProperty(
                                "com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex",
                                SPOKeyOrder.POCS);

                    }
                    
                    notExistsSubquery1.setAskVar(askVar1.getValueExpression());
       
                } // not-exists-1

                /**
                 * <pre>
                 *       FILTER( com.bigdata.rdf.sparql.ast.NotExistsNode(VarNode(-exists-1))[
                 *       com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.bigdata.com/sparql-1.1-undefined-functionsnot-exists,
                 *       graphPattern=JoinGroupNode,
                 *       valueExpr=com.bigdata.rdf.internal.constraints.NotBOp(com.bigdata.rdf.internal.constraints.EBVBOp(-exists-1))
                 *       ] )
                 * </pre>
                 */
                {

                    @SuppressWarnings("unchecked")
                    final NotExistsNode notExistsNode1 = new NotExistsNode(
                            askVar1, notExistsSubquery1.getWhereClause());
                    
                    final FilterNode filter1 = new FilterNode(notExistsNode1);
                    
                    group.addChild(filter1);

                    AST2BOpUtility.toVE(globals, filter1.getValueExpressionNode());

                }

                /** 
                 * <pre>
                 * JOIN ON (ar)
                 * </pre>
                 */

                group.setJoinVars(new IVariable[]{Var.var("ar")});

            } // end group

            /**
             * <pre>
             *     FILTER( com.bigdata.rdf.sparql.ast.NotExistsNode(VarNode(-exists-2))[
             *     com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.bigdata.com/sparql-1.1-undefined-functionsnot-exists,
             *     graphPattern=JoinGroupNode,
             *     valueExpr=com.bigdata.rdf.internal.constraints.NotBOp(com.bigdata.rdf.internal.constraints.EBVBOp(-exists-2))
             *     ] )
             * </pre>
             */
            {
                @SuppressWarnings("unchecked")
                final NotExistsNode notExistsNode2 = new NotExistsNode(askVar2,
                        notExistsSubquery2.getWhereClause());

                final FilterNode filter2 = new FilterNode(notExistsNode2);
                
                whereClause.addChild(filter2);
                
                AST2BOpUtility.toVE(globals, filter2.getValueExpressionNode());
                
            }

        }

        // verify the query results.
        Throwable cause = null;
        try {

            // Run the query. Optimizes the AST as a side-effect.
            h.runTest();

        } catch (Throwable t) {

            // Make a note of any query failure.
            cause = t;
            
        }

        /*
         * Verify the expected AST.
         */
        {

            // Get a reference to the ASTContainer.
            final ASTContainer astContainer = h.getASTContainer();

            // verify the optimized AST against the target.
            assertSameAST(expected, astContainer.getOptimizedAST());

        }
        
        if (cause != null) {

            /*
             * If the AST was verified, then report any error from running the
             * query.
             */
            
            fail("Query failed: " + cause, cause);

        }
        
    }

}
