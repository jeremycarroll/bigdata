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
 * Created on Oct 19, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTComplexOptionalOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTComplexOptionalOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTComplexOptionalOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTComplexOptionalOptimizer(String name) {
        super(name);
    }

    /**
     * This unit test is a based on
     * <code>bigdata-perf/CI/govtrack/queries/query10.rq</code>
     * <p>
     * Rewrite:
     * 
     * <pre>
     * SELECT ?_var1
     *  WHERE {
     *         ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
     *         OPTIONAL {
     *                 ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
     *         }.
     *         OPTIONAL {
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
     *         }.
     *         OPTIONAL {
     *                 ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
     *                 ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?_var10
     *         }
     * }
     * </pre>
     * 
     * as:
     * 
     * <pre>
     * SELECT  ?_var1
     * WITH {
     *         SELECT ?_var1 ?_var6
     *         WHERE {
     *                 ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
     *                 OPTIONAL {
     *                      ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
     *                 }.
     *         }
     * } as %_set1
     * WITH {
     *         SELECT ?_var1 ?_var4 ?_var12
     *         WHERE {
     *            INCLUDE %_set1
     *            OPTIONAL {
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
     *            }.
     *         }
     * } as %_set2
     * WITH {
     *      SELECT ?_var1 ?_var10 ?_var13
     *      WHERE {
     *         INCLUDE %_set1
     *         OPTIONAL {
     *                 ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
     *                 ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?_var10
     *         }
     *     }
     * } as %_set3
     *  WHERE {
     *         INCLUDE %_set2 .
     *         INCLUDE %_set3 JOIN ON (?_var1) .
     * }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
     * 
     *      TODO Pruning out unnecessary variables from the named subquery
     *      projections should be either encapsulated by a static analysis
     *      helper method or done by a different optimizer. Actually, a
     *      different optimizer looks interesting. It could drop variables at
     *      group/subquery boundaries but also in the middle of a group. That
     *      is a long standing issue with pruning the variables eagerly.
     */
    public void test_rewriteComplexOptional() {
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV polititian = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/Politician"));
        @SuppressWarnings("rawtypes")
        final IV name = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/name"));
        @SuppressWarnings("rawtypes")
        final IV sponsor = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/sponsor"));
        @SuppressWarnings("rawtypes")
        final IV title = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/title"));
        @SuppressWarnings("rawtypes")
        final IV N = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#N"));
        @SuppressWarnings("rawtypes")
        final IV family = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#Family"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.addProjectionVar(new VarNode("var1"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
            whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
                    new ConstantNode(a), new ConstantNode(polititian),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

            // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
            {
                final StatementPatternNode sp = new StatementPatternNode(
                        new VarNode("var1"), new ConstantNode(name),
                        new VarNode("var6"), null/* c */,
                        Scope.DEFAULT_CONTEXTS);
                sp.setSimpleOptional(true);
                whereClause.addChild(sp);
            }

            {
                final JoinGroupNode optionalGroup1 = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optionalGroup1);

                // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
                optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                        "var12"), new ConstantNode(sponsor), new VarNode(
                        "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
                
                // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
                optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                        "var12"), new ConstantNode(title), new VarNode(
                        "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
                
            }

            {
                final JoinGroupNode optionalGroup2 = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optionalGroup2);

                // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
                optionalGroup2.addChild(new StatementPatternNode(new VarNode(
                        "var1"), new ConstantNode(N), new VarNode(
                        "var13"), null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?_var10
                optionalGroup2.addChild(new StatementPatternNode(new VarNode(
                        "var13"), new ConstantNode(family), new VarNode(
                        "var10"), null/* c */, Scope.DEFAULT_CONTEXTS));

            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
            }

            final String set1 = "set1";
            final String set2 = "set2";
            final String set3 = "set3";

            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
            expected.setNamedSubqueries(namedSubqueries);

            // Required joins and simple optionals lifted into a named subquery.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set1);
                namedSubqueries.add(nsr);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                
                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
                whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
                        new ConstantNode(a), new ConstantNode(polititian),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
                {
                    final StatementPatternNode sp = new StatementPatternNode(
                            new VarNode("var1"), new ConstantNode(name),
                            new VarNode("var6"), null/* c */,
                            Scope.DEFAULT_CONTEXTS);
                    sp.setSimpleOptional(true);
                    whereClause.addChild(sp);
                }
                
            }

            // First complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set2);
                namedSubqueries.add(nsr);

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var4"));
                projection.addProjectionVar(new VarNode("var12"));
                
                whereClause.addChild(new NamedSubqueryInclude(set1));

                {
                    
                    final JoinGroupNode optionalGroup1 = new JoinGroupNode(true/* optional */);
                    whereClause.addChild(optionalGroup1);

                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                            "var12"), new ConstantNode(sponsor), new VarNode(
                            "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
                    
                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                            "var12"), new ConstantNode(title), new VarNode(
                            "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
                    
                }

            }

            // Second complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set3);
                namedSubqueries.add(nsr);

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var13"));
                projection.addProjectionVar(new VarNode("var10"));
                
                whereClause.addChild(new NamedSubqueryInclude(set1));

                {

                    final JoinGroupNode optionalGroup2 = new JoinGroupNode(true/* optional */);
                    whereClause.addChild(optionalGroup2);

                    // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
                    optionalGroup2.addChild(new StatementPatternNode(
                            new VarNode("var1"), new ConstantNode(N),
                            new VarNode("var13"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family>
                    // ?_var10
                    optionalGroup2.addChild(new StatementPatternNode(
                            new VarNode("var13"), new ConstantNode(family),
                            new VarNode("var10"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                }
            }
            
            // WHERE
            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
                whereClause.addChild(new NamedSubqueryInclude(set2));
                whereClause.addChild(new NamedSubqueryInclude(set3));
            }

        }

        final IASTOptimizer rewriter = new ASTSparql11SubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);
        
    }
    
}
