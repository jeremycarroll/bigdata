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
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.parser.sparql.FOAF;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for translating queries which use subquery constructions,
 * including {@link SubqueryRoot}, {@link NamedSubqueryRoot} and
 * {@link NamedSubqueryInclude} and {@link ExistsNode}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestGroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 */
public class TestSubqueryPatterns extends
        AbstractBigdataExprBuilderTestCase {

    /**
     * 
     */
    public TestSubqueryPatterns() {
    }

    /**
     * @param name
     */
    public TestSubqueryPatterns(String name) {
        super(name);
    }
    
    /**
     * Unit test for simple subquery without anything else in the outer join
     * group.
     * 
     * <pre>
     * SELECT ?s where {{SELECT ?s where {?s ?p ?o}}}
     * </pre>
     * 
     * Note: This requires recursion back in through the
     * {@link BigdataExprBuilder}.
     */
    public void test_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where { {select ?s where { ?s ?p ?o  } } }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                subSelect = new SubqueryRoot(QueryType.SELECT);
//                whereClause.addChild(subSelect);

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("s"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
   
    /**
     * Unit test for simple subquery joined with a triple pattern in the outer
     * join group.
     * 
     * <pre>
     * SELECT ?s where { ?s ?x ?o . {SELECT ?x where {?x ?p ?x}}}
     * </pre>
     */
    public void test_triplePattern_join_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s " //
                + "where {"//
                + " ?s ?x ?o "
                + " {"//
                + "   select ?x where { ?x ?p ?x }" //
                +"  }"//
                + "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(
                                new VarNode("s"),
                                new VarNode("x"),
                                new VarNode("o"),
                                null/*c*/,
                                StatementPattern.Scope.DEFAULT_CONTEXTS
                                ));
                
                subSelect = new SubqueryRoot(QueryType.SELECT);
                
//                whereClause.addChild(subSelect);
                
                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("x"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("x"), new VarNode("p"), new VarNode("x"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    
    /**
     * Unit test for simple subquery joined with a bind.
     * 
     * <pre>
     * SELECT ?s where { bind(<http://www.bigdata.com> as ?o) { SELECT ?s where {?s ?p ?o} } }
     * </pre>
     * 
     * Note: This requires recursion back in through the
     * {@link BigdataExprBuilder}.
     */
    public void test_bind_join_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql//
              = "select ?s" +//
        		" where {" +//
        		"   bind( <http://www.bigdata.com> as ?o )" +//
        		"   {" +//
        		"     select ?s where { ?s ?p ?o  }" +//
        		"   }" +//
        		"}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {

                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new AssignmentNode(new VarNode("o"),
                        new ConstantNode(makeIV(valueFactory
                                .createURI("http://www.bigdata.com")))));

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                
                subSelect = new SubqueryRoot(QueryType.SELECT);
                wrapperGroup.addChild(subSelect);
                
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("s"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
   
    /**
     * Unit test for sub-SubSelect. There is a top-level query. It uses a
     * subquery. The subquery uses a subquery. The purpose of this is to test
     * for the correct nesting of the generated AST.
     */
    public void test_subSubSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql//
              = "select ?s \n" +//
                " where { \n" +//
                "   bind( <http://www.bigdata.com> as ?o ) \n" +//
                "   { \n" +//
                "     select ?s \n" +//
                "      where { \n" +//
                "        ?s ?p ?o . \n" +//
                "        { select ?o { bind( 12 as ?o ) } } \n" +//
                "      } \n" +//
                "   }\n" +//
                "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect1;
        final SubqueryRoot subSelect2;
        {

            final VarNode s = new VarNode("s");
            final VarNode p = new VarNode("p");
            final VarNode o = new VarNode("o");
            
            final ConstantNode const1 = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com")));
            
            final ConstantNode const2 = new ConstantNode(
                    makeIV(valueFactory.createLiteral("12", XSD.INTEGER)));
            
            {

                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(s);
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new AssignmentNode(o, const1));

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                
                subSelect1 = new SubqueryRoot(QueryType.SELECT);
                wrapperGroup.addChild(subSelect1);
                
            }

            // subSelect2
            {
                
                subSelect2 = new SubqueryRoot(QueryType.SELECT);
                
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(o);
                subSelect2.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                subSelect2.setWhereClause(whereClause);

                whereClause.addChild(new AssignmentNode(o, const2));

            }
            
            // subSelect1
            {

                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(s);
                subSelect1.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                subSelect1.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(s, p, o,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                wrapperGroup.addChild(subSelect2);
                whereClause.addChild(wrapperGroup);
                
            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for NOT EXISTS.
     * 
     * <pre>
     * PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
     * PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> 
     * 
     * SELECT ?person
     * WHERE 
     * {
     *     ?person rdf:type  foaf:Person .
     *     FILTER NOT EXISTS { ?person foaf:name ?name }
     * }
     * </pre>
     * 
     * TODO Also unit test EXISTS.
     */
    public void test_not_exists() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = ""//
                + "PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"//
                + "PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> \n"//
                + "SELECT ?person \n"//
                + " WHERE { \n"//
                + "       ?person rdf:type  foaf:Person . \n"//
                + "       FILTER NOT EXISTS { ?person foaf:name ?name } \n"//
                + "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode rdfType = new ConstantNode(
                    makeIV(valueFactory.createURI(RDF.TYPE.stringValue())));

            final ConstantNode foafPerson = new ConstantNode(
                    makeIV(valueFactory.createURI(FOAF.PERSON.stringValue())));

            final ConstantNode foafName = new ConstantNode(
                    makeIV(valueFactory.createURI(FOAF.NAME.stringValue())));

            final VarNode person = new VarNode("person");

            final VarNode name = new VarNode("name");

            final VarNode anonvar = mockAnonVar("-exists-1");

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(person);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(person, rdfType,
                    foafPerson, null/* c */, Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode existsPattern = new JoinGroupNode();
            existsPattern.addChild(new StatementPatternNode(person, foafName,
                    name, null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new NotExistsNode(lex, anonvar,
                    existsPattern)));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

}
