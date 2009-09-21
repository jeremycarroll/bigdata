/*
 * Copyright SYSTAP, LLC 2006-2009.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

/**
 * Unit tests for named graphs. Specify
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code> to
 * run this test suite.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 * 
 *          FIXME quads : "tests" do not test correctness.
 */
public class TestNamedGraphs extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestNamedGraphs.class);
    
    /**
     * 
     */
    public TestNamedGraphs() {
    }

    /**
     * @param arg0
     */
    public TestNamedGraphs(String arg0) {
        super(arg0);
    }

    /**
     * The foaf: namespace.
     */
    final String FOAF = "http://xmlns.com/foaf/0.1/";
    
    /**
     * foaf:name
     */
    final URI FOAF_NAME = new URIImpl(FOAF+"name"); 
    
    /**
     * foaf:mbox
     */
    final URI FOAF_MBOX = new URIImpl(FOAF+"mbox"); 
    
    /**
     * foaf:nick
     */
    final URI FOAF_NICK = new URIImpl(FOAF+"nick"); 
    
    /**
     * foaf:PersonalProfileDocument
     */
    final URI FOAF_PPD = new URIImpl(FOAF+"PersonalProfileDocument"); 
    
    /**
     * foaf:knows
     */
    final URI FOAF_KNOWS = new URIImpl(FOAF+"knows"); 
    
    /**
     * The dc: namespace.
     */
    final String DC = "http://purl.org/dc/elements/1.1/";
    
    /**
     * dc:publisher
     */
    final URI DC_PUBLISHER = new URIImpl(DC+"publisher"); 
    
    
    protected BindingSet createBindingSet(Binding... bindings) {
        QueryBindingSet bindingSet = new QueryBindingSet();
        if (bindings != null) {
            for (Binding b : bindings) {
                bindingSet.addBinding(b);
            }
        }
        return bindingSet;
    }
    
    protected void compare(TupleQueryResult result, Collection<BindingSet> answer) throws QueryEvaluationException {
        
        Collection<BindingSet> extraResults = new LinkedList<BindingSet>();
        Collection<BindingSet> missingResults = new LinkedList<BindingSet>();
        
        int resultCount = 0;
        while (result.hasNext()) {
            BindingSet bindingSet = result.next();
            resultCount++;
            boolean match = false;
            if(log.isInfoEnabled())
                log.info(bindingSet);
            Iterator<BindingSet> it = answer.iterator();
            while (it.hasNext()) {
                if (it.next().equals(bindingSet)) {
                    it.remove();
                    match = true;
                    break;
                }
            }
            if (match == false) {
                extraResults.add(bindingSet);
            }
        }
        missingResults = answer;

        for (BindingSet bs : extraResults) {
            if (log.isInfoEnabled()) {
                log.info("extra result: " + bs);
            }
        }
        
        for (BindingSet bs : missingResults) {
            if (log.isInfoEnabled()) {
                log.info("missing result: " + bs);
            }
        }
        
        assertTrue("extra result count: " + extraResults.size(), extraResults.size() == 0);
        assertTrue("missing result count: " + missingResults.size(), missingResults.size() == 0);
        
    }
    
    /**
     * 8.2.1 Specifying the Default Graph
     * 
     * Each FROM clause contains an IRI that indicates a graph to be used to 
     * form the default graph. This does not put the graph in as a named graph.
     * 
     * In this example, the RDF Dataset contains a single default graph and no 
     * named graphs:
     * 
     * # Default graph (stored at http://example.org/foaf/aliceFoaf)
     * @prefix  foaf:  <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT  ?name
     * FROM    <http://example.org/foaf/aliceFoaf>
     * WHERE   { ?x foaf:name ?name }
     * 
     * name
     * "Alice"
     * 
     * If a query provides more than one FROM clause, providing more than one 
     * IRI to indicate the default graph, then the default graph is based on 
     * the RDF merge of the graphs obtained from representations of the 
     * resources identified by the given IRIs.     
     */
    public void test_8_2_1() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.2.1 Specifying the Default Graph");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final URI graph = new URIImpl("http://example.org/foaf/aliceFoaf");
/**/            
            cxn.add(
                    a,
                    new URIImpl(FOAF+"name"),
                    new LiteralImpl("Alice"),
                    graph
                    );
            cxn.add(
                    a,
                    new URIImpl(FOAF+"mbox"),
                    new URIImpl("mailto:alice@work.example"),
                    graph
                    );
            cxn.commit();
/**/            
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "SELECT  ?name " +
                "FROM    <http://example.org/foaf/aliceFoaf> " +
                "WHERE   { ?x foaf:name ?name }";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("name", new LiteralImpl("Alice"))));
            
            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.shutdownAndDelete();
        }

    }
    
    /**
     * 8.2.3 Combining FROM and FROM NAMED
     * 
     * The FROM clause and FROM NAMED clause can be used in the same query.
     * 
     * # Default graph (stored at http://example.org/dft.ttl)
     * @prefix dc: <http://purl.org/dc/elements/1.1/> .
     * 
     * <http://example.org/bob>    dc:publisher  "Bob Hacker" .
     * <http://example.org/alice>  dc:publisher  "Alice Hacker" .
     * 
     * # Named graph: http://example.org/bob
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a foaf:name "Bob" .
     * _:a foaf:mbox <mailto:bob@oldcorp.example.org> .
     * 
     * # Named graph: http://example.org/alice
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a foaf:name "Alice" .
     * _:a foaf:mbox <mailto:alice@work.example.org> .
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * 
     * SELECT ?who ?g ?mbox
     * FROM <http://example.org/dft.ttl>
     * FROM NAMED <http://example.org/alice>
     * FROM NAMED <http://example.org/bob>
     * WHERE
     * {
     *    ?g dc:publisher ?who .
     *    GRAPH ?g { ?x foaf:mbox ?mbox }
     * }
     * 
     * The RDF Dataset for this query contains a default graph and two named 
     * graphs. The GRAPH keyword is described below.
     * 
     * The actions required to construct the dataset are not determined by the 
     * dataset description alone. If an IRI is given twice in a dataset 
     * description, either by using two FROM clauses, or a FROM clause and a 
     * FROM NAMED clause, then it does not assume that exactly one or 
     * exactly two attempts are made to obtain an RDF graph associated with the 
     * IRI. Therefore, no assumptions can be made about blank node identity in 
     * triples obtained from the two occurrences in the dataset description. 
     * In general, no assumptions can be made about the equivalence of the 
     * graphs. 
     */
    public void test_8_2_3() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.2.3 Combining FROM and FROM NAMED");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final URI bob = new URIImpl("http://example.org/bob");
            final URI alice = new URIImpl("http://example.org/alice");
            final URI graph = new URIImpl("http://example.org/dft.ttl");
            final URI aliceMbox = new URIImpl("mailto:alice@work.example.org");
            final URI bobMbox = new URIImpl("mailto:bob@oldcorp.example.org");
/**/            
            cxn.add(
                    bob,
                    DC_PUBLISHER,
                    new LiteralImpl("Bob Hacker"),
                    graph
                    );
            cxn.add(
                    alice,
                    DC_PUBLISHER,
                    new LiteralImpl("Alice Hacker"),
                    graph
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    bob
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    bobMbox,
                    bob
                    );
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    aliceMbox,
                    alice
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
/**/            
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
                "SELECT ?who ?g ?mbox " +
                "FROM <http://example.org/dft.ttl> " +
                "FROM NAMED <http://example.org/alice> " +
                "FROM NAMED <http://example.org/bob> " +
                "WHERE " +
                "{ " +
                "    ?g dc:publisher ?who . " +
                "    GRAPH ?g { ?x foaf:mbox ?mbox } " +
                "}";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results
            
            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("g", alice),
                    new BindingImpl("mbox", aliceMbox),
                    new BindingImpl("who", new LiteralImpl("Alice Hacker"))));
            answer.add(createBindingSet(
                    new BindingImpl("g", bob),
                    new BindingImpl("mbox", bobMbox),
                    new BindingImpl("who", new LiteralImpl("Bob Hacker"))));
            
            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.shutdownAndDelete();
        }

    }
    
    /**
     * 8.3.1 Accessing Graph Names
     * 
     * The following two graphs will be used in examples:
     * 
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * The query below matches the graph pattern against each of the named 
     * graphs in the dataset and forms solutions which have the src variable 
     * bound to IRIs of the graph being matched. The graph pattern is matched 
     * with the active graph being each of the named graphs in the dataset.
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?src ?bobNick
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     *   {
     *     GRAPH ?src
     *     { ?x foaf:mbox <mailto:bob@work.example> .
     *       ?x foaf:nick ?bobNick
     *     }
     *   }
     * 
     * The query result gives the name of the graphs where the information was 
     * found and the value for Bob's nick:
     * src     bobNick
     * <http://example.org/foaf/aliceFoaf>     "Bobby"
     * <http://example.org/foaf/bobFoaf>   "Robert"
     */
    public void test_8_3_1() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.3.1 Accessing Graph Names");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode z = new BNodeImpl("_:b");
            final URI alice = new URIImpl("http://example.org/foaf/aliceFoaf");
            final URI bob = new URIImpl("http://example.org/foaf/bobFoaf");
            
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    new URIImpl("mailto:alice@work.example"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_KNOWS,
                    b,
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NICK,
                    new LiteralImpl("Bobby"),
                    alice
                    );
            cxn.add(
                    b,
                    RDFS.SEEALSO,
                    bob,
                    alice
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    alice
                    );
            cxn.add(
                    z,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    bob
                    );
            cxn.add(
                    z,
                    RDFS.SEEALSO,
                    bob,
                    bob
                    );
            cxn.add(
                    z,
                    FOAF_NICK,
                    new LiteralImpl("Robert"),
                    bob
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    bob
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "SELECT ?src ?bobNick " +
                "FROM NAMED <http://example.org/foaf/bobFoaf> " +
                "FROM NAMED <http://example.org/foaf/aliceFoaf> " +
                "WHERE " +
                "  { " +
                "    GRAPH ?src " +
                "    { ?x foaf:mbox <mailto:bob@work.example> . " +
                "      ?x foaf:nick ?bobNick " +
                "    } " +
                "  }"; 
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("src", alice),
                    new BindingImpl("bobNick", new LiteralImpl("Bobby"))));
            answer.add(createBindingSet(
                    new BindingImpl("src", bob),
                    new BindingImpl("bobNick", new LiteralImpl("Robert"))));
            
            compare(result, answer);

        } finally {
            cxn.close();
            sail.shutdownAndDelete();
        }

    }
    
    /**
     * 8.3.2 Restricting by Graph IRI
     * 
     * The following two graphs will be used in examples:
     * 
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * The query can restrict the matching applied to a specific graph by 
     * supplying the graph IRI. This sets the active graph to the graph named 
     * by the IRI. This query looks for Bob's nick as given in the graph 
     * http://example.org/foaf/bobFoaf.
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX data: <http://example.org/foaf/>
     * 
     * SELECT ?nick
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     *   {
     *      GRAPH data:bobFoaf {
     *          ?x foaf:mbox <mailto:bob@work.example> .
     *          ?x foaf:nick ?nick }
     *   }
     * 
     * which yields a single solution:
     * nick
     * "Robert"
     */
    public void test_8_3_2() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.3.2 Restricting by Graph IRI");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode z = new BNodeImpl("_:b");
            final URI alice = new URIImpl("http://example.org/foaf/aliceFoaf");
            final URI bob = new URIImpl("http://example.org/foaf/bobFoaf");
            
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    new URIImpl("mailto:alice@work.example"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_KNOWS,
                    b,
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NICK,
                    new LiteralImpl("Bobby"),
                    alice
                    );
            cxn.add(
                    b,
                    RDFS.SEEALSO,
                    bob,
                    alice
                    );
            cxn.add(
                    b,
                    RDF.TYPE,
                    FOAF_PPD,
                    alice
                    );
            cxn.add(
                    z,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    bob
                    );
            cxn.add(
                    z,
                    RDFS.SEEALSO,
                    bob,
                    bob
                    );
            cxn.add(
                    z,
                    FOAF_NICK,
                    new LiteralImpl("Robert"),
                    bob
                    );
            cxn.add(
                    z,
                    RDF.TYPE,
                    FOAF_PPD,
                    bob
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "PREFIX data: <http://example.org/foaf/> " +
                "SELECT ?nick " +
                "FROM NAMED <http://example.org/foaf/aliceFoaf> " +
                "FROM NAMED <http://example.org/foaf/bobFoaf> " +
                "WHERE " +
                "  { " +
                "     GRAPH data:bobFoaf { " +
                "         ?x foaf:mbox <mailto:bob@work.example> . " +
                "         ?x foaf:nick ?nick } " +
                "  }";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                if(log.isInfoEnabled())
                    log.info(bindingSet);
            }

        } finally {
            cxn.close();
            sail.shutdownAndDelete();
        }

    }
    
    /**
     * 8.3.3 Restricting Possible Graph IRIs
     * 
     * The following two graphs will be used in examples:
     * 
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * A variable used in the GRAPH clause may also be used in another GRAPH 
     * clause or in a graph pattern matched against the default graph in the 
     * dataset.
     *
     * The query below uses the graph with IRI http://example.org/foaf/aliceFoaf 
     * to find the profile document for Bob; it then matches another pattern 
     * against that graph. The pattern in the second GRAPH clause finds the 
     * blank node (variable w) for the person with the same mail box (given by 
     * variable mbox) as found in the first GRAPH clause (variable whom), 
     * because the blank node used to match for variable whom from Alice's FOAF 
     * file is not the same as the blank node in the profile document (they are 
     * in different graphs).
     * 
     * PREFIX  data:  <http://example.org/foaf/>
     * PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>
     * PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * SELECT ?mbox ?nick ?ppd
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     * {
     *   GRAPH data:aliceFoaf
     *   {
     *     ?alice foaf:mbox <mailto:alice@work.example> ;
     *            foaf:knows ?whom .
     *     ?whom  foaf:mbox ?mbox ;
     *            rdfs:seeAlso ?ppd .
     *     ?ppd  a foaf:PersonalProfileDocument .
     *   } .
     *   GRAPH ?ppd
     *   {
     *       ?w foaf:mbox ?mbox ;
     *          foaf:nick ?nick
     *   }
     * }
     * 
     * mbox    nick    ppd
     * <mailto:bob@work.example>   "Robert"    <http://example.org/foaf/bobFoaf>
     * 
     * Any triple in Alice's FOAF file giving Bob's nick is not used to provide 
     * a nick for Bob because the pattern involving variable nick is restricted 
     * by ppd to a particular Personal Profile Document.
     */
    public void test_8_3_3() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.3.3 Restricting Possible Graph IRIs");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode z = new BNodeImpl("_:b");
            final URI alice = new URIImpl("http://example.org/foaf/aliceFoaf");
            final URI bob = new URIImpl("http://example.org/foaf/bobFoaf");
            
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    new URIImpl("mailto:alice@work.example"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_KNOWS,
                    b,
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NICK,
                    new LiteralImpl("Bobby"),
                    alice
                    );
            cxn.add(
                    b,
                    RDFS.SEEALSO,
                    bob,
                    alice
                    );
            cxn.add(
                    b,
                    RDF.TYPE,
                    FOAF_PPD,
                    alice
                    );
            cxn.add(
                    z,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    bob
                    );
            cxn.add(
                    z,
                    RDFS.SEEALSO,
                    bob,
                    bob
                    );
            cxn.add(
                    z,
                    FOAF_NICK,
                    new LiteralImpl("Robert"),
                    bob
                    );
            cxn.add(
                    z,
                    RDF.TYPE,
                    FOAF_PPD,
                    bob
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            
            String query = 
                "PREFIX  data:  <http://example.org/foaf/> " +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/> " +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#> " +
                "SELECT ?mbox ?nick ?ppd " +
                "FROM NAMED <http://example.org/foaf/aliceFoaf> " +
                "FROM NAMED <http://example.org/foaf/bobFoaf> " +
                "WHERE " +
                "{ " +
                "  GRAPH data:aliceFoaf " +
                "  { " +
                "    ?alice foaf:mbox <mailto:alice@work.example> ; " +
                "           foaf:knows ?whom . " +
                "    ?whom  foaf:mbox ?mbox ; " +
                "           rdfs:seeAlso ?ppd . " +
                "    ?ppd  a foaf:PersonalProfileDocument . " +
                "  } . " +
                "  GRAPH ?ppd " +
                "  { " +
                "      ?w foaf:mbox ?mbox ; " +
                "         foaf:nick ?nick " +
                "  } " +
                "}"; 
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                if(log.isInfoEnabled())
                    log.info(bindingSet);
            }

        } finally {
            cxn.close();
            sail.shutdownAndDelete();
        }

    }
    
}
