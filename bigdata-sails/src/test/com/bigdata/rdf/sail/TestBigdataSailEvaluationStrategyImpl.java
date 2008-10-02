/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
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
 * Created on Jan 4, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.xml.XMLWriter;

import java.io.File;
import java.io.StringWriter;
import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BNS;

/**
 * Unit tests for high-level query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBigdataSailEvaluationStrategyImpl extends TestCase2 {

    /**
     * 
     */
    public TestBigdataSailEvaluationStrategyImpl() {
        
    }

    /**
     * @param arg0
     */
    public TestBigdataSailEvaluationStrategyImpl(String arg0) {
        super(arg0);
    }

    public void test_or_equals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI property1 = new URIImpl(ns+"property1");
        
        final URI property2 = new URIImpl(ns+"property2");
        
        final Literal label = new LiteralImpl("The Label");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, property1, label));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, property2, label));
                
                cxn.commit();
                
                String query = 
                    "select ?s ?p "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s ?p \""+label.getLabel()+"\" . " +
                    "  FILTER(?p = <"+RDFS.LABEL+"> || ?p = <"+RDFS.COMMENT+"> || ?p = <"+property1+">) " +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(mike));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 1);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
    public void test_and_equals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI property1 = new URIImpl(ns+"property1");
        
        final Literal label = new LiteralImpl("The Label");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, label));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, property1, label));
                
                cxn.commit();
                
                String query = 
                    "select ?s ?type "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> ?type . " +
                    "  ?s ?p \""+label.getLabel()+"\" . " +
                    "  FILTER((?p = <"+RDFS.LABEL+"> || ?p = <"+RDFS.LABEL+">) && " +
                    "         (?type = <"+person+"> || ?type = <"+person+">)) " +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(mike));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 1);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
    public void test_and_nequals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI entity = new URIImpl(ns+"Entity");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI place = new URIImpl(ns+"Place");
        
        final URI thing = new URIImpl(ns+"Thing");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(person, RDFS.SUBCLASSOF, entity));
                
                cxn.add(new StatementImpl(place, RDFS.SUBCLASSOF, entity));
                
                cxn.add(new StatementImpl(thing, RDFS.SUBCLASSOF, entity));
                
                cxn.add(new StatementImpl(person, RDFS.LABEL, 
                        new LiteralImpl(person.getLocalName())));
                
                cxn.add(new StatementImpl(place, RDFS.LABEL, 
                        new LiteralImpl(place.getLocalName())));
                
                cxn.add(new StatementImpl(thing, RDFS.LABEL, 
                        new LiteralImpl(thing.getLocalName())));
                
                cxn.commit();
                
                String query = 
                    "select ?s ?sco ?label "+
                    "where { " +
                    "  ?s <"+RDFS.SUBCLASSOF+"> <"+entity+"> . " +
                    // "  ?s <"+RDFS.SUBCLASSOF+"> ?sco . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER(?s != <"+entity+"> && ?s != <"+person+"> && ?s != <"+place+">) " +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(thing));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 1);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
    public void test_filter_literals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI property1 = new URIImpl(ns+"property1");
        
        final URI property2 = new URIImpl(ns+"property2");
        
        final Literal label = new LiteralImpl("The Label");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, label));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, label));
                
                cxn.commit();
                
                String query = 
                    "select ?s "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER(?label = \""+label.getLabel()+"\" || ?label = \""+label.getLabel()+"\")" +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(mike) || valueOfS.equals(jane));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 2);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
    public void test_filter_regex() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, new LiteralImpl("Mike")));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, new LiteralImpl("Jane")));
                
                cxn.commit();
                
                String query = 
                    "select ?s "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER REGEX(?label, 'Mi*', 'i')" +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(mike));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 1);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
    public void test_union() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, new LiteralImpl("Mike")));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, new LiteralImpl("Jane")));
                
                cxn.commit();
                
                String query = 
                    "select distinct ?s "+
                    "where { {" +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER (?label = \"Mike\")" +
                    "} union { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER (?label = \"Jane\")" +
                    "} }";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(mike) || valueOfS.equals(jane));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 2);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
    public void test_free_text_search() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI search = new URIImpl(BNS.SEARCH); 
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI bryan = new URIImpl(ns+"Bryan");
        
        final URI person = new URIImpl(ns+"Person");
        
        final File journal = File.createTempFile("bigdata", ".jnl");
        
        journal.deleteOnExit();
        
        final Properties props = new Properties();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

        props.setProperty(AbstractTripleStore.Options.MAX_PARALLEL_SUBQUERIES,"0");
        
        final BigdataSail sail = new BigdataSail(props);
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, new LiteralImpl("Mike")));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, new LiteralImpl("Jane")));
                
                cxn.add(new StatementImpl(bryan, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(bryan, RDFS.LABEL, new LiteralImpl("Bryan")));
                
                cxn.commit();
                
                System.err.println("<mike> = " + sail.getDatabase().getTermId(mike));
                System.err.println("<jane> = " + sail.getDatabase().getTermId(jane));
                System.err.println("\"Mike\" = " + sail.getDatabase().getTermId(new LiteralImpl("Mike")));
                System.err.println("\"Jane\" = " + sail.getDatabase().getTermId(new LiteralImpl("Jane")));
                
                String query = 
                    "select ?s ?label " +
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +   // [160, 8, 164], [156, 8, 164]
                    "  ?s <"+RDFS.LABEL+"> ?label . " +       // [160, 148, 174], [156, 148, 170] 
                    "  ?label <"+search+"> \"Mi\" . " +     // [174, 0, 0]
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    try {
                        
                        int numResults = 0;
                        
                        while (result.hasNext()) {
                            
                            BindingSet bindingSet = result.next();
                           
                            Value valueOfS = bindingSet.getValue("s");
                           
                            assertTrue(valueOfS.equals(mike));

                            numResults++;
                           
                        }
                        
                        assertTrue(numResults == 1);
                       
                    } finally {
                       
                        result.close();
                       
                    }
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.shutdownAndDelete();
            
        }

    }
    
}
