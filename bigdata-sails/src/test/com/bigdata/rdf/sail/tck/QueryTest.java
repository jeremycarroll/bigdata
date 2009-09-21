/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008.
 *
 * Licensed under the Open Software License version 3.0.
 */
package com.bigdata.rdf.sail.tck;

import static org.openrdf.query.QueryLanguage.SPARQL;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSail.Options;

public class QueryTest extends TestCase {

	private Repository repository;

	private RepositoryConnection con;

	private URI subj1 = new URIImpl("urn:test:subj1");

	private URI pred1 = new URIImpl("urn:test:pred1");

	private URI pred2 = new URIImpl("urn:test:pred2");

	private URI pred3 = new URIImpl("urn:test:pred3");

	private URI obj1 = new URIImpl("urn:test:obj1");

	private URI obj2 = new URIImpl("urn:test:obj2");

	private URI obj3 = new URIImpl("urn:test:obj3");

	private URI obj4 = new URIImpl("urn:test:obj4");

	public void setUp()
		throws Exception
	{
		repository = createRepository();
		con = repository.getConnection();
	}

	public Repository createRepository()
		throws RepositoryException
	{
        File journal = BigdataStoreTest.createTempFile();
        
        Properties props = new Properties();
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        props.setProperty(Options.QUADS, "true");
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        BigdataSail sail = new BigdataSail(props);
        repository = new BigdataSailRepository(sail);
        repository.initialize();
        
		return repository;
	}

	public void tearDown()
		throws Exception
	{
		con.close();
		repository.shutDown();
	}

	public void testSelect()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		con.add(subj1, pred2, obj2);
		con.add(subj1, pred3, obj3);
		List<Value> list = execute("s", "SELECT ?s WHERE { ?s ?p ?o }");
		assertEquals(3, list.size());
		assertEquals(1, new HashSet<Value>(list).size());
		assertTrue(list.contains(subj1));
	}

	public void testSelectReduce()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		con.add(subj1, pred2, obj2);
		con.add(subj1, pred3, obj3);
		List<Value> list = execute("s", "SELECT REDUCED ?s WHERE { ?s ?p ?o }");
		assertEquals(1, list.size());
		assertEquals(1, new HashSet<Value>(list).size());
		assertTrue(list.contains(subj1));
	}

	public void testSelectDistinct()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		con.add(subj1, pred2, obj2);
		con.add(subj1, pred3, obj3);
		List<Value> list = execute("s", "SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
		assertEquals(1, list.size());
		assertEquals(1, new HashSet<Value>(list).size());
		assertTrue(list.contains(subj1));
	}

	public void testOptionalJoin()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		con.add(subj1, pred2, obj2);
		con.add(subj1, pred3, obj3);
		List<Value> list = execute("o2",
				"SELECT DISTINCT ?o1 ?o2 WHERE { ?s <urn:test:pred1> ?o1  OPTIONAL { ?s <urn:test:pred2> ?o2 } }");
		assertEquals(1, list.size());
		assertEquals(Arrays.asList(obj2), list);
	}

	public void testTwoOptionalJoin()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		con.add(subj1, pred2, obj2);
		con.add(subj1, pred2, obj4);
		con.add(subj1, pred3, obj3);
		List<Value> list = execute(
				"o2",
				"SELECT DISTINCT ?o1 ?o2 WHERE { ?s <urn:test:pred1> ?o1  OPTIONAL { ?s <urn:test:pred2> ?o2 } OPTIONAL { ?s <urn:test:pred2> ?o2; <urn:test:pred3> ?o1 } }");
		assertEquals(2, list.size());
		assertEquals(Arrays.asList(obj2, obj4), list);
	}

	public void testFilterReduced()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		List<Value> list = execute("o1",
				"SELECT REDUCED ?o1 WHERE { ?s <urn:test:pred1> ?o1  FILTER bound(?o1) }");
		assertEquals(1, list.size());
		assertEquals(Arrays.asList(obj1), list);
	}

	public void testDescribe()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		List<Statement> list = execute("DESCRIBE ?s WHERE { ?s <urn:test:pred1> ?o1  FILTER bound(?o1) }");
		assertEquals(1, list.size());
	}

	public void testConstruct1()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		List<Statement> list = execute("CONSTRUCT { ?s <urn:test:pred1> ?o1 } WHERE { ?s <urn:test:pred1> ?o1  FILTER bound(?o1) }");
		assertEquals(1, list.size());
	}

	public void testConstruct2()
		throws Exception
	{
		con.add(subj1, pred1, obj1);
		List<Statement> list = execute("CONSTRUCT { ?s <urn:test:pred1> ?o1 ; <urn:test:pred2> ?o1 } WHERE { ?s <urn:test:pred1> ?o1  FILTER bound(?o1) }");
		assertEquals(2, list.size());
	}

	private List<Value> execute(String name, String qry)
		throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		TupleQuery query = con.prepareTupleQuery(SPARQL, qry);
		TupleQueryResult result = query.evaluate();
		List<Value> list = new ArrayList<Value>();
		while (result.hasNext()) {
			list.add(result.next().getValue(name));
		}
		return list;
	}

	private List<Statement> execute(String qry)
		throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		GraphQuery query = con.prepareGraphQuery(SPARQL, qry);
		GraphQueryResult result = query.evaluate();
		List<Statement> list = new ArrayList<Statement>();
		while (result.hasNext()) {
			list.add(result.next());
		}
		return list;
	}
}
