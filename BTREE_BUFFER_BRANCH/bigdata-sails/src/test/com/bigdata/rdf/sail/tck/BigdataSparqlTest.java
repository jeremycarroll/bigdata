/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2008.
 *
 * Licensed under the Open Software License version 3.0.
 */
package com.bigdata.rdf.sail.tck;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import junit.framework.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.dataset.DatasetRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.sail.Sail;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSail.Options;

public class BigdataSparqlTest extends SPARQLQueryTest {

	static Set<String> sameTermMatching = new HashSet<String>(asList("\"open-eq-01\""));

	static Set<String> lexicalPreversed = new HashSet<String>(asList("\"Basic - Term 8\"", "\"str-1\"",
			"\"str-2\"", "\"sameTerm-simple\"", "\"sameTerm-eq\"", "\"sameTerm-not-eq\"",
			"\"Equality 1-1 -- graph\"", "\"Equality 1-2 -- graph\"", "\"Numbers: Distinct\"",
			"\"All: Distinct\""));

	static Set<String> noReducing = new HashSet<String>(asList("\"SELECT REDUCED *\"",
			"\"SELECT REDUCED ?x with strings\""));

	public static Test suite()
		throws Exception
	{
		return ManifestTest.suite(new Factory() {

			public SPARQLQueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
					String resultFileURL, Dataset dataSet)
			{
/*			    
				if (lexicalPreversed.contains(name))
					return null;
				if (sameTermMatching.contains(name))
					return null;
				if (noReducing.contains(name))
					return null;
*/					
				return new BigdataSparqlTest(testURI, name, queryFileURL, resultFileURL, dataSet);
			}
		});
	}

	public BigdataSparqlTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet)
	{
		super(testURI, name, queryFileURL, resultFileURL, dataSet);
	}

	@Override
	protected Repository newRepository()
		throws RepositoryException
	{
        File journal = BigdataStoreTest.createTempFile();
        
        Properties props = new Properties();
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        props.setProperty(Options.QUADS, "true");
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        BigdataSail sail = new BigdataSail(props);
        return new BigdataSailRepository(sail);
	}

	@Override
    protected Repository createRepository()
        throws Exception
    {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }

}
