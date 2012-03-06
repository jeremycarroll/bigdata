/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 6, 2012
 */
package com.bigdata.rdf.sail.webapp;

import info.aduna.io.IOUtil;
import info.aduna.iteration.Iterations;
import info.aduna.text.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.dawg.DAWGTestResultSetUtil;
import org.openrdf.query.impl.MutableTupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.parser.sparql.FOAF;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.SailException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;

/**
 * Proxied test suite for SPARQL 1.1 Federated Query. In general, each test
 * loads some data into the KB and then issues a federated query. Many of the
 * test cases were imported from openrdf.
 * 
 * @param <S>
 */
public class TestFederatedQuery<S extends IIndexManager> extends
        AbstractNanoSparqlServerTestCase<S> {

    public TestFederatedQuery() {

    }

    public TestFederatedQuery(final String name) {

        super(name);

    }

    /**
     * A local repository (NOT exposed via HTTP).
     */
    private BigdataSail localSail;
    
    /**
     * A {@link Repository} view of the {@link #localSail}.
     */
    private BigdataSailRepository localRepository;
    
    @Override
    public void setUp() throws Exception {
        
        super.setUp();
        
        final Properties p = getProperties();
       
        p.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE, "true");
        
        localSail = new BigdataSail(p);
//        localSail.initialize();
        
        localRepository = new BigdataSailRepository(localSail);
        localRepository.initialize();
        
    }

    @Override
    public void tearDown() throws Exception {

        if (localSail != null) {
            
            localSail.__tearDownUnitTest();
         
            localSail = null;
            localRepository = null;
            
        }
        
        super.tearDown();
        
    }
    
    private String getNamespace(final int i) {
        
        if (i < 1)
            throw new IllegalArgumentException();
        
        return namespace + "_" + i;
        
    }
    
    /**
     * Get the repository url, initialized repositories are called
     * 
     * endpoint1 endpoint2 .. endpoint%MAX_ENDPOINTS%
     * 
     * @param i
     *            the index of the repository, starting with 1
     * @return
     */
    protected String getRepositoryUrl(int i) {

        return getRepositoryUrlBase() + i;

    }
    
    protected String getRepositoryUrlBase() {
        
        return m_serviceURL + requestPath + "/namespace/" + namespace + "_";
        
    }
    
    /**
     * Get the repository, initialized repositories are called
     * <pre>
     * endpoint1
     * endpoint2
     * ..
     * endpoint%MAX_ENDPOINTS%
     * </pre>
     * @param i the index of the repository, starting with 1
     * @return
     * @throws SailException 
     * @throws RepositoryException 
     */
    public BigdataSailRepository getRepository(final int i)
            throws SailException, RepositoryException {

        final String ns = getNamespace(i);

        BigdataSail sail = getSail(ns);

        if (sail == null) {

            sail = new BigdataSail(createTripleStore(getIndexManager(), ns,
                    getProperties()));

        }

        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        
        repo.initialize();
        
        return repo;
        
    }

    /**
     * The <code>local</code> repository.
     * @return
     */
    public BigdataSailRepository getRepository() {
        
        return localRepository;
        
    }
    
    /**
     * Prepare a particular test, and load the specified data.
     * 
     * Note: the repositories are cleared before loading data
     * 
     * @param localData
     *            a local data file that is added to local repository, use null
     *            if there is no local data
     * @param endpointData
     *            a list of endpoint data files, dataFile at index is loaded to
     *            endpoint%i%, use empty list for no remote data
     * @throws Exception
     */
    protected void prepareTest(final String localData,
            final List<String> endpointData) throws Exception {

//        if (endpointData.size() > MAX_ENDPOINTS)
//            throw new RuntimeException("MAX_ENDPOINTs to low, "
//                    + endpointData.size()
//                    + " repositories needed. Adjust configuration");

        if (localData != null) {
            
            final BigdataSailRepository repo = getRepository();
                    
            if (log.isInfoEnabled())
                log.info("Loading: " + localData + " into local repository as "
                        + repo);

            loadDataSet(repo, localData);
            
        }

        int i = 1; // endpoint id, start with 1
        for (String s : endpointData) {

            final BigdataSailRepository repo = getRepository(i);

            if (log.isInfoEnabled())
                log.info("Loading: " + s + " into " + getRepositoryUrl(i)
                        + " as " + repo);

            loadDataSet(repo, s);
            
            i++;
            
        }

    }
    
    /**
     * Load a dataset.
     * 
     * @param rep
     * @param datasetFile
     */
    protected void loadDataSet(final Repository rep, final String datasetFile)
            throws RDFParseException, RepositoryException, IOException {

        final InputStream dataset = TestFederatedQuery.class
                .getResourceAsStream(datasetFile);

        if (dataset == null)
            throw new IllegalArgumentException("Datasetfile not found: "
                    + datasetFile);

        try {

            RepositoryConnection con = rep.getConnection();
            try {
                con.setAutoCommit(false);
                // con.clear();
                con.add(dataset, ""/* baseURI */,
                        RDFFormat.forFileName(datasetFile));
                con.commit();
            } finally {
                con.close();
            }
        } finally {
            dataset.close();
        }
        
    }

    public void testSimpleServiceQuery() throws Exception {

        // test setup
        final String EX_NS = "http://example.org/";
        final ValueFactory f = getRepository().getValueFactory();
        final URI bob = f.createURI(EX_NS, "bob");
        final URI alice = f.createURI(EX_NS, "alice");
        final URI william = f.createURI(EX_NS, "william");

        // clears the repository and adds new data
        prepareTest(PREFIX+"simple-default-graph.ttl",
                Arrays.asList(PREFIX+"simple.ttl"));
            
        final StringBuilder qb = new StringBuilder();
        qb.append(" SELECT * \n"); 
        qb.append(" WHERE { \n");
        qb.append("     SERVICE <" + getRepositoryUrl(1) + "> { \n");
        qb.append("             ?X <"   + FOAF.NAME + "> ?Y \n ");
        qb.append("     } \n ");
        qb.append("     ?X a <" + FOAF.PERSON + "> . \n");
        qb.append(" } \n");

        final BigdataSailRepositoryConnection conn = localRepository
                .getConnection();
        try {

            final TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL,
                    qb.toString());

            final TupleQueryResult tqr = tq.evaluate();

            assertNotNull(tqr);
            assertTrue("No solutions.", tqr.hasNext());

            int count = 0;
            while (tqr.hasNext()) {
                final BindingSet bs = tqr.next();
                count++;

                final Value x = bs.getValue("X");
                final Value y = bs.getValue("Y");

                assertFalse(william.equals(x));

                assertTrue(bob.equals(x) || alice.equals(x));
                if (bob.equals(x)) {
                    f.createLiteral("Bob").equals(y);
                } else if (alice.equals(x)) {
                    f.createLiteral("Alice").equals(y);
                }
            }

            assertEquals(2, count);

//        } catch (MalformedQueryException e) {
//            fail(e.getMessage());
//        } catch (QueryEvaluationException e) {
//            fail(e.getMessage());
        } finally {
            conn.close();
        }

    }

    /**
     * The openrdf services test suite data.
     */
    private static final String PREFIX = "openrdf-service/";
    
//  @Test
    public void test1() throws Exception{
        prepareTest(PREFIX+"data01.ttl", Arrays.asList(PREFIX+"data01endpoint.ttl"));
        execute(PREFIX+"service01.rq", PREFIX+"service01.srx", false);          
    }

//  @Test
    public void test2() throws Exception {      
        prepareTest(null, Arrays.asList(PREFIX+"data02endpoint1.ttl", PREFIX+"data02endpoint2.ttl"));
        execute(PREFIX+"service02.rq", PREFIX+"service02.srx", false);          
    }
    
    
//  @Test
    public void test3() throws Exception {      
        prepareTest(null, Arrays.asList(PREFIX+"data03endpoint1.ttl", PREFIX+"data03endpoint2.ttl"));
        execute(PREFIX+"service03.rq", PREFIX+"service03.srx", false);  
    }
    
//  @Test
    public void test4() throws Exception {      
        prepareTest(PREFIX+"data04.ttl", Arrays.asList(PREFIX+"data04endpoint.ttl"));
        execute(PREFIX+"service04.rq", PREFIX+"service04.srx", false);          
    }
    
//  @Test
    public void test5() throws Exception {      
        prepareTest(PREFIX+"data05.ttl", Arrays.asList(PREFIX+"data05endpoint1.ttl", PREFIX+"data05endpoint2.ttl"));
        execute(PREFIX+"service05.rq", PREFIX+"service05.srx", false);          
    }
    
//  @Test
    public void test6() throws Exception {      
        prepareTest(null, Arrays.asList(PREFIX+"data06endpoint1.ttl"));
        execute(PREFIX+"service06.rq", PREFIX+"service06.srx", false);          
    }
    
//  @Test
    public void test7() throws Exception {      
        // clears the repository and adds new data + execute
        prepareTest(PREFIX+"data07.ttl", Collections.<String>emptyList());
        execute(PREFIX+"service07.rq", PREFIX+"service07.srx", false);          
    }
    
//  @Test
    public void test8() throws Exception {
        /* test where the SERVICE expression is to be evaluated as ASK request */
        prepareTest(PREFIX+"data08.ttl", Arrays.asList(PREFIX+"data08endpoint.ttl"));
        execute(PREFIX+"service08.rq", PREFIX+"service08.srx", false);          
    }   
    
//  @Test
    public void test9() throws Exception {
        /* test where the service endpoint is bound at runtime through BIND */
        prepareTest(null, Arrays.asList(PREFIX+"data09endpoint.ttl"));
        execute(PREFIX+"service09.rq", PREFIX+"service09.srx", false);          
    }
    
//  @Test
    public void test10() throws Exception {
        /* test how we deal with blank node */
        prepareTest(PREFIX+"data10.ttl", Arrays.asList(PREFIX+"data10endpoint.ttl"));
        execute(PREFIX+"service10.rq", PREFIX+"service10.srx", false);          
    }
    
//  @Test
    public void test11() throws Exception {
        /* test vectored join with more intermediate results */
        // clears the repository and adds new data + execute
        prepareTest(PREFIX+"data11.ttl", Arrays.asList(PREFIX+"data11endpoint.ttl"));
        execute(PREFIX+"service11.rq", PREFIX+"service11.srx", false);      
    }
    
    /**
     * This is a manual test to see the Fallback in action. Query asks
     * DBpedia, which does not support BINDINGS
     * 
     * @throws Exception
     */
    public void test12() throws Exception {
        /*
         * TODO Enable once I debug the service ASTs. It was running this query
         * multiple times (or perhaps without appropriate constraints) for some
         * reason and there is no reason to put that load onto dbPedia.
         */
        fail("Enable test");
        /* test vectored join with more intermediate results */
        // clears the repository and adds new data + execute
        prepareTest(PREFIX+"data12.ttl", Collections.<String>emptyList());
        execute(PREFIX+"service12.rq", PREFIX+"service12.srx", false);      
    }
    
    public void test13() throws Exception {
        /* test for bug SES-899: cross product is required */
        prepareTest(null, Arrays.asList(PREFIX+"data13.ttl"));
        execute(PREFIX+"service13.rq", PREFIX+"service13.srx", false);              
    }
    
    public void testEmptyServiceBlock() throws Exception {
        /* test for bug SES-900: nullpointer for empty service block */
        prepareTest(null, Arrays.asList(PREFIX+"data13.ttl"));
        execute(PREFIX+"service14.rq", PREFIX+"service14.srx", false);  
    }
    
    /**
     * Execute a testcase, both queryFile and expectedResultFile must be files 
     * located on the class path.
     * 
     * @param queryFile
     * @param expectedResultFile
     * @param checkOrder
     * @throws Exception
     */
    private void execute(final String queryFile,
            final String expectedResultFile, final boolean checkOrder)
            throws Exception {
        
        RepositoryConnection conn = localRepository.getConnection();
        String queryString = readQueryString(queryFile);

        /*
         * Replace the constants in the query strings with the actual service
         * end point.
         */
        queryString = queryString.replace(
                "http://localhost:18080/openrdf/repositories/endpoint",
                getRepositoryUrlBase());
        
        try {
            Query query = conn.prepareQuery(QueryLanguage.SPARQL, queryString);
            
            if (query instanceof TupleQuery) {
                TupleQueryResult queryResult = ((TupleQuery)query).evaluate();
    
                TupleQueryResult expectedResult = readExpectedTupleQueryResult(expectedResultFile);
                
                compareTupleQueryResults(queryResult, expectedResult, checkOrder);
    
            } else if (query instanceof GraphQuery) {
                GraphQueryResult gqr = ((GraphQuery)query).evaluate();
                Set<Statement> queryResult = Iterations.asSet(gqr);
    
                Set<Statement> expectedResult = readExpectedGraphQueryResult(expectedResultFile);
    
                compareGraphs(queryResult, expectedResult);
                
            } else if (query instanceof BooleanQuery) {
                // TODO implement if needed
                throw new RuntimeException("Not yet supported " + query.getClass());
            }
            else {
                throw new RuntimeException("Unexpected query type: " + query.getClass());
            }
        } finally {
            conn.close();
        }
    }
    
    /**
     * Read the query string from the specified resource
     * 
     * @param queryResource
     * @return
     * @throws RepositoryException
     * @throws IOException
     */
    private String readQueryString(String queryResource) throws RepositoryException, IOException {
        InputStream stream = TestFederatedQuery.class.getResourceAsStream(queryResource);
        try {
            return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
        } finally {
            stream.close();
        }
    }
    
    /**
     * Read the expected tuple query result from the specified resource
     * 
     * @param queryResource
     * @return
     * @throws RepositoryException
     * @throws IOException
     */
    private TupleQueryResult readExpectedTupleQueryResult(String resultFile)    throws Exception
    {
        TupleQueryResultFormat tqrFormat = QueryResultIO.getParserFormatForFileName(resultFile);
    
        if (tqrFormat != null) {
            InputStream in = TestFederatedQuery.class.getResourceAsStream(resultFile);
            try {
                TupleQueryResultParser parser = QueryResultIO.createParser(tqrFormat);
                parser.setValueFactory(ValueFactoryImpl.getInstance());
    
                TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();
                parser.setTupleQueryResultHandler(qrBuilder);
    
                parser.parse(in);
                return qrBuilder.getQueryResult();
            }
            finally {
                in.close();
            }
        }
        else {
            Set<Statement> resultGraph = readExpectedGraphQueryResult(resultFile);
            return DAWGTestResultSetUtil.toTupleQueryResult(resultGraph);
        }
    }
    
    /**
     * Read the expected graph query result from the specified resource
     * 
     * @param resultFile
     * @return
     * @throws Exception
     */
    private Set<Statement> readExpectedGraphQueryResult(String resultFile) throws Exception
    {
        RDFFormat rdfFormat = Rio.getParserFormatForFileName(resultFile);
    
        if (rdfFormat != null) {
            RDFParser parser = Rio.createParser(rdfFormat);
            parser.setDatatypeHandling(DatatypeHandling.IGNORE);
            parser.setPreserveBNodeIDs(true);
            parser.setValueFactory(ValueFactoryImpl.getInstance());
    
            Set<Statement> result = new LinkedHashSet<Statement>();
            parser.setRDFHandler(new StatementCollector(result));
    
            InputStream in = TestFederatedQuery.class.getResourceAsStream(resultFile);
            try {
                parser.parse(in, null);     // TODO check
            }
            finally {
                in.close();
            }
    
            return result;
        }
        else {
            throw new RuntimeException("Unable to determine file type of results file");
        }
    }

    /**
     * Compare two tuple query results
     * 
     * @param queryResult
     * @param expectedResult
     * @param checkOrder
     * @throws Exception
     * 
     * FIXME Use the code from {@link AbstractDataDrivenSPARQLTestCase}
     */
    private void compareTupleQueryResults(TupleQueryResult queryResult, TupleQueryResult expectedResult, boolean checkOrder)
        throws Exception
    {
        // Create MutableTupleQueryResult to be able to re-iterate over the
        // results
        MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
        MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);
    
        boolean resultsEqual;
        
        resultsEqual = QueryResultUtil.equals(queryResultTable, expectedResultTable);
        
        if (checkOrder) {
            // also check the order in which solutions occur.
            queryResultTable.beforeFirst();
            expectedResultTable.beforeFirst();

            while (queryResultTable.hasNext()) {
                BindingSet bs = queryResultTable.next();
                BindingSet expectedBs = expectedResultTable.next();
                
                if (! bs.equals(expectedBs)) {
                    resultsEqual = false;
                    break;
                }
            }
        }
        
    
        if (!resultsEqual) {
            queryResultTable.beforeFirst();
            expectedResultTable.beforeFirst();
    
            /*
             * StringBuilder message = new StringBuilder(128);
             * message.append("\n============ "); message.append(getName());
             * message.append(" =======================\n");
             * message.append("Expected result: \n"); while
             * (expectedResultTable.hasNext()) {
             * message.append(expectedResultTable.next()); message.append("\n"); }
             * message.append("============="); StringUtil.appendN('=',
             * getName().length(), message);
             * message.append("========================\n"); message.append("Query
             * result: \n"); while (queryResultTable.hasNext()) {
             * message.append(queryResultTable.next()); message.append("\n"); }
             * message.append("============="); StringUtil.appendN('=',
             * getName().length(), message);
             * message.append("========================\n");
             */
    
            List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
            
            List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);
    
            List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
            missingBindings.removeAll(queryBindings);
    
            List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
            unexpectedBindings.removeAll(expectedBindings);
    
            StringBuilder message = new StringBuilder(128);
            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");
    
            if (!missingBindings.isEmpty()) {
    
                message.append("Missing bindings: \n");
                for (BindingSet bs : missingBindings) {
                    message.append(bs);
                    message.append("\n");
                }
    
                message.append("=============");
                StringUtil.appendN('=', getName().length(), message);
                message.append("========================\n");
            }
    
            if (!unexpectedBindings.isEmpty()) {
                message.append("Unexpected bindings: \n");
                for (BindingSet bs : unexpectedBindings) {
                    message.append(bs);
                    message.append("\n");
                }
    
                message.append("=============");
                StringUtil.appendN('=', getName().length(), message);
                message.append("========================\n");
            }
            
            if (checkOrder && missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
                message.append("Results are not in expected order.\n");
                message.append(" =======================\n");
                message.append("query result: \n");
                for (BindingSet bs: queryBindings) {
                    message.append(bs);
                    message.append("\n");
                }
                message.append(" =======================\n");
                message.append("expected result: \n");
                for (BindingSet bs: expectedBindings) {
                    message.append(bs);
                    message.append("\n");
                }
                message.append(" =======================\n");
    
                System.out.print(message.toString());
            }
    
            log.error(message.toString());
            fail(message.toString());
        }
        /* debugging only: print out result when test succeeds 
        else {
            queryResultTable.beforeFirst();
    
            List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
            StringBuilder message = new StringBuilder(128);
    
            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");
    
            message.append(" =======================\n");
            message.append("query result: \n");
            for (BindingSet bs: queryBindings) {
                message.append(bs);
                message.append("\n");
            }
            
            System.out.print(message.toString());
        }
        */
    }
    
    /**
     * Compare two graphs
     * 
     * @param queryResult
     * @param expectedResult
     * @throws Exception
     */
    private void compareGraphs(Set<Statement> queryResult, Set<Statement> expectedResult)
        throws Exception
    {
        if (!ModelUtil.equals(expectedResult, queryResult)) {
            // Don't use RepositoryUtil.difference, it reports incorrect diffs
            /*
             * Collection<? extends Statement> unexpectedStatements =
             * RepositoryUtil.difference(queryResult, expectedResult); Collection<?
             * extends Statement> missingStatements =
             * RepositoryUtil.difference(expectedResult, queryResult);
             * StringBuilder message = new StringBuilder(128);
             * message.append("\n=======Diff: "); message.append(getName());
             * message.append("========================\n"); if
             * (!unexpectedStatements.isEmpty()) { message.append("Unexpected
             * statements in result: \n"); for (Statement st :
             * unexpectedStatements) { message.append(st.toString());
             * message.append("\n"); } message.append("============="); for (int i =
             * 0; i < getName().length(); i++) { message.append("="); }
             * message.append("========================\n"); } if
             * (!missingStatements.isEmpty()) { message.append("Statements missing
             * in result: \n"); for (Statement st : missingStatements) {
             * message.append(st.toString()); message.append("\n"); }
             * message.append("============="); for (int i = 0; i <
             * getName().length(); i++) { message.append("="); }
             * message.append("========================\n"); }
             */
            StringBuilder message = new StringBuilder(128);
            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");
            message.append("Expected result: \n");
            for (Statement st : expectedResult) {
                message.append(st.toString());
                message.append("\n");
            }
            message.append("=============");
            StringUtil.appendN('=', getName().length(), message);
            message.append("========================\n");
    
            message.append("Query result: \n");
            for (Statement st : queryResult) {
                message.append(st.toString());
                message.append("\n");
            }
            message.append("=============");
            StringUtil.appendN('=', getName().length(), message);
            message.append("========================\n");
    
            log.error(message.toString());
            fail(message.toString());
        }
    }

}
