/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import info.aduna.iteration.Iterations;

import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailQuery;

import com.bigdata.rdf.sail.BigdataSailQuery;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

/**
 * A variant on the {@link SPARQLQueryTest} which uses the bigdata AST and does
 * not rely on {@link TupleExpr} and related classes.
 * 
 * @openrdf
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class SPARQLASTQueryTest extends SPARQLQueryTest {

	/*-----------*
	 * Constants *
	 *-----------*/

//	private static final Logger log = LoggerFactory.getLogger(SPARQLASTQueryTest.class);
//
//	protected final String testURI;
//
//	protected final String queryFileURL;
//
//	protected final String resultFileURL;
//
//	protected final Dataset dataset;
//
//	protected final boolean laxCardinality;
//
//	protected final boolean checkOrder;
//
//	/*-----------*
//	 * Variables *
//	 *-----------*/
//
//	protected Repository dataRep;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public SPARQLASTQueryTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet, boolean laxCardinality)
	{
		this(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, false);
	}
	
	public SPARQLASTQueryTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet, boolean laxCardinality, boolean checkOrder)
	{
		super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, checkOrder);

//		this.testURI = testURI;
//		this.queryFileURL = queryFileURL;
//		this.resultFileURL = resultFileURL;
//		this.dataset = dataSet;
//		this.laxCardinality = laxCardinality;
//		this.checkOrder = checkOrder;
	}

	/*---------*
	 * Methods *
	 *---------*/

//	@Override
//	protected void setUp()
//		throws Exception
//	{
//		dataRep = createRepository();
//
//		if (dataset != null) {
//			try {
//				uploadDataset(dataset);
//			}
//			catch (Exception exc) {
//				try {
//					dataRep.shutDown();
//					dataRep = null;
//				}
//				catch (Exception e2) {
//					logger.error(e2.toString(), e2);
//				}
//				throw exc;
//			}
//		}
//	}
//
//	protected Repository createRepository()
//		throws Exception
//	{
//		/*
//		 * Note: We override this for bigdata and use a new repository instance
//		 * for each test.  See the various subclasses for examples.
//		 */
//		throw new UnsupportedOperationException();
////		Repository repo = newRepository();
////		repo.initialize();
////		RepositoryConnection con = repo.getConnection();
////		try {
////			con.clear();
////			con.clearNamespaces();
////		}
////		finally {
////			con.close();
////		}
////		return repo;
//	}
//
//	protected abstract Repository newRepository()
//		throws Exception;
//
//	@Override
//	protected void tearDown()
//		throws Exception
//	{
//		if (dataRep != null) {
//			dataRep.shutDown();
//			dataRep = null;
//		}
//	}

	/**
	 * Return the connection which will be used to query the repository.
	 * @param dataRep The repository.
	 * @return The connection.
	 * @throws Exception
	 */
    abstract protected BigdataSailRepositoryConnection getQueryConnection(
            Repository dataRep) throws Exception;

//	{
//    	return dataRep.getConnection();
//    }
	
	@Override
	protected void runTest()
		throws Exception
	{
//		RepositoryConnection con = dataRep.getConnection();
		BigdataSailRepositoryConnection con = getQueryConnection(dataRep);
		try {
			
		    final String queryString = readQueryString();
		    
//		    System.err.println(queryString);
            
            final BigdataSailQuery query = con
                    .prepareNativeSPARQLQuery(QueryLanguage.SPARQL,
                            queryString, queryFileURL/* baseURI */);
		    
//			Query query = con.prepareQuery(QueryLanguage.SPARQL, queryString, queryFileURL);
            
			if (dataset != null) {
				
			    ((SailQuery)query).setDataset(dataset);
			    
//			    // Batch resolve Values to IVs.
//                final Object[] tmp = new BigdataValueReplacer(
//                        con.getTripleStore()).replaceValues(dataset,
//                        null/* tupleExpr */, null/* bindings */);
//			    
//                final Dataset dataset = (Dataset) tmp[0];
//                
//                // Set dataset with resolved IVs on QueryRoot.
//                query.getASTContainer().getOriginalAST().setDataset(new DatasetNode(dataset));
                
			}

			if (query instanceof TupleQuery) {
				TupleQueryResult queryResult = ((TupleQuery)query).evaluate();

				TupleQueryResult expectedResult = readExpectedTupleQueryResult();
				compareTupleQueryResults(queryResult, expectedResult);

				// Graph queryGraph = RepositoryUtil.asGraph(queryResult);
				// Graph expectedGraph = readExpectedTupleQueryResult();
				// compareGraphs(queryGraph, expectedGraph);
			}
			else if (query instanceof GraphQuery) {
				GraphQueryResult gqr = ((GraphQuery)query).evaluate();
				Set<Statement> queryResult = Iterations.asSet(gqr);

				Set<Statement> expectedResult = readExpectedGraphQueryResult();

				compareGraphs(queryResult, expectedResult);
			}
			else if (query instanceof BooleanQuery) {
				boolean queryResult = ((BooleanQuery)query).evaluate();
				boolean expectedResult = readExpectedBooleanQueryResult();
				assertEquals(expectedResult, queryResult);
			}
			else {
				throw new RuntimeException("Unexpected query type: " + query.getClass());
			}
		}
		finally {
			con.close();
		}
	}

//	private void compareTupleQueryResults(TupleQueryResult queryResult, TupleQueryResult expectedResult)
//		throws Exception
//	{
//		// Create MutableTupleQueryResult to be able to re-iterate over the
//		// results
//		MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
//		MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);
//
//		boolean resultsEqual;
//		if (laxCardinality) {
//			resultsEqual = QueryResultUtil.isSubset(queryResultTable, expectedResultTable);
//		}
//		else {
//			resultsEqual = QueryResultUtil.equals(queryResultTable, expectedResultTable);
//			
//			if (checkOrder) {
//				// also check the order in which solutions occur.
//				queryResultTable.beforeFirst();
//				expectedResultTable.beforeFirst();
//
//				while (queryResultTable.hasNext()) {
//					BindingSet bs = queryResultTable.next();
//					BindingSet expectedBs = expectedResultTable.next();
//					
//					if (! bs.equals(expectedBs)) {
//						resultsEqual = false;
//						break;
//					}
//				}
//			}
//		}
//
//		if (!resultsEqual) {
//			queryResultTable.beforeFirst();
//			expectedResultTable.beforeFirst();
//
//			/*
//			 * StringBuilder message = new StringBuilder(128);
//			 * message.append("\n============ "); message.append(getName());
//			 * message.append(" =======================\n");
//			 * message.append("Expected result: \n"); while
//			 * (expectedResultTable.hasNext()) {
//			 * message.append(expectedResultTable.next()); message.append("\n"); }
//			 * message.append("============="); StringUtil.appendN('=',
//			 * getName().length(), message);
//			 * message.append("========================\n"); message.append("Query
//			 * result: \n"); while (queryResultTable.hasNext()) {
//			 * message.append(queryResultTable.next()); message.append("\n"); }
//			 * message.append("============="); StringUtil.appendN('=',
//			 * getName().length(), message);
//			 * message.append("========================\n");
//			 */
//
//			List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
//			List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);
//
//			List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
//			missingBindings.removeAll(queryBindings);
//
//			List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
//			unexpectedBindings.removeAll(expectedBindings);
//
//			StringBuilder message = new StringBuilder(128);
//            message.append("\n");
//            message.append(testURI);
//            message.append("\n");
//            message.append(getName());
//			message.append("\n===================================\n");
//
//			if (!missingBindings.isEmpty()) {
//
//				message.append("Missing bindings: \n");
//				for (BindingSet bs : missingBindings) {
//					message.append(bs);
//					message.append("\n");
//				}
//
//				message.append("=============");
//				StringUtil.appendN('=', getName().length(), message);
//				message.append("========================\n");
//			}
//
//			if (!unexpectedBindings.isEmpty()) {
//				message.append("Unexpected bindings: \n");
//				for (BindingSet bs : unexpectedBindings) {
//					message.append(bs);
//					message.append("\n");
//				}
//
//				message.append("=============");
//				StringUtil.appendN('=', getName().length(), message);
//				message.append("========================\n");
//			}
//			
//			if (checkOrder && missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
//				message.append("Results are not in expected order.\n");
//				message.append(" =======================\n");
//				message.append("query result: \n");
//				for (BindingSet bs: queryBindings) {
//					message.append(bs);
//					message.append("\n");
//				}
//				message.append(" =======================\n");
//				message.append("expected result: \n");
//				for (BindingSet bs: expectedBindings) {
//					message.append(bs);
//					message.append("\n");
//				}
//				message.append(" =======================\n");
//
//				System.out.print(message.toString());
//			}
//
//            RepositoryConnection con = ((DatasetRepository)dataRep).getDelegate().getConnection();
////            System.err.println(con.getClass());
//            try {
//                String queryString = readQueryString();
//                Query query = con.prepareQuery(QueryLanguage.SPARQL, queryString, queryFileURL);
//                if (dataset != null) {
//                    query.setDataset(dataset);
//                }
//                TupleExpr tupleExpr = ((BigdataSailQuery) query).getTupleExpr();
//                message.append(queryString.trim());
//                message.append("\n===================================\n");
//                message.append(tupleExpr);
//                
//                message.append("\n===================================\n");
//                message.append("database dump:\n");
//                RepositoryResult<Statement> stmts = con.getStatements(null, null, null, false);
//                while (stmts.hasNext()) {
//                    message.append(stmts.next());
//                    message.append("\n");
//                }
//            } finally {
//                con.close();
//            }
//            
//			logger.error(message.toString());
//			fail(message.toString());
//		}
//		/* debugging only: print out result when test succeeds 
//		else {
//			queryResultTable.beforeFirst();
//
//			List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
//			StringBuilder message = new StringBuilder(128);
//
//			message.append("\n============ ");
//			message.append(getName());
//			message.append(" =======================\n");
//
//			message.append(" =======================\n");
//			message.append("query result: \n");
//			for (BindingSet bs: queryBindings) {
//				message.append(bs);
//				message.append("\n");
//			}
//			
//			System.out.print(message.toString());
//		}
//		*/
//	}
//
//	private void compareGraphs(Set<Statement> queryResult, Set<Statement> expectedResult)
//		throws Exception
//	{
//		if (!ModelUtil.equals(expectedResult, queryResult)) {
//			// Don't use RepositoryUtil.difference, it reports incorrect diffs
//			/*
//			 * Collection<? extends Statement> unexpectedStatements =
//			 * RepositoryUtil.difference(queryResult, expectedResult); Collection<?
//			 * extends Statement> missingStatements =
//			 * RepositoryUtil.difference(expectedResult, queryResult);
//			 * StringBuilder message = new StringBuilder(128);
//			 * message.append("\n=======Diff: "); message.append(getName());
//			 * message.append("========================\n"); if
//			 * (!unexpectedStatements.isEmpty()) { message.append("Unexpected
//			 * statements in result: \n"); for (Statement st :
//			 * unexpectedStatements) { message.append(st.toString());
//			 * message.append("\n"); } message.append("============="); for (int i =
//			 * 0; i < getName().length(); i++) { message.append("="); }
//			 * message.append("========================\n"); } if
//			 * (!missingStatements.isEmpty()) { message.append("Statements missing
//			 * in result: \n"); for (Statement st : missingStatements) {
//			 * message.append(st.toString()); message.append("\n"); }
//			 * message.append("============="); for (int i = 0; i <
//			 * getName().length(); i++) { message.append("="); }
//			 * message.append("========================\n"); }
//			 */
//			StringBuilder message = new StringBuilder(128);
//			message.append("\n============ ");
//			message.append(getName());
//			message.append(" =======================\n");
//			message.append("Expected result: \n");
//			for (Statement st : expectedResult) {
//				message.append(st.toString());
//				message.append("\n");
//			}
//			message.append("=============");
//			StringUtil.appendN('=', getName().length(), message);
//			message.append("========================\n");
//
//			message.append("Query result: \n");
//			for (Statement st : queryResult) {
//				message.append(st.toString());
//				message.append("\n");
//			}
//			message.append("=============");
//			StringUtil.appendN('=', getName().length(), message);
//			message.append("========================\n");
//
//			logger.error(message.toString());
//			fail(message.toString());
//		}
//	}
//
//	private void uploadDataset(Dataset dataset)
//		throws Exception
//	{
////		RepositoryConnection con = dataRep.getConnection();
////		try {
//			// Merge default and named graphs to filter duplicates
//			Set<URI> graphURIs = new HashSet<URI>();
//			graphURIs.addAll(dataset.getDefaultGraphs());
//			graphURIs.addAll(dataset.getNamedGraphs());
//
//			for (Resource graphURI : graphURIs) {
//				upload(((URI)graphURI), graphURI);
//			}
////		}
////		finally {
////			con.close();
////		}
//	}
//
//	private void upload(URI graphURI, Resource context)
//		throws Exception
//	{
//		final RepositoryConnection con = dataRep.getConnection();
//		try {
//			con.setAutoCommit(false);
//			RDFFormat rdfFormat = Rio.getParserFormatForFileName(graphURI.toString(), RDFFormat.TURTLE);
//			RDFParser rdfParser = Rio.createParser(rdfFormat, dataRep.getValueFactory());
//			rdfParser.setVerifyData(false);
//			rdfParser.setDatatypeHandling(DatatypeHandling.IGNORE);
//			// rdfParser.setPreserveBNodeIDs(true);
//
//			RDFInserter rdfInserter = new RDFInserter(con);
//			rdfInserter.enforceContext(context);
//			rdfParser.setRDFHandler(rdfInserter);
//
//			URL graphURL = new URL(graphURI.toString());
//			InputStream in = graphURL.openStream();
//			try {
//				rdfParser.parse(in, graphURI.toString());
//			}
//			finally {
//				in.close();
//			}
//
//			/*
//			 * Modified Oct 11th 2010 by BBT.  Do not enable auto-commit. Just
//			 * commit the connection.
//			 */
////			con.setAutoCommit(true);
//			con.commit();
//		}
//		finally {
//			con.close();
//		}
//	}
//
//	protected String readQueryString()
//		throws IOException
//	{
//		InputStream stream = new URL(queryFileURL).openStream();
//		try {
//			return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
//		}
//		finally {
//			stream.close();
//		}
//	}
//
//	private TupleQueryResult readExpectedTupleQueryResult()
//		throws Exception
//	{
//		TupleQueryResultFormat tqrFormat = QueryResultIO.getParserFormatForFileName(resultFileURL);
//
//		if (tqrFormat != null) {
//			InputStream in = new URL(resultFileURL).openStream();
//			try {
//				TupleQueryResultParser parser = QueryResultIO.createParser(tqrFormat);
//				parser.setValueFactory(dataRep.getValueFactory());
//
//				TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();
//				parser.setTupleQueryResultHandler(qrBuilder);
//
//				parser.parse(in);
//				return qrBuilder.getQueryResult();
//			}
//			finally {
//				in.close();
//			}
//		}
//		else {
//			Set<Statement> resultGraph = readExpectedGraphQueryResult();
//			return DAWGTestResultSetUtil.toTupleQueryResult(resultGraph);
//		}
//	}
//
//	private boolean readExpectedBooleanQueryResult()
//		throws Exception
//	{
//		BooleanQueryResultFormat bqrFormat = BooleanQueryResultParserRegistry.getInstance().getFileFormatForFileName(
//				resultFileURL);
//
//		if (bqrFormat != null) {
//			InputStream in = new URL(resultFileURL).openStream();
//			try {
//				return QueryResultIO.parse(in, bqrFormat);
//			}
//			finally {
//				in.close();
//			}
//		}
//		else {
//			Set<Statement> resultGraph = readExpectedGraphQueryResult();
//			return DAWGTestResultSetUtil.toBooleanQueryResult(resultGraph);
//		}
//	}
//
//	private Set<Statement> readExpectedGraphQueryResult()
//		throws Exception
//	{
//		RDFFormat rdfFormat = Rio.getParserFormatForFileName(resultFileURL);
//
//		if (rdfFormat != null) {
//			RDFParser parser = Rio.createParser(rdfFormat);
//			parser.setDatatypeHandling(DatatypeHandling.IGNORE);
//			parser.setPreserveBNodeIDs(true);
//			parser.setValueFactory(dataRep.getValueFactory());
//
//			Set<Statement> result = new LinkedHashSet<Statement>();
//			parser.setRDFHandler(new StatementCollector(result));
//
//			InputStream in = new URL(resultFileURL).openStream();
//			try {
//				parser.parse(in, resultFileURL);
//			}
//			finally {
//				in.close();
//			}
//
//			return result;
//		}
//		else {
//			throw new RuntimeException("Unable to determine file type of results file");
//		}
//	}
//
//	public interface Factory {
//
//		SPARQLASTQueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
//				String resultFileURL, Dataset dataSet, boolean laxCardinality);
//
//		SPARQLASTQueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
//				String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder);
//	}
//
//	public static TestSuite suite(String manifestFileURL, Factory factory)
//		throws Exception
//	{
//		return suite(manifestFileURL, factory, true);
//	}
//
//	public static TestSuite suite(String manifestFileURL, Factory factory, boolean approvedOnly)
//		throws Exception
//	{
//		logger.info("Building test suite for {}", manifestFileURL);
//
//		TestSuite suite = new TestSuite(factory.getClass().getName());
//
//		// Read manifest and create declared test cases
//		Repository manifestRep = new SailRepository(new MemoryStore());
//		manifestRep.initialize();
//		RepositoryConnection con = manifestRep.getConnection();
//
//		ManifestTest.addTurtle(con, new URL(manifestFileURL), manifestFileURL);
//
//		suite.setName(getManifestName(manifestRep, con, manifestFileURL));
//
//		// Extract test case information from the manifest file. Note that we only
//		// select those test cases that are mentioned in the list.
//		StringBuilder query = new StringBuilder(512);
//		query.append(" SELECT DISTINCT testURI, testName, resultFile, action, queryFile, defaultGraph, ordered ");
//		query.append(" FROM {} rdf:first {testURI} ");
//		if (approvedOnly) {
//			query.append("                          dawgt:approval {dawgt:Approved}; ");
//		}
//		query.append("                             mf:name {testName}; ");
//		query.append("                             mf:result {resultFile}; ");
//		query.append("                             [ mf:checkOrder {ordered} ]; ");
//		query.append("                             mf:action {action} qt:query {queryFile}; ");
//		query.append("                                               [qt:data {defaultGraph}] ");
//		query.append(" USING NAMESPACE ");
//		query.append("  mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>, ");
//		query.append("  dawgt = <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>, ");
//		query.append("  qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>");
//		TupleQuery testCaseQuery = con.prepareTupleQuery(QueryLanguage.SERQL, query.toString());
//
//		query.setLength(0);
//		query.append(" SELECT graph ");
//		query.append(" FROM {action} qt:graphData {graph} ");
//		query.append(" USING NAMESPACE ");
//		query.append(" qt = <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>");
//		TupleQuery namedGraphsQuery = con.prepareTupleQuery(QueryLanguage.SERQL, query.toString());
//
//		query.setLength(0);
//		query.append("SELECT 1 ");
//		query.append(" FROM {testURI} mf:resultCardinality {mf:LaxCardinality}");
//		query.append(" USING NAMESPACE mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>");
//		TupleQuery laxCardinalityQuery = con.prepareTupleQuery(QueryLanguage.SERQL, query.toString());
//
//		logger.debug("evaluating query..");
//		TupleQueryResult testCases = testCaseQuery.evaluate();
//		while (testCases.hasNext()) {
//			BindingSet bindingSet = testCases.next();
//
//			URI testURI = (URI)bindingSet.getValue("testURI");
//			String testName = bindingSet.getValue("testName").toString();
//			String resultFile = bindingSet.getValue("resultFile").toString();
//			String queryFile = bindingSet.getValue("queryFile").toString();
//			URI defaultGraphURI = (URI)bindingSet.getValue("defaultGraph");
//			Value action = bindingSet.getValue("action");
//			Value ordered = bindingSet.getValue("ordered");
//
//			logger.debug("found test case : {}", testName);
//
//			// Query named graphs
//			namedGraphsQuery.setBinding("action", action);
//			TupleQueryResult namedGraphs = namedGraphsQuery.evaluate();
//
//			DatasetImpl dataset = null;
//
//			if (defaultGraphURI != null || namedGraphs.hasNext()) {
//				dataset = new DatasetImpl();
//
//				if (defaultGraphURI != null) {
//					dataset.addDefaultGraph(defaultGraphURI);
//				}
//
//				while (namedGraphs.hasNext()) {
//					BindingSet graphBindings = namedGraphs.next();
//					URI namedGraphURI = (URI)graphBindings.getValue("graph");
//					dataset.addNamedGraph(namedGraphURI);
//				}
//			}
//
//			// Check for lax-cardinality conditions
//			boolean laxCardinality = false;
//			laxCardinalityQuery.setBinding("testURI", testURI);
//			TupleQueryResult laxCardinalityResult = laxCardinalityQuery.evaluate();
//			try {
//				laxCardinality = laxCardinalityResult.hasNext();
//			}
//			finally {
//				laxCardinalityResult.close();
//			}
//
//			// check if we should test for query result ordering
//			boolean checkOrder = false;
//			if (ordered != null) {
//				checkOrder = Boolean.parseBoolean(ordered.stringValue());
//			}
//
//			SPARQLASTQueryTest test = factory.createSPARQLQueryTest(testURI.toString(), testName, queryFile,
//					resultFile, dataset, laxCardinality, checkOrder);
//			if (test != null) {
//				suite.addTest(test);
//			}
//		}
//
//		testCases.close();
//		con.close();
//
//		manifestRep.shutDown();
//		logger.info("Created test suite with " + suite.countTestCases() + " test cases.");
//		return suite;
//	}
//
//	protected static String getManifestName(Repository manifestRep, RepositoryConnection con,
//			String manifestFileURL)
//		throws QueryEvaluationException, RepositoryException, MalformedQueryException
//	{
//		// Try to extract suite name from manifest file
//		TupleQuery manifestNameQuery = con.prepareTupleQuery(QueryLanguage.SERQL,
//				"SELECT ManifestName FROM {ManifestURL} rdfs:label {ManifestName}");
//		manifestNameQuery.setBinding("ManifestURL", manifestRep.getValueFactory().createURI(manifestFileURL));
//		TupleQueryResult manifestNames = manifestNameQuery.evaluate();
//		try {
//			if (manifestNames.hasNext()) {
//				return manifestNames.next().getValue("ManifestName").stringValue();
//			}
//		}
//		finally {
//			manifestNames.close();
//		}
//
//		// Derive name from manifest URL
//		int lastSlashIdx = manifestFileURL.lastIndexOf('/');
//		int secLastSlashIdx = manifestFileURL.lastIndexOf('/', lastSlashIdx - 1);
//		return manifestFileURL.substring(secLastSlashIdx + 1, lastSlashIdx);
//	}
//
//	/**
//	 * Made visible to the test suites so we can filter for specific tests.
//	 */
//	public String getTestURI() {
//        return testURI;
//    }
 
/*
 * TODO These are bigdata aware variants, but we need to hook the QueryRoot
 * and query plan (pipeline) for them to provide additional information.  
 */
	
//	@Override
//    protected void compareTupleQueryResults(
//            final TupleQueryResult queryResult,
//            final TupleQueryResult expectedResult) throws Exception {
//        
//        /*
//         * Create MutableTupleQueryResult to be able to re-iterate over the
//         * results.
//         */
//        
//        final MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
//        
//        final MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);
//
//        boolean resultsEqual;
//        if (laxCardinality) {
//            resultsEqual = QueryResultUtil.isSubset(queryResultTable, expectedResultTable);
//        }
//        else {
//            resultsEqual = QueryResultUtil.equals(queryResultTable, expectedResultTable);
//            
//            if (checkOrder) {
//                // also check the order in which solutions occur.
//                queryResultTable.beforeFirst();
//                expectedResultTable.beforeFirst();
//
//                while (queryResultTable.hasNext()) {
//                    final BindingSet bs = queryResultTable.next();
//                    final BindingSet expectedBs = expectedResultTable.next();
//                    
//                    if (! bs.equals(expectedBs)) {
//                        resultsEqual = false;
//                        break;
//                    }
//                }
//            }
//        }
//
//        if (!resultsEqual) {
//            queryResultTable.beforeFirst();
//            expectedResultTable.beforeFirst();
//
////Note: code block shows the expected and actual results.
////            
////            if(false) {
////                StringBuilder message = new StringBuilder(2048);
////                message.append("\n============ ");
////                message.append(getName());
////                message.append(" =======================\n");
////                message.append("Expected result: \n");
////                while (expectedResultTable.hasNext()) {
////                    message.append(expectedResultTable.next());
////                    message.append("\n");
////                }
////                message.append("=============");
////                StringUtil.appendN('=', getName().length(), message);
////                message.append("========================\n");
////                message.append("Query result: \n");
////                while (queryResultTable.hasNext()) {
////                    message.append(queryResultTable.next());
////                    message.append("\n");
////                }
////                message.append("=============");
////                StringUtil.appendN('=', getName().length(), message);
////                message.append("========================\n");
////                System.err.println(message);
////            }
//
//            final List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
//            final List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);
//
//            final List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
//            missingBindings.removeAll(queryBindings);
//
//            final List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
//            unexpectedBindings.removeAll(expectedBindings);
//
//            final StringBuilder message = new StringBuilder(2048);
//            message.append("\n");
//            message.append(testURI);
//            message.append("\n");
//            message.append(getName());
//            message.append("\n===================================\n");
//
//            if (!missingBindings.isEmpty()) {
//
//                message.append("Missing bindings: \n");
//                for (BindingSet bs : missingBindings) {
//                    message.append(bs);
//                    message.append("\n");
//                }
//
//                message.append("=============");
//                StringUtil.appendN('=', getName().length(), message);
//                message.append("========================\n");
//            }
//
//            if (!unexpectedBindings.isEmpty()) {
//                message.append("Unexpected bindings: \n");
//                for (BindingSet bs : unexpectedBindings) {
//                    message.append(bs);
//                    message.append("\n");
//                }
//
//                message.append("=============");
//                StringUtil.appendN('=', getName().length(), message);
//                message.append("========================\n");
//            }
//            
//            if (checkOrder && missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
//                message.append("Results are not in expected order.\n");
//                message.append(" =======================\n");
//                message.append("query result: \n");
//                for (BindingSet bs: queryBindings) {
//                    message.append(bs);
//                    message.append("\n");
//                }
//                message.append(" =======================\n");
//                message.append("expected result: \n");
//                for (BindingSet bs: expectedBindings) {
//                    message.append(bs);
//                    message.append("\n");
//                }
//                message.append(" =======================\n");
//
//                System.out.print(message.toString());
//            }
//
//                RepositoryConnection con = ((DatasetRepository)dataRep).getDelegate().getConnection();
//                System.err.println(con.getClass());
//                try {
//                message.append("\n===================================\n");
//                message.append("\nqueryRoot:\n");
//                message.append(BOpUtility.toString(queryRoot));
//                message.append("\noptimizedQuery:\n");
//                message.append(BOpUtility.toString(optimizedQuery));
//                message.append("\nqueryPlan:\n");
//                message.append(BOpUtility.toString(queryPlan));
//                message.append("\n===================================\n");
//                message.append("database dump:\n");
////                message.append(store.dumpStore());
//                    RepositoryResult<Statement> stmts = con.getStatements(null, null, null, false);
//                    while (stmts.hasNext()) {
//                        message.append(stmts.next());
//                        message.append("\n");
//                    }
//                } finally {
//                    con.close();
//                }
//                
//            log.error(message.toString());
//            fail(message.toString());
//        }
//            /* debugging only: print out result when test succeeds 
//            else {
//                queryResultTable.beforeFirst();
//
//                List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
//                StringBuilder message = new StringBuilder(128);
//
//                message.append("\n============ ");
//                message.append(getName());
//                message.append(" =======================\n");
//
//                message.append(" =======================\n");
//                message.append("query result: \n");
//                for (BindingSet bs: queryBindings) {
//                    message.append(bs);
//                    message.append("\n");
//                }
//                
//                System.out.print(message.toString());
//            }
//            */
//        }
//
//    @Override
//    protected void compareGraphs(Set<Statement> queryResult, Set<Statement> expectedResult)
//            throws Exception
//        {
//            if (!ModelUtil.equals(expectedResult, queryResult)) {
//                // Don't use RepositoryUtil.difference, it reports incorrect diffs
//                /*
//                 * Collection<? extends Statement> unexpectedStatements =
//                 * RepositoryUtil.difference(queryResult, expectedResult); Collection<?
//                 * extends Statement> missingStatements =
//                 * RepositoryUtil.difference(expectedResult, queryResult);
//                 * StringBuilder message = new StringBuilder(128);
//                 * message.append("\n=======Diff: "); message.append(getName());
//                 * message.append("========================\n"); if
//                 * (!unexpectedStatements.isEmpty()) { message.append("Unexpected
//                 * statements in result: \n"); for (Statement st :
//                 * unexpectedStatements) { message.append(st.toString());
//                 * message.append("\n"); } message.append("============="); for (int i =
//                 * 0; i < getName().length(); i++) { message.append("="); }
//                 * message.append("========================\n"); } if
//                 * (!missingStatements.isEmpty()) { message.append("Statements missing
//                 * in result: \n"); for (Statement st : missingStatements) {
//                 * message.append(st.toString()); message.append("\n"); }
//                 * message.append("============="); for (int i = 0; i <
//                 * getName().length(); i++) { message.append("="); }
//                 * message.append("========================\n"); }
//                 */
//                StringBuilder message = new StringBuilder(128);
//                message.append("\n============ ");
//                message.append(getName());
//                message.append(" =======================\n");
//                message.append("Expected result: \n");
//                for (Statement st : expectedResult) {
//                    message.append(st.toString());
//                    message.append("\n");
//                }
//                message.append("=============");
//                StringUtil.appendN('=', getName().length(), message);
//                message.append("========================\n");
//
//                message.append("Query result: \n");
//                for (Statement st : queryResult) {
//                    message.append(st.toString());
//                    message.append("\n");
//                }
//                message.append("=============");
//                StringUtil.appendN('=', getName().length(), message);
//                message.append("========================\n");
//
//                log.error(message.toString());
//                fail(message.toString());
//            }
//        }
        
}
