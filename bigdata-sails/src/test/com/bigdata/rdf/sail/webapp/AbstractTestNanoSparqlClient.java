/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.eclipse.jetty.server.Server;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.SailException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.GraphQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.TupleQuery;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.util.config.NicUtil;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public abstract class AbstractTestNanoSparqlClient<S extends IIndexManager> extends ProxyTestCase<S> {

    /**
     * The path used to resolve resources in this package when they are being
     * uploaded to the {@link NanoSparqlServer}.
     */
    protected static final String packagePath = "bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/";

	/**
	 * A jetty {@link Server} running a {@link NanoSparqlServer} instance which
	 * is running against that {@link #m_indexManager}.
	 */
	protected Server m_fixture;

	/**
	 * The namespace of the {@link AbstractTripleStore} instance against which
	 * the test is running. A unique namespace is used for each test run, but
	 * the namespace is based on the test name.
	 */
	protected String namespace;
	
	/**
	 * The effective {@link NanoSparqlServer} http end point.
	 */
	protected String m_serviceURL;

//	/**
//	 * The request path for the REST API under test.
//	 */
//	final protected static String requestPath = "/sparql";

	public AbstractTestNanoSparqlClient() {
		
	}

	public AbstractTestNanoSparqlClient(final String name) {

		super(name);

	}

	protected AbstractTripleStore createTripleStore(
			final IIndexManager indexManager, final String namespace,
			final Properties properties) {
        
		if(log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

		// Locate the resource declaration (aka "open"). This tells us if it
		// exists already.
		AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore != null) {

			fail("exists: " + namespace);
			
		}

		/*
		 * Create the KB instance.
		 */

		if (log.isInfoEnabled()) {
			log.info("Creating KB instance: namespace="+namespace);
			log.info("Properties=" + properties.toString());
		}

		if (indexManager instanceof Journal) {

	        // Create the kb instance.
			tripleStore = new LocalTripleStore(indexManager, namespace,
					ITx.UNISOLATED, properties);

		} else {

			tripleStore = new ScaleOutTripleStore(indexManager, namespace,
					ITx.UNISOLATED, properties);
		}

        // create the triple store.
        tripleStore.create();

        if(log.isInfoEnabled())
        	log.info("Created tripleStore: " + namespace);

        // New KB instance was created.
        return tripleStore;

    }

	protected void dropTripleStore(final IIndexManager indexManager,
			final String namespace) {

		if(log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

		// Locate the resource declaration (aka "open"). This tells us if it
		// exists already.
		final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore != null) {

			if (log.isInfoEnabled())
				log.info("Destroying: " + namespace);

			tripleStore.destroy();
			
		}

	}
	
	TestMode testMode = null;
	
	@Override
	public void setUp() throws Exception {
	    
		super.setUp();

		log.warn("Setting up test:" + getName());
		
		final Properties properties = getProperties();

		// guaranteed distinct namespace for the KB instance.
		namespace = getName() + UUID.randomUUID();

		final IIndexManager m_indexManager = getIndexManager();
		
		// Create the triple store instance.
        final AbstractTripleStore tripleStore = createTripleStore(m_indexManager,
                namespace, properties);
        
        if (tripleStore.isStatementIdentifiers()) {
            testMode = TestMode.sids;
        } else if (tripleStore.isQuads()) {
            testMode = TestMode.quads;
        } else {
            testMode = TestMode.triples;
        }
		
        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, namespace);

            initParams.put(ConfigParams.CREATE, "false");
            
        }
        // Start server for that kb instance.
        m_fixture = NanoSparqlServer.newInstance(0/* port */,
                m_indexManager, initParams);

        m_fixture.start();

		final int port = m_fixture.getConnectors()[0].getLocalPort();

		// log.info("Getting host address");

        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);

        if (hostAddr == null) {

            fail("Could not identify network address for this host.");

        }

        m_serviceURL = new URL("http", hostAddr, port, "/sparql"/* file */)
                .toExternalForm();

        if (log.isInfoEnabled())
            log.info("Setup done: name=" + getName() + ", namespace="
                    + namespace + ", serviceURL=" + m_serviceURL);

    }

    @Override
	public void tearDown() throws Exception {

//		if (log.isInfoEnabled())
			log.warn("tearing down test: " + getName());

		if (m_fixture != null) {

			m_fixture.stop();

			m_fixture = null;

		}

		final IIndexManager m_indexManager = getIndexManager();
		
		if (m_indexManager != null && namespace != null) {

			dropTripleStore(m_indexManager, namespace);

		}
		
//		m_indexManager = null;

		namespace = null;
		
		m_serviceURL = null;

		log.info("tear down done");
		
		super.tearDown();

	}

    /**
     * Returns a view of the triple store using the sail interface.
     */
    protected BigdataSail getSail() {

		final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

        return new BigdataSail(tripleStore);

    }

	protected String getStreamContents(final InputStream inputStream)
            throws IOException {

        final Reader rdr = new InputStreamReader(inputStream);
		
	    final StringBuffer sb = new StringBuffer();
		
	    final char[] buf = new char[512];
	    
		while (true) {
		
		    final int rdlen = rdr.read(buf);
			
		    if (rdlen == -1)
				break;
			
		    sb.append(buf, 0, rdlen);
		    
		}
		
		return sb.toString();

	}

	/**
	 * Counts the #of results in a SPARQL result set.
	 * 
	 * @param conn
	 *            The connection from which to read the results.
	 * 
	 * @return The #of results.
	 * 
	 * @throws Exception
	 *             If anything goes wrong.
	 */
	protected long countResults(final TupleQueryResult result) throws Exception {

    	long count = 0;
    	
    	while(result.hasNext()) {
    		
    		result.next();
    		
    		count++;
    		
    	}
    	
    	result.close();
    	
    	return count;
    	
	}

    /**
     * Generates some statements and serializes them using the specified
     * {@link RDFFormat}.
     * 
     * @param ntriples
     *            The #of statements to generate.
     * @param format
     *            The format.
     * 
     * @return the serialized statements.
     */
    protected byte[] genNTRIPLES(final int ntriples, final RDFFormat format)
            throws RDFHandlerException {

        final Graph g = genNTRIPLES2(ntriples);
        
        final RDFWriterFactory writerFactory = RDFWriterRegistry.getInstance()
                .get(format);

        if (writerFactory == null)
            fail("RDFWriterFactory not found: format=" + format);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final RDFWriter writer = writerFactory.getWriter(baos);

        writer.startRDF();

        for (Statement stmt : g) {

            writer.handleStatement(stmt);

        }

        writer.endRDF();

        return baos.toByteArray();
        
    }
    
    protected Graph genNTRIPLES2(final int ntriples)
			throws RDFHandlerException {

		final Graph g = new GraphImpl();

		final ValueFactory f = new ValueFactoryImpl();

		final URI s = f.createURI("http://www.bigdata.org/b");

		final URI rdfType = f
				.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		for (int i = 0; i < ntriples; i++) {

			final URI o = f.createURI("http://www.bigdata.org/c#" + i);

			g.add(s, rdfType, o);

		}
		
		return g;

	}

//    /**
//     * "ASK" query using POST with an empty KB.
//     */
//    public void test_POST_ASK() throws Exception {
//        
//        final String queryStr = "ASK where {?s ?p ?o}";
//
//        final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "POST";
//
//        opts.acceptHeader = BooleanQueryResultFormat.SPARQL.getDefaultMIMEType();
//        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
//
//        opts.acceptHeader = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
//        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
//        
//    }

    /**
     * Generate and return a very large literal.
     */
    protected Literal getVeryLargeLiteral() {

        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }
        
        return new LiteralImpl(sb.toString());
        
    }

    public long countAll() throws Exception {
    	
    	return getSail().getDatabase().getExplicitStatementCount(null);
    	
//    	final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//    	
//    	final String countQuery = "select * where {?s ?p ?o}";
//    	
//		final TupleQuery query = repo.prepareTupleQuery(countQuery);
//		
//		return countResults(query.evaluate());
    	
    }
    
    /**
     * Test helps PUTs some data, verifies that it is visible, DELETEs the data,
     * and then verifies that it is gone.
     * 
     * @param format
     *            The interchange format.
     */
    protected void doDeleteWithPostTest(final RDFFormat format) throws Exception {

//        final String queryStr = "select * where {?s ?p ?o}";

//        final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "POST";

        doInsertWithBodyTest("POST", 23, /*requestPath,*/ format);

//        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));
        assertEquals(23, countAll());

        doDeleteWithBody(/*requestPath,*/ 23, format);

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
        assertEquals(0, countAll());
        
    }

	protected long doDeleteWithQuery(/*final String servlet, */final String query) throws Exception {
		
//		HttpURLConnection conn = null;
//		try {
//
//			final URL url = new URL(m_serviceURL + servlet + "?query="
//					+ URLEncoder.encode(query, "UTF-8"));
//			conn = (HttpURLConnection) url.openConnection();
//			conn.setRequestMethod("DELETE");
//			conn.setDoOutput(true);
//			conn.setDoInput(true);
//			conn.setUseCaches(false);
//			conn.setReadTimeout(0);
//
//			conn.connect();
//
//			if (log.isInfoEnabled())
//				log.info(conn.getResponseMessage());
//
//			final int rc = conn.getResponseCode();
//			
//			if (rc < 200 || rc >= 300) {
//				throw new IOException(conn.getResponseMessage());
//			}
//
//		} catch (Throwable t) {
//			// clean up the connection resources
//			if (conn != null)
//				conn.disconnect();
//			throw new RuntimeException(t);
//		}
		
		final RemoteRepository repo = new RemoteRepository(m_serviceURL);
		
		final RemoveOp remove = new RemoveOp(query);
		
		return repo.remove(remove);
		
    }

    protected long doDeleteWithAccessPath(//
//            final String servlet,//
            final URI s,//
            final URI p,//
            final Value o,//
            final URI c//
            ) throws Exception {
    	
//        HttpURLConnection conn = null;
//        try {
//
//            final LinkedHashMap<String, String[]> requestParams = new LinkedHashMap<String, String[]>();
//
//            if (s != null)
//                requestParams.put("s",
//                        new String[] { EncodeDecodeValue.encodeValue(s) });
//            
//            if (p != null)
//                requestParams.put("p",
//                        new String[] { EncodeDecodeValue.encodeValue(p) });
//            
//            if (o != null)
//                requestParams.put("o",
//                        new String[] { EncodeDecodeValue.encodeValue(o) });
//            
//            if (c != null)
//                requestParams.put("c",
//                        new String[] { EncodeDecodeValue.encodeValue(c) });
//
//            final StringBuilder urlString = new StringBuilder();
//            urlString.append(m_serviceURL).append(servlet);
//            addQueryParams(urlString, requestParams);
//
//            final URL url = new URL(urlString.toString());
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod("DELETE");
//            conn.setDoOutput(false);
//            conn.setDoInput(true);
//            conn.setUseCaches(false);
//            conn.setReadTimeout(0);
//
//            conn.connect();
//
////            if (log.isInfoEnabled())
////                log.info(conn.getResponseMessage());
//
//            final int rc = conn.getResponseCode();
//            
//            if (rc < 200 || rc >= 300) {
//                throw new IOException(conn.getResponseMessage());
//            }
//
//            return getMutationResult(conn);
//            
//        } catch (Throwable t) {
//            // clean up the connection resources
//            if (conn != null)
//                conn.disconnect();
//            throw new RuntimeException(t);
//        }
    	
    	final RemoteRepository repo = new RemoteRepository(m_serviceURL);
    	
    	final RemoveOp remove = new RemoveOp(s, p, o, c);
    	
    	return repo.remove(remove);
    	
    }

    protected void doDeleteWithBody(
            /* final String servlet, */final int ntriples,
            final RDFFormat format) throws Exception {

//        HttpURLConnection conn = null;
//		try {
//
//            final URL url = new URL(m_serviceURL + servlet + "?delete");
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod("POST");
//			conn.setDoOutput(true);
//			conn.setDoInput(true);
//			conn.setUseCaches(false);
//			conn.setReadTimeout(0);
//
//            conn
//                    .setRequestProperty("Content-Type", format
//                            .getDefaultMIMEType());

            final byte[] data = genNTRIPLES(ntriples, format);
			
//            conn.setRequestProperty("Content-Length", ""
//                    + Integer.toString(data.length));
//
//			final OutputStream os = conn.getOutputStream();
//			try {
//			    os.write(data);
//				os.flush();
//			} finally {
//				os.close();
//			}
//
//			if (log.isInfoEnabled())
//				log.info(conn.getResponseMessage());
//
//			final int rc = conn.getResponseCode();
//			
//			if (rc < 200 || rc >= 300) {
//				throw new IOException(conn.getResponseMessage());
//			}
//
//		} catch (Throwable t) {
//			// clean up the connection resources
//			if (conn != null)
//				conn.disconnect();
//			throw new RuntimeException(t);
//		}
//
//        // Verify the mutation count.
//        assertEquals(ntriples, getMutationResult(conn).mutationCount);

            final RemoteRepository repo = new RemoteRepository(m_serviceURL);
            
            final RemoveOp remove = new RemoveOp(data, format);
            
            assertEquals(ntriples, repo.remove(remove));
            
    }

	/**
	 * Test of POST w/ BODY having data to be loaded.
	 */
    protected void doInsertWithBodyTest(final String method, final int ntriples,
            /*final String servlet,*/ final RDFFormat format) throws Exception {

//		HttpURLConnection conn = null;
//		try {
//
//			final URL url = new URL(m_serviceURL + servlet);
//			conn = (HttpURLConnection) url.openConnection();
//			conn.setRequestMethod(method);
//			conn.setDoOutput(true);
//			conn.setDoInput(true);
//			conn.setUseCaches(false);
//			conn.setReadTimeout(0);
//			
//            conn.setRequestProperty("Content-Type", format
//                            .getDefaultMIMEType());
//
//            final byte[] data = genNTRIPLES(ntriples, format);
//
//            conn.setRequestProperty("Content-Length", Integer.toString(data
//                    .length));
//
//			final OutputStream os = conn.getOutputStream();
//			try {
//			    os.write(data);
//				os.flush();
//			} finally {
//				os.close();
//			}
//			// conn.connect();
//
//			final int rc = conn.getResponseCode();
//
//            if (log.isInfoEnabled()) {
//                log.info("*** RESPONSE: " + rc + " for " + method);
////                log.info("*** RESPONSE: " + getResponseBody(conn));
//            }
//
//			if (rc < 200 || rc >= 300) {
//
//			    throw new IOException(conn.getResponseMessage());
//			    
//			}
//
//		} catch (Throwable t) {
//			// clean up the connection resources
//			if (conn != null)
//				conn.disconnect();
//			throw new RuntimeException(t);
//		}
//
//        // Verify the mutation count.
//        assertEquals(ntriples, getMutationResult(conn).mutationCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
        final byte[] data = genNTRIPLES(ntriples, format);
//        final File file = File.createTempFile("bigdata-testnssclient", ".data");
        /*
         * Only for testing. Clients should use AddOp(File, RDFFormat).
         */
        final AddOp add = new AddOp(data, format);
        assertEquals(ntriples, repo.add(add));
		
		// Verify the expected #of statements in the store.
		{
			final String queryStr = "select * where {?s ?p ?o}";

//			final QueryOptions opts = new QueryOptions();
//			opts.serviceURL = m_serviceURL;
//			opts.queryStr = queryStr;
//			opts.method = "GET";
//
//			assertEquals(ntriples, countResults(doSparqlQuery(opts, requestPath)));
			
			final TupleQuery query = repo.prepareTupleQuery(queryStr);
			assertEquals(ntriples, countResults(query.evaluate()));
			
		}

    }

    /**
     * Insert a resource into the {@link NanoSparqlServer}.  This is used to
     * load resources in the test package into the server.
     */
    protected long doInsertbyURL(final String method, /*final String servlet,*/
            final String resource) throws Exception {

        final String uri = new File(resource).toURI().toString();

//        HttpURLConnection conn = null;
//        try {
//
//            // Load the resource into the KB.
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "POST";
//            opts.requestParams = new LinkedHashMap<String, String[]>();
//            opts.requestParams.put("uri", new String[] { uri });
//
//            return getMutationResult(doSparqlQuery(opts, requestPath));
//
//        } catch (Throwable t) {
//            // clean up the connection resources
//            if (conn != null)
//                conn.disconnect();
//            throw new RuntimeException(t);
//        }
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final AddOp add = new AddOp(uri);
        return repo.add(add);

    }

    /**
     * Read the contents of a file.
     * 
     * @param file
     *            The file.
     * @return It's contents.
     */
    protected static String readFromFile(final File file) throws IOException {

        final LineNumberReader r = new LineNumberReader(new FileReader(file));

        try {

            final StringBuilder sb = new StringBuilder();

            String s;
            while ((s = r.readLine()) != null) {

                if (r.getLineNumber() > 1)
                    sb.append("\n");

                sb.append(s);

            }

            return sb.toString();

        } finally {

            r.close();

        }

    }
    
    protected static Graph readGraphFromFile(final File file) throws RDFParseException, RDFHandlerException, IOException {
        
        final RDFFormat format = RDFFormat.forFileName(file.getName());
        
        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(format);

        if (rdfParserFactory == null) {
            throw new RuntimeException("Parser not found: file=" + file
                    + ", format=" + format);
        }

        final RDFParser rdfParser = rdfParserFactory
                .getParser();

        rdfParser.setValueFactory(new ValueFactoryImpl());

        rdfParser.setVerifyData(true);

        rdfParser.setStopAtFirstError(true);

        rdfParser
                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

        final StatementCollector rdfHandler = new StatementCollector();
        
        rdfParser.setRDFHandler(rdfHandler);

        /*
         * Run the parser, which will cause statements to be
         * inserted.
         */

        final FileReader r = new FileReader(file);
        try {
            rdfParser.parse(r, file.toURI().toString()/* baseURL */);
        } finally {
            r.close();
        }
        
        final Graph g = new GraphImpl();
        
        g.addAll(rdfHandler.getStatements());

        return g;

    }
    
    /**
     * Write a graph on a buffer suitable for sending as an HTTP request body.
     * 
     * @param format
     *            The RDF Format to use.
     * @param g
     *            The graph.
     *            
     * @return The serialized data.
     * 
     * @throws RDFHandlerException
     */
    static protected byte[] writeOnBuffer(final RDFFormat format, final Graph g)
            throws RDFHandlerException {

        final RDFWriterFactory writerFactory = RDFWriterRegistry.getInstance()
                .get(format);

        if (writerFactory == null)
            fail("RDFWriterFactory not found: format=" + format);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final RDFWriter writer = writerFactory.getWriter(baos);

        writer.startRDF();

        for (Statement stmt : g) {

            writer.handleStatement(stmt);

        }

        writer.endRDF();

        return baos.toByteArray();

    }

    /**
     * Load and return a graph from a resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return The graph.
     */
    protected Graph loadGraphFromResource(final String resource)
            throws RDFParseException, RDFHandlerException, IOException {

//        final RDFFormat rdfFormat = RDFFormat.forFileName(resource);

        final Graph g = readGraphFromFile(new File(resource));

        return g;

    }
    
    /**
     * Reads a resource and sends it using an INSERT with BODY request to be
     * loaded into the database.
     * 
     * @param method
     * @param servlet
     * @param resource
     * @return
     * @throws Exception
     */
    protected long doInsertByBody(final String method,
            /*final String servlet,*/ final RDFFormat rdfFormat, final Graph g,
            final URI defaultContext) throws Exception {

        final byte[] wireData = writeOnBuffer(rdfFormat, g);

//        HttpURLConnection conn = null;
//        try {
//
//            final URL url = new URL(m_serviceURL
//                    + servlet
//                    + (defaultContext == null ? ""
//                            : ("?context-uri=" + URLEncoder.encode(
//                                    defaultContext.stringValue(), "UTF-8"))));
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod(method);
//            conn.setDoOutput(true);
//            conn.setDoInput(true);
//            conn.setUseCaches(false);
//            conn.setReadTimeout(0);
//
//            conn.setRequestProperty("Content-Type",
//                    rdfFormat.getDefaultMIMEType());
//
//            final byte[] data = wireData;
//
//            conn.setRequestProperty("Content-Length",
//                    Integer.toString(data.length));
//
//            final OutputStream os = conn.getOutputStream();
//            try {
//                os.write(data);
//                os.flush();
//            } finally {
//                os.close();
//            }
//            // conn.connect();
//
//            final int rc = conn.getResponseCode();
//
//            if (log.isInfoEnabled()) {
//                log.info("*** RESPONSE: " + rc + " for " + method);
//                // log.info("*** RESPONSE: " + getResponseBody(conn));
//            }
//
//            if (rc < 200 || rc >= 300) {
//
//                throw new IOException(conn.getResponseMessage());
//
//            }
//
//            return getMutationResult(conn);
//
//        } catch (Throwable t) {
//            // clean up the connection resources
//            if (conn != null)
//                conn.disconnect();
//            throw new RuntimeException(t);
//        }

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final AddOp add = new AddOp(wireData, rdfFormat);
        if (defaultContext != null)
        	add.setContext(defaultContext.stringValue());
        return repo.add(add);

    }
    
    protected static String getResponseBody(final HttpURLConnection conn)
            throws IOException {

        final Reader r = new InputStreamReader(conn.getInputStream());

        try {

            final StringWriter w = new StringWriter();

            int ch;
            while ((ch = r.read()) != -1) {

                w.append((char) ch);

            }

            return w.toString();
        
        } finally {
            
            r.close();
            
        }
        
    }

    /**
     * Inserts some data into the KB and then issues a DESCRIBE query against
     * the REST API and verifies the expected results.
     * 
     * @param format
     *            The format is used to specify the Accept header.
     * 
     * @throws Exception
     */
    protected void doDescribeTest(final String method, final RDFFormat format)
            throws Exception {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        
        try {

            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();
            try {

                cxn.setAutoCommit(false);

                cxn.add(mike, RDF.TYPE, person);
                cxn.add(mike, likes, rdf);
                cxn.add(mike, RDFS.LABEL, label1);
                cxn.add(bryan, RDF.TYPE, person);
                cxn.add(bryan, likes, rdfs);
                cxn.add(bryan, RDFS.LABEL, label2);

                /*
                 * Note: The either flush() or commit() is required to flush the
                 * statement buffers to the database before executing any
                 * operations that go around the sail.
                 */
                cxn.commit();
            } finally {
                cxn.close();
            }
             
        } finally {
            sail.shutDown();
        }

        // The expected results.
        final Graph expected = new GraphImpl();
        {
            expected.add(new StatementImpl(mike, likes, rdf));
            expected.add(new StatementImpl(mike, RDF.TYPE, person));
            expected.add(new StatementImpl(mike, RDFS.LABEL, label1));
        }
        
        // Run the query and verify the results.
        {
            
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = method;
//            opts.acceptHeader = format.getDefaultMIMEType();
//            opts.queryStr =//
//                "prefix bd: <"+BD.NAMESPACE+"> " +//
//                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
//                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
//                "DESCRIBE ?x " +//
//                "WHERE { " +//
//                "  ?x rdf:type bd:Person . " +//
//                "  ?x bd:likes bd:RDF " +//
//                "}";
//
//            final Graph actual = buildGraph(doSparqlQuery(opts, requestPath));
            
        	final String queryStr =
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "DESCRIBE ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";

        	final RemoteRepository remote = new RemoteRepository(m_serviceURL);
            final GraphQuery query = remote.prepareGraphQuery(queryStr);
            final Graph actual = query.evaluate();

            assertSameGraph(expected, actual);
            
        }

    }
    
    /**
     * Sets up a simple data set on the server.
     * 
     * @throws SailException
     * @throws RepositoryException
     */
    protected void setupDataOnServer() throws SailException, RepositoryException {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

        final BigdataSail sail = getSail();
        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            
            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();
            try {

                cxn.setAutoCommit(false);

                cxn.add(mike, RDF.TYPE, person);
                cxn.add(mike, likes, rdf);
                cxn.add(mike, RDFS.LABEL, label1);
                cxn.add(bryan, RDF.TYPE, person);
                cxn.add(bryan, likes, rdfs);
                cxn.add(bryan, RDFS.LABEL, label2);

                /*
                 * Note: The either flush() or commit() is required to flush the
                 * statement buffers to the database before executing any
                 * operations that go around the sail.
                 */
                cxn.commit();
            } finally {
                cxn.close();
            }
             
        } finally {
            sail.shutDown();
        }
    }
    
    /**
     * Sets up a simple data set on the server.
     * 
     * @throws SailException
     * @throws RepositoryException
     */
    protected void setupQuadsDataOnServer() throws SailException, RepositoryException {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final URI c1 = new URIImpl(BD.NAMESPACE + "c1");
        final URI c2 = new URIImpl(BD.NAMESPACE + "c2");
        final URI c3 = new URIImpl(BD.NAMESPACE + "c3");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

        final BigdataSail sail = getSail();
        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            
            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();
            try {

                cxn.setAutoCommit(false);

                cxn.add(mike, RDF.TYPE, person, c1, c2, c3);
                cxn.add(mike, likes, rdf, c1, c2, c3);
                cxn.add(mike, RDFS.LABEL, label1, c1, c2, c3);
                cxn.add(bryan, RDF.TYPE, person, c1, c2, c3);
                cxn.add(bryan, likes, rdfs, c1, c2, c3);
                cxn.add(bryan, RDFS.LABEL, label2, c1, c2, c3);

                /*
                 * Note: The either flush() or commit() is required to flush the
                 * statement buffers to the database before executing any
                 * operations that go around the sail.
                 */
                cxn.commit();
            } finally {
                cxn.close();
            }
             
        } finally {
            sail.shutDown();
        }
    }
    
    protected void doConstructTest(final String method, final RDFFormat format)
            throws Exception {
        
        setupDataOnServer();
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
//        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
//        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
//        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
//        final Literal label1 = new LiteralImpl("Mike");
//        final Literal label2 = new LiteralImpl("Bryan");
//
//        final BigdataSail sail = getSail();
//        sail.initialize();
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//        
//        try {
//
//            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
//                    .getConnection();
//            try {
//
//                cxn.setAutoCommit(false);
//
//                cxn.add(mike, RDF.TYPE, person);
//                cxn.add(mike, likes, rdf);
//                cxn.add(mike, RDFS.LABEL, label1);
//                cxn.add(bryan, RDF.TYPE, person);
//                cxn.add(bryan, likes, rdfs);
//                cxn.add(bryan, RDFS.LABEL, label2);
//
//                /*
//                 * Note: The either flush() or commit() is required to flush the
//                 * statement buffers to the database before executing any
//                 * operations that go around the sail.
//                 */
//                cxn.commit();
//            } finally {
//                cxn.close();
//            }
//             
//        } finally {
//            sail.shutDown();
//        }

        // The expected results.
        final Graph expected = new GraphImpl();
        {
//            expected.add(new StatementImpl(mike, likes, rdf));
            expected.add(new StatementImpl(mike, RDF.TYPE, person));
            expected.add(new StatementImpl(bryan, RDF.TYPE, person));
//            expected.add(new StatementImpl(mike, RDFS.LABEL, label1));
        }
        
        // Run the query and verify the results.
        {
            
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = method;
//            opts.acceptHeader = format.getDefaultMIMEType();
//            opts.queryStr =//
//                "prefix bd: <"+BD.NAMESPACE+"> " +//
//                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
//                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
//                "CONSTRUCT { ?x rdf:type bd:Person }" +//
//                "WHERE { " +//
//                "  ?x rdf:type bd:Person . " +//
////                "  ?x bd:likes bd:RDF " +//
//                "}";
//
//            final Graph actual = buildGraph(doSparqlQuery(opts, requestPath));

            final String queryStr =
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "CONSTRUCT { ?x rdf:type bd:Person }" +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
//                "  ?x bd:likes bd:RDF " +//
                "}";

        	final RemoteRepository remote = new RemoteRepository(m_serviceURL);
            final GraphQuery query = remote.prepareGraphQuery(queryStr);
            final Graph actual = query.evaluate();

            assertSameGraph(expected, actual);
            
        }
    
    }
    
    /**
     * Compare two graphs for equality.
     * <p>
     * Note: This is not very efficient if the {@link Graph} implementations are
     * not indexed.
     * <p>
     * Note: This does not handle equality testing with blank nodes (it does not
     * test for isomorphic graphs).
     * 
     * @param expected
     * @param actual
     */
    protected void assertSameGraph(final Graph expected, final Graph actual) {

        for (Statement s : expected) {

            if (!actual.contains(s))
                fail("Expecting: " + s);

        }

        assertEquals("size", expected.size(), actual.size());

    }

}
