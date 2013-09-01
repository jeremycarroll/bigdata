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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestListener;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.ResultPrinter;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * Test suite for {@link RESTServlet} (SPARQL end point and REST API for RDF
 * data).
 * 
 * TODO Add unit tests which exercise the full text index.
 * 
 * TODO Add unit tests which are specific to sids and quads modes. These tests
 * should look at the semantics of interchange of sids or quads specific data;
 * queries which exercise the context position; and the default-graph and
 * named-graph URL query parameters for quads.
 * 
 * @todo Security model?
 * 
 * @todo An NQUADS RDFWriter needs to be written. Then we can test NQUADS
 *       interchange.
 * 
 * @todo A SPARQL result sets JSON parser needs to be written (Sesame bundles a
 *       writer, but not a parser) before we can test queries which CONNEG for a
 *       JSON result set.
 * 
 * @todo Tests which verify the correct rejection of illegal or ill-formed
 *       requests.
 * 
 * @todo Test suite for reading from a historical commit point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestNanoSparqlServer.java 4398 2011-04-14 13:55:29Z thompsonbry
 *          $
 */
public class TestNanoSparqlServerWithProxyIndexManager<S extends IIndexManager>
        extends AbstractIndexManagerTestCase<S> {
	
	static {
		ProxySuiteHelper.proxyIndexManagerTestingHasStarted = true;
	}

	/**
	 * The {@link IIndexManager} for the backing persistence engine (may be a
	 * {@link Journal} or {@link JiniFederation}).
	 */
	private IIndexManager m_indexManager;

	/**
	 * The mode in which the test is running.
	 */
	private TestMode testMode;

	/**
	 * Run in triples mode on a temporary journal.
	 */
	public TestNanoSparqlServerWithProxyIndexManager() {

		this(null/* name */, getTemporaryJournal(), TestMode.triples);

	}

	/**
	 * Run in triples mode on a temporary journal.
	 */
	public TestNanoSparqlServerWithProxyIndexManager(String name) {

		this(name, getTemporaryJournal(), TestMode.triples);

	}

	static Journal getTemporaryJournal() {

		final Properties properties = new Properties();

		properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
				BufferMode.Transient.toString());

		properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, ""
				+ (Bytes.megabyte32 * 1));

		return new Journal(properties);

	}

	/**
	 * Run test suite against an embedded {@link NanoSparqlServer} instance,
	 * which is in turn running against the caller's {@link IIndexManager}.
	 * 
	 * @param indexManager
	 *            The {@link Journal} or {@link JiniFederation}.
	 * @param testMode
	 *            Identifies what mode the kb instance will be using.
	 */
	private TestNanoSparqlServerWithProxyIndexManager(final String name,
			final IIndexManager indexManager, final TestMode testMode) {

		super(name == null ? TestNanoSparqlServerWithProxyIndexManager.class.getName()
				: name);

		this.m_indexManager = indexManager;
		
		this.testMode = testMode;
		
	}

	/**
	 * Return suite running in triples mode against a temporary journal.
	 */
	public static Test suite() {

		return suite(TestMode.triples);

	}
	
	/**
	 * Return suite running in the specified mode against a temporary journal.
	 */
	public static Test suite(final TestMode testMode) {

		return suite(getTemporaryJournal(), testMode);

	}
	
    /**
     * The {@link TestMode#triples} test suite.
     */
    public static class test_NSS_triples extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(), TestMode.triples);
        }
    }
    
    /**
     * The {@link TestMode#quads} test suite.
     */
    public static class Test_NSS_quads extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(), TestMode.quads);
        }
    }
    
    /**
     * The {@link TestMode#sids} test suite.
     */
    public static class Test_NSS_sids extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(), TestMode.sids);
        }
    }
    
	/**
	 * Return suite running in the given mode against the given
	 * {@link IIndexManager}.
	 */
	public static TestSuite suite(final IIndexManager indexManager,
			final TestMode testMode) {

		final ProxyTestSuite suite = createProxyTestSuite(indexManager,testMode);

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class (if any).
//        suite.addTestSuite(TestNanoSparqlServerOnFederation.class);

        /*
         * Proxied test suites.
         */
		
		//Protocol
		suite.addTest(TestProtocolAll.suite());
        
        // Multi-tenancy API.
        suite.addTestSuite(TestMultiTenancyAPI.class);

        // RemoteRepository test (nano sparql server client-wrapper)
        suite.addTestSuite(TestNanoSparqlClient.class);

        // BigdataSailRemoteRepository test (nano sparql server client-wrapper)
        suite.addTestSuite(TestBigdataSailRemoteRepository.class);
        
        // Insert tests from trac issues
        suite.addTestSuite(TestInsertFilterFalse727.class);
        suite.addTestSuite(TestCBD731.class);
        

        // SPARQL UPDATE test suite.
        switch(testMode) {
        case triples:
            // TODO TRIPLES mode UPDATE test suite.
            break;
        case sids:
            // TODO SIDS mode UPDATE test suite.
            break;
        case quads:
            // QUADS mode UPDATE test suite. 
            suite.addTestSuite(TestSparqlUpdate.class);
            break;
        default: throw new UnsupportedOperationException();
        }

        // SPARQL 1.1 Federated Query.
        suite.addTestSuite(TestFederatedQuery.class);

        return suite;
    
    }

	static ProxyTestSuite createProxyTestSuite(final IIndexManager indexManager, final TestMode testMode) {
		final TestNanoSparqlServerWithProxyIndexManager<?> delegate = new TestNanoSparqlServerWithProxyIndexManager(
				null/* name */, indexManager, testMode); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "NanoSparqlServer Proxied Test Suite");
		return suite;
	}

	@SuppressWarnings("unchecked")
    public S getIndexManager() {

		return (S) m_indexManager;

	}
    
    @Override
	public Properties getProperties() {

//    	System.err.println("testMode="+testMode);
    	
	    final Properties properties = new Properties();

		switch (testMode) {
		case quads:
			properties.setProperty(AbstractTripleStore.Options.QUADS_MODE,
					"true");
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
					"false");
			properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
					NoAxioms.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.VOCABULARY_CLASS,
					NoVocabulary.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
			break;
		case triples:
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
					"false");
			properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
					NoAxioms.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.VOCABULARY_CLASS,
					NoVocabulary.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
			break;
		case sids:
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
					"false");
			properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
					NoAxioms.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.VOCABULARY_CLASS,
					NoVocabulary.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "true");
			break;
		default:
			fail("Unknown mode: " + testMode);
		}
		// if (false/* triples w/ truth maintenance */) {
		// properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
		// "false");
		// }
		// if (false/* sids w/ truth maintenance */) {
		// properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
		// "true");
		// }

		return properties;
	}

    /**
     * Open the {@link IIndexManager} identified by the property file.
     * 
     * @param propertyFile
     *            The property file (for a standalone bigdata instance) or the
     *            jini configuration file (for a bigdata federation). The file
     *            must end with either ".properties" or ".config".
     *            
     * @return The {@link IIndexManager}.
     */
    static private IIndexManager openIndexManager(final String propertyFile) {

        final File file = new File(propertyFile);

        if (!file.exists()) {

            throw new RuntimeException("Could not find file: " + file);

        }

        boolean isJini = false;
        if (propertyFile.endsWith(".config")) {
            // scale-out.
            isJini = true;
        } else if (propertyFile.endsWith(".properties")) {
            // local journal.
            isJini = false;
        } else {
            /*
             * Note: This is a hack, but we are recognizing the jini
             * configuration file with a .config extension and the journal
             * properties file with a .properties extension.
             */
            throw new RuntimeException(
                    "File must have '.config' or '.properties' extension: "
                            + file);
        }

        final IIndexManager indexManager;
        try {

            if (isJini) {

                /*
                 * A bigdata federation.
                 */

				@SuppressWarnings("rawtypes")
                final JiniClient<?> jiniClient = new JiniClient(
						new String[] { propertyFile });

                indexManager = jiniClient.connect();

            } else {

                /*
                 * Note: we only need to specify the FILE when re-opening a
                 * journal containing a pre-existing KB.
                 */
                final Properties properties = new Properties();
                {
                    // Read the properties from the file.
                    final InputStream is = new BufferedInputStream(
                            new FileInputStream(propertyFile));
                    try {
                        properties.load(is);
                    } finally {
                        is.close();
                    }
                    if (System.getProperty(BigdataSail.Options.FILE) != null) {
                        // Override/set from the environment.
                        properties.setProperty(BigdataSail.Options.FILE, System
                                .getProperty(BigdataSail.Options.FILE));
                    }
					if (properties
							.getProperty(com.bigdata.journal.Options.FILE) == null) {
						// Run against a transient journal if no file was
						// specified.
						properties.setProperty(
								com.bigdata.journal.Options.BUFFER_MODE,
								BufferMode.Transient.toString());
						properties.setProperty(
								com.bigdata.journal.Options.INITIAL_EXTENT, ""
										+ (Bytes.megabyte32 * 1));
					}

                }

                indexManager = new Journal(properties);

            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return indexManager;
        
    }

	/**
	 * Runs the test suite against an {@link IBigdataFederation} or a
	 * {@link Journal}. The federation must already be up and running. An
	 * embedded {@link NanoSparqlServer} instance will be created for each test
	 * run. Each test will run against a distinct KB instance within a unique
	 * bigdata namespace on the same backing {@link IIndexManager}.
	 * <p>
	 * When run for CI, this can be executed as:
	 * <pre>
	 * ... -Djava.security.policy=policy.all TestNanoSparqlServerWithProxyIndexManager triples /nas/bigdata/benchmark/config/bigdataStandalone.config
	 * </pre>
	 * 
	 * @param args
	 *            <code>
	 * (testMode) (propertyFile|configFile)
	 * </code>
	 * 
	 *            where propertyFile is the configuration file for a
	 *            {@link Journal}. <br/>
	 *            where configFile is the configuration file for an
	 *            {@link IBigdataFederation}.<br/>
	 *            where <i>triples</i> or <i>sids</i> or <i>quads</i> is the
	 *            database mode.</br> where <i>tm</i> indicates that truth
	 *            maintenance should be enabled (only valid with triples or
	 *            sids).
	 */
    public static void main(final String[] args) throws Exception {

		if (args.length < 2) {
			System.err
					.println("(triples|sids|quads) (propertyFile|configFile) (tm)?");
			System.exit(1);
		}

		final TestMode testMode = TestMode.valueOf(args[0]);

//		if (testMode != TestMode.triples)
//			fail("Unsupported test mode: " + testMode);
		
		final File propertyFile = new File(args[1]);

		if (!propertyFile.exists())
			fail("No such file: " + propertyFile);

    	// Setup test result.
    	final TestResult result = new TestResult();
    	
    	// Setup listener, which will write the result on System.out
    	result.addListener(new ResultPrinter(System.out));
    	
    	result.addListener(new TestListener() {
			
			public void startTest(Test arg0) {
				log.info(arg0);
			}
			
			public void endTest(Test arg0) {
				log.info(arg0);
			}
			
			public void addFailure(Test arg0, AssertionFailedError arg1) {
				log.error(arg0,arg1);
			}
			
			public void addError(Test arg0, Throwable arg1) {
				log.error(arg0,arg1);
			}
		});
    	
    	// Open Journal / Connect to the configured federation.
		final IIndexManager indexManager = openIndexManager(propertyFile
				.getAbsolutePath());

        try {

        	// Setup test suite
			final Test test = TestNanoSparqlServerWithProxyIndexManager.suite(
					indexManager, testMode);

        	// Run the test suite.
        	test.run(result);
        	
        } finally {

			if (indexManager instanceof JiniFederation<?>) {
				// disconnect
				((JiniFederation<?>) indexManager).shutdownNow();
			} else {
				// destroy journal.
				((Journal) indexManager).destroy();
			}

        }

		final String msg = "nerrors=" + result.errorCount() + ", nfailures="
				+ result.failureCount() + ", nrun=" + result.runCount();
        
		if (result.errorCount() > 0 || result.failureCount() > 0) {

			// At least one test failed.
			fail(msg);

		}

        // All green.
		System.out.println(msg);
        
    }
    
}
