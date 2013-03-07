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
/*
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.btree.BytesUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.msg.HADigestRequest;
import com.bigdata.ha.msg.HALogDigestRequest;
import com.bigdata.io.TestCase3;
import com.bigdata.rdf.sail.TestConcurrentKBCreate;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for {@link HAJournalServer} test suites.
 * <p>
 * Note: jini and zookeeper MUST be running and configured appropriate for the
 * configuration files used by the {@link HAJournalServer} instances in this
 * test suite. You MUST specify a sufficiently lax security policy.
 * 
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractHAJournalServerTestCase extends TestCase3 {

    public AbstractHAJournalServerTestCase() {
    }

    public AbstractHAJournalServerTestCase(String name) {
        super(name);
    }

    /**
     * Path to the config files.
     */
    static final protected String SRC_PATH = "bigdata-jini/src/test/com/bigdata/journal/jini/ha/";

    /**
     * Path to the directory in which the service directories exist. The
     * individual service directories are formed by adding the service name.
     */
    static final protected String TGT_PATH = "benchmark/CI-HAJournal-1/";

    /**
     * A service used to run tasks for some tests.
     */
    protected ExecutorService executorService = null;

    /**
     * Used to talk to the {@link NanoSparqlServer}.
     */
    protected ClientConnectionManager ccm = null;
    
    @Override
    protected void setUp() throws Exception {

        executorService = Executors
                .newCachedThreadPool(new DaemonThreadFactory(getName()));

        ccm = DefaultClientConnectionManagerFactory.getInstance().newInstance();

    }

    @Override
    protected void tearDown() throws Exception {

        if (executorService != null) {

            executorService.shutdownNow();

            executorService = null;

        }

        if (ccm != null) {

            ccm.shutdown();

            ccm = null;

        }

    }

    /**
     * Http connection.
     * 
     * @param opts
     *            The connection options.
     * 
     * @return The connection.
     */
    protected HttpResponse doConnect(final HttpClient httpClient,
            final ConnectOptions opts) throws Exception {

        /*
         * Generate the fully formed and encoded URL.
         */
        
        final StringBuilder urlString = new StringBuilder(opts.serviceURL);

        ConnectOptions.addQueryParams(urlString, opts.requestParams);

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(opts.serviceURL);
            log.debug(opts.method);
            log.debug("query=" + opts.getRequestParam("query"));
        }

        HttpUriRequest request = null;
        try {

            request = newRequest(urlString.toString(), opts.method);
            
            if (opts.acceptHeader != null) {
            
                request.addHeader("Accept", opts.acceptHeader);
                
                if (log.isDebugEnabled())
                    log.debug("Accept: " + opts.acceptHeader);
                
            }

            if (opts.entity != null) {

                ((HttpEntityEnclosingRequestBase) request).setEntity(opts.entity);

            }

            final HttpResponse response = httpClient.execute(request);
            
            return response;
            
//            // connect.
//            conn.connect();
//
//            return conn;

        } catch (Throwable t) {
            /*
             * If something goes wrong, then close the http connection.
             * Otherwise, the connection will be closed by the caller.
             */
            try {
                
                if (request != null)
                    request.abort();
                
//                // clean up the connection resources
//                if (conn != null)
//                    conn.disconnect();
                
            } catch (Throwable t2) {
                // ignored.
            }
            throw new RuntimeException(opts.serviceURL + " : " + t, t);
        }

    }

    static protected HttpUriRequest newRequest(final String uri,
            final String method) {
        if (method.equals("GET")) {
            return new HttpGet(uri);
        } else if (method.equals("POST")) {
            return new HttpPost(uri);
        } else if (method.equals("DELETE")) {
            return new HttpDelete(uri);
        } else if (method.equals("PUT")) {
            return new HttpPut(uri);
        } else {
            throw new IllegalArgumentException();
        }
    }

    void doNSSStatusRequest(final HAGlue haGlue) throws Exception {

        // The port that is exposing the NSS.
        final int NSSPort = haGlue.getNSSPort();
        
        // Client for talking to the NSS.
        final HttpClient httpClient = new DefaultHttpClient(ccm);

        /*
         * Check the NSS API. It should be up.
         * 
         * Note: The NSS server will not allow reads or writes until the
         * quorum meets.
         * 
         * Note: The quorum will not meet for this unit test since we are
         * only running one HAJournalServer here.
         */
        {
            // The NSS service URL (NOT the SPARQL end point).
            final String serviceURL = "http://localhost:" + NSSPort;

            final ConnectOptions opts = new ConnectOptions(serviceURL
                    + "/status");

            opts.method = "GET";

            try {
                final HttpResponse response;

                RemoteRepository.checkResponseCode(response = doConnect(
                        httpClient, opts));
                
                EntityUtils.consume(response.getEntity());
                
            } catch (IOException ex) {
                
                log.error(ex, ex);
                
            }

        }

    }
    
    /**
     * Return a {@link RemoteRepository} for talking to the
     * {@link NanoSparqlServer} instance associated with an {@link HAGlue}
     * interface.
     * 
     * @throws IOException
     */
    protected RemoteRepository getRemoteRepository(final HAGlue haGlue)
            throws IOException {

        final String sparqlEndpointURL = "http://localhost:"
                + haGlue.getNSSPort() + "/sparql";

        // Client for talking to the NSS.
        final HttpClient httpClient = new DefaultHttpClient(ccm);

        final RemoteRepository repo = new RemoteRepository(sparqlEndpointURL,
                httpClient, executorService);

        return repo;
        
    }

    /**
     * Counts the #of results in a SPARQL result set.
     * 
     * @param result
     *            The connection from which to read the results.
     * 
     * @return The #of results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    protected long countResults(final TupleQueryResult result) throws Exception {

        long count = 0;

        while (result.hasNext()) {

            result.next();

            count++;

        }

        result.close();

        return count;

    }

    /**
     * Wait until the KB exists.
     * 
     * Note: There is a data race when creating the a KB (especially the default
     * KB) and verifying that the KB exists. If we find the KB in the row store
     * cache but we do not find the axioms, then the subsequent attempts to
     * resolve the KB fail - probably due to an issue with the default resource
     * locator cache.
     * 
     * <pre>
     * INFO : 41211 2012-11-06 08:38:41,874 : WARN : 8542 2012-11-06 08:38:41,873      qtp877533177-45 org.eclipse.jetty.util.log.Slf4jLog.warn(Slf4jLog.java:50): /sparql
     * INFO : 41211 2012-11-06 08:38:41,874 : java.lang.RuntimeException: java.lang.RuntimeException: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
     * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1120)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doGet(QueryServlet.java:178)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.RESTServlet.doGet(RESTServlet.java:175)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at javax.servlet.http.HttpServlet.service(HttpServlet.java:707)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at javax.servlet.http.HttpServlet.service(HttpServlet.java:820)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHolder.handle(ServletHolder.java:534)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:475)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:929)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:403)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:864)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:117)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.HandlerList.handle(HandlerList.java:47)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:114)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.Server.handle(Server.java:352)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection.handleRequest(HttpConnection.java:596)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection$RequestHandler.headerComplete(HttpConnection.java:1051)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.http.HttpParser.parseNext(HttpParser.java:590)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.http.HttpParser.parseAvailable(HttpParser.java:212)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection.handle(HttpConnection.java:426)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint.handle(SelectChannelEndPoint.java:508)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint.access$000(SelectChannelEndPoint.java:34)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint$1.run(SelectChannelEndPoint.java:40)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.util.thread.QueuedThreadPool$2.run(QueuedThreadPool.java:451)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at java.lang.Thread.run(Thread.java:680)
     * INFO : 41212 2012-11-06 08:38:41,875 : Caused by: java.lang.RuntimeException: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1102)
     * INFO : 41212 2012-11-06 08:38:41,875 :    ... 23 more
     * INFO : 41212 2012-11-06 08:38:41,875 : Caused by: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.store.AbstractTripleStore.getAxioms(AbstractTripleStore.java:1787)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.BigdataSail.<init>(BigdataSail.java:934)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.BigdataSail.<init>(BigdataSail.java:891)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.BigdataRDFContext.getQueryConnection(BigdataRDFContext.java:1858)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1074)
     * INFO : 41212 2012-11-06 08:38:41,875 :    ... 23 more
     * </pre>
     * 
     * @param haGlue
     *            The server.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/617" >
     *      Concurrent KB create fails with "No axioms defined?" </a>
     * 
     * @see TestConcurrentKBCreate
     * 
     *      FIXME This method sometimes deadlocks in the repo.size() call on the
     *      leader (HTTP end point). We need to chase this down with a thread
     *      dump of the leader when it happens.
     */
    protected void awaitKBExists(final HAGlue haGlue) throws IOException {
      
        final RemoteRepository repo = getRemoteRepository(haGlue);
        
        assertCondition(new Runnable() {
            public void run() {
                try {
                    repo.size();
                } catch (Exception e) {
                    // KB does not exist.
                    fail();
                }
            }

        }, 5, TimeUnit.SECONDS);
        
    }
    
    /**
     * A short sleep of the current thread. If interrupted, the interrupt is
     * propagated to the current thread (that is, the interrupt is reset).
     */
    protected void shortSleep() {
        try {
            Thread.sleep(100/*ms*/);
        } catch (InterruptedException e1) {
            // Propagate the interrupt.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Verify that the digests for the backing files on the specified services
     * are equal.
     * 
     * @param services
     *            The services.
     *            
     * @throws NoSuchAlgorithmException
     * @throws DigestException
     * @throws IOException
     */
    protected void assertDigestsEquals(final HAGlue[] services)
            throws NoSuchAlgorithmException, DigestException, IOException {

        // The digest for each HALog file for each commit point.
        final byte[][] digests = new byte[services.length][];

        for (int i = 0; i < services.length; i++) {

            final HAGlue haGlue = services[i];

            digests[i] = haGlue.computeDigest(
                    new HADigestRequest(null/* storeUUID */)).getDigest();

            if (i > 0) {

                if (!BytesUtil.bytesEqual(digests[i - 1], digests[i])) {

                    fail("Services have different digest: serviceA="
                            + services[i - 1] + ", serviceB=" + services[i]);

                }

            }

        }

    }

    /**
     * Verify that HALog files were generated and are available for the
     * specified commit points.
     * <p>
     * Note: The HALog files are identified by the commit counter of the closing
     * commit point. The commit counter is ZERO (0) until the first commit.
     * Thus, the commit counter is ONE (1) for the first closing commit point.
     * 
     * @param firstCommitCounter
     *            The first commit point to be verified.
     * @param lastCommitCounter
     *            The last commit point to be verified.
     * @param services
     *            The set of services whose HALog files will be tested.
     * 
     * @throws IOException
     * @throws DigestException
     * @throws NoSuchAlgorithmException
     */
    protected void assertHALogDigestsEquals(final long firstCommitCounter,
            final long lastCommitCounter, final HAGlue[] services)
            throws NoSuchAlgorithmException, DigestException, IOException {

        // The digest for each HALog file for each commit point.
        final byte[][] digests = new byte[services.length][];

        for (long commitCounter = firstCommitCounter; commitCounter <= lastCommitCounter; commitCounter++) {

            for (int i = 0; i < services.length; i++) {

                final HAGlue haGlue = services[i];

                digests[i] = haGlue.computeHALogDigest(
                        new HALogDigestRequest(commitCounter)).getDigest();

                if (i > 0) {

                    if (!BytesUtil.bytesEqual(digests[i - 1], digests[i])) {

                        fail("Services have different digest: commitCounter="
                                + commitCounter + ", serviceA="
                                + services[i - 1] + ", serviceB=" + services[i]);

                    }

                }

            }

        }

    }

    /**
     * Verify that NO HALog files exist for the specified commit points.
     * <p>
     * Note: The HALog files are identified by the commit counter of the closing
     * commit point. The commit counter is ZERO (0) until the first commit.
     * Thus, the commit counter is ONE (1) for the first closing commit point.
     * 
     * @param firstCommitCounter
     *            The first commit point to be verified.
     * @param lastCommitCounter
     *            The last commit point to be verified.
     * @param services
     *            The set of services whose HALog files will be tested.
     * 
     * @throws IOException
     * @throws DigestException
     * @throws NoSuchAlgorithmException
     */
    protected void assertHALogNotFound(final long firstCommitCounter,
            final long lastCommitCounter, final HAGlue[] services)
            throws NoSuchAlgorithmException, DigestException, IOException {

        for (long commitCounter = firstCommitCounter; commitCounter <= lastCommitCounter; commitCounter++) {

            for (int i = 0; i < services.length; i++) {

                final HAGlue haGlue = services[i];

                try {
                    /*
                     * Request the digest.
                     * 
                     * Note: This will through a (wrapped) FileNotFoundException
                     * if the corresponding HALog does not exist.
                     */
                    haGlue.computeHALogDigest(
                            new HALogDigestRequest(commitCounter)).getDigest();
                    fail("HALog exists: commitCounter=" + commitCounter
                            + ", service=" + services[i]);
                } catch (IOException ex) {
                    if (InnerCause
                            .isInnerCause(ex, FileNotFoundException.class)) {
                        // Expected exception.
                        continue;
                    }

                }
                
            }

        }

    }

    /**
     * Return the name of the foaf data set.
     * 
     * @param string
     *            The data set.
     * @return The name that can be used with a SPARQL UPDATE "LOAD" operation.
     */
    protected String getFoafFileUrl(String string) {

        //sb.append("LOAD <file:/Users/bryan/Documents/workspace/BIGDATA_RELEASE_1_2_0/data-1.nq.gz>;\n");

        /*
         * Note: The file path needs to be absolute since the HAJournalServer
         * instances each run in their own service directory.
         */
 
        final String s = "file:"
                + new File("bigdata-rdf/src/resources/data/foaf/", string)
                        .getAbsolutePath();

        return s;
    }
    
}
