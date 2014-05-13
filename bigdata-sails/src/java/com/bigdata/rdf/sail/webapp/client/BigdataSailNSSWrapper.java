/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.client;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;

import com.bigdata.BigdataStatics;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.util.config.NicUtil;

public class BigdataSailNSSWrapper {
    
    private static final transient Logger log = Logger
            .getLogger(BigdataSailNSSWrapper.class);


    public final BigdataSail sail;
    
    /**
     * A jetty {@link Server} running a {@link NanoSparqlServer} instance which
     * is running against that {@link #m_indexManager}.
     */
    protected Server m_fixture;

    /**
     * The {@link ClientConnectionManager} for the {@link HttpClient} used by
     * the {@link RemoteRepository}. This is used when we tear down the
     * {@link RemoteRepository}.
     */
    private ClientConnectionManager m_cm;
    
    /**
     * Exposed to tests that do direct HTTP GET/POST operations.
     */
    protected HttpClient m_httpClient = null;

    /**
     * The client-API wrapper to the NSS.
     */
    public RemoteRepositoryManager m_repo;

    /**
     * The effective {@link NanoSparqlServer} http end point (including the
     * ContextPath).
     */
    protected String m_serviceURL;

    /**
     * The URL of the root of the web application server. This does NOT include
     * the ContextPath for the webapp.
     * 
     * <pre>
     * http://localhost:8080 -- root URL
     * http://localhost:8080/bigdata -- webapp URL (includes "/bigdata" context path.
     * </pre>
     */
    protected String m_rootURL;
    
    public BigdataSailNSSWrapper(final BigdataSail sail) {
        this.sail = sail;
    }
    
    public void init() throws Exception {
        
        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, sail.getDatabase().getNamespace());

            initParams.put(ConfigParams.CREATE, "false");
            
        }
        // Start server for that kb instance.
        m_fixture = NanoSparqlServer.newInstance(0/* port */,
                sail.getDatabase().getIndexManager(), initParams);

        m_fixture.start();

        final int port = NanoSparqlServer.getLocalPort(m_fixture);

        // log.info("Getting host address");

        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);

        if (hostAddr == null) {

            throw new RuntimeException("Could not identify network address for this host.");

        }

        m_rootURL = new URL("http", hostAddr, port, ""/* contextPath */
        ).toExternalForm();

        m_serviceURL = new URL("http", hostAddr, port,
                BigdataStatics.getContextPath()).toExternalForm();

        if (log.isInfoEnabled())
            log.info("Setup done: \nrootURL=" + m_rootURL + "\nserviceURL="
                    + m_serviceURL);

//        final HttpClient httpClient = new DefaultHttpClient();

//        m_cm = httpClient.getConnectionManager();
        
        m_cm = DefaultClientConnectionManagerFactory.getInstance()
                .newInstance();

        final DefaultHttpClient httpClient = new DefaultHttpClient(m_cm);
        m_httpClient = httpClient;
        
        /*
         * Ensure that the client follows redirects using a standard policy.
         * 
         * Note: This is necessary for tests of the webapp structure since the
         * container may respond with a redirect (302) to the location of the
         * webapp when the client requests the root URL.
         */
        httpClient.setRedirectStrategy(new DefaultRedirectStrategy());

        m_repo = new RemoteRepositoryManager(m_serviceURL,
                m_httpClient,
                sail.getDatabase().getIndexManager().getExecutorService());

    }

    public void shutdown() throws Exception {

        if (m_fixture != null) {

            m_fixture.stop();

            m_fixture = null;

        }

        m_rootURL = null;
        m_serviceURL = null;
        
        if (m_cm != null) {
            m_cm.shutdown();
            m_cm = null;
        }

        m_httpClient = null;
        m_repo = null;
        
        if (log.isInfoEnabled())
            log.info("tear down done");
        
    }

    
}

