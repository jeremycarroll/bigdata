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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.impl.GraphImpl;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.properties.PropertiesFormat;
import com.bigdata.rdf.properties.PropertiesParser;
import com.bigdata.rdf.properties.PropertiesParserFactory;
import com.bigdata.rdf.properties.PropertiesParserRegistry;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.PropertyUtil;

/**
 * Mult-tenancy Administration Servlet (management for bigdata namespaces). A
 * bigdata namespace corresponds to a partition in the naming of durable
 * resources. A {@link Journal} or {@link IBigdataFederation} may have multiple
 * KB instances, each in their own namespace. This servlet allows you to manage
 * those KB instances using CRUD operations.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/575">
 *      NanoSparqlServer Admin API for Multi-tenant deployments</a>
 * 
 * @author thompsonbry
 */
public class MultiTenancyServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(MultiTenancyServlet.class); 

    /**
     * URL query parameter used to override the servlet init parameter
     * {@link ConfigParams#DESCRIBE_EACH_NAMED_GRAPH}.
     */
    protected static final String DESCRIBE_EACH_NAMED_GRAPH = "describe-each-named-graph";
    
    /**
     * URL query parameter used to specify that only the default namespace
     * should be described.
     */
    protected static final String DESCRIBE_DEFAULT_NAMESPACE = "describe-default-namespace";
    
    /**
     * Delegate for the sparql end point expressed by
     * <code>.../namespace/NAMESPACE/sparql</code>.
     */
    private RESTServlet m_restServlet;

    public MultiTenancyServlet() {

    }

    /**
     * Overridden to create and initialize the delegate {@link Servlet}
     * instances.
     */
    @Override
    public void init() throws ServletException {

        super.init();
        
        m_restServlet = new RESTServlet();
    
        m_restServlet.init(getServletConfig());

    }
        
    /**
     * Handle namespace create.
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (req.getRequestURI().endsWith("/namespace")) {

            // CREATE NAMESPACE.
            doCreateNamespace(req, resp);

            return;
            
        }

        /*
         * Pass through to the SPARQL end point REST API.
         * 
         * Note: This also handles CANCEL QUERY, which is a POST.
         */
        m_restServlet.doPost(req, resp);

    }

    /**
     * Delete the KB associated with the effective namespace.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/689" >
     *      Missing URL encoding in RemoteRepositoryManager </a>
     */
    @Override
    protected void doDelete(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        final String namespace = getNamespace(req);

        if (req.getRequestURI().endsWith(
                "/namespace/" + ConnectOptions.urlEncode(namespace))) {

            // Delete that namespace.
            doDeleteNamespace(req, resp);

            return;

        }
        
        // Pass through to the SPARQL end point REST API.
        m_restServlet.doDelete(req, resp);

    }

    @Override
    protected void doPut(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        // Pass through to the SPARQL end point REST API.
        m_restServlet.doPut(req, resp);

    }
    
    /**
     * Handles all read-only namespace oriented administration requests.
     */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (req.getRequestURI().endsWith("/namespace")) {

            // Describe all namespaces.

            doDescribeNamespaces(req, resp);

            return;
            
        } else if (req.getRequestURI().endsWith("/properties")) {

            // Show properties.

            doShowProperties(req, resp);

            return;
            
        }
        
        // Pass through to the SPARQL end point REST API.
        m_restServlet.doGet(req, resp);

        return;

    }

    /**
     * Create a new namespace.
     * 
     * <pre>
     * Request-URI
     * ...
     * Content-Type=...
     * ...
     * PropertySet
     * </pre>
     * 
     * @param req
     * @param resp
     * @throws IOException
     */
    private void doCreateNamespace(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        final BigdataRDFContext context = getBigdataRDFContext();

        final IIndexManager indexManager = context.getIndexManager();

        /*
         * 1. Read the request entity, which must be some kind of Properties
         * object. The BigdataSail.Options.NAMESPACE property defaults to "kb".
         * A non-default value SHOULD be specified by the client.
         * 
         * 2. Wrap and flatten the base properties for the Journal or
         * Federation. This provides defaults for properties which were not
         * explicitly configured for this KB instance.
         * 
         * 3. Add the given properties to the flattened defaults to obtain the
         * effective properties.
         */
        final Properties given, defaults, effective;
        {        

            final String contentType = req.getContentType();

            if (log.isInfoEnabled())
                log.info("Request body: " + contentType);

            final PropertiesFormat format = PropertiesFormat.forMIMEType(contentType);

            if (format == null) {

                buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "Content-Type not recognized as Properties: "
                                + contentType);

                return;

            }

            if (log.isInfoEnabled())
                log.info("Format=" + format);
            
            final PropertiesParserFactory parserFactory = PropertiesParserRegistry
                    .getInstance().get(format);

            if (parserFactory == null) {

                buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                        "Parser factory not found: Content-Type="
                                + contentType + ", format=" + format);
                
                return;

            }

            /*
             * There is a request body, so let's try and parse it.
             */

            final PropertiesParser parser = parserFactory.getParser();

            // The given Properties.
            given = parser.parse(req.getInputStream());

            /*
             * Get the default Properties.
             */
            if (indexManager instanceof Journal) {

                final Journal jnl = (Journal) indexManager;

                defaults = new Properties(jnl.getProperties());

            } else {

                final JiniFederation<?> fed = (JiniFederation<?>) indexManager;

                defaults = fed.getClient().getProperties();

            }

            /*
             * Produce the effective properties.
             */
            {
                
                effective = PropertyUtil.flatCopy(defaults);

                for (Map.Entry<Object, Object> e : given.entrySet()) {

                    final String name = (String) e.getKey();

                    final Object val = e.getValue();

                    if (val != null) {

                        // Note: Hashtable does not allow nulls.
                        effective.put(name, val);

                    }

                }

            }

        }

        // The effective namespace for the new KB.
        final String namespace = effective.getProperty(
                BigdataSail.Options.NAMESPACE,
                BigdataSail.Options.DEFAULT_NAMESPACE);

        {

            final long timestamp = ITx.UNISOLATED;
            
            // resolve the namespace.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                    .getResourceLocator().locate(namespace, timestamp);

            if (tripleStore != null) {
                /*
                 * Already exists.
                 */
                buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                        "EXISTS: " + namespace);
                return;
            }
            
        }

        try {

            if (indexManager instanceof Journal) {

                /*
                 * Create a local triple store.
                 * 
                 * Note: This hands over the logic to some custom code located
                 * on the BigdataSail.
                 */

                final Journal jnl = (Journal) indexManager;

                // create the appropriate as configured triple/quad store.
                BigdataSail.createLTS(jnl, effective);

            } else {

                /*
                 * Register triple store for scale-out.
                 * 
                 * TODO Review guidance for creation of KBs on a cluster.
                 */

                final ScaleOutTripleStore ts = new ScaleOutTripleStore(
                        indexManager, namespace, ITx.UNISOLATED, effective);

                ts.create();

            }

            buildResponse(resp, HTTP_OK, MIME_TEXT_PLAIN, "CREATED: "
                    + namespace);

        } catch (Throwable e) {

            throw launderThrowable(e, resp, "");

        }
        
    }

    /**
     * Delete an existing namespace.
     * 
     * @param req
     * @param resp
     * @throws IOException
     */
    private void doDeleteNamespace(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final String namespace = getNamespace(req);

        final long timestamp = ITx.UNISOLATED;

        boolean acquiredConnection = false;
        try {
            
            if (getIndexManager() instanceof Journal) {
                // acquire permit from Journal.
                ((Journal) getIndexManager()).acquireUnisolatedConnection();
                acquiredConnection = true;
            }

            final AbstractTripleStore tripleStore = getBigdataRDFContext()
                    .getTripleStore(namespace, timestamp); 

            if (tripleStore == null) {
                /*
                 * There is no such triple/quad store instance.
                 */
                buildResponse(resp, HTTP_NOTFOUND, MIME_TEXT_PLAIN);
                return;
            }

            // Destroy the KB instance.
            tripleStore.destroy();
            
            tripleStore.commit();
            
            buildResponse(resp, HTTP_OK, MIME_TEXT_PLAIN, "DELETED: "
                    + namespace);

        } catch (Throwable e) {

            throw launderThrowable(e, resp, "");

        } finally {

            if (acquiredConnection) {
            
                ((Journal) getIndexManager()).releaseUnisolatedConnection();
                
            }
            
        }

    }
    
//    private void doDeleteNamespace(final HttpServletRequest req,
//            final HttpServletResponse resp) throws IOException {
//
//        final String namespace = getNamespace(req);
//
//        final long timestamp = ITx.UNISOLATED;
//
//        final AbstractTripleStore tripleStore = getBigdataRDFContext()
//                .getTripleStore(namespace, timestamp);
//
//        if (tripleStore == null) {
//            /*
//             * There is no such triple/quad store instance.
//             */
//            buildResponse(resp, HTTP_NOTFOUND, MIME_TEXT_PLAIN);
//            return;
//        }
//
//        try {
//
//            final BigdataSail sail = new BigdataSail(tripleStore);
//
//            BigdataSailConnection con = null;
//
//            try {
//
//                sail.initialize();
//                // This basically puts a lock on the KB instance.
//                con = sail.getUnisolatedConnection();
//                // Destroy the KB instance.
//                tripleStore.destroy();
//                // Commit.
//                con.commit();
//
//            } finally {
//
//                if (con != null)
//                    con.close();
//
//                sail.shutDown();
//                
//            }
//
//            buildResponse(resp, HTTP_OK, MIME_TEXT_PLAIN, "DELETED: "
//                    + namespace);
//
//        } catch (Throwable e) {
//
//            log.error(e, e);
//            
//            throw launderThrowable(e, resp, "");
//            
//        }
//
//    }

    /**
     * Send the configuration properties for the addressed KB instance.
     * 
     * @param req
     * @param resp
     * @throws IOException
     */
    private void doShowProperties(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {

		final String namespace = getNamespace(req);

		final long timestamp = getTimestamp(req);

//		if (timestamp == ITx.READ_COMMITTED) {
//
//			// Use the last commit point.
//			timestamp = getIndexManager().getLastCommitTime();
//
//		}

		final long tx = getBigdataRDFContext().newTx(timestamp);
		
		try {
		    
			final AbstractTripleStore tripleStore = getBigdataRDFContext()
					.getTripleStore(namespace, tx);

			if (tripleStore == null) {
				/*
				 * There is no such triple/quad store instance.
				 */
				buildResponse(resp, HTTP_NOTFOUND, MIME_TEXT_PLAIN);
				return;
			}

			final Properties properties = PropertyUtil.flatCopy(tripleStore
					.getProperties());

			sendProperties(req, resp, properties);

		} finally {

		    getBigdataRDFContext().abortTx(tx);
		    
		}

	}

    /**
     * Generate a VoID Description for the known namespaces.
     */
    private void doDescribeNamespaces(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final long timestamp = getTimestamp(req);

//        if (timestamp == ITx.READ_COMMITTED) {
//        
//            // Use the last commit point.
//            timestamp = getIndexManager().getLastCommitTime();
//            
//        }
        
        final boolean describeEachNamedGraph;
        {
            final String s = req.getParameter(DESCRIBE_EACH_NAMED_GRAPH);
        
            describeEachNamedGraph = s != null ?
                Boolean.valueOf(s) : 
                    getBigdataRDFContext().getConfig().describeEachNamedGraph;
        }

        final boolean describeDefaultNamespace;
        {
            final String s = req.getParameter(DESCRIBE_DEFAULT_NAMESPACE);

            describeDefaultNamespace = s != null ? Boolean.valueOf(s) : false;
        }

        /**
         * Protect the entire operation with a transaction, including the
         * describe of each namespace that we discover.
         * 
         * @see <a href="http://trac.bigdata.com/ticket/867"> NSS concurrency
         *      problem with list namespaces and create namespace </a>
         */
        final long tx = getBigdataRDFContext().newTx(timestamp);

        try {
            
            final Graph g = new GraphImpl();

            if (describeDefaultNamespace) {

                final String namespace = getBigdataRDFContext().getConfig().namespace;

                describeNamespaceTx(req, g, namespace, describeEachNamedGraph,
                        tx);

            } else {

                /*
                 * The set of registered namespaces for KBs.
                 */
                final List<String> namespaces = getBigdataRDFContext()
                        .getNamespacesTx(tx);

                for (String namespace : namespaces) {

                    describeNamespaceTx(req, g, namespace,
                            describeEachNamedGraph, tx);

                }

            }

            sendGraph(req, resp, g);

        } finally {

            getBigdataRDFContext().abortTx(tx);
            
        }

    }
    
    /**
     * Describe a namespace into the supplied Graph object.
     */
    private void describeNamespaceTx(final HttpServletRequest req,
            final Graph g, final String namespace,
            final boolean describeEachNamedGraph, final long tx) 
                    throws IOException {
        
        // Get a view onto that KB instance for that timestamp.
        final AbstractTripleStore tripleStore = getBigdataRDFContext()
                .getTripleStore(namespace, tx);

        if (tripleStore == null) {

            /*
             * There is no such triple/quad store instance (could be a
             * concurrent delete of the namespace).
             */
            
            return;
            
        }

        final BNode aDataSet = g.getValueFactory().createBNode();
        
        // Figure out the service end point(s).
        final String[] serviceURI = getServiceURIs(getServletContext(), req);

        final VoID v = new VoID(g, tripleStore, serviceURI, aDataSet);

        v.describeDataSet(false/* describeStatistics */,
//                getBigdataRDFContext().getConfig().describeEachNamedGraph);
                describeEachNamedGraph);
        
    }

}
