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
package com.bigdata.rdf.sail.webapp.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.openrdf.query.GraphQueryResult;

import com.bigdata.rdf.properties.PropertiesFormat;
import com.bigdata.rdf.properties.PropertiesParser;
import com.bigdata.rdf.properties.PropertiesParserFactory;
import com.bigdata.rdf.properties.PropertiesParserRegistry;
import com.bigdata.rdf.properties.PropertiesWriter;
import com.bigdata.rdf.properties.PropertiesWriterRegistry;

/**
 * Java client for the Multi-Tenancy API on a remote Nano Sparql Server.
 * 
 * <p>
 * Note: The {@link RemoteRepository} object SHOULD be reused for multiple
 * operations against the same end point.
 * 
 * @see <a href=
 *      "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer"
 *      > NanoSparqlServer REST API </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/628" > Create
 *      a bigdata-client jar for the NSS REST API </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/696" >
 *      Incorrect HttpEntity consuming in RemoteRepositoryManager </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RemoteRepositoryManager extends RemoteRepository {

    /**
     * The path to the root of the web application (without the trailing "/").
     * <p>
     * Note: This SHOULD NOT be the SPARQL end point URL. The NanoSparqlServer
     * has a wider interface. This should be the base URL of that interface. The
     * SPARQL end point URL for the default data set is formed by appending
     * <code>/sparql</code>.
     */
    private final String baseServiceURL;

    /**
     * The path to the root of the web application (without the trailing "/").
     * <p>
     * Note: This SHOULD NOT be the SPARQL end point URL. The NanoSparqlServer
     * has a wider interface. This should be the base URL of that interface. The
     * SPARQL end point URL for the default data set is formed by appending
     * <code>/sparql</code>.
     */
    public String getBaseServiceURL() {
        
        return baseServiceURL;
        
    }
    
    public RemoteRepositoryManager(final String serviceURL,
            final HttpClient httpClient, final Executor executor) {

        this(serviceURL, false/* useLBS */, httpClient, executor);

    }
    
    /**
     * 
     * @param serviceURL
     *            The path to the root of the web application (without the
     *            trailing "/"). <code>/sparql</code> will be appended to this
     *            path to obtain the SPARQL end point for the default data set.
     * @param useLBS
     *            When <code>true</code>, the REST API methods will use the load
     *            balancer aware requestURLs. The load balancer has essentially
     *            zero cost when not using HA, so it is recommended to always
     *            specify <code>true</code>. When <code>false</code>, the REST
     *            API methods will NOT use the load balancer aware requestURLs.
     * @param httpClient
     * @param executor
     */
    public RemoteRepositoryManager(final String serviceURL,
            final boolean useLBS, final HttpClient httpClient,
            final Executor executor) {

        super(serviceURL + "/sparql", useLBS, httpClient, executor);

        this.baseServiceURL = serviceURL;

    }

    /**
     * Return the base URL for a remote repository (less the /sparql path
     * component).
     * 
     * @param namespace
     *            The namespace.
     *            
     * @return The base URL.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/689" >
     *      Missing URL encoding in RemoteRepositoryManager </a>
     */
    protected String getRepositoryBaseURLForNamespace(final String namespace) {
    
        return baseServiceURL + "/namespace/"
                + ConnectOptions.urlEncode(namespace);
    }

    /**
     * Obtain a {@link RemoteRepository} for a data set managed by the remote
     * service.
     * 
     * @param namespace
     *            The name of the data set (its bigdata namespace).
     *            
     * @return An interface which may be used to talk to that data set.
     */
    public RemoteRepository getRepositoryForNamespace(final String namespace) {

        return new RemoteRepository(getRepositoryBaseURLForNamespace(namespace)
                + "/sparql", useLBS, httpClient, executor);
        
    }

    /**
     * Obtain a {@link RemoteRepository} for the data set having the specified
     * SPARQL end point.
     * 
     * @param sparqlEndpointURL
     *            The URL of the SPARQL end point.
     * @param useLBS
     *            When <code>true</code>, the REST API methods will use the load
     *            balancer aware requestURLs. The load balancer has essentially
     *            zero cost when not using HA, so it is recommended to always
     *            specify <code>true</code>. When <code>false</code>, the REST
     *            API methods will NOT use the load balancer aware requestURLs.
     * 
     * @return An interface which may be used to talk to that data set.
     */
    public RemoteRepository getRepositoryForURL(final String sparqlEndpointURL,
            final boolean useLBS) {

        return new RemoteRepository(sparqlEndpointURL, useLBS, httpClient,
                executor);

    }

    /**
     * Obtain a {@link RemoteRepository} for the data set having the specified
     * SPARQL end point. The load balancer will be used or not as per the
     * parameters to the {@link RemoteRepositoryManager} constructor.
     * 
     * @param sparqlEndpointURL
     *            The URL of the SPARQL end point.
     * 
     * @return An interface which may be used to talk to that data set.
     */
    public RemoteRepository getRepositoryForURL(final String sparqlEndpointURL) {

        return getRepositoryForURL(sparqlEndpointURL, useLBS);

    }

    /**
     * Obtain a <a href="http://vocab.deri.ie/void/"> VoID </a> description of
     * the configured KBs. Each KB has its own namespace and corresponds to a
     * VoID "data set".
     * <p>
     * Note: This method uses an HTTP GET and hence can be cached by the server.
     * 
     * @return A <a href="http://vocab.deri.ie/void/"> VoID </a> description of
     *         the configured KBs.
     * 
     * @throws Exception
     */
    public GraphQueryResult getRepositoryDescriptions() throws Exception {

        final ConnectOptions opts = newConnectOptions(baseServiceURL + "/namespace");
        
        opts.method = "GET";

//        HttpResponse response = null;
//        GraphQueryResult result = null;
        
        opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

        return graphResults(opts, null);
        
//        try {
//            // check response in try.
//            checkResponseCode(response = doConnect(opts));
//
//            // return asynchronous parse of result.
//            return result = graphResults(response);
//
//        } finally {
//            if (result == null) {
//                // Consume entity if bad response.
//                try {
//                    EntityUtils.consume(response.getEntity());
//                } catch (IOException ex) {
//                }
//            }
//        }
    }

    /**
     * Create a new KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * @param properties
     *            The configuration properties for that KB instance.
     *            
     * @throws Exception 
     */
    public void createRepository(final String namespace,
            final Properties properties) throws Exception {

        final ConnectOptions opts = newConnectOptions(baseServiceURL
                + "/namespace");

        opts.method = "POST";

        HttpResponse response = null;

        // Setup the request entity.
        {

            final PropertiesFormat format = PropertiesFormat.XML;
            
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            
            final PropertiesWriter writer = PropertiesWriterRegistry
                    .getInstance().get(format).getWriter(baos);

            writer.write(properties);
            
            final byte[] data = baos.toByteArray();
            
            final ByteArrayEntity entity = new ByteArrayEntity(data);

            entity.setContentType(format.getDefaultMIMEType());

            opts.entity = entity;
        
        }

        try {

            checkResponseCode(response = doConnect(opts));

        } finally {

            if (response != null) {
                try {
                    EntityUtils.consume(response.getEntity());
                } catch (IOException ex) {
                }
            }

        }
        
    }

    /**
     * Create a new KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * @param properties
     *            The configuration properties for that KB instance.
     *            
     * @throws Exception 
     */
    public void deleteRepository(final String namespace) throws Exception {

        final ConnectOptions opts = newConnectOptions(getRepositoryBaseURLForNamespace(namespace));

        opts.method = "DELETE";

        HttpResponse response = null;

        try {

            checkResponseCode(response = doConnect(opts));

        } finally {

            if (response != null) {
                try {
                    EntityUtils.consume(response.getEntity());
                } catch (IOException ex) {
                }
            }

        }
        
    }

    /**
     * Return the effective configuration properties for the named data set.
     * <p>
     * Note: While it is possible to change some configuration options are a
     * data set has been created, many aspects of a "data set" configuration are
     * "baked in" when the data set is created and can not be changed. For this
     * reason, no general purpose mechanism is being offered to change the
     * properties for a configured data set instance.
     * 
     * @param namespace
     *            The name of the data set.
     * 
     * @return The effective configuration properties for that named data set.
     * 
     * @throws Exception
     */
    public Properties getRepositoryProperties(final String namespace)
            throws Exception {

        final ConnectOptions opts = newConnectOptions(getRepositoryBaseURLForNamespace(namespace)
                + "/properties");

        opts.method = "GET";

        HttpResponse response = null;

        opts.setAcceptHeader(ConnectOptions.MIME_PROPERTIES_XML);

        try {

            checkResponseCode(response = doConnect(opts));

            final HttpEntity entity = response.getEntity();

            final String contentType = entity.getContentType().getValue();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");

            final MiniMime mimeType = new MiniMime(contentType);

            final PropertiesFormat format = PropertiesFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + sparqlEndpointURL + ", contentType="
                                + contentType + " : response="
                                + getResponseBody(response));

            final PropertiesParserFactory factory = PropertiesParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                throw new RuntimeException(
                        "ParserFactory not found: Content-Type=" + contentType
                                + ", format=" + format);

            final PropertiesParser parser = factory.getParser();

            final Properties properties = parser.parse(entity.getContent());

            return properties;

        } finally {

            if (response != null) {
                try {
                    EntityUtils.consume(response.getEntity());
                } catch (IOException ex) {
                }
            }

        }

    }

}
