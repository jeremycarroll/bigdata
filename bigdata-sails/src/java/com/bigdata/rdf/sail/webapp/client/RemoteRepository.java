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

import info.aduna.io.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.openrdf.OpenRDFUtil;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParser;
import org.openrdf.query.resultio.BooleanQueryResultParserFactory;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.repository.sparql.query.InsertBindingSetCursor;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;

// Note: Do not import. Not part of the bigdata-client.jar
//
//import com.bigdata.rdf.sparql.ast.service.RemoteServiceOptions;


/**
 * Java API to the Nano Sparql Server.
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
 */
public class RemoteRepository {

    private static final transient Logger log = Logger
            .getLogger(RemoteRepository.class);

    /*
     * Note: These fields are replicated from the com.bigdata.rdf.store.BD
     * interface in order to avoid dragging in other aspects of the bigdata code
     * base.
     */
    
    /**
     * The namespace used for bigdata specific extensions.
     */
    private static final String BD_NAMESPACE = "http://www.bigdata.com/rdf#";

    private static final URI BD_NULL_GRAPH = new URIImpl(BD_NAMESPACE + "nullGraph");
    
    /**
     * The name of the <code>UTF-8</code> character encoding.
     */
    static protected final String UTF8 = "UTF-8";

    /**
     * Note: The default is <code>false</code>. This supports use cases where
     * the end points are read/write databases and http caching must be defeated
     * in order to gain access to the most recent committed state of the end
     * point.
     * 
     * @see #getQueryMethod()
     * @see #setQueryMethod(String)
     */
    static private final String DEFAULT_QUERY_METHOD = "POST";
    
    /**
     * The default maximum limit on a requestURL before the request is converted
     * into a POST using a <code>application/x-www-form-urlencoded</code>
     * request entity.
     * <p>
     * Note: I suspect that 2000 might be a better default limit. If the limit
     * is 4096 bytes on the target, then, even with UTF encoding, most queries
     * having a request URL that is 2000 characters long should go through with
     * a GET. 1000 is a safe value but it could reduce http caching.
     */
    static private final int DEFAULT_MAX_REQUEST_URL_LENGTH = 1000;
    
    /**
     * The service end point for the default data set.
     */
    protected final String sparqlEndpointURL;

    /**
     * The client used for http connections.
     */
    protected final HttpClient httpClient;

    /**
     * Thread pool for processing HTTP responses in background.
     */
    protected final Executor executor;

    /**
     * The maximum requestURL length before the request is converted into a POST
     * using a <code>application/x-www-form-urlencoded</code> request entity.
     */
    private volatile int maxRequestURLLength = DEFAULT_MAX_REQUEST_URL_LENGTH;
    
    /**
     * The HTTP verb that will be used for a QUERY (versus a UPDATE or other
     * mutation operation).
     */
    private volatile String queryMethod = DEFAULT_QUERY_METHOD;

    /**
     * Return the maximum requestURL length before the request is converted into
     * a POST using a <code>application/x-www-form-urlencoded</code> request
     * entity.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/619">
     *      RemoteRepository class should use application/x-www-form-urlencoded
     *      for large POST requests </a>
     */
    public int getMaxRequestURLLength() {

        return maxRequestURLLength;
        
    }    

    public void setMaxRequestURLLength(final int newVal) {

        if (newVal <= 0)
            throw new IllegalArgumentException();

        this.maxRequestURLLength = newVal;
        
    }
    
    /**
     * Return the HTTP verb that will be used for a QUERY (versus an UPDATE or
     * other mutation operations) (default {@value #DEFAULT_IS_GET}). POST can
     * often handle larger queries than GET due to limits at the HTTP client
     * layer and will defeat http caching and thus provide a current view of the
     * committed state of the SPARQL end point when the end point is a
     * read/write database. However, GET supports HTTP caching and can scale
     * much better when the SPARQL end point is a read-only resource or a
     * read-mostly resource where stale reads are acceptable.
     * 
     * @see #setQueryMethod(String)
     */
    public String getQueryMethod() {
     
        return queryMethod;
        
    }

    /**
     * Set the default HTTP verb for QUERY and other idempotant operations.
     * 
     * @param method
     *            The method which may be "POST" or "GET".
     * 
     * @see #getQueryMethod()
     */
    public void setQueryMethod(final String method) {

        if ("POST".equalsIgnoreCase(method) || "GET".equalsIgnoreCase(method)) {

            this.queryMethod = method.toUpperCase();

        } else {
            
            throw new IllegalArgumentException();
            
        }

    }
    
    /**
     * Create a connection to a remote repository. A typical invocation looks
     * like:
     * 
     * <pre>
     * cm = ...
     * executor = ...
     * new RemoteRepository(serviceURL, new DefaultHttpClient(cm), executor);
     * </pre>
     * <p>
     * Note: You SHOULD reuse the backing {@link ClientConnectionManager} for
     * the {@link HttpClient}. It generally relies on a thread pool and the life
     * cycle of the {@link ClientConnectionManager} needs to be properly
     * managed. Some hooks for this are listed below.
     * <p>
     * Note: You SHOULD reuse an existing thread pool {@link Executor} and the
     * life cycle of that {@link Executor} needs to be properly managed. Again,
     * see below for some hooks.
     * 
     * @param sparqlEndpointURL
     *            The SPARQL http end point for the data set.
     * @param httpClient
     *            The {@link HttpClient}.
     * @param executor
     *            The thread pool for processing HTTP responses.
     * 
     * @see DefaultClientConnectionManagerFactory
     */
    public RemoteRepository(final String sparqlEndpointURL,
            final HttpClient httpClient, final Executor executor) {
        
        if (sparqlEndpointURL == null)
            throw new IllegalArgumentException();

        if (httpClient == null)
            throw new IllegalArgumentException();

        if (executor == null)
            throw new IllegalArgumentException();

        this.sparqlEndpointURL = sparqlEndpointURL;

        this.httpClient = httpClient;
        
        this.executor = executor;

    }

    @Override
    public String toString() {

        return super.toString() + "{sparqlEndpoint=" + sparqlEndpointURL + "}";

    }

    /**
     * Return the SPARQL end point.
     */
    public String getSparqlEndPoint() {
        
        return sparqlEndpointURL;
        
    }
    
    public void setTruthMaintenance(final boolean tm) throws Exception {
    	
        final ConnectOptions opts = newConnectOptions();

        opts.addRequestParam("truthMaintenance", String.valueOf(tm));

        checkResponseCode(doConnect(opts));

    }
    
    public long doClosure() throws Exception {
    	
        final ConnectOptions opts = newConnectOptions();

        opts.addRequestParam("doClosure");

        HttpResponse response = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            return result.mutationCount;
            
        } finally {
            
            try {
                
                if (response != null)
                    EntityUtils.consume(response.getEntity());
                
            } catch (Exception ex) { }
            
        }
    	
    }
    
    /**
     * Return the SPARQL 1.1 Service Description for the end point.
     */
    public GraphQueryResult getServiceDescription() throws Exception {

        final ConnectOptions opts = newConnectOptions();

        opts.method = "GET";

//        HttpResponse response = null;

        opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

//        checkResponseCode(response = doConnect(opts));

        return graphResults(opts, null);

    }

    /**
     * Prepare a tuple (select) query.
     * 
     * @param query
     *            the query string
     * @return the {@link TupleQuery}
     */
    public IPreparedTupleQuery prepareTupleQuery(final String query)
            throws Exception {

        return new TupleQuery(newQueryConnectOptions(), UUID.randomUUID(), query);

    }

    /**
     * Prepare a graph query.
     * 
     * @param query
     *            the query string
     *            
     * @return the {@link IPreparedGraphQuery}
     */
    public IPreparedGraphQuery prepareGraphQuery(final String query)
            throws Exception {

        return new GraphQuery(newQueryConnectOptions(), UUID.randomUUID(), query);

    }

    /**
     * Prepare a boolean (ask) query.
     * 
     * @param query
     *            the query string
     * 
     * @return the {@link IPreparedBooleanQuery}
     */
    public IPreparedBooleanQuery prepareBooleanQuery(final String query)
            throws Exception {

        return new BooleanQuery(newQueryConnectOptions(), UUID.randomUUID(), query);

    }

    /**
     * Prepare a SPARQL UPDATE request.
     * 
     * @param updateStr
     *            The SPARQL UPDATE request.
     * 
     * @return The {@link SparqlUpdate} opertion.
     * 
     * @throws Exception
     */
    public IPreparedSparqlUpdate prepareUpdate(final String updateStr)
            throws Exception {

        return new SparqlUpdate(newUpdateConnectOptions(), UUID.randomUUID(),
                updateStr);

    }

    /**
     * Return all matching statements.
     * 
     * @param subj
     * @param pred
     * @param obj
     * @param includeInferred
     * @param contexts
     * @return
     * @throws Exception
     * 
     *             TODO includeInferred is currently ignored.
     */
    public GraphQueryResult getStatements(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred,
            final Resource... contexts) throws Exception {

        OpenRDFUtil.verifyContextNotNull(contexts);

        final Map<String, String> prefixDecls = Collections.emptyMap();

        final AST2SPARQLUtil util = new AST2SPARQLUtil(prefixDecls);

        final StringBuilder sb = new StringBuilder();

        /*
         * Note: You can not use the CONSTRUCT WHERE shortcut with a data set
         * declaration (FROM, FROM NAMED)....
         */

        if (contexts.length > 0) {

            sb.append("CONSTRUCT {\n");

            sb.append(asConstOrVar(util, "?s", subj));
            
            sb.append(" ");
            
            sb.append(asConstOrVar(util, "?p", pred));
            
            sb.append(" ");
            
            sb.append(asConstOrVar(util, "?o", obj));

            sb.append("\n}\n");

            // Add FROM clause for each context to establish the defaultGraph.
            for (int i = 0; i < contexts.length; i++) {

                /*
                 * Interpret a [null] entry in contexts[] as a reference to the
                 * openrdf nullGraph.
                 */

                final Resource c = contexts[i] == null ? BD_NULL_GRAPH
                        : contexts[i];

                sb.append("FROM " + util.toExternal(c) + "\n");

            }
            
            sb.append("WHERE {\n");

        } else {
            
            // CONSTRUCT WHERE shortcut form.
            sb.append("CONSTRUCT WHERE {\n");
            
        }

        sb.append(asConstOrVar(util, "?s", subj));
        
        sb.append(" ");
        
        sb.append(asConstOrVar(util, "?p", pred));
        
        sb.append(" ");
        
        sb.append(asConstOrVar(util, "?o", obj));

        sb.append("\n}");
        
        final String queryStr = sb.toString();
        
        final IPreparedGraphQuery query = prepareGraphQuery(queryStr);
        
        return query.evaluate();
                
    }

    /**
     * Method to line up with the Sesame interface.
     * 
     * @param subj
     * @param pred
     * @param obj
     * @param includeInferred
     * @param contexts
     * @return
     * @throws Exception
     * 
     *             TODO includeInferred is currently ignored.
     */
    public boolean hasStatement(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred,
            final Resource... contexts) throws Exception {
        
        return (rangeCount(subj, pred, obj, contexts) > 0);
        
    }
    
    private String asConstOrVar(final AST2SPARQLUtil util, final String var,
            final Value val) {

        if (val == null)
            return var;

        return util.toExternal(val);
        
    }
    
    
    /**
     * Cancel a query running remotely on the server.
     * 
     * @param queryID
     *             the UUID of the query to cancel
     */
    public void cancel(final UUID queryId) throws Exception {
        
    	if (queryId == null)
    		return;
    	
        final ConnectOptions opts = newUpdateConnectOptions();

        opts.addRequestParam("cancelQuery");

        opts.addRequestParam("queryId", queryId.toString());

        checkResponseCode(doConnect(opts));
            
    }

    /**
     * Perform a fast range count on the statement indices for a given
     * triple (quad) pattern.
     * 
     * @param s
     *             the subject (can be null)
     * @param p
     *             the predicate (can be null)
     * @param o
     *             the object (can be null)
     * @param c
     *             the context (can be null)
     * @return
     *             the range count
     */
    public long rangeCount(final Resource s, final URI p, final Value o, final Resource... c) 
            throws Exception {

        final ConnectOptions opts = newQueryConnectOptions();

        opts.addRequestParam("ESTCARD");
        if (s != null) {
            opts.addRequestParam("s", EncodeDecodeValue.encodeValue(s));
        }
        if (p != null) {
            opts.addRequestParam("p", EncodeDecodeValue.encodeValue(p));
        }
        if (o != null) {
            opts.addRequestParam("o", EncodeDecodeValue.encodeValue(o));
        }
        if (c != null && c.length > 0) {
            opts.addRequestParam("c", EncodeDecodeValue.encodeValues(c));
        }

        HttpResponse resp = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(resp = doConnect(opts));
            
            final RangeCountResult result = rangeCountResults(resp);
            
            return result.rangeCount;
            
        } finally {
            
            try {
            
                if (resp != null)
                    EntityUtils.consume(resp.getEntity());
                
            } catch (Exception ex) { }
            
        }

    }
    
    /**
     * Perform a fast range count on the statement indices.
     * 
     * @param s
     *            the subject (can be null)
     * @param p
     *            the predicate (can be null)
     * @param o
     *            the object (can be null)
     * @param c
     *            the context (can be null)
     *            
     * @return the range count (#of statements in the database).
     */
    public long size() throws Exception {
        
        return rangeCount(null, null, null);
        
    }
    
    /**
     * Return a list of contexts in use in a remote quads database.
     */
    public Collection<Resource> getContexts() throws Exception {
    	
        final ConnectOptions opts = newQueryConnectOptions();

        opts.addRequestParam("CONTEXTS");

        HttpResponse resp = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(resp = doConnect(opts));
            
            final ContextsResult result = contextsResults(resp);
            
            return result.contexts;
            
        } finally {
            
            try {
            
                if (resp != null)
                    EntityUtils.consume(resp.getEntity());
                
            } catch (Exception ex) { }
            
        }
    	
    }
    

    /**
     * Adds RDF data to the remote repository.
     * 
     * @param add
     *            The RDF data to be added.
     * 
     * @return The mutation count.
     */
    public long add(final AddOp add) throws Exception {
        
        final ConnectOptions opts = newUpdateConnectOptions();
        
        add.prepareForWire();
        
        if (add.format != null) {
            
            final ByteArrayEntity entity = new ByteArrayEntity(add.data);

            entity.setContentType(add.format.getDefaultMIMEType());
            
            opts.entity = entity;
            
        }
            
        if (add.uri != null) {
            // set the resource to load.
            opts.addRequestParam("uri", add.uri);
        }
        
        if (add.context != null && add.context.length > 0) {
            // set the default context.
            opts.addRequestParam("context-uri", toStrings(add.context));
        }
        
        HttpResponse response = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            return result.mutationCount;
            
        } finally {
            
            try {
                
                if (response != null)
                    EntityUtils.consume(response.getEntity());
                
            } catch (Exception ex) { }
            
        }
        
    }
            
    /**
     * Removes RDF data from the remote repository.
     * 
     * @param remove
     *        The RDF data to be removed.
     *        
     * @return The mutation count.
     */
    public long remove(final RemoveOp remove) throws Exception {
        
        final ConnectOptions opts = newUpdateConnectOptions();
        
        remove.prepareForWire();
            
        if (remove.format != null) {
            
            opts.method = "POST";
            opts.addRequestParam("delete");
            
            final ByteArrayEntity entity = new ByteArrayEntity(remove.data);

            entity.setContentType(remove.format.getDefaultMIMEType());
            
            opts.entity = entity;
            
            if (remove.context != null && remove.context.length > 0) {
                // set the default context.
                opts.addRequestParam("context-uri", toStrings(remove.context));
            }
            
        } else {
            
            opts.method = "DELETE";
        
	        if (remove.query != null) {
	            opts.addRequestParam("query", remove.query);
	        }
	        
	        if (remove.s != null) {
	            opts.addRequestParam("s", EncodeDecodeValue.encodeValue(remove.s));
	        }
	        
	        if (remove.p != null) {
	            opts.addRequestParam("p", EncodeDecodeValue.encodeValue(remove.p));
	        }
	        
	        if (remove.o != null) {
	            opts.addRequestParam("o", EncodeDecodeValue.encodeValue(remove.o));
	        }
	        
	        if (remove.c != null && remove.c.length > 0) {
	            opts.addRequestParam("c", EncodeDecodeValue.encodeValues(remove.c));
	        }
        
        }
        
        HttpResponse response = null;
        try {
            
            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);

            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            return result.mutationCount;
            
        } finally {
            
            try {
                
                if (response != null)
                    EntityUtils.consume(response.getEntity());
                
            } catch (Exception ex) { }
            
        }
        
    }

    /**
     * Perform an ACID update (delete+insert) per the semantics of <a href=
     * "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer#UPDATE_.28DELETE_.2B_INSERT.29"
     * > the NanoSparqlServer. </a>
     * <p>
     * Currently, the only combination supported is delete by query with add by
     * post (Iterable<Statement> and File). You can embed statements you want to
     * delete inside a construct query without a where clause.
     * 
     * @param remove
     *            The RDF data to be removed.
     * @param add
     *            The RDF data to be added.
     *            
     * @return The mutation count.
     */
    public long update(final RemoveOp remove, final AddOp add) throws Exception {
        
        final ConnectOptions opts = newUpdateConnectOptions();
        
        remove.prepareForWire();
        add.prepareForWire();
        
        if (remove.format != null) {
        
            opts.method = "POST";
            opts.addRequestParam("update");
            
            final MultipartEntity entity = new MultipartEntity();
            entity.addPart(new FormBodyPart("remove", 
                    new ByteArrayBody(
                            remove.data, 
                            remove.format.getDefaultMIMEType(), 
                            "remove")));
            entity.addPart(new FormBodyPart("add", 
                    new ByteArrayBody(
                            add.data, 
                            add.format.getDefaultMIMEType(), 
                            "add")));
            
            opts.entity = entity;
        
        } else {
            
            opts.method = "PUT";
            opts.addRequestParam("query", remove.query);
            
            final ByteArrayEntity entity = new ByteArrayEntity(add.data);
            entity.setContentType(add.format.getDefaultMIMEType());
        
            opts.entity = entity;
            
        }
        
        if (add.context != null) {
            // set the default context for insert.
            opts.addRequestParam("context-uri-insert", toStrings(add.context));
        }
        
        if (remove.context != null) {
            // set the default context for delete.
            opts.addRequestParam("context-uri-delete", toStrings(remove.context));
        }
        
        HttpResponse response = null;
        try {

            opts.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);
            
            checkResponseCode(response = doConnect(opts));
            
            final MutationResult result = mutationResults(response);
            
            return result.mutationCount;
            
        } finally {
            
            try {
                
                if (response != null)
                    EntityUtils.consume(response.getEntity());
                
            } catch (Exception ex) { }
            
        }
        
    }
    
    /**
     * A prepared query will hold metadata for a particular query instance.
     * <p>
     * Right now, the only metadata is the query ID.
     */
    protected abstract class Query implements IPreparedOperation, IPreparedQuery {
        
        protected final ConnectOptions opts;
        
        protected final UUID id;

        protected final String query;

        private final boolean update;

        public Query(final ConnectOptions opts, final UUID id,
                final String query) {

            this(opts, id, query, false/* update */);

        }

        /**
         * 
         * @param id
         *            The query id.
         * @param query
         *            The SPARQL query or update string.
         * @param update
         *            <code>true</code> iff this is a SPARQL update.
         */
        public Query(final ConnectOptions opts, final UUID id,
                final String query, final boolean update) {

            if (opts == null)
                throw new IllegalArgumentException();
            
            if (query == null)
                throw new IllegalArgumentException();
            
            this.opts = opts;
            this.id = id;
            this.query = query;
            this.update = update;
            
        }

        @Override
        final public UUID getQueryId() {
            
            return id;
            
        }
        
        @Override
        public final boolean isUpdate() {
            
            return update;
            
        }
        
        /**
         * Setup the connection options.
         */
        protected void setupConnectOptions() {
            
            opts.method = "POST";

            if(update) {
            
                opts.addRequestParam("update", query);
                
            } else {
                
                opts.addRequestParam("query", query);
                
            }

            if (id != null)
                opts.addRequestParam("queryId", getQueryId().toString());
                
        }
        
        @Override
        public void setAcceptHeader(final String value) {
            
            opts.setAcceptHeader(value);
            
        }
        
        @Override
        public void setHeader(final String name, final String value) {

            opts.setHeader(name, value);
            
        }

        @Override
        public String getHeader(final String name) {
            
            return opts.getHeader(name);
            
        }
        
    }

    private final class TupleQuery extends Query implements IPreparedTupleQuery {
        
        public TupleQuery(final ConnectOptions opts, final UUID id,
                final String query) {

            super(opts, id, query);

        }

        @Override
        protected void setupConnectOptions() {

            super.setupConnectOptions();

            if (opts.getAcceptHeader() == null)
                opts.setAcceptHeader(ConnectOptions.DEFAULT_SOLUTIONS_ACCEPT_HEADER);

        }

        @Override
        public TupleQueryResult evaluate() throws Exception {
            
            setupConnectOptions();

            return tupleResults(opts, getQueryId());
                
        }
        
    }

    private final class GraphQuery extends Query implements IPreparedGraphQuery {

        public GraphQuery(final ConnectOptions opts, final UUID id,
                final String query) {

            super(opts, id, query);

        }

        @Override
        protected void setupConnectOptions() {

            super.setupConnectOptions();

            if (opts.getAcceptHeader() == null)
                opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

        }

        @Override
        public GraphQueryResult evaluate() throws Exception {

            setupConnectOptions();
            
            return graphResults(opts, getQueryId());

        }

    }

    private final class BooleanQuery extends Query implements
            IPreparedBooleanQuery {
        
        public BooleanQuery(final ConnectOptions opts, final UUID id,
                final String query) {
        
            super(opts, id, query);
            
        }
        

        @Override
        protected void setupConnectOptions() {

            super.setupConnectOptions();

            if (opts.getAcceptHeader() == null)
                opts.setAcceptHeader(ConnectOptions.DEFAULT_BOOLEAN_ACCEPT_HEADER);

        }
        
        @Override
        public boolean evaluate() throws Exception {
           
            setupConnectOptions();
            
            return booleanResults(opts, getQueryId());

//            HttpResponse response = null;
//            try {
//
//                setupConnectOptions();
//                
//                checkResponseCode(response = doConnect(opts));
//                
//                return booleanResults(response);
//                
//            } finally {
//                
//                try {
//                    
//                    if (response != null)
//                        EntityUtils.consume(response.getEntity());
//                    
//                } catch (Exception ex) { 
//                    
//                      log.warn(ex);
//
//                }
//                
//            }
            
        }
        
    }

    private final class SparqlUpdate extends Query implements
            IPreparedSparqlUpdate {
        
        public SparqlUpdate(final ConnectOptions opts, final UUID id,
                final String updateStr) {

            super(opts, id, updateStr, true/*update*/);

        }
        
        @Override
        public void evaluate() throws Exception {
         
            HttpResponse response = null;
            try {

                setupConnectOptions();

                // Note: No response body is expected.
                
                checkResponseCode(response = doConnect(opts));
                
            } finally {
                
                try {
                    
                    if (response != null)
                        EntityUtils.consume(response.getEntity());
                    
                } catch (Exception ex) {
                    
                    log.warn(ex);
                    
                }
                
            }
            
        }
        
    }
   
    /**
     * Add by URI, statements, or file.
     */
    public static class AddOp {

        private String uri;
        private Iterable<? extends Statement> stmts;
        private byte[] data;
        private File file;
        private InputStream is;
        private Reader reader;
        private RDFFormat format;
        private Resource[] context;
        
        public AddOp(final String uri) {
            this.uri = uri;
        }
        
        public AddOp(final Iterable<? extends Statement> stmts) {
            this.stmts = stmts;
        }
        
        public AddOp(final File file, final RDFFormat format) {
            this.file = file;
            this.format = format;
        }
        
        public AddOp(final InputStream is, final RDFFormat format) {
            this.is = is;
            this.format = format;
        }
        
        public AddOp(final Reader reader, final RDFFormat format) {
            this.reader = reader;
            this.format = format;
        }
        
        /**
         * This ctor is for the test cases.
         */
        public AddOp(final byte[] data, final RDFFormat format) {
            this.data = data;
            this.format = format;
        }
        
        public void setContext(final Resource... context) {
            this.context = context;
        }
        
        private void prepareForWire() throws Exception {
            
            if (file != null) {

                // set the data
                data = IOUtil.readBytes(file);
                
            } else if (is != null) {

                // set the data
                data = IOUtil.readBytes(is);
                
            } else if (reader != null) {

                // set the data
                data = IOUtil.readString(reader).getBytes();
                
            } else if (stmts != null) {
                
                // set the data and content type (TRIG by default)
                format = RDFFormat.TRIG;
                data = serialize(stmts, format);

            }
            
        }
        
    }
    
    /**
     * Remove by query, access path, statements, or file.
     */
    public static class RemoveOp {
        
        private String query;
        
        private Iterable<? extends Statement> stmts;
        
        private Value s, p, o;
        
        private Value[] c;
        
        private byte[] data;
        
        private File file;
        
        private RDFFormat format;
        
        private Resource[] context;
        
        public RemoveOp(final String query) {
            this.query = query;
        }
        
        public RemoveOp(final Iterable<? extends Statement> stmts) {
            this.stmts = stmts;
        }
        
        public RemoveOp(final Resource s, final URI p, final Value o, final Resource... c) {
            this.s = s;
            this.p = p;
            this.o = o;
            this.c = c;
        }

        public RemoveOp(final File file, final RDFFormat format) {
            this.file = file;
            this.format = format;
        }
        
        /**
         * This ctor is for the test cases.
         */
        public RemoveOp(final byte[] data, final RDFFormat format) {
            this.data = data;
            this.format = format;
        }
                    
        public void setContext(final Resource... context) {
            this.context = context;
        }
        
        private void prepareForWire() throws Exception {
            
            if (file != null) {

                // set the data
                data = IOUtil.readBytes(file);
                
            } else if (stmts != null) {
                
                // set the data and content type (TRIG by default)
                format = RDFFormat.TRIG;
                data = serialize(stmts, format);

            }
            
        }
        
    }
    
    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The connection options.
     * 
     * @return The connection.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/619">
     *      RemoteRepository class should use application/x-www-form-urlencoded
     *      for large POST requests </a>
     */
    public HttpResponse doConnect(final ConnectOptions opts) throws Exception {

        /*
         * Generate the fully formed and encoded URL.
         */
        
        final StringBuilder urlString = new StringBuilder(opts.serviceURL);

        ConnectOptions.addQueryParams(urlString, opts.requestParams);

        final boolean isLongRequestURL = urlString.length() > getMaxRequestURLLength();

        if (isLongRequestURL && opts.method.equals("POST")
                && opts.entity == null) {

            /*
             * URL is too long. Reset the URL to just the service endpoint and
             * use application/x-www-form-urlencoded entity instead. Only in
             * cases where there is not already a request entity (SPARQL query
             * and SPARQL update).
             */

            urlString.setLength(0);
            urlString.append(opts.serviceURL);

            opts.entity = ConnectOptions.getFormEntity(opts.requestParams);

        } else if (isLongRequestURL && opts.method.equals("GET")
                && opts.entity == null) {

            /*
             * Convert automatically to a POST if the request URL is too long.
             * 
             * Note: [opts.entity == null] should always be true for a GET so
             * this bit is a paranoia check.
             */

            opts.method = "POST";

            urlString.setLength(0);
            urlString.append(opts.serviceURL);

            opts.entity = ConnectOptions.getFormEntity(opts.requestParams);
            
        }

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(sparqlEndpointURL);
            log.debug(opts.method);
            log.debug("query=" + opts.getRequestParam("query"));
            log.debug(urlString.toString());
        }

        HttpUriRequest request = null;
        try {

            request = newRequest(urlString.toString(), opts.method);

            if (opts.requestHeaders != null) {

                for (Map.Entry<String, String> e : opts.requestHeaders
                        .entrySet()) {

                    request.addHeader(e.getKey(), e.getValue());

                    if (log.isDebugEnabled())
                        log.debug(e.getKey() + ": " + e.getValue());

                }

            }
            
//            // conn = doConnect(urlString.toString(), opts.method);
//            final URL url = new URL(urlString.toString());
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod(opts.method);
//            conn.setDoOutput(true);
//            conn.setDoInput(true);
//            conn.setUseCaches(false);
//            conn.setReadTimeout(opts.timeout);
//            conn.setRequestProperty("Accept", opts.acceptHeader);
//            if (log.isDebugEnabled())
//                log.debug("Accept: " + opts.acceptHeader);
            
            if (opts.entity != null) {

//                if (opts.data == null)
//                    throw new AssertionError();

//                final String contentLength = Integer.toString(opts.data.length);

//                conn.setRequestProperty("Content-Type", opts.contentType);
//                conn.setRequestProperty("Content-Length", contentLength);

//                if (log.isDebugEnabled()) {
//                    log.debug("Content-Type: " + opts.contentType);
//                    log.debug("Content-Length: " + contentLength);
//                }

//                final ByteArrayEntity entity = new ByteArrayEntity(opts.data);
//                entity.setContentType(opts.contentType);

                ((HttpEntityEnclosingRequestBase) request).setEntity(opts.entity);
                
//                final OutputStream os = conn.getOutputStream();
//                try {
//                    os.write(opts.data);
//                    os.flush();
//                } finally {
//                    os.close();
//                }

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
            throw new RuntimeException(sparqlEndpointURL + " : " + t, t);
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

    /**
     * Throw an exception if the status code does not indicate success.
     * 
     * @param response
     *            The response.
     *            
     * @return The response.
     * 
     * @throws IOException
     */
    static public HttpResponse checkResponseCode(final HttpResponse response)
            throws IOException {
        
        final int rc = response.getStatusLine().getStatusCode();
        
        if (rc < 200 || rc >= 300) {
        
            throw new HttpException(rc, "Status Code=" + rc + ", Status Line="
                    + response.getStatusLine() + ", Response="
                    + getResponseBody(response));

        }

        if (log.isDebugEnabled()) {
            /*
             * write out the status list, headers, etc.
             */
            log.debug("*** Response ***");
            log.debug("Status Line: " + response.getStatusLine());
        }

        return response;
        
    }

    protected static String getResponseBody(final HttpResponse response)
            throws IOException {

        final Reader r = new InputStreamReader(response.getEntity().getContent());

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
     * Extracts the solutions from a SPARQL query.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return The results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    public TupleQueryResult tupleResults(final ConnectOptions opts, final UUID queryId)
            throws Exception {

    	HttpResponse response = null;
        HttpEntity entity = null;
        BackgroundTupleResult result = null;
        TupleQueryResultImpl tqrImpl = null;
        try {

            response = doConnect(opts);

            checkResponseCode(response);
            
            entity = response.getEntity();
            
            final String contentType = entity.getContentType().getValue();
    
            final MiniMime mimeType = new MiniMime(contentType);
            
            final TupleQueryResultFormat format = TupleQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());
    
            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + sparqlEndpointURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

            final TupleQueryResultParserFactory parserFactory = TupleQueryResultParserRegistry
                    .getInstance().get(format);

            if (parserFactory == null)
                throw new IOException(
                        "No parser for format for service response: serviceURI="
                                + sparqlEndpointURL + ", contentType=" + contentType
                                + ", format=" + format + " : response="
                                + getResponseBody(response));

            final TupleQueryResultParser parser = parserFactory.getParser();
    
            final InputStream in = entity.getContent();
            
            result = new BackgroundTupleResult(parser, in, entity);
            
            executor.execute(result);
            
            final MapBindingSet bindings = new MapBindingSet();
            
            final InsertBindingSetCursor cursor = 
                new InsertBindingSetCursor(result, bindings);
            
            final List<String> list = new ArrayList<String>(
                    result.getBindingNames());
            
            tqrImpl = new TupleQueryResultImpl(list, cursor) {

            	final AtomicBoolean notDone = new AtomicBoolean(true);
            	
            	@Override
            	public boolean hasNext() throws QueryEvaluationException {
            	
            		final boolean hasNext = super.hasNext();
            		
            		if (hasNext == false) {
            			
            			notDone.set(false);
            			
            		}
            		
            		return hasNext;
            		
            	}
            	
            	@Override
            	public void close() throws QueryEvaluationException {
            		
            		try {
        			
            			super.close();
        			
            		} finally {
            			
		    			if (notDone.compareAndSet(true, false)) {
		    				
		    				try {
		    					cancel(queryId);
		    				} catch (Exception ex) { }
		    				
		    			}
		    			
            		}
        			
            	};
            	
            };
            
            return tqrImpl;
            
//            final TupleQueryResultBuilder handler = new TupleQueryResultBuilder();
//    
//            parser.setTupleQueryResultHandler(handler);
//    
//            parser.parse(entity.getContent());
//    
//            // done.
//            return handler.getQueryResult();
            
        } finally {
            
//            // terminate the http connection.
//            response.disconnect();
            if (entity != null && result == null) {
                try {
                    EntityUtils.consume(entity);
                } catch (IOException ex) { }
            }
            
            if (response != null && tqrImpl == null) {
            	try {
            		cancel(queryId);
            	} catch(Exception ex) { }
            }
            
        }

    }
    
    /**
     * Builds a graph from an RDF result set (statements, not binding sets).
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return The graph
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    public GraphQueryResult graphResults(final ConnectOptions opts, final UUID queryId) 
    		throws Exception {

    	HttpResponse response = null;
        HttpEntity entity = null;
        BackgroundGraphResult result = null;
        try {

            response = doConnect(opts);

            checkResponseCode(response);
            
            entity = response.getEntity();

            final String baseURI = "";

            final String contentType = entity.getContentType().getValue();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");
            
            final MiniMime mimeType = new MiniMime(contentType);
            
            final RDFFormat format = RDFFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + sparqlEndpointURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

            final RDFParserFactory factory = RDFParserRegistry.getInstance().get(format);

            if (factory == null)
                throw new RuntimeException(
                        "RDFParserFactory not found: Content-Type="
                                + contentType + ", format=" + format);

            final RDFParser parser = factory.getParser();
            
            // TODO These options should be configurable using RDFParserOptions.
            parser.setValueFactory(new ValueFactoryImpl());

            parser.setVerifyData(true);

            parser.setStopAtFirstError(true);

            parser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

            Charset charset = Charset.forName(UTF8);
            try {
                final Header encoding = entity.getContentEncoding();
                if (encoding != null)
                    charset = Charset.forName(encoding.getValue());
            } catch (IllegalCharsetNameException e) {
                // work around for Joseki-3.2
                // Content-Type: application/rdf+xml;
                // charset=application/rdf+xml
            }
            
            final BackgroundGraphResult tmp = new BackgroundGraphResult(
                    parser, entity.getContent(), charset, baseURI, entity) {
            	
            	final AtomicBoolean notDone = new AtomicBoolean(true);
            	
            	@Override
            	public boolean hasNext() throws QueryEvaluationException {
            	
            		final boolean hasNext = super.hasNext();
            		
            		if (hasNext == false) {
            			
            			notDone.set(false);
            			
            		}
            		
            		return hasNext;
            		
            	}
            	
            	@Override
            	public void close() throws QueryEvaluationException {
            		
            		try {
        			
            			super.close();
        			
            		} finally {
            			
		    			if (notDone.compareAndSet(true, false)) {
		    				
		    				try {
		    					cancel(queryId);
		    				} catch (Exception ex) { }
		    				
		    			}
		    			
            		}
        			
            	};
            	
            };
            
            executor.execute(tmp);
            
            result = tmp;
            
            return result;

//            final Graph g = new GraphImpl();
//
//            parser.setRDFHandler(new StatementCollector(g));
//
//            parser.parse(entity.getContent(), baseURI);
//
////            return new GraphQueryResultImpl(Collections.EMPTY_MAP, g);
//            return g;

        } finally {

//            // terminate the http connection.
//            response.disconnect();
            if (response != null && result == null) {
                try {
                    EntityUtils.consume(entity);
                } catch (IOException ex) { }
                
                try {
                	cancel(queryId);
                } catch (Exception ex) { }
            }

        }

    }

    /**
     * Parse a SPARQL result set for an ASK query.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return <code>true</code> or <code>false</code> depending on what was
     *         encoded in the SPARQL result set.
     * 
     * @throws Exception
     *             If anything goes wrong, including if the result set does not
     *             encode a single boolean value.
     */
    protected boolean booleanResults(final ConnectOptions opts, final UUID queryId) throws Exception {

    	HttpResponse response = null;
        HttpEntity entity = null;
        Boolean result = null;
        try {

            response = doConnect(opts);

            checkResponseCode(response);
            
            entity = response.getEntity();
            
            final String contentType = entity.getContentType().getValue();

            final MiniMime mimeType = new MiniMime(contentType);
            
            final BooleanQueryResultFormat format = BooleanQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + sparqlEndpointURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

            final BooleanQueryResultParserFactory factory = BooleanQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("No factory for Content-Type: " + contentType);

            final BooleanQueryResultParser parser = factory.getParser();

            result = parser.parse(entity.getContent());
            
            return result;

        } finally {

//            // terminate the http connection.
//            response.disconnect();
        	if (result == null) {
	            try {
	                EntityUtils.consume(entity);
	            } catch (IOException ex) { }
	            
	            try {
	            	cancel(queryId);
	            } catch (Exception ex) { }
        	}

        }

    }

    /**
     * Counts the #of results in a SPARQL result set.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return The #of results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    protected long countResults(final HttpResponse response) throws Exception {

        HttpEntity entity = null;
        try {

            entity = response.getEntity();
            
            final String contentType = entity.getContentType().getValue();

            final MiniMime mimeType = new MiniMime(contentType);
            
            final TupleQueryResultFormat format = TupleQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + sparqlEndpointURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

            final TupleQueryResultParserFactory factory = TupleQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("No factory for Content-Type: " + contentType);

            final TupleQueryResultParser parser = factory.getParser();

            final AtomicLong nsolutions = new AtomicLong();

            parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
                // Indicates the end of a sequence of solutions.
                public void endQueryResult() {
                    // connection close is handled in finally{}
                }

                // Handles a solution.
                public void handleSolution(final BindingSet bset) {
                    if (log.isDebugEnabled())
                        log.debug(bset.toString());
                    nsolutions.incrementAndGet();
                }

                // Indicates the start of a sequence of Solutions.
                public void startQueryResult(List<String> bindingNames) {
                }
            });

            parser.parse(entity.getContent());

            if (log.isInfoEnabled())
                log.info("nsolutions=" + nsolutions);

            // done.
            return nsolutions.longValue();

        } finally {

//            // terminate the http connection.
//            response.disconnect();
            try {
                EntityUtils.consume(entity);
            } catch (IOException ex) { }

        }

    }

    static private MutationResult mutationResults(final HttpResponse response)
            throws Exception {

        HttpEntity entity = null;
        try {

            entity = response.getEntity();
            
            final String contentType = entity.getContentType().getValue();

            if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

                throw new RuntimeException("Expecting Content-Type of "
                        + IMimeTypes.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final AtomicLong mutationCount = new AtomicLong();
            final AtomicLong elapsedMillis = new AtomicLong();

            /*
             * For example: <data modified="5" milliseconds="112"/>
             */
            parser.parse(entity.getContent(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if (!"data".equals(qName))
                        throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                                + ", localName=" + localName + ", qName="
                                + qName);

                    mutationCount.set(Long.valueOf(attributes
                            .getValue("modified")));

                    elapsedMillis.set(Long.valueOf(attributes
                            .getValue("milliseconds")));
                           
                }
                
            });
            
            // done.
            return new MutationResult(mutationCount.get(), elapsedMillis.get());

        } finally {

//            // terminate the http connection.
//            response.disconnect();
            try {
                EntityUtils.consume(entity);
            } catch (IOException ex) { }

        }

    }

    static protected RangeCountResult rangeCountResults(
            final HttpResponse response) throws Exception {

        HttpEntity entity = null;
        try {

            entity = response.getEntity();
            
            final String contentType = entity.getContentType().getValue();

            if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

                throw new RuntimeException("Expecting Content-Type of "
                        + IMimeTypes.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final AtomicLong rangeCount = new AtomicLong();
            final AtomicLong elapsedMillis = new AtomicLong();

            /*
             * For example: <data rangeCount="5" milliseconds="112"/>
             */
            parser.parse(entity.getContent(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if (!"data".equals(qName))
                        throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                                + ", localName=" + localName + ", qName="
                                + qName);

                    rangeCount.set(Long.valueOf(attributes
                            .getValue("rangeCount")));

                    elapsedMillis.set(Long.valueOf(attributes
                            .getValue("milliseconds")));
                           
                }
                
            });
            
            // done.
            return new RangeCountResult(rangeCount.get(), elapsedMillis.get());

        } finally {

//            // terminate the http connection.
//            response.disconnect();
            try {
                EntityUtils.consume(entity);
            } catch (IOException ex) { }

        }

    }

    static protected ContextsResult contextsResults(
            final HttpResponse response) throws Exception {

        HttpEntity entity = null;
        try {

            entity = response.getEntity();
            
            final String contentType = entity.getContentType().getValue();

            if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

                throw new RuntimeException("Expecting Content-Type of "
                        + IMimeTypes.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final Collection<Resource> contexts = 
            		Collections.synchronizedCollection(new LinkedList<Resource>());

            /*
             * For example: 
             * <contexts>
             * <context uri="http://foo"/>
             * <context uri="http://bar"/>
             * </contexts>
             */
            parser.parse(entity.getContent(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if ("context".equals(qName))
                    	contexts.add(new URIImpl(attributes.getValue("uri")));

                }
                
            });
            
            // done.
            return new ContextsResult(contexts);

        } finally {

//            // terminate the http connection.
//            response.disconnect();
            try {
                EntityUtils.consume(entity);
            } catch (IOException ex) { }

        }

    }

    /**
     * Serialize an iteration of statements into a byte[] to send across the
     * wire.
     */
    protected static byte[] serialize(final Iterable<? extends Statement> stmts,
            final RDFFormat format) throws Exception {
        
        final RDFWriterFactory writerFactory = 
            RDFWriterRegistry.getInstance().get(format);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        final RDFWriter writer = writerFactory.getWriter(baos);
        
        writer.startRDF();
        
        for (Statement stmt : stmts) {
        
            writer.handleStatement(stmt);
            
        }

        writer.endRDF();
        
        final byte[] data = baos.toByteArray();
        
        return data;

    }

    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * SPARQL end point for a QUERY or other idempotent operation.
     */
    final protected ConnectOptions newQueryConnectOptions() {

        final ConnectOptions opts = newConnectOptions(sparqlEndpointURL);

        opts.method = getQueryMethod();

        return opts;

    }

    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * SPARQL end point for an UPDATE or other non-idempotant operation.
     */
    final protected ConnectOptions newUpdateConnectOptions() {

        final ConnectOptions opts = newConnectOptions(sparqlEndpointURL);
        
        opts.method = "POST";
        
        return opts;

    }
    
    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * SPARQL end point.
     */
    final protected ConnectOptions newConnectOptions() {
    
        return newConnectOptions(sparqlEndpointURL);
        
    }
    
    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * specified service URL.
     * 
     * @param serviceURL
     *            The URL of the service for the request.
     */
    protected ConnectOptions newConnectOptions(final String serviceURL) {

        final ConnectOptions opts = new ConnectOptions(serviceURL);

        return opts;

    }

    /**
     * Utility method to turn a {@link GraphQueryResult} into a {@link Graph}.
     * 
     * @param result
     *            The {@link GraphQueryResult}.
     * 
     * @return The {@link Graph}.
     * 
     * @throws Exception
     */
    static public Graph asGraph(final GraphQueryResult result) throws Exception {

        final Graph g = new GraphImpl();

        while (result.hasNext()) {

            g.add(result.next());

        }

        return g;

    }
    
    /**
     * Convert an array of URIs to an array of URI strings.
     */
    protected String[] toStrings(final Resource[] resources) {
    	
    	if (resources == null)
    		return null;
    	
    	if (resources.length == 0)
    		return new String[0];

    	final String[] uris = new String[resources.length];
    	
    	for (int i = 0; i < resources.length; i++) {
    		
    		uris[i] = resources[i].stringValue();
    		
    	}
    	
    	return uris;
    	
    }

}
