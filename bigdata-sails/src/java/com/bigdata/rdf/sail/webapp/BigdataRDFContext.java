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

import info.aduna.xml.XMLWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.AbstractOperation;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultWriter;
import org.openrdf.query.resultio.BooleanQueryResultWriterRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.sail.SailException;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.counters.CAT;
import com.bigdata.io.NullOutputStream;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailBooleanQuery;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.BigdataSailUpdate;
import com.bigdata.rdf.sail.ISPARQLUpdateListener;
import com.bigdata.rdf.sail.SPARQLUpdateEvent;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.webapp.client.StringUtil;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.RelationSchema;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

/**
 * Class encapsulates state shared by {@link QueryServlet}(s) for the same
 * {@link IIndexManager}.
 * 
 * @author Martyn Cutcher
 * @author thompsonbry@users.sourceforge.net
 */
public class BigdataRDFContext extends BigdataBaseContext {

    static private final transient Logger log = Logger
            .getLogger(BigdataRDFContext.class);

    /**
     * URL Query parameter used to request the explanation of a query rather
     * than its results.
     */
    protected static final String EXPLAIN = "explain";
    
    /**
     * Optional value for the {@link #EXPLAIN} URL query parameter that may be
     * used to request more detail in the "EXPLAIN" of a query.
     */
    protected static final String EXPLAIN_DETAILS = "details";
    
    /**
     * URL Query parameter used to request the "analytic" query hints. MAY be
     * <code>null</code>, in which case we do not set
     * {@link QueryHints#ANALYTIC} query hint.
     */
    protected static final String ANALYTIC = "analytic";
    
    /**
     * URL Query parameter used to request the use of the Runtime Query
     * Optimizer.
     */
    protected static final String RTO = "RTO";
    
    /**
     * URL Query parameter used to request an XHTML response for SPARQL
     * QUERY or SPARQL UPDATE.  For SPARQL QUERY, this provides an XHTML
     * table view of the solutions.  For SPARQL UPDATE, this provides an
     * incremental progress report on the UPDATE request.
     */
    protected static final String XHTML = "xhtml";
    
    /**
     * URL Query parameter used to specify an XSL style sheet to be associated
     * with the response in combination with the {@link #XHTML} URL query
     * parameter.
     */
    protected static final String XSL_STYLESHEET = "xsl-stylesheet";
    
    /**
     * The default XSL style sheet.
     * 
     * @see #XSL_STYLESHEET
     */
    protected static final String DEFAULT_XSL_STYLESHEET = BigdataStatics
            .getContextPath() + "/html/result-to-html.xsl";
    
    /**
     * URL Query parameter used to request an incremental XHTML representation
     * reporting on the progress of a SPARQL UPDATE.
     * <p>
     * Note: When this option is specified, the SPARQL UPDATE will use an HTTP
     * status code of 200 (Ok) even if the UPDATE fails. In the event of an
     * UPDATE failure, the stack trace will be formatted into the response.
     */
    protected static final String MONITOR = "monitor";
    
    /**
     * URL query parameter used to specify a URI in the default graph for SPARQL
     * query (but not for SPARQL update).
     */
    protected static final String DEFAULT_GRAPH_URI = "default-graph-uri";

    /**
     * URL query parameter used to specify a URI in the set of named graphs for
     * SPARQL query (but not for SPARQL update).
     */
    protected static final String NAMED_GRAPH_URI = "named-graph-uri";
    
    /**
     * URL query parameter used to specify a URI in the default graph for SPARQL
     * UPDATE.
     */
    protected static final String USING_GRAPH_URI = "using-graph-uri";

    /**
     * URL query parameter used to specify a URI in the set of named graphs for
     * SPARQL UPDATE.
     */
    protected static final String USING_NAMED_GRAPH_URI = "using-named-graph-uri";

    /**
     * URL query parameter used to specify a non-default KB namespace (as an
     * alternative to having it in the URL path). The path takes precendence
     * over this query parameter.
     * 
     * @see BigdataRDFServlet#getNamespace(HttpServletRequest)
     */
    protected static final String NAMESPACE = "namespace";
    
    /**
     * HTTP header may be used to specify the timeout for a query.
     * 
     * @see http://trac.bigdata.com/ticket/914 (Set timeout on remote query)
     */
    static private final String HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS = "X-BIGDATA-MAX-QUERY-MILLIS";

    private final SparqlEndpointConfig m_config;

    /**
     * A thread pool for running accepted queries against the
     * {@link QueryEngine}.
     */
    /*package*/final ExecutorService queryService;
	
	private final ScheduledFuture<?> m_queueStatsFuture;
	private final ThreadPoolExecutorBaseStatisticsTask m_queueSampleTask;

    /**
     * The currently executing queries (does not include queries where a client
     * has established a connection but the query is not running because the
     * {@link #queryService} is blocking).
     * <p>
     * Note: This includes both SPARQL QUERY and SPARQL UPDATE requests.
     * However, the {@link AbstractQueryTask#queryId2} might not yet be bound
     * since it is not set until the request begins to execute. See
     * {@link AbstractQueryTask#setQueryId(ASTContainer)}.
     */
    private final ConcurrentHashMap<Long/* queryId */, RunningQuery> m_queries = new ConcurrentHashMap<Long, RunningQuery>();
    
    /**
     * The currently executing QUERY and UPDATE requests.
     * <p>
     * Note: This does not include requests where a client has established a
     * connection to the SPARQL end point but the request is not executing
     * because the {@link #queryService} is blocking).
     * <p>
     * Note: This collection was introduced because the SPARQL UPDATE requests
     * are not executed on the {@link QueryEngine} and hence we can not use
     * {@link QueryEngine#getRunningQuery(UUID)} to resolve the {@link Future}
     * of an {@link UpdateTask}.
     */
    private final ConcurrentHashMap<UUID/* queryId2 */, RunningQuery> m_queries2 = new ConcurrentHashMap<UUID, RunningQuery>();

    /**
     * Return the {@link RunningQuery} for a currently executing SPARQL QUERY or
     * UPDATE request.
     * 
     * @param queryId2
     *            The {@link UUID} for the request.
     *            
     * @return The {@link RunningQuery} iff it was found.
     */
    RunningQuery getQueryById(final UUID queryId2) {

        return m_queries2.get(queryId2);
        
    }
    
    /**
     * Factory for the query identifiers.
     */
    private final AtomicLong m_queryIdFactory = new AtomicLong();
    
    final public Map<Long, RunningQuery> getQueries() {

        return m_queries;
        
    }
    
    final public AtomicLong getQueryIdFactory() {
    
        return m_queryIdFactory;
        
    }
    
    public BigdataRDFContext(final SparqlEndpointConfig config,
            final IIndexManager indexManager) {

        super(indexManager);
        
        if(config == null)
            throw new IllegalArgumentException();
        
		if (config.namespace == null)
			throw new IllegalArgumentException();

		m_config = config;

        if (config.queryThreadPoolSize == 0) {

            queryService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".queryService"));

        } else {

            queryService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    config.queryThreadPoolSize, new DaemonThreadFactory(
                            getClass().getName() + ".queryService"));

        }

		if (indexManager.getCollectQueueStatistics()) {

			final long initialDelay = 0; // initial delay in ms.
			final long delay = 1000; // delay in ms.
			final TimeUnit unit = TimeUnit.MILLISECONDS;

            m_queueSampleTask = new ThreadPoolExecutorBaseStatisticsTask(
                    (ThreadPoolExecutor) queryService);

            m_queueStatsFuture = indexManager.addScheduledTask(
                    m_queueSampleTask, initialDelay, delay, unit);

		} else {

			m_queueSampleTask = null;

			m_queueStatsFuture = null;

		}

	}

//    /**
//     * Normal shutdown waits until all accepted queries are done. 
//     */
//    void shutdown() {
//        
//        if(log.isInfoEnabled())
//            log.info("Normal shutdown.");
//
//        // Stop collecting queue statistics.
//        if (m_queueStatsFuture != null)
//            m_queueStatsFuture.cancel(true/* mayInterruptIfRunning */);
//
//        // Stop servicing new requests. 
//        queryService.shutdown();
//        try {
//            queryService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
//        } catch (InterruptedException ex) {
//            throw new RuntimeException(ex);
//        }
//        
//    }

    /**
     * Immediate shutdown interrupts any running queries.
     * 
     * FIXME Must abort any open transactions. This does not matter for the
     * standalone database, but it will make a difference in scale-out. The
     * transaction identifiers could be obtained from the {@link #queries} map.
     * 
     * FIXME This must also abort any running updates. Those are currently
     * running in thread handling the {@link HttpServletRequest}, however it
     * probably makes sense to execute them on a bounded thread pool as well.
     */
    void shutdownNow() {

        if(log.isInfoEnabled())
            log.info("Immediate shutdown.");
        
        // Stop collecting queue statistics.
        if (m_queueStatsFuture != null)
            m_queueStatsFuture.cancel(true/* mayInterruptIfRunning */);

        // Interrupt all running queries.
        queryService.shutdownNow();
        
    }

    public SparqlEndpointConfig getConfig() {
		
	    return m_config;
	    
	}

	public ThreadPoolExecutorBaseStatisticsTask getSampleTask() {

	    return m_queueSampleTask;
	    
	}

    /**
     * Return the effective boolean value of a URL query parameter such as
     * "analytic". If the URL query parameter was not given, then the effective
     * boolean value is the <i>defaultValue</o>. If a URL query parameter which
     * is given without an explicit value, such as <code>&analytic</code>, then
     * it will be reported as <code>true</code>.
     * 
     * @param s
     *            The value of the URL query parameter.
     * @param defaultValue
     *            The default value to return if the parameter was not given.
     * 
     * @return The effective boolean value.
     */
    protected static Boolean getEffectiveBooleanValue(String s,
            final Boolean defaultValue) {

        if (s == null)
            return defaultValue;

        s = s.trim();

        if (s.length() == 0) {

            return true;

        }

        return Boolean.valueOf(s);

    }

    /**
     * Return the effective string value of a URL query parameter. If the URL
     * query parameter was not given, or if it was given without an explicit
     * value, then the effective string value is the <i>defaultValue</o>.
     * 
     * @param s
     *            The value of the URL query parameter.
     * @param defaultValue
     *            The default value to return if the parameter was not given or
     *            given without an explicit value.
     * 
     * @return The effective value.
     */
    protected static String getEffectiveStringValue(String s,
            final String defaultValue) {

        if (s == null)
            return defaultValue;

        s = s.trim();

        if (s.length() == 0) {

            return defaultValue;

        }

        return s;

    }
    
    /**
     * Invoked if {@link #EXPLAIN} is found as a URL request parameter to
     * see whether it exists with {@link #EXPLAIN_DETAILS} as a value. We
     * have to check each value since there might be more than one.
     * 
     * @param req
     *            The request.
     * @return
     */
    static private boolean isExplainDetails(final HttpServletRequest req) {

        final String[] vals = req.getParameterValues(EXPLAIN);

        if (vals == null) {

            return false;

        }

        for (String val : vals) {

            if (val.equals(EXPLAIN_DETAILS))
                return true;

        }

        return false;

    }
    
	/**
     * Abstract base class for running queries handles the timing, pipe,
     * reporting, obtains the connection, and provides the finally {} semantics
     * for each type of query task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public abstract class AbstractQueryTask implements Callable<Void> {
        
        /** The namespace against which the query will be run. */
        private final String namespace;

        /**
         * The timestamp of the view for that namespace against which the query
         * will be run.
         */
        public final long timestamp;

        /**
         * The baseURI is set from the effective request URI.
         */
        protected final String baseURI;

        /**
         * The {@link ASTContainer} provides access to the original SPARQL
         * query, the query model, the query plan, etc.
         */
        protected final ASTContainer astContainer;

        /**
         * <code>true</code> iff this is a SPARQL UPDATE request.
         */
        protected final boolean update;

        /**
         * A symbolic constant indicating the type of query.
         */
        protected final QueryType queryType;
        
        /**
         * The negotiated MIME type to be used for the query response (this
         * does not include the charset encoding) -or- <code>null</code> if
         * this is a SPARQL UPDATE request.
         */
        protected final String mimeType;

        /**
         * The character encoding to use with the negotiated {@link #mimeType}
         * -or- <code>null</code> (it will be <code>null</code> for a binary
         * encoding).
         */
        protected final Charset charset;
        
        /**
         * The file extension (without the leading ".") to use with the
         * negotiated {@link #mimeType} -or- <code>null</code> if this is a
         * SPARQL UPDATE request
         */
        protected final String fileExt;
        
        /** The request. */
        protected final HttpServletRequest req;

        /** The response. */
        protected final HttpServletResponse resp;

        /** Where to write the response. */
        protected final OutputStream os;

//		/**
//		 * Set to the timestamp as reported by {@link System#nanoTime()} when
//		 * the query begins to execute.
//		 */
//        final AtomicLong beginTime = new AtomicLong();
        
        /**
         * The queryId as assigned by the SPARQL end point (rather than the
         * {@link QueryEngine}).
         */
        protected final Long queryId;

        /**
         * The queryId used by the {@link QueryEngine}. If the application has
         * not specified this using {@link QueryHints#QUERYID} then this is
         * assigned and set on the query using {@link QueryHints#QUERYID}. This
         * decision can not be made until we parse the query so the behavior is
         * handled by the subclasses. This also means that {@link #queryId2} is
         * NOT available until the {@link AbstractQueryTask} begins to execute.
         * <p>
         * Note: Even though a SPARQL UPDATE request does not execute on the
         * {@link QueryEngine}, a {@link UUID} will also be bound for the SPARQL
         * UPDATE request. This provides us with a consistent identifier that
         * can be used by clients and in the XHTML UI on the status page to
         * refer to a SPARQL UPDATE request.
         * 
         * @see AbstractQueryTask#setQueryId(ASTContainer)
         */
        volatile protected UUID queryId2;

        /**
         * The parsed query. It will be one of the {@link BigdataSailQuery}
         * implementations or {@link BigdataSailUpdate}. They all extend
         * {@link AbstractOperation}.
         * <p>
         * Note: This field is made visible by the volatile write on
         * {@link #queryId2}.
         */
        protected AbstractOperation sailQueryOrUpdate;

        /**
         * The {@link Future} of the {@link UpdateTask} and <code>null</code> if
         * this is not a SPARQL UPDATE or if the {@link UpdateTask} has not
         * begun execution.
         */
        volatile protected Future<Void> updateFuture;
        
        /**
         * When <code>true</code>, provide an "explanation" for the query (query
         * plan, query evaluation statistics) rather than the results of the
         * query.
         */
        final boolean explain;

        /**
         * When <code>true</code>, provide an additional level of detail for the
         * query explanation.
         */
        final boolean explainDetails;

        /**
         * When <code>true</code>, enable the "analytic" query hints. 
         */
        final boolean analytic;

        /**
         * When <code>true</code>, enable the Runtime Query Optimizer.
         */
        final boolean rto;
        
        /**
         * When <code>true</code>, provide an view of the XHTML representation
         * of the solutions or graph result (SPARQL QUERY)
         * 
         * @see BigdataRDFContext#XHTML
         */
        final boolean xhtml;
        
        /**
         * When <code>true</code>, provide an incremental XHTML representation
         * reporting on the progress of a SPARQL UPDATE.
         * <p>
         * Note: When <code>true</code>, the SPARQL UPDATE will use an HTTP
         * status code of 200 (Ok) even if the UPDATE fails. In the event of an
         * UPDATE failure, the stack trace will be formatted into the response.
         * 
         * @see BigdataRDFContext#MONITOR
         */
        final boolean monitor;
        
        /**
         * The timstamp (in nanoseconds) when the task obtains its connection
         * and begins to execute. 
         */
        private volatile long beginNanos = 0L;
        
        /**
         * The timstamp (in nanoseconds) when the task finishes its execution.
         */
        private volatile long endNanos = 0L;
        
        /**
         * Return the elapsed execution time in milliseconds. This will be ZERO
         * (0) until the task begins to execute.
         * <p>
         * Note: This is used to report the elapsed time for the execution of
         * SPARQL UPDATE requests. Since SPARQL UPDATE requests are not run on
         * the {@link QueryEngine}, {@link IRunningQuery#getElapsed()} can not
         * be used for UPDATEs. It could also be used for QUERYs, but those are
         * run on the {@link QueryEngine} and {@link IRunningQuery#getElapsed()}
         * may be used instead.
         */
        public long getElapsedExecutionMillis() {

            if (beginNanos == 0L) {
                // Not yet executing.
                return 0L;
            }
            long now = endNanos;
            if (now == 0L) {
                // Not yet done executing.
                now = System.nanoTime();
            }
            // Elasped execution time (wall clock).
            final long elapsed = now - beginNanos;
            // Convert to milliseconds.
            return TimeUnit.NANOSECONDS.toMillis(elapsed);
        }

        /**
         * 
         * @param namespace
         *            The namespace against which the query will be run.
         * @param timestamp
         *            The timestamp of the view for that namespace against which
         *            the query will be run.
         * @param baseURI
         *            The base URI.
         * @param astContainer
         *            The container with all the information about the submitted
         *            query, including the original SPARQL query, the parse
         *            tree, etc.
         * @param queryType
         *            The {@link QueryType}.
         * @param mimeType
         *            The MIME type to be used for the response. The caller must
         *            verify that the MIME Type is appropriate for the query
         *            type.
         * @param charset
         *            The character encoding to use with the negotiated MIME
         *            type (this is <code>null</code> for binary encodings).
         * @param fileExt
         *            The file extension (without the leading ".") to use with
         *            that MIME Type.
         * @param req
         *            The request.
         * @param os
         *            Where to write the data for the query result.
         */
        protected AbstractQueryTask(//
                final String namespace,//
                final long timestamp, //
                final String baseURI,
                final ASTContainer astContainer,//
                final QueryType queryType,//
                final String mimeType,//
                final Charset charset,//
                final String fileExt,//
                final HttpServletRequest req,//
                final HttpServletResponse resp,//
                final OutputStream os//
        ) {

            if (namespace == null)
                throw new IllegalArgumentException();
            if (baseURI == null)
                throw new IllegalArgumentException();
            if (astContainer == null)
                throw new IllegalArgumentException();
            if (queryType == null)
                throw new IllegalArgumentException();
            if (mimeType == null)
                throw new IllegalArgumentException();
            if (fileExt == null)
                throw new IllegalArgumentException();
            if (req == null)
                throw new IllegalArgumentException();
            if (resp == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.baseURI = baseURI;
            this.astContainer = astContainer;
            this.update = false;
            this.queryType = queryType;
            this.mimeType = mimeType;
            this.charset = charset;
            this.fileExt = fileExt;
            this.req = req;
            this.resp = resp;
            this.explain = req.getParameter(EXPLAIN) != null;
            this.explainDetails = explain && isExplainDetails(req);
            this.analytic = getEffectiveBooleanValue(
                    req.getParameter(ANALYTIC), QueryHints.DEFAULT_ANALYTIC);
            this.rto = getEffectiveBooleanValue(req.getParameter(RTO),
                    QueryHints.DEFAULT_OPTIMIZER
                            .equals(QueryOptimizerEnum.Runtime));
            this.xhtml = getEffectiveBooleanValue(req.getParameter(XHTML),
                    false);
            this.monitor = getEffectiveBooleanValue(req.getParameter(MONITOR),
                    false);
            this.os = os;
            this.queryId = Long.valueOf(m_queryIdFactory.incrementAndGet());

        }

        /**
         * 
         * @param namespace
         *            The namespace against which the query will be run.
         * @param timestamp
         *            The timestamp of the view for that namespace against which
         *            the query will be run.
         * @param baseURI
         *            The base URI.
         * @param astContainer
         *            The container with all the information about the submitted
         *            query, including the original SPARQL query, the parse
         *            tree, etc.
         * @param req
         *            The request.
         * @param resp
         *            The response.
         * @param os
         *            Where to write the data for the query result.
         */
        protected AbstractQueryTask(//
                final String namespace,//
                final long timestamp, //
                final String baseURI,
                final ASTContainer astContainer,//
//                final QueryType queryType,//
//                final String mimeType,//
//                final Charset charset,//
//                final String fileExt,//
                final HttpServletRequest req,//
                final HttpServletResponse resp,//
                final OutputStream os//
        ) {

            if (namespace == null)
                throw new IllegalArgumentException();
            if (baseURI == null)
                throw new IllegalArgumentException();
            if (astContainer == null)
                throw new IllegalArgumentException();
            if (req == null)
                throw new IllegalArgumentException();
            if (resp == null)
                throw new IllegalArgumentException();
            if (os == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;
            this.timestamp = timestamp;
            this.baseURI = baseURI;
            this.astContainer = astContainer;
            this.update = true;
            this.queryType = null;
            this.mimeType = null;
            this.charset = Charset.forName("UTF-8");
            this.fileExt = null;
            this.req = req;
            this.resp = resp;
            this.explain = req.getParameter(EXPLAIN) != null;
            this.explainDetails = explain && isExplainDetails(req);
            this.analytic = getEffectiveBooleanValue(
                    req.getParameter(ANALYTIC), QueryHints.DEFAULT_ANALYTIC);
            this.rto = getEffectiveBooleanValue(req.getParameter(RTO),
                    QueryHints.DEFAULT_OPTIMIZER
                            .equals(QueryOptimizerEnum.Runtime));
            this.xhtml = getEffectiveBooleanValue(req.getParameter(XHTML),
                    false);
            this.monitor = getEffectiveBooleanValue(req.getParameter(MONITOR),
                    false);
            this.os = os;
            this.queryId = Long.valueOf(m_queryIdFactory.incrementAndGet());

        }

        /**
         * If the {@link HttpServletRequest} included one or more of
         * <ul>
         * <li>{@value BigdataRDFContext#DEFAULT_GRAPH_URI}</li>
         * <li>{@value BigdataRDFContext#NAMED_GRAPH_URI}</li>
         * <li>{@value BigdataRDFContext#USING_GRAPH_URI}</li>
         * <li>{@value BigdataRDFContext#USING_NAMED_GRAPH_URI}</li>
         * </ul>
         * then the {@link Dataset} for the query is replaced by the
         * {@link Dataset} constructed from those protocol parameters (the
         * parameters which are recognized are different for query and SPARQL
         * update).
         * 
         * @param queryOrUpdate
         *            The query.
         */
        protected void overrideDataset(final AbstractOperation queryOrUpdate) {
            
            final String[] defaultGraphURIs = req
                    .getParameterValues(update ? USING_GRAPH_URI
                            : DEFAULT_GRAPH_URI);

            final String[] namedGraphURIs = req
                    .getParameterValues(update ? USING_NAMED_GRAPH_URI
                            : NAMED_GRAPH_URI);

            if (defaultGraphURIs != null || namedGraphURIs != null) {

                final DatasetImpl dataset = new DatasetImpl();

                if (defaultGraphURIs != null)
                    for (String graphURI : defaultGraphURIs)
                        dataset.addDefaultGraph(new URIImpl(graphURI));

                if (namedGraphURIs != null)
                    for (String graphURI : namedGraphURIs)
                        dataset.addNamedGraph(new URIImpl(graphURI));

                queryOrUpdate.setDataset(dataset);

            }

        }

        /**
         * 
		 * <p>
		 * Note: This is also responsible for noticing the time at which the
		 * query begins to execute and storing the {@link RunningQuery} in the
		 * {@link #m_queries} map.
		 * 
         * @param The connection.
         */
        final AbstractQuery setupQuery(final BigdataSailRepositoryConnection cxn) {

            // Note the begin time for the query.
            final long begin = System.nanoTime();

            final AbstractQuery query = newQuery(cxn);

            // Figure out the UUID under which the query will execute.
            final UUID queryId2 = setQueryId(((BigdataSailQuery) query)
                    .getASTContainer());
            
            // Override query if data set protocol parameters were used.
			overrideDataset(query);

            if (analytic) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.ANALYTIC, "true");

            }

            if (rto) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.OPTIMIZER,
                        QueryOptimizerEnum.Runtime.toString());
                
            }

			// Set the query object.
			this.sailQueryOrUpdate = query;
			
			// Set the IRunningQuery's UUID (volatile write!) 
			this.queryId2 = queryId2;
			
            final RunningQuery r = new RunningQuery(queryId.longValue(),
                    queryId2, begin, this);

            // Stuff it in the maps of running queries.
            m_queries.put(queryId, r);
            m_queries2.put(queryId2, r);

            return query;
            
        }

        /**
         * 
         * <p>
         * Note: This is also responsible for noticing the time at which the
         * query begins to execute and storing the {@link RunningQuery} in the
         * {@link #m_queries} map.
         * 
         * @param cxn
         *            The connection.
         */
        final BigdataSailUpdate setupUpdate(
                final BigdataSailRepositoryConnection cxn) {

            // Note the begin time for the query.
            final long begin =  System.nanoTime();
            
            final BigdataSailUpdate update = new BigdataSailUpdate(astContainer,
                    cxn);

            // Figure out the UUID under which the query will execute.
            final UUID queryId2 = setQueryId(((BigdataSailUpdate) update)
                    .getASTContainer());
            
            // Override query if data set protocol parameters were used.
            overrideDataset(update);

            if (analytic) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.ANALYTIC, "true");

            }

            if (rto) {

                // Turn analytic query on/off as requested.
                astContainer.setQueryHint(QueryHints.OPTIMIZER,
                        QueryOptimizerEnum.Runtime.toString());
                
            }

            // Set the query object.
            this.sailQueryOrUpdate = update;
            
            /*
             * Make a note of the UUID associated with this UPDATE request
             * (volatile write!)
             * 
             * Note: While the UPDATE request does not directly execute on the
             * QueryEngine, each request UPDATE is assigned a UUID. The UUID is
             * either assigned by a query hint specified with the SPARQL UPDATE
             * request or generated automatically. In either case, it becomes
             * bound on the ASTContainer as a query hint.
             */
            this.queryId2 = queryId2;

            final RunningQuery r = new RunningQuery(queryId.longValue(),
                    queryId2, begin, this);
            
            // Stuff it in the maps of running queries.
            m_queries.put(queryId, r);
            m_queries2.put(queryId2, r);

            return update;
            
        }

        /**
         * Wrap the {@link ParsedQuery} as a {@link SailQuery}.
         * <p>
         * Note: This is achieved without reparsing the query.
         * 
         * @param cxn
         *            The connection.
         * 
         * @return The query.
         * 
         * @see http://trac.bigdata.com/ticket/914 (Set timeout on remote query)
         */
        private AbstractQuery newQuery(final BigdataSailRepositoryConnection cxn) {

            /*
             * Establish the query timeout. This may be set in web.xml, which
             * overrides all queries and sets a maximum allowed time for query
             * execution. This may also be set either via setMaxQuery() or
             * setMaxQueryMillis() which set a HTTP header (in milliseconds).
             */
            long queryTimeoutMillis = getConfig().queryTimeout;

            {
                final String s = req
                        .getHeader(HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS);
                if (s != null) {
                    long tmp = StringUtil.toLong(s);
                    if (tmp != -1L && //
                            (queryTimeoutMillis == 0/* noLimit */
                            || //
                            tmp < queryTimeoutMillis/* shorterLimit */)//
                    ) {
                        // Set based on the http header value.
                        queryTimeoutMillis = tmp;
                    }
                }
            }

            if (queryTimeoutMillis > 0) {

                final QueryRoot originalQuery = astContainer.getOriginalAST();

                originalQuery.setTimeout(queryTimeoutMillis);

            }
            
//            final ASTContainer astContainer = ((BigdataParsedQuery) parsedQuery)
//                    .getASTContainer();

//            final QueryType queryType = ((BigdataParsedQuery) parsedQuery)
//                    .getQueryType();

            switch (queryType) {
            case SELECT:
                return new BigdataSailTupleQuery(astContainer, cxn);
            case DESCRIBE:
            case CONSTRUCT:
                return new BigdataSailGraphQuery(astContainer, cxn);
            case ASK: {
                return new BigdataSailBooleanQuery(astContainer, cxn);
            }
            default:
                throw new RuntimeException("Unknown query type: " + queryType);
            }

        }

        /**
         * Determines the {@link UUID} which will be associated with the
         * {@link IRunningQuery}. If {@link QueryHints#QUERYID} has already been
         * used by the application to specify the {@link UUID} then that
         * {@link UUID} is noted. Otherwise, a random {@link UUID} is generated
         * and assigned to the query by binding it on the query hints.
         * <p>
         * Note: The ability to provide metadata from the {@link ASTContainer}
         * in the {@link StatusServlet} or the "EXPLAIN" page depends on the
         * ability to cross walk the queryIds as established by this method.
         * 
         * @param query
         *            The query.
         * 
         * @return The {@link UUID} which will be associated with the
         *         {@link IRunningQuery} and never <code>null</code>.
         */
		protected UUID setQueryId(final ASTContainer astContainer) {
			assert queryId2 == null; // precondition.
            // Figure out the effective UUID under which the query will run.
            final String queryIdStr = astContainer.getQueryHint(
                    QueryHints.QUERYID);
            if (queryIdStr == null) {
                // Not specified, so generate and set on query hint.
                queryId2 = UUID.randomUUID();
                astContainer.setQueryHint(QueryHints.QUERYID,
                        queryId2.toString());
			} else {
			    // Specified by a query hint.
				queryId2 = UUID.fromString(queryIdStr);
			}
            return queryId2;
		}

        /**
         * Execute the query.
         * 
         * @param cxn
         *            The connection.
         * @param os
         *            Where the write the query results.
         * 
         * @throws Exception
         */
        abstract protected void doQuery(BigdataSailRepositoryConnection cxn,
                OutputStream os) throws Exception;

        final public Void call() throws Exception {
			BigdataSailRepositoryConnection cxn = null;
            try {
                cxn = getQueryConnection(namespace, timestamp);
                if(log.isTraceEnabled())
                    log.trace("Query running...");
                beginNanos = System.nanoTime();
//                try {
            if (explain && !update) {
					/*
					 * The data goes to a bit bucket and we send an
					 * "explanation" of the query evaluation back to the caller.
					 * 
					 * Note: The trick is how to get hold of the IRunningQuery
					 * object. It is created deep within the Sail when we
					 * finally submit a query plan to the query engine. We have
					 * the queryId (on queryId2), so we can look up the
					 * IRunningQuery in [m_queries] while it is running, but
					 * once it is terminated the IRunningQuery will have been
					 * cleared from the internal map maintained by the
					 * QueryEngine, at which point we can not longer find it.
					 * 
					 * Note: We can't do this for UPDATE since it would have
					 * a side-effect anyway.  The way to "EXPLAIN" an UPDATE 
					 * is to break it down into the component QUERY bits and
					 * execute those.
					 */
    				doQuery(cxn, new NullOutputStream());
    			} else {
    				doQuery(cxn, os);
                    os.flush();
                    os.close();
    			}
            	if(log.isTraceEnabled())
            	    log.trace("Query done.");
//                } catch(Throwable t) {
//                	/*
//                	 * Log the query and the exception together.
//                	 */
//					log.error(t.getLocalizedMessage() + ":\n" + queryStr, t);
//                }
                return null;
            } catch (Throwable t) {
                log.error("Will abort: " + t, t);
                if (cxn != null && !cxn.isReadOnly()) {
                    /*
                     * Force rollback of the connection.
                     * 
                     * Note: It is possible that the commit has already been
                     * processed, in which case this rollback() will be a NOP.
                     * This can happen when there is an IO error when
                     * communicating with the client, but the database has
                     * already gone through a commit.
                     */
                    cxn.rollback();
                }
                throw new Exception(t);
            } finally {
                endNanos = System.nanoTime();
                m_queries.remove(queryId);
                if (queryId2 != null) m_queries2.remove(queryId2);
//                if (os != null) {
//                    try {
//                        os.close();
//                    } catch (Throwable t) {
//                        log.error(t, t);
//                    }
//                }
                if (cxn != null) {
                    try {
                        // Force close of the connection.
                        cxn.close();
                        if(log.isTraceEnabled())
                            log.trace("Connection closed.");
                    } catch (Throwable t) {
                        log.error(t, t);
                    }
                }
            }
        } // call()

    } // class AbstractQueryTask

    /**
     * Executes a ASK query.
     */
    private class AskQueryTask extends AbstractQueryTask {

        public AskQueryTask(final String namespace, final long timestamp,
                final String baseURI,
                final ASTContainer astContainer, final QueryType queryType,
                final BooleanQueryResultFormat format,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer, queryType,
                    format.getDefaultMIMEType(), format.getCharset(), format
                            .getDefaultFileExtension(), req, resp, os);

        }

        protected void doQuery(final BigdataSailRepositoryConnection cxn,
                final OutputStream os) throws Exception {

            final BigdataSailBooleanQuery query = (BigdataSailBooleanQuery) setupQuery(cxn);

            // Note: getQueryTask() verifies that format will be non-null.
            final BooleanQueryResultFormat format = BooleanQueryResultWriterRegistry
                    .getInstance().getFileFormatForMIMEType(mimeType);

            final BooleanQueryResultWriter w = BooleanQueryResultWriterRegistry
                    .getInstance().get(format).getWriter(os);

            final boolean result = query.evaluate();

            w.write(result);

        }

    }

    /**
     * Executes a tuple query.
     */
    private class TupleQueryTask extends AbstractQueryTask {

        public TupleQueryTask(final String namespace, final long timestamp,
                final String baseURI, final ASTContainer astContainer,
                final QueryType queryType, final String mimeType,
                final Charset charset, final String fileExt,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer, queryType,
                    mimeType, charset, fileExt, req, resp, os);

		}

		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) setupQuery(cxn);
			
            final TupleQueryResultWriter w;

            if (xhtml) {
            
                /*
                 * Override the XMLWriter to ensure that the XSL style sheet is
                 * declared in the generated XML document. This will tell the
                 * browser that it should style the result.
                 * 
                 * Note: The Content-Type header also needs to be overridden in
                 * order to have the browser apply the style sheet.
                 */
                
                final String stylesheet = getEffectiveStringValue(
                        req.getParameter(XSL_STYLESHEET),
                        DEFAULT_XSL_STYLESHEET);
                
                final XMLWriter xmlWriter = new MyXMLWriter(os, stylesheet);
                
                w = new SPARQLResultsXMLWriter(xmlWriter);
                
            } else {

                // Note: getQueryTask() verifies that format will be non-null.
                final TupleQueryResultFormat format = TupleQueryResultWriterRegistry
                        .getInstance().getFileFormatForMIMEType(mimeType);

                w = TupleQueryResultWriterRegistry.getInstance().get(format)
                        .getWriter(os);
                
            }

            query.evaluate(w);

		}

	}
    
    private static class MyXMLWriter extends XMLWriter {

        final private String stylesheet;
        
        public MyXMLWriter(final OutputStream outputStream,
                final String stylesheet) {
            
            super(outputStream);
            
            this.stylesheet = stylesheet;
            
        }

        @Override
        public void startDocument() throws IOException {

            super.startDocument();
            
            _writeLn("<?xml-stylesheet type=\"text/xsl\" href=\"" + stylesheet
                    + "\" ?>");

        }
        
    }

	/**
	 * Executes a graph query.
	 */
    private class GraphQueryTask extends AbstractQueryTask {

        public GraphQueryTask(final String namespace, final long timestamp,
                final String baseURI, final ASTContainer astContainer,
                final QueryType queryType, final RDFFormat format,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer, queryType,
                    format.getDefaultMIMEType(), format.getCharset(), format
                            .getDefaultFileExtension(), req, resp, os);

        }

		@Override
		protected void doQuery(final BigdataSailRepositoryConnection cxn,
				final OutputStream os) throws Exception {

            final BigdataSailGraphQuery query = (BigdataSailGraphQuery) setupQuery(cxn);
            
            /*
             * FIXME An error thrown here (such as if format is null and we do
             * not check it) will cause the response to hang, at least for the
             * test suite. Look into this further and make the error handling
             * bullet proof!
             * 
             * This may be related to queryId2. That should be imposed on the
             * IRunningQuery via QueryHints.QUERYID such that the QueryEngine
             * assigns that UUID to the query. We can then correlate the queryId
             * to the IRunningQuery, which is important for some of the status
             * pages. This will also let us INTERRUPT the IRunningQuery if there
             * is an error during evaluation, which might be necessary. For
             * example, if the client dies while the query is running. Look at
             * the old NSS code and see what it was doing and whether this was
             * logic was lost of simply never implemented.
             * 
             * However, I do not see how that would explain the failure of the
             * ft.get() method to return.
             */
//			if(true)
//			    throw new RuntimeException();

            // Note: getQueryTask() verifies that format will be non-null.
            final RDFFormat format = RDFWriterRegistry.getInstance()
                    .getFileFormatForMIMEType(mimeType);

            final RDFWriter w = RDFWriterRegistry.getInstance().get(format)
                    .getWriter(os);

			query.evaluate(w);

        }

	}

    /**
     * Executes a SPARQL UPDATE.
     */
    class UpdateTask extends AbstractQueryTask {

        /**
         * The timestamp for the commit point associated with the update and
         * <code>-1</code> if the commit point has not yet been assigned.
         */
        public final AtomicLong commitTime = new AtomicLong(-1);
        
        public UpdateTask(final String namespace, final long timestamp,
                final String baseURI, final ASTContainer astContainer,
                final HttpServletRequest req, final HttpServletResponse resp,
                final OutputStream os) {

            super(namespace, timestamp, baseURI, astContainer,
//                    null,//queryType
//                    null,//format.getDefaultMIMEType()
//                    null,//format.getCharset(), 
//                    null,//format.getDefaultFileExtension(), 
                    req,//
                    resp,//
                    os//
                    );

        }

        /**
         * {@inheritDoc}
         * <p>
         * This executes the SPARQL UPDATE and formats the HTTP response.
         */
        protected void doQuery(final BigdataSailRepositoryConnection cxn,
                final OutputStream os) throws Exception {
            
            /*
             * Setup a change listener. It will notice the #of mutations.
             */
            final CAT mutationCount = new CAT();
            cxn.addChangeLog(new IChangeLog(){
                @Override
                public void changeEvent(final IChangeRecord record) {
                    mutationCount.increment();
                }
                @Override
                public void transactionBegin() {
                }
                @Override
                public void transactionPrepare() {
                }
                @Override
                public void transactionCommited(long commitTime) {
                }
                @Override
                public void transactionAborted() {
                }});
            
            // Prepare the UPDATE request.
            final BigdataSailUpdate update = setupUpdate(cxn);

            final SparqlUpdateResponseWriter listener;
            final ByteArrayOutputStream baos;
            if (monitor) {

                /*
                 * Establish a listener that will log the process onto an XHTML
                 * document that will be delivered (flushed) incrementally to
                 * the client. The status code for the response will always be
                 * 200 (Ok). If there is an error, then that error will be
                 * reported in the XHTML response but NOT in the status code.
                 */
                
                // Do not buffer the response.
                baos = null;
                
                // Always sending an OK with a response entity.
                resp.setStatus(BigdataServlet.HTTP_OK);

                // This will write the response entity.
                listener = new SparqlUpdateResponseWriter(resp, os, charset,
                        true /* reportLoadProgress */, true/* flushEachEvent */,
                        mutationCount);

            } else {

                /*
                 * The listener logs the progress report (with the incremental
                 * load events) onto an xml document model but DOES NOT write
                 * anything on the servlet response. If there is an error, the
                 * HTTP status code will reflect that error. Otherwise we send
                 * back the XML document with a 200 (Ok) status code.
                 * 
                 * Note: This code path supports REST clients that expect the
                 * status code to reflect the *outcome* of the SPARQL UPDATE
                 * request. We MUST NOT write the XML document onto the response
                 * incrementally since the servlet response can become committed
                 * and we will be unable to change the status code from Ok (200)
                 * if an error occurs.
                 */

                // buffer the response here.
                baos = new ByteArrayOutputStream();

                listener = new SparqlUpdateResponseWriter(resp, baos, charset,
                        false/* reportLoadProgress */, false/* flushEachEvent */,
                        mutationCount);

            }

            /*
             * Run the update.
             */
            {

                // Setup the SPARQL UPDATE listener.
                cxn.getSailConnection().addListener(listener);

                // Execute the SPARQL UPDATE.
                this.commitTime.set(update.execute2());

                // Write out the response.
                listener.commit(this.commitTime.get());

                // Flush the listener (close document elements, etc).
                listener.flush();

            }

            if (baos != null) {

                /*
                 * Since we buffered the response, we have to send it out now.
                 */
                
                // Send an OK with a response entity.
                resp.setStatus(BigdataServlet.HTTP_OK);

                // Copy the document into the response.
                baos.flush();
                os.write(baos.toByteArray());

                // Flush the response.
                os.flush();

            }

        }

    }

    /**
     * Writes the SPARQL UPDATE response document onto the caller's
     * {@link OutputStream}. Depending on the use case, the stream will either
     * write directly onto the servlet response or it will be buffered until the
     * UPDATE request is finished.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/597">
     *      SPARQL UPDATE Listener </a>
     */
    private static class SparqlUpdateResponseWriter implements
            ISPARQLUpdateListener {

        private final long begin;
        private final HttpServletResponse resp;
        private final OutputStream os;
        private final Writer w;
        private final HTMLBuilder doc;
        private final Charset charset;
        private final XMLBuilder.Node body;
        private final boolean reportLoadProgress;
        private final boolean flushEachEvent;
        private final CAT mutationCount;
        
        /**
         * Used to correlate incremental LOAD progress messages.
         */
        private volatile Update lastOp = null;

        /**
         * 
         * 
         * @param os
         *            The {@link OutputStream}.
         * @param charset
         *            The character set.
         * @param reportLoadProgress
         *            When <code>true</code>, the incremental load progress will
         *            be included in the document. Note that this only makes
         *            sense when the document will be delivered incrementally to
         *            the client rather than "at-once" after the completion of
         *            the UPDATE operation.
         * @param flushEachEvent
         *            When <code>true</code>, each the {@link Writer} will be
         *            flushed after each logged event in order to ensure timely
         *            delivery to the client.
         * @param mutationCount
         *            A counter that is updated as mutations are applied.
         * @throws IOException
         */
        public SparqlUpdateResponseWriter(final HttpServletResponse resp,
                final OutputStream os, final Charset charset,
                final boolean reportLoadProgress, final boolean flushEachEvent,
                final CAT mutationCount)
                throws IOException {

            if (resp == null)
                throw new IllegalArgumentException();
            
            if (os == null)
                throw new IllegalArgumentException();
            
            this.resp = resp;
            
            /** Content Type header is required:
            http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1
            */
            resp.setContentType("text/html; charset="+charset.name());
            
            this.os = os;
            
            this.charset = charset;

            this.w = new OutputStreamWriter(os, charset);

            this.doc = new HTMLBuilder(charset.name(), w);

            this.reportLoadProgress = reportLoadProgress;
            
            this.flushEachEvent = flushEachEvent;
        
            this.mutationCount = mutationCount;
            
            this.begin = System.nanoTime();
            
            this.body = writeSparqlUpdateResponseHeader();

        }

        /**
         * Write the header of the SPARQL UPDATE response.
         * 
         * @return The body.
         * 
         * @throws IOException
         */
        private XMLBuilder.Node writeSparqlUpdateResponseHeader()
                throws IOException {

            XMLBuilder.Node current = doc.root("html");
            {
                current = current.node("head");
                current.node("meta").attr("http-equiv", "Content-Type")
                        .attr("content", "text/html;charset=" + charset.name())
                        .close();
                current.node("title").textNoEncode("bigdata&#174;").close();
                current = current.close();// close the head.
            }

            // open the body
            current = current.node("body");

            return current;
            
        }

        @Override
        public void updateEvent(final SPARQLUpdateEvent e) {

            try {

                // Total elapsed milliseconds from start of update request.
                final long totalElapsedMillis = TimeUnit.NANOSECONDS
                        .toMillis(System.nanoTime() - begin);

                // Elapsed milliseconds for this update operation.
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(e
                        .getElapsedNanos());
            
                if (e instanceof SPARQLUpdateEvent.LoadProgress) {
                
                    if (reportLoadProgress) {

                        /*
                         * Incremental progress on LOAD.
                         */
                        
                        final SPARQLUpdateEvent.LoadProgress tmp = (SPARQLUpdateEvent.LoadProgress) e;
                        
                        final long parsed = tmp.getParsedCount();
                        
                        final Update thisOp = e.getUpdate();

                        if (thisOp != lastOp) {

                            /*
                             * This is the first incremental load progress
                             * report for this LOAD operation.
                             */
                            lastOp = thisOp;

                            // Write out the LOAD operation.
                            body.node("pre").text(thisOp.toString())//
                                    .close();

                        }
                        
                        body.node("br")
                                .text("totalElapsed=" + totalElapsedMillis
                                        + "ms, elapsed=" + elapsedMillis
                                        + "ms, parsed=" + parsed + ", tps="
                                        + tmp.triplesPerSecond() + ", done="
                                        + tmp.isDone()).close();

                    }

                } else if (e.getCause() != null) {

                    /*
                     * An exception occurred when processing some update
                     * operation.
                     */
                    
                    final Throwable t = e.getCause();
                    
                    final StringWriter w = new StringWriter();
                    
                    final PrintWriter pw = new PrintWriter(w);
                    
                    t.printStackTrace(pw);
                    
                    pw.flush();
                    pw.close();
                    
                    body.node("p").text("ABORT").close()//
                    .node("pre").text(e.getUpdate().toString()).close()//
                    .node("pre").text(w.toString()).close()//
                    .node("p").text("totalElapsed=" + totalElapsedMillis
                            + "ms, elapsed=" + elapsedMillis + "ms")
                    .close();

                    // horizontal line after each operation.
                    body.node("hr").close();

                } else {

                    /*
                     * End of some UPDATE operation.
                     */

                    if (lastOp == e.getUpdate()) {
                        /*
                         * The end of a LOAD operation for which we reported the
                         * incremental progress. In this case, the LOAD
                         * operation was already written onto the response
                         * document, including the final report from the end of
                         * the parser run. So, all we have to do here is clear
                         * the reference.
                         */
                        lastOp = null;
//                        body.node("p")
//                                //
////                                .node("pre")
////                                .text(e.getUpdate().toString())
////                                .close()
//                                //
//                                .text("totalElapsed=" + totalElapsedMillis
//                                        + "ms, elapsed=" + elapsedMillis + "ms")//
//                                .close();
                    } else {
                    
                        body.node("pre")
                                .text(e.getUpdate().toString())
                                .close()
                                //
                                .node("p")
                                .text("totalElapsed=" + totalElapsedMillis
                                        + "ms, elapsed=" + elapsedMillis + "ms")//
                                .close();
                   }
                    
                    // horizontal line after each operation.
                    body.node("hr").close();

                }

                if (flushEachEvent) {

                    /*
                     * Flush the response for each event so the client
                     * (presumably a human operator) can see the progress log
                     * update "live".
                     */
                    
                    w.flush();
                    
                    os.flush();
                    
                    /*
                     * Note: appears to be necessary for incremental writes.
                     */
                    resp.flushBuffer();
                    
                }

            } catch (IOException e1) {
            
                throw new RuntimeException(e1);
                
            }

        }

        /**
         * Write the commit time into the document.
         * 
         * @param commitTime
         *            The commit time.
         */
        public void commit(final long commitTime) throws IOException {

            // Total elapsed milliseconds from start of update request.
            final long totalElapsedMillis = TimeUnit.NANOSECONDS
                    .toMillis(System.nanoTime() - begin);

            body.node("p")
                    .text("COMMIT: totalElapsed=" + totalElapsedMillis
                            + "ms, commitTime=" + commitTime
                            + ", mutationCount=" + mutationCount.get())//
                    .close();

        }

        public void flush() throws IOException {

            doc.closeAll(body);

            w.flush();

            w.close();
            
        }
        
    }

    /**
     * Return the task which will execute the SPARQL Query -or- SPARQL UPDATE.
     * <p>
     * Note: The {@link OutputStream} is passed in rather than the
     * {@link HttpServletResponse} in order to permit operations such as
     * "DELETE WITH QUERY" where this method is used in a context which writes
     * onto an internal pipe rather than onto the {@link HttpServletResponse}.
     * 
     * @param namespace
     *            The namespace associated with the {@link AbstractTripleStore}
     *            view.
     * @param timestamp
     *            The timestamp associated with the {@link AbstractTripleStore}
     *            view.
     * @param queryStr
     *            The query.
     * @param acceptOverride
     *            Override the Accept header (optional). This is used by UPDATE
     *            and DELETE so they can control the {@link RDFFormat} of the
     *            materialized query results.
     * @param req
     *            The request.
     * @param os
     *            Where to write the results.
     * @param update
     *            <code>true</code> iff this is a SPARQL UPDATE request.
     * 
     * @return The task -or- <code>null</code> if the named data set was not
     *         found. When <code>null</code> is returned, the
     *         {@link HttpServletResponse} will also have been committed.
     * @throws IOException 
     */
    public AbstractQueryTask getQueryTask(//
            final String namespace,//
            final long timestamp,//
            final String queryStr,//
            final String acceptOverride,//
            final HttpServletRequest req,//
            final HttpServletResponse resp,//
            final OutputStream os,//
            final boolean update//
            ) throws MalformedQueryException, IOException {

        /*
         * Setup the baseURI for this request. It will be set to the requestURI.
         */
        final String baseURI = req.getRequestURL().toString();

        final AbstractTripleStore tripleStore = getTripleStore(namespace,
                timestamp);

        if (tripleStore == null) {
            /*
             * There is no such triple/quad store instance.
             */
            BigdataServlet.buildResponse(resp, BigdataServlet.HTTP_NOTFOUND,
                    BigdataServlet.MIME_TEXT_PLAIN);
            return null;
        }

        if (update) {

            /*
             * Parse the query so we can figure out how it will need to be executed.
             * 
             * Note: This goes through some pains to make sure that we parse the
             * query exactly once in order to minimize the resources associated with
             * the query parser.
             */
            final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(
                    tripleStore).parseUpdate2(queryStr, baseURI);

            if (log.isDebugEnabled())
                log.debug(astContainer.toString());

            return new UpdateTask(namespace, timestamp, baseURI, astContainer,
                    req, resp, os);

        }
        
        /*
         * Parse the query so we can figure out how it will need to be executed.
         * 
         * Note: This goes through some pains to make sure that we parse the
         * query exactly once in order to minimize the resources associated with
         * the query parser.
         */
        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(
                tripleStore).parseQuery2(queryStr, baseURI);

        if (log.isDebugEnabled())
            log.debug(astContainer.toString());

        final QueryType queryType = astContainer.getOriginalAST()
                .getQueryType();
        
		/*
		 * When true, provide an "explanation" for the query (query plan, query
		 * evaluation statistics) rather than the results of the query.
		 */
		final boolean explain = req.getParameter(EXPLAIN) != null;

		final boolean xhtml = req.getParameter(XHTML) != null;

        /*
         * CONNEG for the MIME type.
         * 
         * Note: An attempt to CONNEG for a MIME type which can not be used with
         * a given type of query will result in a response using a default MIME
         * Type for that query.
         */
        final String acceptStr;
        if (explain) {
            acceptStr = BigdataServlet.MIME_TEXT_HTML;
        } else if (acceptOverride != null) {
            acceptStr = acceptOverride;
        } else if (xhtml) {
            switch (queryType) {
            case ASK:
                /*
                 * TODO This is just sending back text/plain. If we want to keep
                 * to the XHTML semantics, then we should send back XML with an
                 * XSL style sheet.
                 */
                acceptStr = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
                break;
            case SELECT:
                /*
                 * We will send back an XML document with an XSLT style sheet
                 * declaration. The Content-Type needs to be application/xml in
                 * order for the browser to interpret the style sheet
                 * declaration.
                 */
                // Generate XML solutions so we can apply XSLT transform.
                acceptStr = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
                break;
            case DESCRIBE:
            case CONSTRUCT:
                /* Generate RDF/XML so we can apply XSLT transform.
                 * 
                 * FIXME This should be sending back RDFs or using a lens.
                 */
                acceptStr = RDFFormat.RDFXML.getDefaultMIMEType();
                break;
            default:
                throw new AssertionError("QueryType=" + queryType);
            }
        } else {
            // Use whatever was specified by the client.
            acceptStr = req.getHeader("Accept");
        }

        // Do conneg.
        final ConnegUtil util = new ConnegUtil(acceptStr);

        switch (queryType) {
        case ASK: {

            final BooleanQueryResultFormat format = util
                    .getBooleanQueryResultFormat(BooleanQueryResultFormat.SPARQL);

            return new AskQueryTask(namespace, timestamp, baseURI,
                    astContainer, queryType, format, req, resp, os);

        }
        case DESCRIBE:
        case CONSTRUCT: {

            final RDFFormat format = util.getRDFFormat(RDFFormat.RDFXML);

            return new GraphQueryTask(namespace, timestamp, baseURI,
                    astContainer, queryType, format, req, resp, os);

        }
        case SELECT: {

            final TupleQueryResultFormat format = util
                    .getTupleQueryResultFormat(TupleQueryResultFormat.SPARQL);

            final String mimeType;
            final Charset charset;
            final String fileExt;
            if(xhtml) {
                /*
                 * Override as application/xml so the browser will interpret the
                 * XSL style sheet directive.
                 */
                mimeType = BigdataServlet.MIME_APPLICATION_XML;
                charset = Charset.forName(BigdataRDFServlet.UTF8);
                fileExt = "xml";
            } else {
                mimeType = format.getDefaultMIMEType();
                charset = format.getCharset();
                fileExt = format.getDefaultFileExtension();
            }
            return new TupleQueryTask(namespace, timestamp, baseURI,
                    astContainer, queryType, mimeType, charset, fileExt, req,
                    resp, os);

        }
        } // switch(queryType)

        throw new RuntimeException("Unknown query type: " + queryType);

    }

	/**
     * Metadata about running {@link AbstractQueryTask}s (this includes both
     * queries and update requests).
     */
	static class RunningQuery {

		/**
		 * The unique identifier for this query as assigned by the SPARQL 
		 * end point (rather than the {@link QueryEngine}).
		 */
		final long queryId;

        /**
         * The unique identifier for this query for the {@link QueryEngine}
         * (non-<code>null</code>).
         * 
         * @see QueryEngine#getRunningQuery(UUID)
         */
        final UUID queryId2;

        /**
         * The task executing the query (non-<code>null</code>).
         */
		final AbstractQueryTask queryTask;
		
//		/** The query. */
//		final String query;
		
		/** The timestamp when the query was accepted (ns). */
		final long begin;

		public RunningQuery(final long queryId, final UUID queryId2,
				final long begin,
				final AbstractQueryTask queryTask) {

            if (queryId2 == null)
                throw new IllegalArgumentException();
            
            if (queryTask == null)
                throw new IllegalArgumentException();

			this.queryId = queryId;

			this.queryId2 = queryId2;
			
//			this.query = query;

			this.begin = begin;
			
			this.queryTask = queryTask;

		}

	}

    /**
     * Return a connection transaction. When the timestamp is associated with a
     * historical commit point, this will be a read-only connection. When it is
     * associated with the {@link ITx#UNISOLATED} view or a read-write
     * transaction, this will be a mutable connection.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            The timestamp.
     * 
     * @throws RepositoryException
     */
    public BigdataSailRepositoryConnection getQueryConnection(
            final String namespace, final long timestamp)
            throws RepositoryException {

        /*
         * Note: [timestamp] will be a read-only tx view of the triple store if
         * a READ_LOCK was specified when the NanoSparqlServer was started
         * (unless the query explicitly overrides the timestamp of the view on
         * which it will operate).
         */
        final AbstractTripleStore tripleStore = getTripleStore(namespace,
                timestamp);
        
        if (tripleStore == null) {

            throw new DatasetNotFoundException("Not found: namespace="
                    + namespace + ", timestamp="
                    + TimestampUtility.toString(timestamp));

        }
        
        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        if (TimestampUtility.isReadOnly(timestamp)) {

            return (BigdataSailRepositoryConnection) repo
                    .getReadOnlyConnection(timestamp);

        }
        
        // Read-write connection.
        final BigdataSailRepositoryConnection conn = repo.getConnection();
        
        conn.setAutoCommit(false);
        
        return conn;

    }

    /**
     * Return a read-only view of the {@link AbstractTripleStore} for the given
     * namespace will read from the commit point associated with the given
     * timestamp.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            A timestamp -or- a tx identifier.
     * 
     * @return The {@link AbstractTripleStore} -or- <code>null</code> if none is
     *         found for that namespace and timestamp.
     * 
     * @todo enforce historical query by making sure timestamps conform (we do
     *       not want to allow read/write tx queries unless update semantics are
     *       introduced ala SPARQL 1.1).
     * 
     * @todo Use a distributed read-only tx for queries (it would be nice if a
     *       tx used 2PL to specify which namespaces it could touch).
     */
    public AbstractTripleStore getTripleStore(final String namespace,
            final long timestamp) {

//        if (timestamp == ITx.UNISOLATED)
//            throw new IllegalArgumentException("UNISOLATED reads disallowed.");

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, timestamp);

        return tripleStore;
        
    }

    /**
     * Return an UNISOLATED connection.
     * 
     * @param namespace
     *            The namespace.
     * 
     * @return The UNISOLATED connection.
     * 
     * @throws SailException
     * 
     * @throws RepositoryException
     */
    public BigdataSailRepositoryConnection getUnisolatedConnection( // FIXME REVIEW CALLERS
            final String namespace) throws SailException, RepositoryException {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("Not found: namespace=" + namespace);

        }

        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        final BigdataSailRepositoryConnection conn = (BigdataSailRepositoryConnection) repo
                .getUnisolatedConnection();

        conn.setAutoCommit(false);

        return conn;

    }

    /**
     * Return a list of the namespaces for the {@link AbstractTripleStore}s
     * registered against the bigdata instance.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/867"> NSS concurrency
     *      problem with list namespaces and create namespace </a>
     */
    /*package*/ List<String> getNamespaces(final long timestamp) {
        
        final long tx = newTx(timestamp);
        
		try {
			
		    return getNamespacesTx(tx);
			
		} finally {
			
		    abortTx(tx);
		    
        }

    }

	/*package*/ List<String> getNamespacesTx(final long tx) {

//        if (timestamp == ITx.READ_COMMITTED) {
//
//            // Use the last commit point.
//            timestamp = getIndexManager().getLastCommitTime();
//
//        }

        // the triple store namespaces.
		final List<String> namespaces = new LinkedList<String>();

		final SparseRowStore grs = getIndexManager().getGlobalRowStore(
				tx);

		if (grs == null) {

			log.warn("No GRS @ tx="
					+ TimestampUtility.toString(tx));

			// Empty.
			return namespaces;

		}

		// scan the relation schema in the global row store.
		@SuppressWarnings("unchecked")
		final Iterator<ITPS> itr = (Iterator<ITPS>) grs
				.rangeIterator(RelationSchema.INSTANCE);

		while (itr.hasNext()) {

			// A timestamped property value set is a logical row with
			// timestamped property values.
			final ITPS tps = itr.next();

			// If you want to see what is in the TPS, uncomment this.
			// System.err.println(tps.toString());

			// The namespace is the primary key of the logical row for the
			// relation schema.
			final String namespace = (String) tps.getPrimaryKey();

			// Get the name of the implementation class
			// (AbstractTripleStore, SPORelation, LexiconRelation, etc.)
			final String className = (String) tps.get(RelationSchema.CLASS)
					.getValue();

			if (className == null) {
				// Skip deleted triple store entry.
				continue;
			}

			try {
				final Class<?> cls = Class.forName(className);
				if (AbstractTripleStore.class.isAssignableFrom(cls)) {
					// this is a triple store (vs something else).
					namespaces.add(namespace);
				}
			} catch (ClassNotFoundException e) {
				log.error(e, e);
			}

		}

		return namespaces;

	}
    
	/**
     * Obtain a new transaction to protect operations against the specified view
     * of the database.
     * 
     * @param timestamp
     *            The timestamp for the desired view.
     * 
     * @return The transaction identifier -or- <code>timestamp</code> if the
     *         {@link IIndexManager} is not a {@link Journal}.
     * 
     * @see ITransactionService#newTx(long)
     * 
     * @see <a href="http://trac.bigdata.com/ticket/867"> NSS concurrency
     *      problem with list namespaces and create namespace </a>
     */
    public long newTx(final long timestamp) {

        long tx = timestamp; // use dirty reads unless Journal.

        if (getIndexManager() instanceof Journal) {
            
            final ITransactionService txs = ((Journal) getIndexManager())
                    .getLocalTransactionManager().getTransactionService();

            try {
                tx = txs.newTx(timestamp);
            } catch (IOException e) {
                // Note: Local operation.  Will not throw IOException.
                throw new RuntimeException(e);
            }

        }

        return tx;
    }

	/**
	 * Abort a transaction obtained by {@link #newTx(long)}.
	 * 
	 * @param tx
	 *            The transaction identifier.
	 */
	public void abortTx(final long tx) {

	    if (getIndexManager() instanceof Journal) {

			final ITransactionService txs = ((Journal) getIndexManager())
					.getLocalTransactionManager().getTransactionService();

			try {
				txs.abort(tx);
			} catch (IOException e) {
				// Note: Local operation. Will not throw IOException.
				throw new RuntimeException(e);
			}

		}

	}
	
//	public void commitTx(final long tx) {
//
//	    if (getIndexManager() instanceof Journal) {
//
//            final ITransactionService txs = ((Journal) getIndexManager())
//                    .getLocalTransactionManager().getTransactionService();
//
//            try {
//                txs.commit(tx);
//            } catch (IOException e) {
//                // Note: Local operation. Will not throw IOException.
//                throw new RuntimeException(e);
//            }
//
//        }
//
//    }
	
}
