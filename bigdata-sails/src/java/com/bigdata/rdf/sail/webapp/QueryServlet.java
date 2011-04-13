package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;

/**
 * SPARQL query handler for GET or POST verbs.
 * 
 * @author martyncutcher
 */
public class QueryServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(QueryServlet.class); 

//	/**
//	 * @todo use to decide ASK, DESCRIBE, CONSTRUCT, SELECT, EXPLAIN, etc.
//	 */
//	private final QueryParser m_engine;

    public QueryServlet() {
    	
//		// used to parse qeries.
//        m_engine = new SPARQLParserFactory().getParser();
        
//        getContext().registerServlet(this);
        
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) {
    
    	doGet(req, resp); // POST is allowed for query
    	
	}

	/**
	 * FIXME Does not handle default-graph-uri or named-graph-uri query
	 * parameters.
	 */
    @Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) {

    	final String namespace = getNamespace(req.getRequestURI());

		final long timestamp = getTimestamp(req.getRequestURI(), req);

//		final String uriqueryStr = req.getQueryString();
//		final String url = req.getRequestURL().toString();
		
		final String queryStr = req.getParameter("query");

		if (queryStr == null) {
			resp.setContentType("text/test;charset=utf-8");
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			try {
				resp.getWriter().println("Specify query using ?query=....");
			} catch (IOException e) {
				e.printStackTrace();
			}

			return;
		}

		/*
		 * Setup pipes. The [os] will be passed into the task that executes
		 * the query. The [is] will be passed into the Response. The task is
		 * executed on a thread pool.
		 * 
		 * Note: If the client closes the connection, then the InputStream
		 * passed into the Response will be closed and the task will
		 * terminate rather than running on in the background with a
		 * disconnected client.
		 */
		OutputStream os;
		try {
			os = resp.getOutputStream();
		} catch (IOException e2) {
			e2.printStackTrace();
			
			throw new RuntimeException(e2);
		}
		try {

            final AbstractQueryTask queryTask = getBigdataRDFContext()
                    .getQueryTask(namespace, timestamp, queryStr, req, os);

			if (log.isTraceEnabled())
				log.trace("Running query: " + queryStr);

			/*
			 * FIXME This needs to run on an ExecutorService with a configured
			 * thread pool size so we can avoid running too many queries
			 * concurrently. Please restore the logic for doing this with the
			 * thread pool scoped appropriately. All non-administrative REST Api
			 * tasks should adhere to this limit. The limit should not apply to
			 * normal http requests against non-API services.
			 */
			
			queryTask.call();

			// Setup the response.
			// TODO Move charset choice into conneg logic.
			buildResponse(resp, HTTP_OK, queryTask.mimeType + "; charset='" + charset + "'");

		} catch (Throwable e) {
			try {
				throw BigdataRDFServlet.launderThrowable(e, os, queryStr);
			} catch (Exception e1) {
				throw new RuntimeException(e);
			}
		}

		
	}

}

