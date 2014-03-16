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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAStatusEnum;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.InitParams;
import com.bigdata.rdf.sail.webapp.client.IMimeTypes;

/**
 * Useful glue for implementing service actions, but does not directly implement
 * any service action/
 */
abstract public class BigdataServlet extends HttpServlet implements IMimeTypes {
	
	/**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger.getLogger(BigdataServlet.class); 

    /**
     * The name of the {@link ServletContext} attribute whose value is the
     * {@link BigdataRDFContext}.
     */
    static public final transient String ATTRIBUTE_RDF_CONTEXT = BigdataRDFContext.class
            .getName();
    
    /**
     * The name of the {@link ServletContext} attribute whose value is the
     * {@link IIndexManager}.
     */
    /*package*/ static final transient String ATTRIBUTE_INDEX_MANAGER = 
        IIndexManager.class.getName();

    /**
     * The {@link ServletContext} attribute whose value is the prefix for the
     * {@link HALoadBalancerServlet} iff it is running.
     */
    static final String ATTRIBUTE_LBS_PREFIX = HALoadBalancerServlet.class
            .getName() + "." + InitParams.PREFIX;
            
//    /**
//     * The {@link ServletContext} attribute whose value is the
//     * {@link SparqlCache}.
//     */
//    /* package */static final transient String ATTRIBUTE_SPARQL_CACHE = SparqlCache.class.getName();

	/**
	 * The character set used for the response (not negotiated).
	 */
    static protected final String charset = "UTF-8";

    protected static final transient String GET = "GET";
    protected static final transient String POST = "POST";
    protected static final transient String PUT = "PUT";
    protected static final transient String DELETE = "DELETE";
    
	/**
	 * Some HTTP response status codes
	 */
	public static final transient int
        HTTP_OK = HttpServletResponse.SC_OK,
//        HTTP_ACCEPTED = HttpServletResponse.SC_ACCEPTED,
//		HTTP_REDIRECT = HttpServletResponse.SC_TEMPORARY_REDIRECT,
//		HTTP_FORBIDDEN = HttpServletResponse.SC_FORBIDDEN,
		HTTP_NOTFOUND = HttpServletResponse.SC_NOT_FOUND,
        HTTP_BADREQUEST = HttpServletResponse.SC_BAD_REQUEST,
        HTTP_METHOD_NOT_ALLOWED = HttpServletResponse.SC_METHOD_NOT_ALLOWED,
		HTTP_INTERNALERROR = HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        HTTP_NOTIMPLEMENTED = HttpServletResponse.SC_NOT_IMPLEMENTED;

    static <T> T getRequiredServletContextAttribute(
            final ServletContext servletContext, final String name) {

        if (servletContext == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();

        @SuppressWarnings("unchecked")
        final T v = (T) servletContext.getAttribute(name);

        if (v == null)
            throw new RuntimeException("Not set: " + name);

        return v;

    }

    static final SparqlEndpointConfig getConfig(
            final ServletContext servletContext) {

        return getBigdataRDFContext(servletContext).getConfig();

    }

    protected final BigdataRDFContext getBigdataRDFContext() {
        
        return getBigdataRDFContext(getServletContext());
        
    }

    static final BigdataRDFContext getBigdataRDFContext(
            final ServletContext servletContext) {

//        if (m_context == null) {
//
//            m_context = 
        return getRequiredServletContextAttribute(servletContext,
                ATTRIBUTE_RDF_CONTEXT);

//        }
//
//        return m_context;

    }

//    private volatile BigdataRDFContext m_context;

    /**
     * The backing {@link IIndexManager}.
     */
    protected IIndexManager getIndexManager() {
        
        return getIndexManager(getServletContext());
        
    }

    /**
     * The backing {@link IIndexManager}.
     */
    static IIndexManager getIndexManager(final ServletContext servletContext) {

        return getRequiredServletContextAttribute(servletContext,
                ATTRIBUTE_INDEX_MANAGER);

    }

//    /**
//     * Return the {@link Quorum} -or- <code>null</code> if the
//     * {@link IIndexManager} is not participating in an HA {@link Quorum}.
//     */
//    protected Quorum<HAGlue, QuorumService<HAGlue>> getQuorum() {
//
//        final IIndexManager indexManager = getIndexManager();
//
//        if (indexManager instanceof Journal) {
//
//            return ((Journal) indexManager).getQuorum();
//
//        }
//
//        return null;
//	    
//	}

    /**
     * Return the {@link HAStatusEnum} -or- <code>null</code> if the
     * {@link IIndexManager} is not an {@link AbstractQuorum} or is not HA
     * enabled.
     */
    static public HAStatusEnum getHAStatus(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();
        
        if (indexManager instanceof AbstractJournal) {

            // Note: Invocation against local object (NOT RMI).
            return ((AbstractJournal) indexManager).getHAStatus();

        }

        return null;
        
    }
    
    /**
     * If the node is not writable, then commit a response and return
     * <code>false</code>. Otherwise return <code>true</code>.
     * 
     * @param req
     * @param resp
     * 
     * @return <code>true</code> iff the node is writable.
     * 
     * @throws IOException
     */
    static boolean isWritable(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
        if(getConfig(req.getServletContext()).readOnly) {
            
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    "Not writable.");

            // Not writable.  Response has been committed.
            return false;
            
        }
        final HAStatusEnum haStatus = getHAStatus(getIndexManager(req
                .getServletContext()));
        if (haStatus == null) {
            // No quorum.
            return true;
        }
        switch (haStatus) {
        case Leader:
            return true;
        default:
            log.warn(haStatus.name());
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    haStatus.name());
            // Not writable.  Response has been committed.
            return false;
        }
                
	}
	
    /**
     * If the node is not readable, then commit a response and return
     * <code>false</code>. Otherwise return <code>true</code>.
     * 
     * @param req
     * @param resp
     * 
     * @return <code>true</code> iff the node is readable.
     * 
     * @throws IOException
     */
    static boolean isReadable(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final HAStatusEnum haStatus = getHAStatus(getIndexManager(req
                .getServletContext()));
        if (haStatus == null) {
            // No quorum.
            return true;
        }
        switch (haStatus) {
        case Leader:
        case Follower:
            return true;
        default:
            // Not ready.
            log.warn(haStatus.name());
            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                    haStatus.name());
            // Response has been committed.
            return false;
        }

//        final long quorumToken = quorum.token();
//
//        if (!quorum.isQuorumMet()) {
//
//            /*
//             * The quorum is not met.
//             */
//
//            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
//                    "Quorum is not met.");
//
//            return false;
//
//        } else if (!quorum.getClient().isJoinedMember(quorumToken)) {
//
//            /*
//             * The quorum is met, but this service is not joined with the met
//             * quorum.
//             */
//
//            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
//                    "Service is not joined with met quorum.");
//
//            return false;
//
//        } else if (getIndexManager() instanceof AbstractJournal
//                && ((AbstractJournal) getIndexManager()).getHAReady() == Quorum.NO_QUORUM) {
//
//            /*
//             * The service is not "HA Ready".
//             * 
//             * Note: There is a lag between when a service joins with a met
//             * quorum and when it has completed some internal asynchronous
//             * processing to become "HA Ready". This handles that gap for the
//             * HAJournalServer.
//             */
//            
//            buildResponse(resp, HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
//                    "Service is not HA Ready.");
//
//            return false;
//
//        } else {
//            
//            /*
//             * There is a quorum. The quorum is met. This service is part of the
//             * met quorum.
//             */
//
//            return true;
//
//        }

    }
    
//	/**
//	 * The {@link SparqlCache}.
//	 */
//    protected SparqlCache getSparqlCache() {
//        
//        return getRequiredServletContextAttribute(ATTRIBUTE_SPARQL_CACHE);
//        
//    }

    /**
     * Return the serviceURI(s) for this service (one or more).
     * 
     * @param req
     *            The request.
     *            
     * @return The known serviceURIs for this service.
     */
    static public String[] getServiceURIs(final HttpServletRequest req) {

        // One or more.
        final List<String> serviceURIs = new LinkedList<String>();

        /*
         * Figure out the service end point.
         * 
         * Note: This is just the requestURL as reported. This makes is
         * possible to support virtual hosting and similar http proxy
         * patterns since the SPARQL end point is just the URL at which the
         * service is responding.
         */
        final String uri;
        {
            
            final StringBuffer sb = req.getRequestURL();

            final int indexOf = sb.indexOf("?");

            if (indexOf == -1) {
                uri = sb.toString();
            } else {
                uri = sb.substring(0, indexOf);
            }
            serviceURIs.add(uri);

        }

        /**
         * If the load balancer servlet is registered, then get its effective
         * service URI. This will be a load balanced version of the serviceURI
         * that we obtained above. We are trying to go from
         * 
         * http://localhost:8080/bigdata/sparql
         * 
         * to
         * 
         * http://localhost:8080/bigdata/LBS/sparql
         * 
         * where LBS is the prefix of the load balancer servlet.
         */
        {
            final String prefix = (String) req.getServletContext()
                    .getAttribute(ATTRIBUTE_LBS_PREFIX);

            if (prefix != null) {
                
                // locate the // in the protocol.
                final int doubleSlash = uri.indexOf("//");
                
                // skip past that and locate the next /
                final int nextSlash = uri
                        .indexOf('/', doubleSlash + 2/* fromIndex */);

                // The ContextPath for the webapp. This should be the next thing
                // in the [uri].
                final String contextPath = req.getServletContext()
                        .getContextPath();

                // The index of the end of the ContextPath.
                final int endContextPath = nextSlash
                        + contextPath.length();

                // everything up to the *start* of the ContextPath
                final String baseURL = uri.substring(0/* beginIndex */,
                        nextSlash/* endIndex */);

                final String s = baseURL // base URL
                        + prefix // LBS prefix (includes ContextPath)
                        + (prefix.endsWith("/") ? "" : "/")
                        + uri.substring(endContextPath + 1) // remainder of requestURL.
                        ;

                serviceURIs.add(s);
                
            }
            
        }

        return serviceURIs.toArray(new String[serviceURIs.size()]);
        
    }

    static public void buildResponse(final HttpServletResponse resp,
            final int status, final String mimeType) throws IOException {

        resp.setStatus(status);

        resp.setContentType(mimeType);

    }

    static public void buildResponse(final HttpServletResponse resp, final int status,
            final String mimeType, final String content) throws IOException {

        buildResponse(resp, status, mimeType);

        final Writer w = resp.getWriter();

        w.write(content);

        w.flush();

    }

    static protected void buildResponse(final HttpServletResponse resp,
            final int status, final String mimeType, final InputStream content)
            throws IOException {

        buildResponse(resp, status, mimeType);

        final OutputStream os = resp.getOutputStream();

        copyStream(content, os);

        os.flush();

    }

    /**
     * Copy the input stream to the output stream.
     * 
     * @param content
     *            The input stream.
     * @param outstr
     *            The output stream.
     *            
     * @throws IOException
     */
    static protected void copyStream(final InputStream content,
            final OutputStream outstr) throws IOException {

        final byte[] buf = new byte[1024];

        while (true) {
        
            final int rdlen = content.read(buf);
            
            if (rdlen <= 0) {
            
                break;
                
            }
            
            outstr.write(buf, 0, rdlen);
            
        }

    }

    /**
     * Conditionally wrap the input stream, causing the data to be logged as
     * characters at DEBUG. Whether or not the input stream is wrapped depends
     * on the current {@link #log} level.
     * 
     * @param instr
     *            The input stream.
     * 
     * @return The wrapped input stream.
     * 
     * @throws IOException
     */
    protected InputStream debugStream(final InputStream instr)
            throws IOException {

        if (!log.isDebugEnabled()) {

            return instr;

        }

        final ByteArrayOutputStream outstr = new ByteArrayOutputStream();

        final byte[] buf = new byte[1024];
        int rdlen = 0;
        while (rdlen >= 0) {
            rdlen = instr.read(buf);
            if (rdlen > 0) {
                outstr.write(buf, 0, rdlen);
            }
        }

        final InputStreamReader rdr = new InputStreamReader(
                new ByteArrayInputStream(outstr.toByteArray()));
        final char[] chars = new char[outstr.size()];
        rdr.read(chars);
        log.debug("debugStream, START");
        log.debug(chars);
        log.debug("debugStream, END");

        return new ByteArrayInputStream(outstr.toByteArray());
        
    }

}
