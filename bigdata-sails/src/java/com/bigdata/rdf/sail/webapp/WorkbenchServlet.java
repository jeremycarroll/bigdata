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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.rdf.sail.webapp.client.MiniMime;

/**
 * Helper servlet for workbench requests.
 */
public class WorkbenchServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(WorkbenchServlet.class); 

    /**
     * Flag to signify a workbench operation.
     */
    static final transient String ATTR_WORKBENCH = "workbench";

    /**
     * Flag to signify a convert operation.  POST an RDF document with a 
     * content type and an accept header for what it should be converted to.
     */
    static final transient String ATTR_CONVERT = "convert";

    
    public WorkbenchServlet() {

    }

    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {


        if (req.getParameter(ATTR_CONVERT) != null) {
            
            // Convert from one format to another
            doConvert(req, resp);
            
        }

    }

    /**
     * Convert RDF data from one format to another.
     */
    private void doConvert(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
    	    final String baseURI = req.getRequestURL().toString();
    	
        final String contentType = req.getContentType();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        /**
         * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/620">
         * UpdateServlet fails to parse MIMEType when doing conneg. </a>
         */

        final RDFFormat requestBodyFormat = RDFFormat.forMIMEType(new MiniMime(
                contentType).getMimeType());

        if (requestBodyFormat == null) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as RDF: " + contentType);

            return;

        }

        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(requestBodyFormat);

        if (rdfParserFactory == null) {

            buildResponse(resp, HTTP_INTERNALERROR, MIME_TEXT_PLAIN,
                    "Parser factory not found: Content-Type="
                            + contentType + ", format=" + requestBodyFormat);
            
            return;

        }

//        final String s= IOUtil.readString(req.getInputStream());
//        System.err.println(s);
        
        final Graph g = new GraphImpl();
        
        try {
        
	        /*
	         * There is a request body, so let's try and parse it.
	         */
	
	        final RDFParser rdfParser = rdfParserFactory
	                .getParser();
	
	        rdfParser.setValueFactory(new ValueFactoryImpl());
	
	        rdfParser.setVerifyData(true);
	
	        rdfParser.setStopAtFirstError(true);
	
	        rdfParser
	                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
	
	        rdfParser.setRDFHandler(new StatementCollector(g));
	
	        /*
	         * Run the parser, which will cause statements to be
	         * inserted.
	         */
	        rdfParser.parse(req.getInputStream(), baseURI);
	
	        sendGraph(req, resp, g);
	        
        } catch (Throwable t) {

            throw BigdataRDFServlet.launderThrowable(t, resp, null);

        }

    }

}
