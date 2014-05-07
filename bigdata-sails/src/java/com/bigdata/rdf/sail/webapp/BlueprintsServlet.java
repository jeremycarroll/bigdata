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
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.blueprints.BigdataGraphBulkLoad;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.MiniMime;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * Helper servlet for the blueprints layer.
 */
public class BlueprintsServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(BlueprintsServlet.class); 

    static public final List<String> mimeTypes = Arrays.asList(new String[] {
        "application/graphml+xml"    
    }) ;
    
    /**
     * Flag to signify a blueprints operation.
     */
    static final transient String ATTR_BLUEPRINTS = "blueprints";

//    /**
//     * Flag to signify a convert operation.  POST an RDF document with a 
//     * content type and an accept header for what it should be converted to.
//     */
//    static final transient String ATTR_CONVERT = "convert";

    
    public BlueprintsServlet() {

    }

    /**
     * Post a GraphML file to the blueprints layer.
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final long begin = System.currentTimeMillis();
        
        final String namespace = getNamespace(req);

        final long timestamp = getTimestamp(req);

        final AbstractTripleStore tripleStore = getBigdataRDFContext()
                .getTripleStore(namespace, timestamp);
        
        if (tripleStore == null) {
            /*
             * There is no such triple/quad store instance.
             */
            buildResponse(resp, HTTP_NOTFOUND, MIME_TEXT_PLAIN);
            return;
        }
        
        final String contentType = req.getContentType();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        final String mimeType = new MiniMime(contentType).getMimeType().toLowerCase();

        if (!mimeTypes.contains(mimeType)) {

            buildResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as graph data: " + contentType);

            return;

        }
        
        try {
            
            BigdataSailRepositoryConnection conn = null;
            try {

                conn = getBigdataRDFContext()
                        .getUnisolatedConnection(namespace);
                
                final BigdataGraphBulkLoad graph = new BigdataGraphBulkLoad(conn);

                GraphMLReader.inputGraph(graph, req.getInputStream());
                
                graph.commit();
                
                final long nmodified = graph.getMutationCountLastCommit();

                final long elapsed = System.currentTimeMillis() - begin;
                
                reportModifiedCount(resp, nmodified, elapsed);
                
                return;

            } catch(Throwable t) {
                
                if(conn != null)
                    conn.rollback();
                
                throw new RuntimeException(t);

            } finally {

                if (conn != null)
                    conn.close();
                
            }

        } catch (Exception ex) {

            // Will be rendered as an INTERNAL_ERROR.
            throw new RuntimeException(ex);
            
        }
        
    }

}
