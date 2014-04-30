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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.journal.IIndexManager;

/**
 * Load balancer policy interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see HALoadBalancerServlet
 * @see <a href="http://trac.bigdata.com/ticket/624">HA Load Balancer</a>
 */
public interface IHALoadBalancerPolicy {

    /**
     * Initialize the load balancer policy.
     * 
     * @param servletConfig
     * @param indexManager
     */
    void init(ServletConfig servletConfig, IIndexManager indexManager)
            throws ServletException;

    /**
     * Destroy the load balancer policy (stop any asynchronous processing,
     * release any resources).
     */
    void destroy();

    /**
     * Invoked for each request. If the response is not committed, then it will
     * be handled by the {@link HALoadBalancerServlet}.
     * 
     * @param isLeaderRequest
     *            <code>true</code> iff this request must be directed to the
     *            leaeder and <code>false</code> iff this request may be load
     *            balanced over the joined services. UPDATEs MUST be handled by
     *            the leader. Read requests can be handled by any service that
     *            is joined with the met quorum.
     * @param request
     *            The request.
     * @param response
     *            The response.
     * 
     * @return <code>true</code> iff the request was handled.
     */
    boolean service(final boolean isLeaderRequest,
            final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException;

    /**
     * Return the URL to which a non-idempotent request will be proxied.
     * 
     * @param req
     *            The request.
     * 
     * @return The proxyTo URL -or- <code>null</code> if we could not find a
     *         service to which we could proxy this request.
     */
    String getLeaderURL(HttpServletRequest req);

    /**
     * Return the URL to which a <strong>read-only</strong> request will be
     * proxied. The returned URL must include the protocol, hostname and port
     * (if a non-default port will be used) as well as the target request path.
     * 
     * @param req
     *            The request.
     * 
     * @return The proxyTo URL -or- <code>null</code> if we could not find a
     *         service to which we could proxy this request.
     */
    String getReaderURL(HttpServletRequest req);
    
}
