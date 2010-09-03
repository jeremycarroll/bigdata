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

package com.bigdata.service;

import com.sun.jini.admin.DestroyAdmin;
import java.io.IOException;

/**
 * Methods for administratively shutting down a given service
 * implementation.
 */
public interface ShutdownAdmin extends DestroyAdmin {

    /**
     * Initiates an orderly shutdown in which no new requests
     * are accepted, but currently executing requests are allowed
     * to terminate "gracefully"; waiting for that termination to
     * complete for a period of time no greater than the timeout
     * value specified in the service's configuration. If the
     * service is already in the shutting down state, then this
     * method will take no action.
     */
    public void shutdown() throws IOException;
    
    /**
     * Tells the service to no longer accept new requests, and make
     * a best effort attempt to terminate all existing requests;
     * releasing all resources and returning as soon as possible.
     * If the service is already in the shutting down state, then
     * this method will take no action.
     */
    public void shutdownNow() throws IOException;

    /** 
     * Tells the service to exit immediately; without performing
     * any "graceful" shutdown tasks such as flushing buffers,
     * closing sockets, releasing resources, etc. This method
     * can be used to simulate a service crash.
     */
    void kill(int status) throws IOException;
}
