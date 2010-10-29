/*

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
package com.bigdata.jini;

import com.bigdata.discovery.IBigdataDiscoveryManagement;
import net.jini.lookup.ServiceDiscoveryManager;

/**
 * Defines the interface for implementations that employ a Jini
 * <code>ServiceDiscoveryManager</code> helper utility to manage
 * the discovery of the services in a Bigdata federation.
 */
public interface IJiniDiscoveryManagement
                     extends IBigdataDiscoveryManagement
{
    /**
     * Returns the <code>ServiceDiscoveryManager</code> instance
     * currently being employed to manage discovery of the 
     * services in the federation.
     */
    ServiceDiscoveryManager getServiceDiscoveryManager();
}
