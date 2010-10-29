/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Mar 18, 2008
 */

package com.bigdata.service.jini.lookup;

import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceTemplate;

import com.bigdata.service.CallableExecutor;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;

//BTM - FOR_CLIENT_SERVICE
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceDiscoveryListener;
import java.util.UUID;

/**
 * Class handles discovery of a callable executor service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientServicesClient extends
        BigdataCachingServiceClient<CallableExecutor> {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public ClientServicesClient(final JiniFederation fed, final long timeout)
//BTM - PRE_CLIENT_SERVICE            throws RemoteException {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        super(fed, CallableExecutor.class, new ServiceTemplate(null,
//BTM - PRE_CLIENT_SERVICE                new Class[] { CallableExecutor.class }, null), null/* filter */,
//BTM - PRE_CLIENT_SERVICE                timeout);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    }
    public ClientServicesClient(final ServiceDiscoveryManager sdm,
                                final ServiceDiscoveryListener listener,
                                final UUID serviceUUID,
                                final Object serviceRef,
                                final long timeout)
               throws RemoteException
    {
        super(sdm, listener, serviceUUID, serviceRef,
              CallableExecutor.class,
              new ServiceTemplate(null, new Class[] {CallableExecutor.class}, null),
              null, //filter
              timeout);
    }
//BTM - PRE_CLIENT_SERVICE - END
}
