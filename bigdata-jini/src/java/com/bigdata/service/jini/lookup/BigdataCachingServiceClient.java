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
 * Created on Apr 24, 2009
 */

package com.bigdata.service.jini.lookup;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.jini.BigdataDiscoveryManager;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataCachingServiceClient<S extends Object> extends
        AbstractCachingServiceClient<S> {

    /**
     * The {@link UUID} of the service owing this cache instance.
     */
    protected final UUID thisServiceUUID;
    
    /**
     * The service owning this cache instance.
     */
    protected final Object thisService;
    
    /**
     * {@inheritDoc}
     * 
     * @param fed
     *            The {@link JiniFederation}. This class will use the
     *            {@link ServiceDiscoveryManager} exposed by the
     *            {@link JiniFederation} and {@link ServiceDiscoveryEvent}s
     *            will be passed by this class to the {@link JiniFederation},
     *            which implements {@link ServiceDiscoveryListener}. Those
     *            events are used to notice service joins.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public BigdataCachingServiceClient(JiniFederation fed, Class serviceIface,
//BTM - PRE_CLIENT_SERVICE            ServiceTemplate template, ServiceItemFilter filter, long timeout)
//BTM - PRE_CLIENT_SERVICE            throws RemoteException {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        super(fed.getServiceDiscoveryManager(),
//BTM - PRE_CLIENT_SERVICE                fed/* serviceDiscoveryListener */, serviceIface, template,
//BTM - PRE_CLIENT_SERVICE                filter, timeout);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        thisServiceUUID = fed.getClient().getDelegate().getServiceUUID();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        thisService = fed.getClient().getDelegate().getService();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    }
/////////////
    public BigdataCachingServiceClient(ServiceDiscoveryManager sdm,
                                       ServiceDiscoveryListener listener,
                                       UUID owningServiceUUID,
                                       Object owningServiceRef,
                                       Class serviceIface,
                                       ServiceTemplate template,
                                       ServiceItemFilter filter,
                                       long timeout)
                throws RemoteException
    {
        super(sdm, listener, serviceIface, template, filter, timeout);
        thisServiceUUID = owningServiceUUID;
        thisService = owningServiceRef;
    }
//BTM - PRE_CLIENT_SERVICE -END
}
