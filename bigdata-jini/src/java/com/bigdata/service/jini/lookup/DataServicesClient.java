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
import java.util.UUID;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.ServiceItemFilter;
import net.jini.lookup.entry.Name;

//BTM import com.bigdata.service.IDataService;
//BTM import com.bigdata.service.IMetadataService;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;

//BTM
import com.bigdata.service.ShardService;

//BTM - FOR_CLIENT_SERVICE
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceDiscoveryListener;
import java.util.UUID;

/**
 * Class handles discovery, caching, and local lookup of 
 * {@link ServiceService}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
//BTM public class DataServicesClient extends BigdataCachingServiceClient<IDataService> {
public class DataServicesClient extends BigdataCachingServiceClient<ShardService> {

//BTM - NOTE: an IMetadataService IS a ShardService, so IMetadataService must be filtered below

    /**
     * {@inheritDoc}
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE     public DataServicesClient(final JiniFederation fed, final long timeout)
//BTM - PRE_CLIENT_SERVICE             throws RemoteException {
//BTM - PRE_CLIENT_SERVICE         /*
//BTM - PRE_CLIENT_SERVICE          * Note: No filter is imposed here. Instead there are type specific
//BTM - PRE_CLIENT_SERVICE          * methods if you want ShardService vs an IMetadataService.
//BTM - PRE_CLIENT_SERVICE          */
//BTM - PRE_CLIENT_SERVICE //BTM        super(fed, IDataService.class, new ServiceTemplate(null,
//BTM - PRE_CLIENT_SERVICE //BTM                new Class[] { IDataService.class }, null), null/* filter */,
//BTM - PRE_CLIENT_SERVICE //BTM                timeout);
//BTM - PRE_CLIENT_SERVICE super(fed, 
//BTM - PRE_CLIENT_SERVICE       ShardService.class, 
//BTM - PRE_CLIENT_SERVICE       new ServiceTemplate(null, new Class[] { ShardService.class }, null), 
//BTM - PRE_CLIENT_SERVICE       null, //filter 
//BTM - PRE_CLIENT_SERVICE       timeout);
//BTM - PRE_CLIENT_SERVICE     }

    public DataServicesClient(final ServiceDiscoveryManager sdm,
                              final ServiceDiscoveryListener listener,
                              final UUID serviceUUID,
                              final Object serviceRef,
                              final long timeout)
                throws RemoteException
    {
        /*
         * Note: No filter is imposed here. Instead there are type specific
         * methods if you want ShardService vs an IMetadataService.
         */
        super(sdm, listener, serviceUUID, serviceRef, 
              ShardService.class, 
              new ServiceTemplate(null, new Class[] {ShardService.class}, null), 
              null, //filter 
              timeout);
    }
//BTM - PRE_CLIENT_SERVICE - END

    /**
     * Return an arbitrary {@link ShardService} instance from the cache -or-
     * <code>null</code> if there is none in the cache and a remote lookup
     * times out.
     * 
     * @return The service.
     */
//BTM    final public IDataService getDataService() {
final public ShardService getDataService() {

        return getService(DataServiceFilter.INSTANCE);
        
    }

    /**
     * Return an arbitrary {@link IMetadataService} from the cache -or-
     * <code>null</code> if there is none in the cache and a remote lookup
     * times out. This method will NOT return a {@link ShardService} unless it
     * also implements {@link IMetadataService}.
     * 
     * @todo handle more than one metadata service. right now registering more
     *       than one will cause problems since different clients might discover
     *       different metadata services and the metadata services are not
     *       arranging themselves into a failover chain or a hash partitioned
     *       service.
     */
//BTM    final public IMetadataService getMetadataService() {
//BTM
//BTM        return (IMetadataService) getService(MetadataServiceFilter.INSTANCE);
//BTM
//BTM    }

    /**
     * Return the proxy for an {@link ShardService} from the local cache -or-
     * the reference to this service if the {@link UUID} identifies this service
     * (this avoids RMI requests from a service to itself).
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link ShardService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link ShardService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     * @throws RuntimeException
     *             if <i>serviceUUID</i> identifies an {@link IMetadataService}.
     */
//BTM    public IDataService getDataService(final UUID serviceUUID) {
public ShardService getDataService(final UUID serviceUUID) {

        /*
         * Note: I have backed out this optimization as it raises concerns that
         * code written to assume RMI might rely on the deserialized objects
         * returned from the proxy being independent of the objects on the
         * remote service. Since the main optimization of interest is joins, I
         * will handle this explicitly from within the distributed join logic.
         */
//        if (serviceUUID.equals(thisServiceUUID)) {
//
//            /*
//             * Return the actual service reference rather than a proxy to avoid
//             * RMI when this service makes a request to itself.
//             */
//
//            return (IDataService) thisService;
//
//        }
        
        final ServiceItem serviceItem = getServiceItem(serviceUUID);
        
        if (serviceItem == null) {

            log.error("No such service: uuid=" + serviceUUID);

            return null;

        }

        if (!DataServiceFilter.INSTANCE.check(serviceItem)) {

            throw new RuntimeException("Not a shard service: " + serviceItem);

        }
        
        // return the data service.
//BTM        return (IDataService) serviceItem.service;
return (ShardService) serviceItem.service;
        
    }
    
    /**
     * Return the proxy for an {@link IMetadataService} from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link IMetadataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link IMetadataService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     * @throws RuntimeException
     *             if <i>serviceUUID</i> identifies a {@link ShardService}.
     */
//BTM    public IMetadataService getMetadataService(final UUID serviceUUID) {
//BTM
//BTM        final ServiceItem serviceItem = getServiceItem(serviceUUID);
//BTM        
//BTM        if (serviceItem == null) {
//BTM
//BTM            log.warn("No such service: uuid=" + serviceUUID);
//BTM
//BTM            return null;
//BTM
//BTM        }
//BTM
//BTM        if (!MetadataServiceFilter.INSTANCE.check(serviceItem)) {
//BTM
//BTM            throw new RuntimeException("Not a metadata service: " + serviceItem);
//BTM
//BTM        }
//BTM
//BTM        // return the metadata service.
//BTM        return (IMetadataService) serviceItem.service;
//BTM
//BTM    }
   
    /**
     * Return an array {@link UUID}s for {@link ShardService}s.
     * 
     * @param maxCount
     *            The maximum #of data services whose {@link UUID} will be
     *            returned. When zero (0) the {@link UUID} for all known data
     *            services will be returned.
     * 
     * @return An array of {@link UUID}s for data services.
     */
    public UUID[] getDataServiceUUIDs(final int maxCount) {

        return super.getServiceUUIDs(maxCount, DataServiceFilter.INSTANCE);
        
//        final ServiceItem[] items = serviceCache.getServiceItems(maxCount,
//                DataServiceFilter.INSTANCE);
//
//        if (INFO)
//            log.info("There are at least " + items.length
//                    + " data services : maxCount=" + maxCount);
//
//        final UUID[] uuids = new UUID[items.length];
//
//        for (int i = 0; i < items.length; i++) {
//
//            uuids[i] = JiniUtil.serviceID2UUID(items[i].serviceID);
//
//        }
//
//        return uuids;

    }

    /**
     * Return an arbitrary {@link ShardService} having the specified service
     * name on an {@link Entry} for that service.
     * 
     * @param name
     *            The service name.
     * 
     * @return The {@link ShardService} -or- <code>null</code> if there is
     *         none in the cache and a remote lookup times out.
     * 
     * @todo refactor into the base class but keep semantics of only matching
     *       data services (vs metadata services) in this class.
     */
//BTM    public IDataService getDataServiceByName(final String name) {
public ShardService getDataServiceByName(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        return getService(new DataServiceFilter() {

            public boolean check(final ServiceItem item) {

                if (super.check(item)) {

                    for (Entry e : item.attributeSets) {

                        if (e instanceof Name) {

                            if (((Name) e).name.equals(name)) {

                                return true;

                            }

                        }

                    }
                    
                }

                return false;
                
            }
            
        });
        
    }
    
}
