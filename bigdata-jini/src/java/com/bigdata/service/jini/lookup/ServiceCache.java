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
/*
 * Created on Apr 25, 2007
 */

package com.bigdata.service.jini.lookup;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

import com.bigdata.service.IService;

/**
 * A mapping from {@link ServiceID} to {@link ServiceItem} that is maintained by
 * a suitable {@link ServiceDiscoveryManager} manager. In order to use this
 * class, you must register it as the {@link ServiceDiscoveryListener} with the
 * {@link ServiceDiscoveryManager}.
 * <p>
 * Note: instances of this class this will track all services discovered by a
 * given {@link ServiceDiscoveryManager}, not just those that the client
 * actually uses. This is not a problem since the notification events will be
 * generated anyway and we will be storing at most 1000s of services in this
 * map.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceCache implements ServiceDiscoveryListener {

    protected static final transient Logger log = Logger
            .getLogger(ServiceCache.class);
        
    private ConcurrentHashMap<ServiceID, ServiceItem> serviceIdMap = new ConcurrentHashMap<ServiceID, ServiceItem>();

    /**
     * An optional delegate listener that will also see the
     * {@link ServiceDiscoveryEvent}s observed by this class=.
     */
    private final ServiceDiscoveryListener listener;
    
    /**
     * 
     * @param listener
     *            An optional delegate listener that will also see the
     *            {@link ServiceDiscoveryEvent}s observed by this class.
     */
    public ServiceCache(final ServiceDiscoveryListener listener) {
        
        // MAY be null;
        this.listener = listener;
        
    }

    /*
     * ServiceDiscoveryListener.
     */

    /**
     * Adds the {@link ServiceItem} to the internal map to support
     * {@link #getServiceByID()}
     * <p>
     * Note: This event is generated by the {@link LookupCache}. There is an
     * event for each service as it joins any registrar in the set of registrars
     * this class is listening. The set of distinct joined services is
     * accessible via the {@link LookupCache}.
     */
    public void serviceAdded(final ServiceDiscoveryEvent e) {

        if (log.isInfoEnabled())
            log.info("" + e + ", class="
                    + e.getPostEventServiceItem().toString());
//BTM
System.err.println("\n**** SERVICE ADDED: "+ ((e.getPostEventServiceItem()).service).getClass());

        serviceIdMap.put(e.getPostEventServiceItem().serviceID, e
                .getPostEventServiceItem());

        if (listener != null) {

            listener.serviceAdded(e);

        }
        
    }

    /**
     * updates the map and delegates to the {@link #listener}.
     */
    public void serviceChanged(final ServiceDiscoveryEvent e) {

        if (log.isInfoEnabled())
            log.info("" + e + ", class="
                    + e.getPostEventServiceItem().toString());
//BTM
System.err.println("\n**** SERVICE CHANGED: "+ ((e.getPostEventServiceItem()).service).getClass());

        serviceIdMap.put(e.getPostEventServiceItem().serviceID, e
                .getPostEventServiceItem());

        if (listener != null) {

            listener.serviceChanged(e);

        }
        
    }

    /**
     * This removes the service from the cache. However, if the service is an
     * {@link IService} and it will still respond to its interface then we DO
     * NOT remove the service from the cache. This makes the cache more robust
     * in the face of transient de-registrations of a service. The most common
     * explanation for those transients is that the host with the service is
     * swapping and has failed to keep up its pings to the jini registrar(s).
     */
    public void serviceRemoved(final ServiceDiscoveryEvent e) {

        final Object service = e.getPreEventServiceItem().service;

        if(service instanceof IService) {
            
            try {
            
                final String serviceName = ((IService)service).getServiceName();

                log.warn("Service still active: "+serviceName);
                
                return;
                
            } catch(IOException ex) {
                
                // ignore, fall through and remove the service from the cache.
                
            }
            
        }
                
        if (log.isInfoEnabled())
            log.info("" + e + ", class="
                    + e.getPreEventServiceItem().toString());
//BTM
System.err.println("\n**** SERVICE REMOVED: "+ ((e.getPreEventServiceItem()).service).getClass());

        serviceIdMap.remove(e.getPreEventServiceItem().serviceID);

        if (listener != null) {

            listener.serviceRemoved(e);

        }

    }

    /*
     * Our own API.
     */
    
    /**
     * Resolve the {@link ServiceID} for a service to the cached
     * {@link ServiceItem} for that service.
     * 
     * @param serviceID
     *            The {@link ServiceID} for the service.
     * 
     * @return The cache {@link ServiceItem} for that service.
     */
    public ServiceItem getServiceItemByID(final ServiceID serviceID) {
        
        return serviceIdMap.get(serviceID);
        
    }
    
    /**
     * Return the #of known services.
     * 
     * @return The #of services in the cache.
     */
    public int getServiceCount() {
        
        return serviceIdMap.size();
        
    }

    /**
     * Return an array of cached service items.
     * 
     * @param maxCount
     *            The maximum #of service items to be returned. When zero (0)
     *            all known service items will be returned.
     * @param filter
     *            An optional filter.
     * 
     * @return An array of {@link ServiceItem}s.
     */
    public ServiceItem[] getServiceItems(final int maxCount,
            final ServiceItemFilter filter) {

        if (maxCount < 0)
            throw new IllegalArgumentException();

        final int n = maxCount == 0 ? getServiceCount() : maxCount;

        final Vector<ServiceItem> v = new Vector<ServiceItem>(n);

        final Iterator<Map.Entry<ServiceID, ServiceItem>> itr = serviceIdMap
                .entrySet().iterator();

        while (itr.hasNext() && v.size() < n) {

            final Map.Entry<ServiceID, ServiceItem> entry = itr.next();

            final ServiceItem item = entry.getValue();

            if (filter == null || filter.check(item)) {

                v.add(item);

            }

        }

        final int m = v.size();

        if (log.isInfoEnabled())
            log.info("found " + m + " matching service items");

        return v.toArray(new ServiceItem[m]);

    }

}
