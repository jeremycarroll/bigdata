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

//BTM import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceTemplate;

//BTM import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.jini.JiniFederation;

//BTM
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.ShutdownAdmin;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniClientConfig;
import com.sun.jini.admin.DestroyAdmin;
import net.jini.admin.Administrable;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceItem;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
BTM * Class handles discovery of an {@link ILoadBalancerService}.
BTM * Clients are responsible for generating notification events.
* Class handles discovery of an {@link LoadBalancer} service.
 * Clients are responsible for generating notification events.
 */
//BTM public class LoadBalancerClient extends
//BTM        BigdataCachingServiceClient<ILoadBalancerService> {
//BTM
//BTM    public LoadBalancerClient(final JiniFederation fed, final long timeout) throws RemoteException {
//BTM
//BTM        super(fed, ILoadBalancerService.class, new ServiceTemplate(null,
//BTM                new Class[] { ILoadBalancerService.class }, null),
//BTM                null/* filter */, timeout);
//BTM        
//BTM    }
//BTM    
//BTM
    /**
     * Return the {@link ILoadBalancerService} service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @todo handle multiple service instances for failover but always designate
     *       a primary.
     */
//BTM    public ILoadBalancerService getLoadBalancerService() {
//BTM
//BTM        return super.getService();
//BTM        
//BTM    }
//BTM
//BTM}

public class LoadBalancerClient {

    private static final Class[] SERVICE_TYPE = 
                                 new Class[] { LoadBalancer.class };

    private LookupDiscoveryManager ldm;
    private ServiceDiscoveryManager sdm;

    private ServiceItemFilter cacheFilter;
    private ServiceTemplate serviceTmpl = 
                new ServiceTemplate(null, SERVICE_TYPE, null);
    private ServiceDiscoveryListener cacheListener;
    private LookupCache lookupCache;

    private ServiceCache serviceCache;
    private long timeout;

    private boolean terminateLdm = false;
    private boolean terminateSdm = false;

    public LoadBalancerClient(final JiniFederation fed, final long timeout)
               throws IOException, ConfigurationException
    {
        this.sdm = fed.getServiceDiscoveryManager();
        if(this.sdm == null) {
//BTM
System.err.println("\n*** LoadBalancerClient: SDM == null ***");
            JiniClient client = fed.getClient();
            LookupDiscoveryManager ldm = fed.getDiscoveryManagement();
            if(this.ldm == null) {
//BTM
System.err.println("*** LoadBalancerClient: LDM == null ***");
                JiniClientConfig jiniCfg = client.getJiniClientConfig();
                this.ldm = new LookupDiscoveryManager
                               (jiniCfg.groups, jiniCfg.locators, null);
                terminateLdm = true;
            }
            Configuration config = client.getConfiguration();
            this.sdm = new ServiceDiscoveryManager(ldm, null, config);
            terminateSdm = true;
        }
//BTM
String[] groups = ((LookupDiscoveryManager)(sdm.getDiscoveryManager())).getGroups();
if(groups == null) {
    System.err.println("*** LoadBalancerClient: groups == ALL_GROUPS ***");
} else {
    System.err.println("*** LoadBalancerClient: groups.length == "+groups.length+" ***");
    for(int i=0; i<groups.length; i++) {
        System.err.println("*** LoadBalancerClient: groups["+i+"] == "+groups[i]);
    }
}

        
        this.cacheFilter = null;
        this.cacheListener = new ServiceCache(fed);
        this.lookupCache = 
            sdm.createLookupCache
                (serviceTmpl, this.cacheFilter, this.cacheListener);

        this.timeout = timeout;
    }

    public void terminate() {
        if(lookupCache != null) lookupCache.terminate();
        if(terminateSdm && (sdm != null)) sdm.terminate();
        if(terminateLdm && (ldm != null)) ldm.terminate();
    }

    public void destroyDiscoveredServices
                    (final ExecutorService executorService,
                     final ServiceItemFilter filter)
                    throws InterruptedException
    {
//BTM executorService and filter may not be needed
        ServiceItem[] items = 
            lookupCache.lookup(cacheFilter, Integer.MAX_VALUE);
        for(ServiceItem item : items) {
            Object serviceRef = item.service;
            if(serviceRef instanceof Administrable) {
                try {
                    Object serviceAdmin = 
                        ((Administrable)serviceRef).getAdmin();
                    if(serviceAdmin instanceof DestroyAdmin) {
                        try {
                            ((DestroyAdmin)serviceAdmin).destroy();
                        } catch(Throwable t) { }
                    }
                } catch(Throwable t) { 
                    t.printStackTrace();
                }
            }
        }
    }

    public void shutdownDiscoveredServices
                    (final ExecutorService executorService,
                     final ServiceItemFilter filter,
                     final boolean immediateShutdown)
                    throws InterruptedException
    {
//BTM for this service, no difference between destroy and shutdown?
        if( !immediateShutdown ) {
            destroyDiscoveredServices(executorService, filter);
        } else {
            ServiceItem[] items = 
            lookupCache.lookup(cacheFilter, Integer.MAX_VALUE);
            for(ServiceItem item : items) {
                Object serviceRef = item.service;
                if(serviceRef instanceof Administrable) {
                    try {
                        Object serviceAdmin = 
                        ((Administrable)serviceRef).getAdmin();
                        if(serviceAdmin instanceof ShutdownAdmin) {
                            try {
                                ((ShutdownAdmin)serviceAdmin).kill(0);
                            } catch(Throwable t) { }
                        }
                    } catch(Throwable t) { 
                        t.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Return the {@link LoadBalancer} service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @todo handle multiple service instances for failover but always designate
     *       a primary.
     */
    public LoadBalancer getLoadBalancerService() {
        ServiceItem item = lookupCache.lookup(null);
        if(item == null) {
            try {
                item = sdm.lookup(serviceTmpl, cacheFilter, timeout);
            } catch(Throwable t) {
                t.printStackTrace();
            }
        }
//BTM
//System.err.println("\n**** LoadBalancerClient: item = "+item);
        if(item == null) return null;
        return (LoadBalancer)(item.service);
    }
}
