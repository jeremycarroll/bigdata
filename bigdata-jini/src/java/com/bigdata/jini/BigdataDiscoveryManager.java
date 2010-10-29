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

import com.bigdata.counters.CounterSet;
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.IServicesManagerService;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.TransactionService;
import com.bigdata.service.HostScore;
import com.bigdata.service.IClientService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceOnlyFilter;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IService;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.Service;
import com.bigdata.service.ServiceScore;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardService;
import com.bigdata.util.EntryUtil;
import com.bigdata.util.Util;
import com.bigdata.util.config.LogUtil;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class that implements the <code>IBigdataDiscoveryManagement<code>
 * interface and manages jini-based discovery processing related to the
 * services that have joined a Bigdata federation.
 */
public class BigdataDiscoveryManager implements IJiniDiscoveryManagement {

    private Logger logger;

    //for jini-based dynamic discovery
    private ServiceDiscoveryManager sdm;

    private LookupCache txnServiceCache;
    private LookupCache lbsServiceCache;
    private LookupCache mdsServiceCache;
    private LookupCache shardCache;
    private LookupCache remoteShardCache;//need because of IMetadataService

    //for embedded federation testing
    private TransactionService embeddedTxnSrvc;
    private LoadBalancer embeddedLbs;
    private ShardLocator embeddedMds;
    private Map<UUID, ShardService> embeddedDsMap;

    //special maps for load balancer - populated as services join/leave
    private ConcurrentHashMap<UUID, String> serviceNameMap;
    private ConcurrentHashMap<String, HostScore> activeHosts;
    private ConcurrentHashMap<UUID, ServiceScore> activeDataServices;

    private CounterSet countersRoot;//set when a service is discovered

    public BigdataDiscoveryManager() {
        // discovery disabled for the instance created here
        this(null, null, null, null, null, null, null, null, null, null);
    }

    public BigdataDiscoveryManager
               (ServiceDiscoveryManager sdm,
                TransactionService embeddedTxnService,
                LoadBalancer embeddedLoadBalancer,
                ShardLocator embeddedShardLocator,
                Map<UUID, ShardService> embeddedDataServiceMap,
                Logger logger)
    {
        this(sdm, embeddedTxnService, embeddedLoadBalancer,
             embeddedShardLocator, embeddedDataServiceMap,
             null, null, null, null,
             logger);
    }

    public BigdataDiscoveryManager
               (ServiceDiscoveryManager sdm,
                TransactionService embeddedTxnService,
                LoadBalancer embeddedLoadBalancer,
                ShardLocator embeddedShardLocator,
                Map<UUID, ShardService> embeddedDataServiceMap,
                ConcurrentHashMap<UUID, String> serviceNameMap,
                ConcurrentHashMap<String, HostScore> activeHosts,
                ConcurrentHashMap<UUID, ServiceScore> activeDataServices,
                CounterSet countersRoot,
                Logger logger)
    {
        this.sdm = sdm;
        ServiceDiscoveryListener cacheListener = new CacheListener(logger);

        this.embeddedTxnSrvc = embeddedTxnService;
        this.embeddedLbs = embeddedLoadBalancer;
        this.embeddedMds = embeddedShardLocator;
        this.embeddedDsMap = embeddedDataServiceMap;

        this.serviceNameMap = 
            (serviceNameMap == null ? new ConcurrentHashMap<UUID, String>()
                                    : serviceNameMap);
        this.activeHosts = activeHosts;
        this.activeDataServices = activeDataServices;

        this.countersRoot = countersRoot;

        if(sdm != null) {
            //for discovering txn, lbs, and shard services
            Class[] txnServiceType = 
                            new Class[] {TransactionService.class};
            ServiceTemplate txnServiceTmpl = 
                            new ServiceTemplate(null, txnServiceType, null);

            Class[] lbsServiceType = 
                            new Class[] {LoadBalancer.class};
            ServiceTemplate lbsServiceTmpl = 
                            new ServiceTemplate(null, lbsServiceType, null);

            Class[] mdsServiceType = 
                            new Class[] {ShardLocator.class};
            ServiceTemplate mdsServiceTmpl = 
                            new ServiceTemplate(null, mdsServiceType, null);

            Class[] shardType = new Class[] {ShardService.class};
            ServiceTemplate shardTmpl = 
                            new ServiceTemplate(null, shardType, null);
            ServiceItemFilter shardFilter = null;
            try {
                //caches for smart proxy implementations
                this.txnServiceCache = sdm.createLookupCache
                                     ( txnServiceTmpl, 
                                       null,
                                       cacheListener );
                this.lbsServiceCache = sdm.createLookupCache
                                     ( lbsServiceTmpl, 
                                       null,
                                       cacheListener );
                this.mdsServiceCache = sdm.createLookupCache
                                     ( mdsServiceTmpl, 
                                       null,
                                       cacheListener );
                this.shardCache = sdm.createLookupCache
                                     ( shardTmpl, 
                                       shardFilter,
                                       cacheListener );
            } catch(RemoteException e) {
                logger.warn(e.getMessage(), e);
            }

            //for remote implementation 

            Class[] remoteShardType = new Class[] {IDataService.class};
            ServiceTemplate remoteShardTmpl = 
                            new ServiceTemplate(null, remoteShardType, null);
            ServiceItemFilter remoteShardFilter = new IDataServiceOnlyFilter();
            try {
                this.remoteShardCache = sdm.createLookupCache
                                     ( remoteShardTmpl,
                                       remoteShardFilter,
                                       cacheListener );
            } catch(RemoteException e) {
                logger.warn(e.getMessage(), e);
            }
        }

        this.logger  = (logger == null ? 
                        LogUtil.getLog4jLogger((this.getClass()).getName()) :
                        logger);
    }

    // Required by IBigdataDiscoveryManagement interface

    public TransactionService getTransactionService() {
        if(txnServiceCache != null) {
            ServiceItem txnItem = txnServiceCache.lookup(null);
            if(txnItem != null) return (TransactionService)txnItem.service;
        }
        return embeddedTxnSrvc;
    }

    public LoadBalancer getLoadBalancerService() {
        if(lbsServiceCache != null) {
            ServiceItem lbsItem = lbsServiceCache.lookup(null);
            if(lbsItem != null) return (LoadBalancer)lbsItem.service;
        }
        return embeddedLbs;
    }

    public ShardLocator getMetadataService() {
        if(mdsServiceCache != null) {
            ServiceItem mdsItem = mdsServiceCache.lookup(null);
            if(mdsItem != null) return (ShardLocator)mdsItem.service;
        }
        return embeddedMds;
    }

    public UUID[] getDataServiceUUIDs(int maxCount) {
        if (maxCount < 0) {
            throw new IllegalArgumentException
                ("negative maxCount ["+maxCount+"]");
        }

        Set<UUID> uuidSet = new HashSet<UUID>();

        //try smart proxy cache first
        if(shardCache != null) {
            ServiceItem[] shardItems = 
                shardCache.lookup(null, maxCount);
            if(shardItems.length > 0) {
                for(int i=0; i<shardItems.length; i++) {
                    uuidSet.add
                        ( JiniUtil.serviceID2UUID
                                       (shardItems[i].serviceID) );
                }
            }
        }
        if(uuidSet.size() >= maxCount) {
            return uuidSet.toArray( new UUID[uuidSet.size()] );
        }

        //not enough smart proxy impls, try remote cache
        if(remoteShardCache != null) {
            ServiceItem[] shardItems = 
                remoteShardCache.lookup(null, maxCount);
            if(shardItems.length > 0) {
                for(int i=0; i<shardItems.length; i++) {
                    uuidSet.add
                        ( JiniUtil.serviceID2UUID
                                       (shardItems[i].serviceID) );
                }
            }
        }

        //don't mix discovery with embedded case
        if(uuidSet.size() > 0) {
            return uuidSet.toArray( new UUID[uuidSet.size()] );
        }

        //neither cache contains elements, assume embedded case
        if(embeddedDsMap != null) {
            uuidSet = embeddedDsMap.keySet();
        }
        return uuidSet.toArray( new UUID[uuidSet.size()] );
    }
    
    public ShardService[] getDataServices(UUID[] uuids) {
        if (uuids == null) {
            throw new NullPointerException("null uuids array");
        }
        Set<ShardService> retSet = new HashSet<ShardService>();
        for(UUID uuid : uuids) {
            ShardService shardService = getDataService(uuid);
            if(shardService == null) continue;
            retSet.add(shardService);
        }
        return retSet.toArray( new ShardService[retSet.size()] );
    }
    
    public ShardService getDataService(UUID uuid) {
        if (uuid == null) {
            throw new NullPointerException("null uuid");
        }

        //try smart proxy cache first
        if(shardCache != null) {
            ServiceItem[] shardItems = 
                shardCache.lookup(null, Integer.MAX_VALUE);
            if(shardItems.length > 0) {
                ServiceID id = JiniUtil.uuid2ServiceID(uuid);
                for(int i=0; i<shardItems.length; i++) {
                    if( id.equals( (shardItems[i]).serviceID ) ) {
                        return (ShardService)((shardItems[i]).service);
                    }
                }
            }
        }

        //no smart proxy impl with uuid, try remote cache
        if(remoteShardCache != null) {
            ServiceItem[] shardItems = 
                remoteShardCache.lookup(null, Integer.MAX_VALUE);
            if(shardItems.length > 0) {
                ServiceID id = JiniUtil.uuid2ServiceID(uuid);
                for(int i=0; i<shardItems.length; i++) {
                    if( id.equals( (shardItems[i]).serviceID ) ) {
                        return (ShardService)((shardItems[i]).service);
                    }
                }
            }
        }

        //nothing in the caches with uuid, try embedded map
        if(embeddedDsMap != null) return embeddedDsMap.get(uuid);

        return null;
    }

    public ShardService getAnyDataService() {
        ServiceItem shardItem = shardCache.lookup(null);
        if(shardItem == null) {//no smart proxy, try remote cache
            shardItem = remoteShardCache.lookup(null);
        }
        if(shardItem != null) {
            return (ShardService)(shardItem.service);
        } else {//try embedded map
            if (embeddedDsMap == null) return null;
            if ( embeddedDsMap.isEmpty() ) return null;
            Iterator<ShardService> itr = 
                (embeddedDsMap.values()).iterator();
            if( itr.hasNext() ) {
                return itr.next();
            }
        }
        return null;
    }
    
    public ShardService getDataServiceByName(String name) {
        if (name == null) {
            throw new NullPointerException("null name");
        }
        if ( !serviceNameMap.containsValue(name) ) return null;

        Set<Map.Entry<UUID, String>> uuidNameEntrySet =
                                         serviceNameMap.entrySet();
        Iterator<Map.Entry<UUID, String>> itr =
                                          uuidNameEntrySet.iterator();
        while( itr.hasNext() ) {
            Map.Entry<UUID, String> pair = itr.next();
            UUID uuid = pair.getKey();
            String nameVal = pair.getValue();
            if( (nameVal != null) && (nameVal.compareTo(name) == 0) ) {
                return getDataService(uuid);
            }
        }
        return null;
    }

    // NOTE: the following two methods are needed for embedded testing.
    //       Currently, EmbeddedFederation creates the shard locator
    //       service (embeddedMds) after it creates the embeddedShardService,
    //       which creates this class. But this class needs to be given
    //       a non-null reference to the embeddedMds once the
    //       EmbeddedFederation creates it, so that when this class'
    //       getMetadataService method is called (ex. TestMove), that
    //       reference can be returned.

    public void setEmbeddedMds(ShardLocator embeddedMds) {
        this.embeddedMds = embeddedMds;
    }

    public void setEmbeddedDsMap(Map<UUID, ShardService> embeddedDsMap) {
        this.embeddedDsMap = embeddedDsMap;
    }

    // Additional public method(s) provided by this class

    public ServiceDiscoveryManager getServiceDiscoveryManager() {
        return sdm;
    }

    // Nested class(es)

    private class CacheListener implements ServiceDiscoveryListener {
        private Logger logger;
        CacheListener(Logger logger) {
            this.logger = logger;
        }
	public void serviceAdded(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPostEventServiceItem();

            ServiceID serviceId = item.serviceID;
            Object service = item.service;
            Entry[] attrs = item.attributeSets;

            Class serviceType = service.getClass();

            UUID serviceUUID = null;
            String hostname = null;
            String serviceName = null;
            Class serviceIface = null;

            if( (IService.class).isAssignableFrom(serviceType) ) {

                // Avoid remote calls by getting info from attrs
                ServiceUUID serviceUUIDAttr = 
                    (ServiceUUID)(EntryUtil.getEntryByType
                        (attrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    if(service != null) {
                        try {
                            serviceUUID = 
                            ((IService)service).getServiceUUID();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"serviceUUID "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }
                Hostname hostNameAttr =
                    (Hostname)(EntryUtil.getEntryByType
                                  (attrs, Hostname.class));
                if(hostNameAttr != null) {
                    hostname = hostNameAttr.hostname;
                } else {
                    if(service != null) {
                        try {
                            hostname = 
                            ((IService)service).getHostname();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"hostname "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }
                Name serviceNameAttr = 
                    (Name)(EntryUtil.getEntryByType
                                         (attrs, Name.class));
                if(serviceNameAttr != null) {
                    serviceName = serviceNameAttr.name;
                } else {
                    if(service != null) {
                        try {
                            serviceName = 
                            ((IService)service).getServiceName();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"serviceName "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }

                if( (IMetadataService.class).isAssignableFrom
                                                 (serviceType) )
                {
                    serviceIface = IMetadataService.class;
                } else if( (IDataService.class).isAssignableFrom
                                             (serviceType) )
                {
                    serviceIface = IDataService.class;
                } else if( (IClientService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = IClientService.class;
                } else if( (ITransactionService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    serviceIface = ITransactionService.class;

                } else if( (IServicesManagerService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.DEBUG, "serviceAdded "
                                   +"[service=IServicesManagerService, "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.WARN, "UNEXPECTED serviceAdded "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                }

            } else if( (Service.class).isAssignableFrom(serviceType) ) {

                serviceUUID = ((Service)service).getServiceUUID();
                hostname = ((Service)service).getHostname();
                serviceName = ((Service)service).getServiceName();
                serviceIface = ((Service)service).getServiceIface();

            } else {

                if(logger.isDebugEnabled()) {
                    logger.log(Level.WARN, "UNEXPECTED serviceAdded "
                               +"[service="+serviceType+", "
                               +"ID="+serviceId+"]");
                }
                return;
            }

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "serviceAdded [service="
                           +serviceIface+", ID="+serviceId+"]");
            }

            if(serviceUUID == null) return;

            if(serviceNameMap != null) {
                serviceNameMap.put(serviceUUID, serviceName);
            }

            if (activeHosts != null) {
                if( activeHosts.putIfAbsent
                        (hostname, new HostScore(hostname)) == null)
                {
                    if(logger.isDebugEnabled()) {
                        logger.debug("new host joined: "
                                     +"[hostname="+hostname+"]");
                    }
                 }
            }

            // Only data/shard services are registered as [activeServices]
            // since load balancing decisions are made on only 
            // those service types.
            if( (ShardService.class).isAssignableFrom(serviceType) ||
                (  (IDataService.class).isAssignableFrom(serviceType) &&
                  !(IMetadataService.class).isAssignableFrom(serviceType) ) )
            {
                if (activeDataServices != null) {
                    if( activeDataServices.putIfAbsent
                        (serviceUUID,
                           new ServiceScore
                             (hostname, serviceUUID, serviceName)) == null)
                    {
                        if (logger.isDebugEnabled()) {
                            logger.debug("shard service join: "
                                         +"[hostname="+hostname+", "
                                         +"serviceUUID="+serviceUUID+"]");
                        }
                    }
                }
            }

            // Create a node for the discovered service's history in
            // this load balancer's counter set.
            if (countersRoot != null) {
                countersRoot.makePath
                    ( Util.getServiceCounterPathPrefix
                          (serviceUUID, serviceIface, hostname) );
            }
	}

	public void serviceRemoved(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPreEventServiceItem();

            ServiceID serviceId = item.serviceID;
            Object service = item.service;
            Entry[] attrs = item.attributeSets;

            Class serviceType = service.getClass();

            UUID serviceUUID = null;
            Class serviceIface = null;

            if( (IService.class).isAssignableFrom(serviceType) ) {

                // Avoid remote calls by getting info from attrs
                ServiceUUID serviceUUIDAttr = 
                    (ServiceUUID)(EntryUtil.getEntryByType
                        (attrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    if(service != null) {
                        try {
                            serviceUUID = 
                            ((IService)service).getServiceUUID();
                        } catch(IOException e) {
                            if(logger.isTraceEnabled()) {
                                logger.log(Level.TRACE, 
                                           "failed to retrieve "
                                           +"serviceUUID "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }

                if( (IMetadataService.class).isAssignableFrom
                                                 (serviceType) )
                {
                    serviceIface = IMetadataService.class;
                } else if( (IDataService.class).isAssignableFrom
                                             (serviceType) )
                {
                    serviceIface = IDataService.class;
                } else if( (IClientService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = IClientService.class;
                } else if( (ITransactionService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    serviceIface = ITransactionService.class;

                } else if( (IServicesManagerService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.DEBUG, "serviceRemoved "
                                   +"[service=IServicesManagerService, "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.WARN, "UNEXPECTED serviceRemoved "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } 

            } else if( (Service.class).isAssignableFrom(serviceType) ) {

                serviceUUID = ((Service)service).getServiceUUID();
                serviceIface = ((Service)service).getServiceIface();

            } else {

                if(logger.isDebugEnabled()) {
                    logger.log(Level.WARN, "UNEXPECTED serviceRemoved "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                }
                return;
            }

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "serviceRemoved [service="
                           +serviceIface+", ID="+serviceId+"]");
            }

            if(serviceUUID == null) return;

            if(activeDataServices != null) {
                activeDataServices.remove(serviceUUID);
            }
            if(serviceNameMap != null) {
                serviceNameMap.remove(serviceUUID);
            }
        }

	public void serviceChanged(ServiceDiscoveryEvent event) {

            ServiceItem preItem  = event.getPreEventServiceItem();
            ServiceItem postItem = event.getPostEventServiceItem();

            ServiceID serviceId = postItem.serviceID;
            Object service = postItem.service;

            Class serviceType = service.getClass();

            Entry[] preAttrs  = preItem.attributeSets;
            Entry[] postAttrs = postItem.attributeSets; 

            UUID serviceUUID = null;
            Class serviceIface = null;

            if( (IService.class).isAssignableFrom(serviceType) ) {

                // Avoid remote calls by getting info from attrs
                ServiceUUID serviceUUIDAttr = 
                    (ServiceUUID)(EntryUtil.getEntryByType
                        (preAttrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    if(service != null) {
                        try {
                            serviceUUID = 
                            ((IService)service).getServiceUUID();
                        } catch(IOException e) {
                            if(logger.isTraceEnabled()) {
                                logger.log(Level.TRACE, 
                                           "failed to retrieve "
                                           +"serviceUUID "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }

                if( (IMetadataService.class).isAssignableFrom
                                                 (serviceType) )
                {
                    serviceIface = IMetadataService.class;
                } else if( (IDataService.class).isAssignableFrom
                                             (serviceType) )
                {
                    serviceIface = IDataService.class;
                } else if( (IClientService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = IClientService.class;
                } else if( (ITransactionService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    serviceIface = ITransactionService.class;

                } else if( (IServicesManagerService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.DEBUG, "serviceChanged "
                                   +"[service=IServicesManagerService, "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.WARN, "UNEXPECTED serviceChanged "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                }

            } else if( (Service.class).isAssignableFrom(serviceType) ) {

                serviceUUID = ((Service)service).getServiceUUID();
                serviceIface = ((Service)service).getServiceIface();

            } else {

                if(logger.isDebugEnabled()) {
                    logger.log(Level.WARN, "UNEXPECTED serviceChanged "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                }
                return;
            }

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "serviceChanged [service="
                           +serviceIface+", ID="+serviceId+"]");
            }
        }
    }
}
