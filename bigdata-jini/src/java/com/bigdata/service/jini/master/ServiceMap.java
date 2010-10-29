package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.ServiceDiscoveryManager;

import com.bigdata.jini.util.JiniUtil;
//BTM import com.bigdata.service.IClientService;
import com.bigdata.service.jini.JiniFederation;

//BTM
import com.bigdata.service.CallableExecutor;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.jini.IJiniDiscoveryManagement;

/**
 * An ordered mapping of indices in <code>[0:N-1]</code> onto the services on
 * which the task with the corresponding index will be executed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Stable assignments across re-runs are only required if the client will
 *       be reading or writing data local to the host on which it is executing.
 *       Otherwise we are free to choose new assignments on restart or even to
 *       add more clients over time in an m/r model.
 *       <p>
 *       If the {@link ServiceItem} to client# assignment can change over time
 *       then we need to use a lock to make that change atomic with respect to
 *       requests for the client's proxy.
 */
public class ServiceMap implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 5704885443752980274L;

    /**
     * The #of tasks to be mapped over the services.
     */
    private final int ntasks;

    /**
     * The mapping of tasks onto the callable executor services on which
     * that task will execute. The index is the task#. The value is the
     * {@link ServiceItem} for the callable executor service on which
     * the corresponding task will execute.
     * <p>
     * This provides richer information than the {@link #serviceUUIDs}, but
     * this information can be (and is) recovered on demand from just the
     * {@link #serviceUUIDs}.
     * <p>
     * Note: This is private since it is used by the master to assign tasks to
     * services. In contrast, the {@link #serviceUUIDs} are serialized
     * and have public scope.
     */
    private transient ServiceItem[] serviceItems;
    
    /**
     * Array of service uuid's; where the index is a task#, and the
     * value is the uuid of the callable executor service on which
     * the associated task will execute.
     */
    private final UUID serviceUUIDs[];

    /**
     * 
     * @param ntasks
     *            The #of tasks to be mapped over the services.
     */
    public ServiceMap(final int ntasks) {

        if (ntasks < 0)
            throw new IllegalArgumentException();
        
        this.ntasks = ntasks;
        
        this.serviceItems = new ServiceItem[ntasks];

        this.serviceUUIDs = new UUID[ntasks];

    }

    /**
     * Populates the elements of the {@link #serviceItems} array by
     * resolving the {@link #serviceUUIDs} to the corresponding
     * {@link ServiceItem}s. For each service, this tests the service
     * cache for each of the desired service types; that is, callable
     * executor (client) service, shard (data) service, and
     * shard locator (metadata) service (in that order). If the 
     * caches do not produce a result, then an explicit service id
     * based lookup -- with a timeout -- is performed.
     * 
     * @throws InterruptedException
     *             If interrupted during service lookup.
     * @throws RemoteException
     *             If there is an RMI problem.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public void resolveServiceUUIDs(final JiniFederation fed)
//BTM - PRE_CLIENT_SERVICE            throws RemoteException, InterruptedException {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE ServiceDiscoveryManager sdm = fed.getServiceDiscoveryManager();
//BTM - PRE_CLIENT_SERVICE long timeoutMillis = 1000L;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        for (int i = 0; i < ntasks; i++) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            final UUID serviceUUID = serviceUUIDs[i];
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            final ServiceID serviceID = JiniUtil.uuid2ServiceID(serviceUUID);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            ServiceItem serviceItem = null;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE            // test client service cache.
//BTM - PRE_CLIENT_SERVICE            serviceItem = fed.getClientServicesClient().getServiceCache()
//BTM - PRE_CLIENT_SERVICE                    .getServiceItemByID(serviceID);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE //BTM            if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                // test data service cache.
//BTM - PRE_CLIENT_SERVICE //BTM                serviceItem = fed.getDataServicesClient().getServiceCache()
//BTM - PRE_CLIENT_SERVICE //BTM                        .getServiceItemByID(serviceID);
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                    // direct lookup.
//BTM - PRE_CLIENT_SERVICE //BTM                    serviceItem = fed.getServiceDiscoveryManager()
//BTM - PRE_CLIENT_SERVICE //BTM                            .lookup(
//BTM - PRE_CLIENT_SERVICE //BTM                                    new ServiceTemplate(
//BTM - PRE_CLIENT_SERVICE //BTM                                            serviceID,
//BTM - PRE_CLIENT_SERVICE //BTM                                            new Class[] { IRemoteExecutor.class }/* types */,
//BTM - PRE_CLIENT_SERVICE //BTM                                            null/* attr */),
//BTM - PRE_CLIENT_SERVICE //BTM                                    null/* filter */, 1000/* timeoutMillis */);
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                    if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                        throw new RuntimeException(
//BTM - PRE_CLIENT_SERVICE //BTM                                "Could not discover service: " + serviceUUID);
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                    }
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM                }
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE //BTM            }
//BTM - PRE_CLIENT_SERVICE //BTM - BEGIN ------------------------------------------------------------------
//BTM - PRE_CLIENT_SERVICE //BTM - PRE_FRED_3481
//BTM - PRE_CLIENT_SERVICE //BTM - NOTE: the code below is different than the corresponding code changes
//BTM - PRE_CLIENT_SERVICE //BTM -       made by fkoliver in changeset 3481. The code in 3481 seems to
//BTM - PRE_CLIENT_SERVICE //BTM -       focus on the ClientService (discover by ClientServicesClient, 
//BTM - PRE_CLIENT_SERVICE //BTM -       discover by serviceID AND IClientService.class type), whereas
//BTM - PRE_CLIENT_SERVICE //BTM -       the code below attempts to duplicate the original logic; that is,
//BTM - PRE_CLIENT_SERVICE //BTM -       discovers ONLY by serviceID, and after failing to discover using
//BTM - PRE_CLIENT_SERVICE //BTM -       the cache of the ClientServicesClient, tries the cache of the 
//BTM - PRE_CLIENT_SERVICE //BTM -       DataServicesClient, then the cache of the ShardLocatorClient,
//BTM - PRE_CLIENT_SERVICE //BTM -       and finally a remote lookup (by serviceID ONLY) through the sdm.
//BTM - PRE_CLIENT_SERVICE //BTM -       When merging to maven_scaleout, it should probably be discussed
//BTM - PRE_CLIENT_SERVICE //BTM -       whether this code should be taken or the code from 3481.
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     //no callable executor (client) service, try shard service
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     serviceItem = 
//BTM - PRE_CLIENT_SERVICE                         fed.getDataServicesClient()
//BTM - PRE_CLIENT_SERVICE                            .getServiceCache()
//BTM - PRE_CLIENT_SERVICE                            .getServiceItemByID(serviceID);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                         //no shard service, try shard locator service
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                         serviceItem = fed.getShardLocatorClient()
//BTM - PRE_CLIENT_SERVICE                                          .getServiceCache()
//BTM - PRE_CLIENT_SERVICE                                          .getServiceItemByID(serviceID);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                         //no callable executor (client) service, no shard
//BTM - PRE_CLIENT_SERVICE                         //service, no shard locator service, try direct lookup
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                         if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE                             ServiceTemplate tmpl = 
//BTM - PRE_CLIENT_SERVICE                                 new ServiceTemplate(serviceID, null, null); 
//BTM - PRE_CLIENT_SERVICE                             serviceItem = sdm.lookup(tmpl, null, timeoutMillis);
//BTM - PRE_CLIENT_SERVICE                             if (serviceItem == null) {
//BTM - PRE_CLIENT_SERVICE                                 throw new RuntimeException
//BTM - PRE_CLIENT_SERVICE                                               ("Could not discover "
//BTM - PRE_CLIENT_SERVICE                                                +"service ["+serviceUUID+"]");
//BTM - PRE_CLIENT_SERVICE                             }
//BTM - PRE_CLIENT_SERVICE                         }
//BTM - PRE_CLIENT_SERVICE                     }
//BTM - PRE_CLIENT_SERVICE                 }
//BTM - PRE_CLIENT_SERVICE //BTM - END --------------------------------------------------------------------
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             if (serviceItems == null) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 /*
//BTM - PRE_CLIENT_SERVICE                  * Lazy initialization when de-serialized since field is
//BTM - PRE_CLIENT_SERVICE                  * transient and will not be initialized by the default
//BTM - PRE_CLIENT_SERVICE                  * de-serialization logic.
//BTM - PRE_CLIENT_SERVICE                  */
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 serviceItems = new ServiceItem[ntasks];
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             serviceItems[i] = serviceItem;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE - NOTE: for the client service smart proxy work, we go back to the version of
//BTM - PRE_CLIENT_SERVICE -       resolveServiceUUIDs that Fred provided in changeset 3481 (with the
//BTM - PRE_CLIENT_SERVICE -       appropriate smart proxy related changes). This is because, after 
//BTM - PRE_CLIENT_SERVICE -       additional analysis, it appears that the intent of this class is
//BTM - PRE_CLIENT_SERVICE -       to discover (and cache in the serviceItems[] array) instances of
//BTM - PRE_CLIENT_SERVICE -       the client service, not the other services. So there doesn't appear
//BTM - PRE_CLIENT_SERVICE -       to be a need to maintain the original logic. Additionally, with the
//BTM - PRE_CLIENT_SERVICE -       work to remove the federation object, the IBigdataDiscoveryManagement
//BTM - PRE_CLIENT_SERVICE -       object does not allow one to 'get' client services by id, like
//BTM - PRE_CLIENT_SERVICE -       getClientServicesClient().getServiceCache() does. So discovery by
//BTM - PRE_CLIENT_SERVICE -       the clientServiceCache cannot be used either. Thus, we go back to
//BTM - PRE_CLIENT_SERVICE -       3481's use of the SDM to populate the array with only client services;
//BTM - PRE_CLIENT_SERVICE -       minus the use of getServicesClient().getServiceCache().
    public void resolveServiceUUIDs
                    (final IJiniDiscoveryManagement discoveryManager)
                        throws RemoteException, InterruptedException
    {
        long timeoutMillis = 1000L;
        ServiceDiscoveryManager sdm =
                discoveryManager.getServiceDiscoveryManager();
        for (int i = 0; i < ntasks; i++) {
            final UUID serviceUUID = serviceUUIDs[i];
            final ServiceID serviceID = JiniUtil.uuid2ServiceID(serviceUUID);

            Class[] clientServiceType = new Class[] {CallableExecutor.class};
            ServiceTemplate tmpl =
                new ServiceTemplate(serviceID, clientServiceType, null);
            ServiceItem serviceItem = sdm.lookup(tmpl, null, timeoutMillis);
            if (serviceItem == null) {
                throw new RuntimeException("Could not discover "
                                           +"service ["+serviceUUID+"]");
            }
            if (serviceItems == null) {
                // Lazy initialization when de-serialized since field is
                // transient and will not be initialized by the default
                // de-serialization logic.
                serviceItems = new ServiceItem[ntasks];
            }
            serviceItems[i] = serviceItem;
        }
    }
//BTM - PRE_CLIENT_SERVICE - END

    /**
     * Assigns clients to services. The assignments are made in the given
     * order MODULO the #of service items.
     * 
     * @param serviceItems
     *            The ordered array of services to which each client will be
     *            assigned.
     */
    public void assignClientsToServices(final ServiceItem[] serviceItems)
            throws Exception {
        
        if (serviceItems == null)
            throw new IllegalArgumentException();
        
        for (int clientNum = 0; clientNum < ntasks; clientNum++) {

            final int i = clientNum % serviceItems.length;

            final ServiceItem serviceItem = serviceItems[i];
            
            assert serviceItem != null : "No service item @ index=" + i;

            this.serviceItems[clientNum] = serviceItem;

            this.serviceUUIDs[clientNum] = JiniUtil
                    .serviceID2UUID(serviceItem.serviceID);

        }
        
    }

    /**
     * Return the {@link ServiceItem} of the service to which the Nth client
     * was assigned.
     * 
     * @param clientNum
     *            The client number in [0:N-1].
     * 
     * @return The {@link ServiceItem} of the service on which that client
     *         should execute.
     */
//BTM    public IClientService getService(final int clientNum) {
public CallableExecutor getService(final int clientNum) {
        ServiceItem serviceItem = serviceItems[clientNum];
        if (serviceItem == null) {
            /*
             * Note: The ServiceItem should have been resolved when we
             * setup the JobState, even if the JobState was read from
             * zookeeper.
             */
            throw new RuntimeException(
                    "ServiceItem not resolved? client#=" + clientNum);
        }
//BTM - BEGIN
//BTM        if (!(serviceItem.service instanceof IClientService)) {
//BTM                throw new RuntimeException("Service does not implement "
//BTM                            + IClientService.class + ", serviceItem="
//BTM                            + serviceItem);
//BTM        }
//BTM        return (IClientService) serviceItems[clientNum].service;
        if (!(serviceItem.service instanceof CallableExecutor)) {
                throw new RuntimeException("Service does not implement "
                            + CallableExecutor.class + ", serviceItem="
                            + serviceItem);
        }
        return (CallableExecutor) serviceItems[clientNum].service;
//BTM - END
    }

}
