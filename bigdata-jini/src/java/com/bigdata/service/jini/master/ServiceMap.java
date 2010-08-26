package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;

import com.bigdata.jini.util.JiniUtil;
import com.bigdata.service.IClientService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;

//BTM
import net.jini.lookup.ServiceDiscoveryManager;
import com.bigdata.service.ShardLocator;

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
    public final int ntasks;

    /**
     * The mapping of tasks onto the {@link IRemoteExecutor}s on which that
     * task will execute. The index is the task#. The value is the
     * {@link ServiceItem} for the {@link IRemoteExecutor} on which that
     * client will execute.
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
     * The mapping of tasks onto the {@link IRemoteExecutor}s on which
     * that task will execute. The index is the task#. The value is the
     * {@link IRemoteExecutor} {@link UUID service UUID}.
     */
    public final UUID serviceUUIDs[];

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
     * {@link ServiceItem}s. For each service, this tests the service cache
     * for {@link IClientService}s and {@link IDataService}s and only then
     * does a lookup with a timeout for the service.
     * 
     * @throws InterruptedException
     *             If interrupted during service lookup.
     * @throws RemoteException
     *             If there is an RMI problem.
     */
    public void resolveServiceUUIDs(final JiniFederation fed)
            throws RemoteException, InterruptedException {

//BTM
//BTM - replace IRemoteExecutor with CallableExecutor & ShardService
//BTM   when those services have been converted
Class[] executorType = new Class[] { IRemoteExecutor.class };
Class[] shardType = new Class[] { IRemoteExecutor.class };
Class[] shardLocatorType = new Class[] { ShardLocator.class };
ServiceDiscoveryManager sdm = fed.getServiceDiscoveryManager();
long timeoutMillis = 1000L;

        for (int i = 0; i < ntasks; i++) {

            final UUID serviceUUID = serviceUUIDs[i];

            final ServiceID serviceID = JiniUtil.uuid2ServiceID(serviceUUID);

            ServiceItem serviceItem = null;

            // test client service cache.
            serviceItem = fed.getClientServicesClient().getServiceCache()
                    .getServiceItemByID(serviceID);

//BTM            if (serviceItem == null) {
//BTM
//BTM                // test data service cache.
//BTM                serviceItem = fed.getDataServicesClient().getServiceCache()
//BTM                        .getServiceItemByID(serviceID);
//BTM
//BTM                if (serviceItem == null) {
//BTM
//BTM                    // direct lookup.
//BTM                    serviceItem = fed.getServiceDiscoveryManager()
//BTM                            .lookup(
//BTM                                    new ServiceTemplate(
//BTM                                            serviceID,
//BTM                                            new Class[] { IRemoteExecutor.class }/* types */,
//BTM                                            null/* attr */),
//BTM                                    null/* filter */, 1000/* timeoutMillis */);
//BTM
//BTM                    if (serviceItem == null) {
//BTM
//BTM                        throw new RuntimeException(
//BTM                                "Could not discover service: " + serviceUUID);
//BTM
//BTM                    }
//BTM
//BTM                }
//BTM
//BTM            }
//BTM - BEGIN
if (serviceItem == null) {//no callable executor service, try shard service

    // test data service cache.
    serviceItem = fed.getDataServicesClient().getServiceCache()
                        .getServiceItemByID(serviceID);

    if (serviceItem == null) {//no shard service, try shard locator service

        // test shard locator service cache.
        serviceItem = fed.getShardLocatorClient().getServiceCache()
                        .getServiceItemByID(serviceID);

        if (serviceItem == null) {//no callable executor service, no shard service, no shard locator service, try direct lookup
            ServiceTemplate tmpl = new ServiceTemplate(serviceID, null, null); 
            serviceItem = sdm.lookup(tmpl, null, timeoutMillis);
            if (serviceItem == null) {
                throw new RuntimeException
                    ("Could not discover service ["+serviceUUID+"]");
            }
        }
    }
}
//BTM - END
            if (serviceItems == null) {

                /*
                 * Lazy initialization when de-serialized since field is
                 * transient and will not be initialized by the default
                 * de-serialization logic.
                 */

                serviceItems = new ServiceItem[ntasks];

            }

            serviceItems[i] = serviceItem;

        }

    }

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
     * Return the {@link UUID} of the service to which the Nth client was
     * assigned.
     * 
     * @param clientNum
     *            The client number in [0:N-1].
     *            
     * @return The {@link UUID} of the service on which that client should
     *         execute.
     */
    public UUID getServiceUUID(final int clientNum) {
        
        return serviceUUIDs[clientNum];
        
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
    public ServiceItem getServiceItem(final int clientNum) {
        
        return serviceItems[clientNum];
        
    }

}
