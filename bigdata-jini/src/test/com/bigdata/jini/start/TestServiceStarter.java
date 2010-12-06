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
 * Created on Jan 5, 2009
 */

package com.bigdata.jini.start;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.BigdataServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.TransactionServerConfiguration;
import com.bigdata.jini.start.config.ManagedServiceConfiguration.ManagedServiceStarter;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.service.IService;
import com.bigdata.service.Service;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.zookeeper.ZNodeDeletedWatcher;
import com.bigdata.zookeeper.ZooHelper;

/**
 * Test suite for starting a bigdata service based on a
 * {@link ServiceConfiguration} stored in {@link ZooKeeper}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestServiceStarter extends AbstractFedZooTestCase {

    protected boolean serviceImplRemote;

    /**
     * 
     */
    public TestServiceStarter() {
        this.serviceImplRemote = false;
        
    }

    /**
     * @param arg0
     */
    public TestServiceStarter(String arg0) {

        super(arg0);
        this.serviceImplRemote = false;
        
    }

    public TestServiceStarter(boolean serviceImplRemote) {
        this.serviceImplRemote = serviceImplRemote;
    }

    public TestServiceStarter(String arg0, boolean serviceImplRemote) {
        super(arg0);
        this.serviceImplRemote = serviceImplRemote;
    }

    /**
     * Unit test verifies that we can start and destroy a service instance using
     * a {@link BigdataServiceConfiguration}. The test waits until the service
     * has been assigned its serviceId by jini and verify that the serviceId is
     * recorded in the physicalService znode.
     * 
     * @throws ConfigurationException
     * @throws Exception
     */
    public void test_startServer() throws ConfigurationException, Exception {

//        // create a unique fake zroot
//        final String zroot = createTestZRoot();
//
//        // the config for that fake zroot.
//        final String zconfig = zroot + BigdataZooDefs.ZSLASH
//                + BigdataZooDefs.CONFIG;

        final ZooKeeper zookeeper = fed.getZookeeper();

        TransactionServerConfiguration serviceConfig = null;
        if(serviceImplRemote) {
            serviceConfig = new TransactionServerConfiguration
                                    (TransactionServer.class, config);
        } else {
            serviceConfig = 
                new TransactionServerConfiguration
                        (com.bigdata.transaction.ServiceImpl.class, config);
        }

        // znode for serviceConfiguration
        final String zserviceConfig = zookeeper.create(fed.getZooConfig().zroot
                + BigdataZooDefs.ZSLASH + BigdataZooDefs.CONFIG
                + BigdataZooDefs.ZSLASH
                + TransactionServer.class.getName(), SerializerUtil
                .serialize(serviceConfig), acl, CreateMode.PERSISTENT);

        /*
         * znode for a logical service (the logical service is either a
         * collection of peers or a service failover chain, depending on the
         * type of the service). Logical services are persistent. Each one is
         * assigned a unique (sequential) identifier by zookeeper. It is also
         * assigned a random UUID.
         */
        final String logicalServiceZPath = zookeeper.create(zserviceConfig
                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX, SerializerUtil.serialize(UUID
                .randomUUID()), acl, CreateMode.PERSISTENT_SEQUENTIAL);

        /*
         * Create the znode that is the parent for the physical service
         * instances (direct child of the logicalSevice znode).
         */
        final String parentZNode = logicalServiceZPath + "/" + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER;
        final ManagedServiceStarter serviceStarter = 
            (ManagedServiceStarter) serviceConfig.newServiceStarter
                                        (fed, listener, logicalServiceZPath, null/* attributes */);
        zookeeper.create(parentZNode, 
                         SerializerUtil.serialize(serviceStarter.serviceUUID),
                         acl, CreateMode.PERSISTENT);

        /*
         * Create the znode for the election of the primary physical service for
         * this logical service (direct child of the logicalSevice znode).
         */
        zookeeper.create(logicalServiceZPath + "/"
                + BigdataZooDefs.MASTER_ELECTION, new byte[0], acl,
                CreateMode.PERSISTENT);

        // will be zero unless we started a zookeeper server above.
        final int processCountBefore = listener.running.size();
        
        // start the service.
        final ProcessHelper processHelper = serviceStarter.call();

        // verify listener was notified of service start.
        assertEquals(processCountBefore + 1, listener.running.size());

        // verify that the physicalService was registered with zookeeper. 
        final ServiceItem serviceItem;
        IService proxy = null;
        Service smartProxy = null;
//BTM (12/06/2010)        final String physicalServiceZPath;
String physicalServiceZPath = null;
        {
            
            final List<String> children = zookeeper.getChildren(
                    logicalServiceZPath, false/* watch */);

            System.err.println("physicalServices=" + children);
            
            // will fail if the znode was not registered.
            assertEquals(2, children.size());

            /*
             * There should be only one child, which is the physical service
             * that we created.
             * 
             * Note: You could explicitly build the correct zpath using the
             * serviceUUID obtained from the service proxy.
             */
//BTM - BEGIN (12/06/2010) ----------------------------------------------
//BTM   The note above appears to be no longer true. That is, 
//BTM   at some point, the tests or infrastructure were changed
//BTM   in such a way that there are now 2 children instead of
//BTM   the 1 child the note above says is expected. Currently,
//BTM   the children are masterElection and physicalServices,
//BTM   but the code below that retrieves the serviceUUID from
//BTM   zookeeper must retrieve it from the physicalServices
//BTM   children, not the masterElection child. The original
//BTM   code invoked children.get(0), assuming either there
//BTM   was only 1 child or assuming that the physicalServices
//BTM   child would always be placed at index 0 of the
//BTM   children list above zookeeper.getChildren() was called.
//BTM   Unfortunately, this is not alway the case. And when/if
//BTM   an attempt is made to deserialize the serviceUUID using
//BTM   the masterElection child, an EOF exception is encountered
//BTM   because serviceUUID information was never stored under
//BTM   the masterElection znode. To address this then, the
//BTM   code below was changed to loop through the children
//BTM   list, catching the exception and exiting when a
//BTM   a valid serviceUUID is successfully deserialized.
//BTM
//BTM            physicalServiceZPath = logicalServiceZPath + "/"
//BTM                    + children.get(0);
//BTM            // get the serviceUUID from the physicalServiceZNode's data.
//BTM            final UUID serviceUUID = (UUID) SerializerUtil
//BTM                    .deserialize(zookeeper.getData(physicalServiceZPath,
//BTM                            false/* watch */, new Stat()));
//BTM
            UUID serviceUUID = null;
            for (String child : children) {
                physicalServiceZPath = logicalServiceZPath+"/"+child;
                try {
                    serviceUUID =
                        (UUID) SerializerUtil.deserialize
                                  ( zookeeper.getData(physicalServiceZPath,
                                                      false,//watch
                                                      new Stat()) );
                    break;
                } catch(Exception e) {//swallow
                }
            }
            assertTrue( "failed to deserialize serviceUUID "
                        +"[children="+children+"]",
                        (serviceUUID != null) );
//BTM - END --------------------------------------------------------------
            serviceItem = discoverService(serviceUUID);

            // verify that the service item is registered with jini. 
            assertNotNull(serviceItem);
            
            // save reference to the service proxy.
            if(serviceItem.service instanceof IService) {
                proxy = (IService)serviceItem.service;
            } else if(serviceItem.service instanceof Service) {
                smartProxy = (Service)serviceItem.service;
            } else {
                fail("service not an instance of either Service or IService");
            }
        }

        // Verify the service UUID using the proxy
        if(proxy == null) {
            assertEquals(JiniUtil.serviceID2UUID(serviceItem.serviceID), smartProxy.getServiceUUID());
        } else {
            assertEquals(JiniUtil.serviceID2UUID(serviceItem.serviceID), proxy.getServiceUUID());
        }

        // Verify the service name using the proxy
        // (Note: only do this for the remote case,
        //  names will be different for smart proxy case)
        if(proxy != null) {
            assertEquals(serviceStarter.serviceName, proxy.getServiceName());
        }

        // Tell the service to destroy itself.
        if(proxy == null) {
            try {
                ((com.sun.jini.admin.DestroyAdmin)(((net.jini.admin.Administrable)smartProxy).getAdmin())).destroy();
            } catch(Throwable t) { 
                System.out.println("TestServiceStarter: SHUTDOWN WARNING ["+t+"]");
                t.printStackTrace();
            }
        } else {
            ((RemoteDestroyAdmin)proxy).destroy();
        }
//        listener.running.get(0).destroy();

        // wait a bit for the process to die.
        processHelper.exitValue(10L, TimeUnit.SECONDS);
        
        // verify that it has been removed from our listener.
        assertEquals("Expected " + processCountBefore + ", but #running="
                + listener.running.size() + ", processes="
                + listener.running.toString(), processCountBefore,
                listener.running.size());

        ZooHelper.destroyZNodes(zookeeper, parentZNode, 1);

        /*
         * Wait until the znode for the physical service has been removed.
         * 
         * Note: An ephemeral znode will be removed once the zookeeper client
         * either times out or is explicitly closed. Since we are killing the
         * process rather than terminating the service normally we may have to
         * raise the timeout before zookeeper will delete the service's znode on
         * its behalf.
         */
        if (!ZNodeDeletedWatcher.awaitDelete(zookeeper, physicalServiceZPath,
                20000, TimeUnit.MILLISECONDS)) {

            fail("znode not removed: zpath=" + physicalServiceZPath);

        }

    }

    /**
     * Looks up the service item in any joined jini registrars but does not wait
     * for the service item to become registered.
     * 
     * @param serviceUUID
     * 
     * @return
     * 
     * @throws IOException
     */
    protected ServiceItem discoverService(final UUID serviceUUID) throws IOException {
        
        final ServiceID serviceId = JiniUtil.uuid2ServiceID(serviceUUID);
        
        ServiceDiscoveryManager serviceDiscoveryManager = null;
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(fed
                    .getDiscoveryManagement(), new LeaseRenewalManager());

            final ServiceItem item = serviceDiscoveryManager
                    .lookup(new ServiceTemplate(serviceId, null/* iface[] */,
                            null/* entry[] */),//
                            null // filter
                    );

            return item;
            
        } finally {

            if (serviceDiscoveryManager != null) {

                serviceDiscoveryManager.terminate();
                
            }

        }

    }
    
}
