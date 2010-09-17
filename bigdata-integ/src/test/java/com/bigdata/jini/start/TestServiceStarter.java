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
import org.junit.Test;

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
import com.bigdata.test.util.Assert;
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


    protected TestServiceStarter(boolean serviceImplRemote) {
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
    @Test
    public void test_startServer() throws ConfigurationException, Exception {
        final ZooKeeper zookeeper = fed.getZookeeper();

        TransactionServerConfiguration serviceConfig = null;
        if(serviceImplRemote) {
            serviceConfig = new TransactionServerConfiguration(TransactionServer.class, config);
        } else {
            serviceConfig = new TransactionServerConfiguration(com.bigdata.transaction.ServiceImpl.class, config);
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
        
        zookeeper.create(parentZNode, SerializerUtil.serialize(serviceStarter.serviceUUID), acl, CreateMode.PERSISTENT);

        /*
         * Create the znode for the election of the primary physical service for
         * this logical service (direct child of the logicalSevice znode).
         */
        zookeeper.create(logicalServiceZPath + "/" + BigdataZooDefs.MASTER_ELECTION, new byte[0], acl, CreateMode.PERSISTENT);

        // will be zero unless we started a zookeeper server above.
        final int processCountBefore = listener.running.size();
        
        // start the service.
        final ProcessHelper processHelper = serviceStarter.call();

        // verify listener was notified of service start.
        Assert.assertEquals(processCountBefore + 1, listener.running.size());

        // verify that the physicalService was registered with zookeeper. 
        final ServiceItem serviceItem;
        IService proxy = null;
        Service smartProxy = null;
        final String physicalServiceZPath;
        {           
            final List<String> children = zookeeper.getChildren(logicalServiceZPath, false/* watch */);

            System.err.println("physicalServices=" + children);
            
            // will fail if the znode was not registered.
            Assert.assertEquals(2, children.size());

            /*
             * There should be only one child, which is the physical service
             * that we created.
             * 
             * Note: You could explicitly build the correct zpath using the
             * serviceUUID obtained from the service proxy.
             */
            physicalServiceZPath = logicalServiceZPath + "/" + children.get(0);

            // get the serviceUUID from the physicalServiceZNode's data.
            final UUID serviceUUID = (UUID) SerializerUtil.deserialize(zookeeper.getData(physicalServiceZPath, false, new Stat()));
            
            serviceItem = discoverService(serviceUUID);

            // verify that the service item is registered with jini. 
            Assert.assertNotNull(serviceItem);
            
            // save reference to the service proxy.
            if(serviceItem.service instanceof IService) {
                proxy = (IService)serviceItem.service;
            } else if(serviceItem.service instanceof Service) {
                smartProxy = (Service)serviceItem.service;
            } else {
                Assert.fail("service not an instance of either Service or IService");
            }
        }

        // Verify the service UUID using the proxy
        if(proxy == null) {
            Assert.assertEquals(JiniUtil.serviceID2UUID(serviceItem.serviceID), smartProxy.getServiceUUID());
        } else {
            Assert.assertEquals(JiniUtil.serviceID2UUID(serviceItem.serviceID), proxy.getServiceUUID());
        }

        // Verify the service name using the proxy
        // (Note: only do this for the remote case,
        //  names will be different for smart proxy case)
        if(proxy != null) {
            Assert.assertEquals(serviceStarter.serviceName, proxy.getServiceName());
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


        // wait a bit for the process to die.
        processHelper.exitValue(10L, TimeUnit.SECONDS);
        
        // verify that it has been removed from our listener.
        Assert.assertEquals("Expected " + processCountBefore + ", but #running="
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
        if (!ZNodeDeletedWatcher.awaitDelete(zookeeper, physicalServiceZPath, 20000, TimeUnit.MILLISECONDS)) {
            Assert.fail("znode not removed: zpath=" + physicalServiceZPath);
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
            serviceDiscoveryManager = new ServiceDiscoveryManager(fed.getDiscoveryManagement(), new LeaseRenewalManager());
            final ServiceItem item = serviceDiscoveryManager.lookup(new ServiceTemplate(serviceId, null, null), null);
            return item;           
        } finally {
            if (serviceDiscoveryManager != null) {
                serviceDiscoveryManager.terminate();               
            }
        }
    }   
}
