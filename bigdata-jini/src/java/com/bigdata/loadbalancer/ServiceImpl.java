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

package com.bigdata.loadbalancer;

import static com.bigdata.loadbalancer.Constants.*;

import com.bigdata.attr.ServiceInfo;
import com.bigdata.service.Event;
import com.bigdata.util.BootStateUtil;
import com.bigdata.util.Util;
import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.config.NicUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.config.Config;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.thread.ReadyState;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;

import net.jini.core.entry.Entry;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lease.Lease;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;

import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.discovery.LookupDiscoveryManager;

import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.ServerEndpoint;
import net.jini.jeri.tcp.TcpServerEndpoint;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceDiscoveryManager;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Backend implementation of the load balancer service.
 *
 * Note: this class is currently declared public rather than the preferred
 *       package protected scope. This is so that the JiniServicesHelper
 *       utility can instantiate this class in the tests that are currently
 *       implemented to interact directly with the service's backend;
 *       as opposed to starting the service with the ServiceStarter and
 *       then interacting with the service through the discovered service
 *       frontend.
 */
public 
class ServiceImpl implements PrivateInterface {

    private static Logger logger = 
        LogUtil.getLog4jLogger(COMPONENT_NAME);
    private static String shutdownStr;
    private static String killStr;

    private final LifeCycle lifeCycle;//for Jini ServiceStarter framework
    private final ReadyState readyState = new ReadyState();//access when ready

    private Configuration config;
    private UUID proxyId = null;
    private ServiceID serviceId = null;

    private Exporter serverExporter;
    private ServiceProxy outerProxy;// outer (smart) proxy to this server
    private PrivateInterface innerProxy;// stub or dynamic proxy to this server
    private AdminProxy adminProxy;

    /* For this service's join state */
    private String[] groupsToJoin = DiscoveryGroupManagement.NO_GROUPS;
    private LookupLocator[] locatorsToJoin = new LookupLocator[0];
    private DiscoveryManagement ldm;
    private JoinManager joinMgr;
    private ServiceDiscoveryManager sdm;

    private EmbeddedLoadBalancer embeddedLoadBalancer;

    private Thread waitThread;

// ************************************************************************************************************
private static UUID smsProxyId = null;
private static ServiceImpl thisImpl;

//To start service using the old bigdata ServicesManagerService mechanism or from the command line or tests without the Jini ServiceStarter
public static void main(String[] args) {
//String str1 = "/raglan/service/com.bigdata.jini.start.IServicesManagerService/61e99f71-a6f7-4a61-a330-ce7a04e6a0fb/Thread Pool/Service Time";
//String str2 = "/raglan/service/com.bigdata.jini.start.IServicesManagerService/61e99f71-a6f7-4a61-a330-ce7a04e6a0fb";
//String str3 = "/raglan/service/com.bigdata.jini.start.IServicesManagerService/";
//System.out.println("\n\n SSSSS >>>>> "+str1+ "str1.contains(//) >>>> "+str1.contains("//"));
//System.out.println("\n\n SSSSS >>>>> "+str2+ "str1.contains(//) >>>> "+str2.contains("//"));
//System.out.println("\n\n SSSSS >>>>> "+str3+ "str1.contains(//) >>>> "+str3.contains("//"));
    try {
        ArrayList<String> argsList = new ArrayList<String>();

        int begIndx = 0;
        String configFile = System.getProperty("config");
        if(configFile != null) {
            // Replace args[1] with config file location
            argsList.add(configFile);
            begIndx = 1;
        }
        for(int i=begIndx; i<args.length; i++) {
            argsList.add(args[i]);
        }
        // ServicesManagerService waits on the discovery of a service
        // of this type having the serviceId assigned in the cluster
        // config file
        if(System.getProperty("usingServicesManagerService") != null) {
            Configuration smsConfig = 
                ConfigurationProvider.getInstance
                    ( args, (ServiceImpl.class).getClassLoader() );

            Entry[] smsEntries = 
                (Entry[])smsConfig.getEntry
                    ("com.bigdata.service.jini.JiniClient",
                     "entries",
                     net.jini.core.entry.Entry[].class,
                     null);
            if(smsEntries != null) {
                //see JiniServiceConfiguration.getEntries
                smsProxyId = 
                ((com.bigdata.jini.lookup.entry.ServiceUUID)smsEntries[3]).serviceUUID;
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: smsProxyId = "+smsProxyId);
            }
            String logicalServiceZPath = 
                (String)smsConfig.getEntry
                    ((ServiceImpl.class).getName(),
                     "logicalServiceZPath",
                     String.class,
                     null);
            String physicalServiceZPath = null;
            if(logicalServiceZPath != null) {
                physicalServiceZPath = logicalServiceZPath
                                       +com.bigdata.jini.start.BigdataZooDefs.ZSLASH 
                                       +com.bigdata.jini.start.BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER 
                                       +com.bigdata.jini.start.BigdataZooDefs.ZSLASH 
                                       +smsProxyId;
            }
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: logicalServiceZPath = "+logicalServiceZPath);
            if(physicalServiceZPath != null) {
                org.apache.zookeeper.data.ACL[] acl =
                (org.apache.zookeeper.data.ACL[])smsConfig.getEntry
                    ("org.apache.zookeeper.ZooKeeper", "acl",
                     org.apache.zookeeper.data.ACL[].class, null);
                if(acl != null) {
                    java.util.List<org.apache.zookeeper.data.ACL> aclList =
                    java.util.Arrays.asList(acl);
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: aclList = "+aclList);
                   String servers = 
                       (String)smsConfig.getEntry
                           ("org.apache.zookeeper.ZooKeeper",
                            "servers", String.class, null);
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: servers = "+servers);
                    if(servers != null) {
                        int sessionTimeout = 
                        (Integer)smsConfig.getEntry
                           ("org.apache.zookeeper.ZooKeeper",
                            "sessionTimeout", int.class, 300000);
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: sessionTimeout = "+sessionTimeout);

                        byte[] data = com.bigdata.io.SerializerUtil.serialize(smsEntries);
                        org.apache.zookeeper.ZooKeeper zookeeperClient =
                        new org.apache.zookeeper.ZooKeeper(servers, sessionTimeout, null);
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: ZOOKEEPER CLIENT CREATED ***");
                        try {
                            zookeeperClient.create(physicalServiceZPath, data, aclList,
                                                   org.apache.zookeeper.CreateMode.PERSISTENT);
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: ZNODE CREATED ["+physicalServiceZPath+"] ***");
                        } catch(org.apache.zookeeper.KeeperException.NodeExistsException e) {
                            zookeeperClient.setData(physicalServiceZPath, data, -1);
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: ZNODE UPDATED ["+physicalServiceZPath+"] ***");
                        }
                    }
}else{
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: acl = "+acl);
                }
            }
        }
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: new ServiceImpl() ....");
        thisImpl = new ServiceImpl
                ( argsList.toArray(new String[argsList.size()]),
                  new com.bigdata.service.jini.FakeLifeCycle() );
    } catch(Throwable t) {
        logger.log(Level.WARN, "failed to start load balancer service", t);
// System.err.println("FAILED TO START LOAD BALANCER ["+t+"]");
// t.printStackTrace();
    }
}
// ************************************************************************************************************


    /* Constructor used by Service Starter Framework to start this service */
    public ServiceImpl(String[] args, LifeCycle lifeCycle) throws Exception {
logger.log(Level.DEBUG, "XXXXX LOAD BALANCER ServiceImpl: constructor");
System.out.println("\nXXXXX LOAD BALANCER ServiceImpl: constructor");
if(args == null) {
    System.out.println("XXXXX LOAD BALANCER ServiceImpl: args = NULL ****");
} else {
    System.out.println("XXXXX LOAD BALANCER ServiceImpl: args.length = "+args.length);
    for(int i=0; i<args.length; i++) {
        System.out.println("XXXXX LOAD BALANCER ServiceImpl: arg["+i+"] = "+args[i]);
    }
}
        this.lifeCycle = lifeCycle;
        try {
            init(args);
        } catch(Throwable e) {
            Util.cleanupOnExit(innerProxy,serverExporter,joinMgr,sdm,ldm);
            Util.handleInitThrowable(e, logger);
        }
    }

    // Remote method(s) required by PrivateInterface

    public void notify(UUID serviceId, byte[] data) 
                    throws RemoteException, IOException
    {
logger.log(Level.DEBUG, ">>>>> **** com.bigdata.loadbalancer.ServiceImpl.notify: CALLING embeddedLoadBalancer.notify\n");
	readyState.check();
        embeddedLoadBalancer.notify(serviceId, data);
    }

    public void warn(String msg, UUID serviceId) throws RemoteException {
	readyState.check();
        embeddedLoadBalancer.warn(msg, serviceId);
    }

    public void urgent(String msg, UUID serviceId) throws RemoteException {
	readyState.check();
        embeddedLoadBalancer.urgent(msg, serviceId);
    }

    public UUID getUnderUtilizedDataService()
        throws RemoteException, IOException,
               TimeoutException, InterruptedException
    {
	readyState.check();
        return embeddedLoadBalancer.getUnderUtilizedDataService();
    }

    public UUID[] getUnderUtilizedDataServices
        (int minCount, int maxCount, UUID exclude)
            throws RemoteException, IOException,
                   TimeoutException, InterruptedException
    {
	readyState.check();
        return embeddedLoadBalancer.getUnderUtilizedDataServices
                                        (minCount, maxCount, exclude);
    }

    public boolean isHighlyUtilizedDataService(UUID serviceId) 
                       throws RemoteException, IOException
    {
	readyState.check();
        return embeddedLoadBalancer.isHighlyUtilizedDataService(serviceId);
    }

    public boolean isUnderUtilizedDataService(UUID serviceId)
                       throws RemoteException, IOException
    {
	readyState.check();
        return embeddedLoadBalancer.isUnderUtilizedDataService(serviceId);
    }

    public void notifyEvent(Event e) throws RemoteException, IOException {
	readyState.check();
        embeddedLoadBalancer.notifyEvent(e);
    }

    public void shutdown() throws RemoteException {
	readyState.check();
	readyState.shutdown();
        shutdownDo();
    }

    public void shutdownNow() throws RemoteException {
        this.shutdown();
    }

    public void kill(int status) throws RemoteException {
	readyState.check();
	readyState.shutdown();
        killDo(status);
    }

    // Required by Administrable

    public Object getAdmin() throws RemoteException {
	readyState.check();
        return adminProxy;
    }

    // Required by DestroyAdmin

    public void destroy() throws RemoteException {
        this.shutdown();
    }

    // Required by JoinAdmin

    public Entry[] getLookupAttributes() throws RemoteException {
	readyState.check();
	return joinMgr.getAttributes();
    }

    public void addLookupAttributes(Entry[] attrSets) throws RemoteException {
	readyState.check();
        joinMgr.addAttributes(attrSets, true);
    }

    public void modifyLookupAttributes(Entry[] attrSetTemplates,
				       Entry[] attrSets)
	throws RemoteException
    {
	readyState.check();
        joinMgr.modifyAttributes(attrSetTemplates, attrSets, true);
    }

    public String[] getLookupGroups() throws RemoteException {
	readyState.check();
	return groupsToJoin;
    }

    public void addLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
        try {
            ((DiscoveryGroupManagement)ldm).addGroups(groups);
        } catch(IOException e) {
            throw new RuntimeException(e.toString());
        }
    }

    public void removeLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
	((DiscoveryGroupManagement)ldm).removeGroups(groups);
    }

    public void setLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
        try {
            ((DiscoveryGroupManagement)ldm).setGroups(groups);
        } catch(IOException e) {
            throw new RuntimeException(e.toString());
        }
    }

    public LookupLocator[] getLookupLocators() throws RemoteException {
	readyState.check();
	return locatorsToJoin;
    }

    public void addLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).addLocators(locators);
    }

    public void removeLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).removeLocators(locators);
    }

    public void setLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).setLocators(locators);
    }

    //Initialize the service from the config
    private void init(String[] args) throws Exception {
        config = ConfigurationProvider.getInstance
                                       ( args,
                                         (this.getClass()).getClassLoader() );
if(smsProxyId == null) {//service assigns its own proxyId
logger.warn("XXXXX LOAD BALANCER ServiceImpl: smsProxyId = null ---> service-assigned id");
        BootStateUtil bootStateUtil = 
           new BootStateUtil(config, COMPONENT_NAME, this.getClass(), logger);
        proxyId   = bootStateUtil.getProxyId();
        serviceId = bootStateUtil.getServiceId();
} else {//ServicesManagerService assigned the proxyId
logger.warn("XXXXX LOAD BALANCER ServiceImpl: smsProxyId NOT null ---> SMS-assigned id");
    proxyId = smsProxyId;
    serviceId = com.bigdata.jini.util.JiniUtil.uuid2ServiceID(proxyId);
}
logger.warn("XXXXX LOAD BALANCER ServiceImpl: proxyId = "+proxyId+"\n");

        //Service export and proxy creation
        ServerEndpoint endpoint = TcpServerEndpoint.getInstance(0);
        InvocationLayerFactory ilFactory = new BasicILFactory();
        Exporter defaultExporter = new BasicJeriExporter(endpoint,
                                                         ilFactory,
                                                         false,
                                                         true);
        //Get the exporter that will be used to export this service
        serverExporter = (Exporter)config.getEntry(COMPONENT_NAME,
                                                   "serverExporter",
                                                   Exporter.class,
                                                   defaultExporter);
        if(serverExporter == null) {
            throw new ConfigurationException("null serverExporter");
        }

        innerProxy = (PrivateInterface)serverExporter.export(this);
        String hostname = NicUtil.getIpAddress("default.nic", ConfigDeployUtil.getString("node.serviceNetwork"), false);
        outerProxy = ServiceProxy.createProxy
                         (innerProxy, proxyId, hostname);
        adminProxy = AdminProxy.createProxy(innerProxy, proxyId);

        //Setup lookup discovery
        this.ldm = Util.getDiscoveryManager(config, COMPONENT_NAME);
        groupsToJoin   = ((DiscoveryGroupManagement)ldm).getGroups();
        locatorsToJoin = ((DiscoveryLocatorManagement)ldm).getLocators();

        //For the service attribute(s)
        ArrayList<Entry> serviceAttrsList = new ArrayList<Entry>();

        ServiceInfo serviceInfo = 
            ConfigDeployUtil.initServiceInfo(proxyId, SERVICE_NAME);

        serviceAttrsList.add(serviceInfo);

        //array of attributes for JoinManager
        Entry[] serviceAttrs = serviceAttrsList.toArray
                                       (new Entry[serviceAttrsList.size()]);

        //Empty properties object for the EmbeddedLoadBalancer
        Properties props = new Properties();
        this.sdm = new ServiceDiscoveryManager(ldm, null, config);
        embeddedLoadBalancer = 
            new EmbeddedLoadBalancer
                    (proxyId, hostname, sdm,
                     (String)config.getEntry(COMPONENT_NAME,
                                             "persistenceDirectory",
                                             String.class, "."),
null,//BTM*** - remove uuid map when DataService converted to smart proxy?
                     props);

        //advertise this service
        joinMgr = new JoinManager(outerProxy, serviceAttrs, serviceId, ldm,
                                  null, config);

        String srvcIdStr = (ServiceImpl.this.serviceId).toString();
        String startStr = SERVICE_NAME+" started: [id="+srvcIdStr+"]";
        shutdownStr = SERVICE_NAME+" shutdown: [id="+srvcIdStr+"]";
        killStr = SERVICE_NAME+" killed: [id="+srvcIdStr+"]";

        logger.log(Level.INFO, startStr);
        logger.log(Level.INFO,
                   "groups="
                   +Util.writeGroupArrayToString(groupsToJoin)
                   +", locators="
                   +Util.writeArrayElementsToString(locatorsToJoin));

        waitThread = new Util.WaitOnInterruptThread(logger);
        waitThread.start();

        readyState.ready();//ready to accept calls from clients
    }

    private void shutdownDo() {
        (new ShutdownThread()).start();
    }

    /**
     * Used to shutdown the service asynchronously.
     */
    private class ShutdownThread extends Thread {

        private long EXECUTOR_TERMINATION_TIMEOUT = 1L*60L*1000L;

        public ShutdownThread() {
            super("Build Server Request Service Shutdown thread");
            setDaemon(false);
        }

        public void run() {

            embeddedLoadBalancer.shutdown();

            //Before terminating the discovery manager, retrieve the
            // current groups and locs; which may have been administratively
            // changed between the time this service was started and now.
            // This information will be logged on exit below.
            String[] groups = ((DiscoveryGroupManagement)ldm).getGroups();
            LookupLocator[] locs = 
                          ((DiscoveryLocatorManagement)ldm).getLocators();

            if( Util.unexportRemoteObject(serverExporter) ) {
                innerProxy = null;
                serverExporter = null;
            }

            waitThread.interrupt();
            try {
                waitThread.join();
            } catch (InterruptedException e) {/*exiting, so swallow*/}

            Util.cleanupOnExit(innerProxy,serverExporter,joinMgr,sdm,ldm);

            // Tell the ServiceStarter framework it's ok to release for gc
            if(lifeCycle != null)  {
                logger.log(Level.DEBUG,"Unregistering the service from "
                           +"the ServiceStarter framework ...");
                lifeCycle.unregister(ServiceImpl.this);
            }
            logger.log(Level.INFO, shutdownStr+" [groups="
                       +Util.writeGroupArrayToString(groupsToJoin)
                       +", locators="
                       +Util.writeArrayElementsToString(locatorsToJoin)+"]");
        }
    }

    private void killDo(int status) {
        String[] groups = ((DiscoveryGroupManagement)ldm).getGroups();
        LookupLocator[] locs = ((DiscoveryLocatorManagement)ldm).getLocators();
        logger.log(Level.INFO, killStr+" [groups="
                   +Util.writeGroupArrayToString(groupsToJoin)
                   +", locators="
                   +Util.writeArrayElementsToString(locatorsToJoin)+"]");

        System.exit(status);
    }
}
