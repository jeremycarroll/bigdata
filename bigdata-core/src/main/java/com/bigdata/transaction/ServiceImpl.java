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

package com.bigdata.transaction;

import static com.bigdata.transaction.Constants.*;

import com.bigdata.attr.ServiceInfo;
import com.bigdata.journal.ValidationError;
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
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

/**
 * Backend implementation of the transaction service.
 */
class ServiceImpl implements PrivateInterface {

    private static Logger logger = LogUtil.getLog4jLogger
                                          ( (ServiceImpl.class).getName() ) ;
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

    private Thread waitThread;

    /* Constructor used by Service Starter Framework to start this service */
    public ServiceImpl(String[] args, LifeCycle lifeCycle) throws Exception
    {
        this.lifeCycle = lifeCycle;
        try {
            init(args);
        } catch(Throwable e) {
            Util.cleanupOnExit(innerProxy, serverExporter, joinMgr, ldm);
            Util.handleInitThrowable(e, logger);
        }
    }

    // Remote method(s) required by PrivateInterface


    public long newTx(long timestamp) throws RemoteException {
	readyState.check();
return 0L;//TODO
    }

    public long commit(long tx) throws ValidationError, RemoteException {
	readyState.check();
return 0L;//TODO
    }

    public void abort(long tx) throws RemoteException {
	readyState.check();
//TODO
    }

    public void notifyCommit(long commitTime) throws RemoteException {
	readyState.check();
//TODO
    }

    public long getLastCommitTime() throws RemoteException {
	readyState.check();
return 0L;//TODO
    }

    public long getReleaseTime() throws RemoteException {
	readyState.check();
return 0L;//TODO
    }

    public void declareResources(long tx, UUID shardService, String[] resource)
            throws RemoteException
    {
	readyState.check();
//TODO
    }

    public long prepared(long tx, UUID shardService) 
         throws RemoteException, InterruptedException, BrokenBarrierException
    {
	readyState.check();
return 0L;//TODO
    }

    public boolean committed(long tx, UUID shardService)
         throws RemoteException, InterruptedException, BrokenBarrierException
    {
	readyState.check();
return false;//TODO
    }

    public long nextTimestamp() throws RemoteException {
	readyState.check();
return 0L;//TODO
    }

    public void kill(int status) throws RemoteException {
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
	readyState.check();
	readyState.shutdown();
        shutdownDo();
    }

    //Required by JoinAdmin

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
        BootStateUtil bootStateUtil = 
           new BootStateUtil(config, COMPONENT_NAME, this.getClass(), logger);
        proxyId   = bootStateUtil.getProxyId();
        serviceId = bootStateUtil.getServiceId();

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

            Util.cleanupOnExit(innerProxy, serverExporter, joinMgr, ldm);

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
