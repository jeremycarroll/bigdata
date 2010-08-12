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
package com.bigdata.process;

import static com.bigdata.process.Constants.*;

import com.bigdata.attr.ServiceInfo;
import com.bigdata.boot.BootManager;
import com.bigdata.boot.ProcessEventListener;
import com.bigdata.boot.ProcessState;
import com.bigdata.boot.ProcessStateChangeEvent;
import com.bigdata.util.BootStateUtil;
import com.bigdata.util.Format;
import com.bigdata.util.Util;
import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.config.NicUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.start.LifeCycle;
import com.sun.jini.thread.ReadyState;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;
import net.jini.config.ConfigurationException;

import net.jini.core.entry.Entry;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;

import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;

import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.ServerEndpoint;
import net.jini.jeri.tcp.TcpServerEndpoint;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceDiscoveryManager;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * Backend implementation of the data cloud build server request processor.
 */
class ServiceImpl implements PrivateInterface, 
                             Environment, ProcessEventListener
{
    private static Logger logger = LogUtil.getLog4jLogger
                                          ( (ServiceImpl.class).getName() );
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

    // Sorted map whose elements consist of a process tag mapped to
    // a ProcessManagement object.
    private final Map<String, ProcessManagement> processes =
                  new TreeMap<String, ProcessManagement>();

    // Sorted map whose elements consist of a of role name mapped to 
    //  RoleInfo object.
    private final Map<ProcessInfo, RoleInfo> roles =
                  new TreeMap<ProcessInfo, RoleInfo>();

    // For communicating with the boot launcher on the current node.
    private BootManager bootMgr;

    // For executing background tasks.
    private ExecutorService executor;

    // For executing delayed background tasks.
    private ScheduledExecutorService scheduledExecutor;

    // For scheduling and running the state machines of processes.
    private ProcessStateRunner runner;

    // Global process synchronization object.
    private final Object gpSync = new Object();

    /* Constructor used by Service Starter Framework to start this service */
    public ServiceImpl(String[] args, LifeCycle lifeCycle) throws Exception {
        this.lifeCycle = lifeCycle;
        try {
            init(args);
        } catch(Throwable e) {
            cleanupOnExit();
            if(e instanceof InitializationException) {
                logger.log(Level.FATAL, "initialization failure ... ", e);
                throw (InitializationException)e;
            }
            Util.handleInitThrowable(e, logger);
        }
    }

    // Required by PrivateInterface

    @Override
    public void kill(int status) throws RemoteException {
	readyState.shutdown();
        killDo(status);
    }

    // Required by Administrable

    @Override
    public Object getAdmin() throws RemoteException {
	readyState.check();
        return adminProxy;
    }

    // Required by DestroyAdmin

    @Override
    public void destroy() throws RemoteException {
	readyState.check();
	readyState.shutdown();
        shutdownDo();
    }

    // Required by JoinAdmin

    @Override
    public Entry[] getLookupAttributes() throws RemoteException {
	readyState.check();
	return joinMgr.getAttributes();
    }

    @Override
    public void addLookupAttributes(Entry[] attrSets) throws RemoteException {
	readyState.check();
        joinMgr.addAttributes(attrSets, true);
    }

    @Override
    public void modifyLookupAttributes(Entry[] attrSetTemplates,
				       Entry[] attrSets)
	throws RemoteException
    {
	readyState.check();
        joinMgr.modifyAttributes(attrSetTemplates, attrSets, true);
    }

    @Override
    public String[] getLookupGroups() throws RemoteException {
	readyState.check();
	return groupsToJoin;
    }

    @Override
    public void addLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
        try {
            ((DiscoveryGroupManagement)ldm).addGroups(groups);
        } catch(IOException e) {
            throw new RuntimeException(e.toString());
        }
    }

    @Override
    public void removeLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
	((DiscoveryGroupManagement)ldm).removeGroups(groups);
    }

    @Override
    public void setLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
        try {
            ((DiscoveryGroupManagement)ldm).setGroups(groups);
        } catch(IOException e) {
            throw new RuntimeException(e.toString());
        }
    }

    @Override
    public LookupLocator[] getLookupLocators() throws RemoteException {
	readyState.check();
	return locatorsToJoin;
    }

    @Override
    public void addLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).addLocators(locators);
    }

    @Override
    public void removeLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).removeLocators(locators);
    }

    @Override
    public void setLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).setLocators(locators);
    }

    // Required by Evironment

    @Override
    public ProcessManagement getProcessInfo(String pTag) {
        return processes.get(pTag);
    }

    @Override
    public void startProcess(String pTag) throws IOException {
        if(bootMgr != null) bootMgr.startProcess(pTag);
    }

    @Override
    public void stopProcess(String pTag) throws IOException {
        if(bootMgr != null) bootMgr.stopProcess(pTag);
    }

    @Override
    public ProcessState getProcessState(String pTag) throws IOException {
        if(bootMgr == null) return null;
        return bootMgr.getState(pTag);
    }

    @Override
    public void schedule(ProcessManagement pInfo) {
        if(runner != null) runner.schedule(pInfo);
    }

    @Override
    public Future<?> schedule(Runnable task) {
        if(executor == null) return null;
        return executor.submit(task);
    }

    @Override
    public Future<?> delayedSchedule(Runnable task, long delay) {
        if(scheduledExecutor == null) return null;
        return scheduledExecutor.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendProcessCrashEvent(String pTag, boolean willRestart) {

        //TODO: define and send an instance of ProcessCrashEvent when
        //      a system-wide event mechanism is defined
    }

    // Required by ProcessEventListener

    @Override
    public void processStateChangeEvent(ProcessStateChangeEvent event) {
        ProcessManagement pInfo = processes.get(event.tag);
        if(pInfo != null) {
            synchronized(gpSync) {
                pInfo.onBootStateChange(event.currState);
            }
        } else {
            logger.log( Level.INFO, new Format("ignoring state change for "
                                               +"process [{0}]", event.tag) );
        }
    }

    //Initialize the service from the config
    private void init(String[] args) throws IOException,
                                            ConfigurationException,
                                            ClassNotFoundException,
                                            InitializationException
    {
        config = ConfigurationProvider.getInstance
                                       ( args,
                                         (this.getClass()).getClassLoader() );
        BootStateUtil bootStateUtil = 
           new BootStateUtil(config, COMPONENT_NAME, this.getClass(), logger);
        proxyId   = bootStateUtil.getProxyId();
        serviceId = bootStateUtil.getServiceId();

        readConfiguration();

        this.bootMgr = new BootManager();

        this.executor = Executors.newCachedThreadPool();
        this.scheduledExecutor = Executors.newScheduledThreadPool(1);

        // Service export and proxy creation
        ServerEndpoint endpoint = TcpServerEndpoint.getInstance(0);
        InvocationLayerFactory ilFactory = new BasicILFactory();
        Exporter defaultExporter = new BasicJeriExporter(endpoint,
                                                         ilFactory,
                                                         false,
                                                         true);
        // Get the exporter that will be used to export this service
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

        // Setup lookup discovery
        this.ldm = Util.getDiscoveryManager(config, COMPONENT_NAME);
        groupsToJoin   = ((DiscoveryGroupManagement)ldm).getGroups();
        locatorsToJoin = ((DiscoveryLocatorManagement)ldm).getLocators();

        // For discovering other services in the federation
        this.sdm = new ServiceDiscoveryManager(this.ldm, null, config);

        this.runner = new ProcessStateRunner(gpSync);
        executor.submit(runner);

        // Register a boot process listener (before retrieving the
        // initial state of each process)
        bootMgr.registerListener(this);

        // Initialize the process structures
        synchronized(gpSync) {
            for(ProcessManagement pInfo : processes.values()) {
                logger.log(Level.DEBUG, 
                           new Format("initialize process ['{0}']",
                                      pInfo.getTag()) );
                pInfo.initialize(this, gpSync);
            }
        }

        //Service attribute(s)
        ArrayList<Entry> serviceAttrsList = new ArrayList<Entry>();

        ServiceInfo serviceInfo = 
            ConfigDeployUtil.initServiceInfo(proxyId, SERVICE_NAME);

        serviceAttrsList.add(serviceInfo);

        //array of attributes for JoinManager
        Entry[] serviceAttrs = serviceAttrsList.toArray
                                       (new Entry[serviceAttrsList.size()]);

        //advertise this service so leader election can occur
        this.joinMgr = new JoinManager(outerProxy, serviceAttrs, serviceId,
                                       this.ldm, null, config);

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

            cleanupOnExit();

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

    private void readConfiguration()
                     throws ConfigurationException, InitializationException
    {
        // Parse the config file
        ProcessConfigXmlHandler configHandler =
                                new ProcessConfigXmlHandler(processes, roles);
        String configFilePath = getProcessDefinitionsPath();
        // Remove redundant elements such as "." and ".."
        try {
            configFilePath = (new File(configFilePath)).getCanonicalPath();
        } catch(IOException e) { /*swallow*/ }
        File configFile = 
            new File(configFilePath+File.separator+"process-definitions.xml");

        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            saxParser.parse(configFile, configHandler);
        } catch(Exception e) {
            throw new InitializationException("error parsing XML config", e);
        }

        // Validate the configuration
        for(RoleInfo rInfo : roles.values()) {
            for(String pTag : rInfo.getProcessSet()) {
                if( !processes.containsKey(pTag) ) {
                    throw new InitializationException
                          ("process not defined [type="+rInfo.getType()
                           +", layout="+rInfo.getLayout()
                           +", role="+rInfo.getRole()+", process='"+pTag+"']");
                }
            }
        }

        // Build the restart groups
        for(Set<String> memberSet : configHandler.getRestartGroups()) {
            RestartGroup rGroup = new RestartGroup();
            for(String pTag : memberSet) {
                ProcessManagement pInfo = processes.get(pTag);
                if(pInfo == null) {
                    throw new InitializationException
                                  ("unknown process ['"+pTag+"']");
                }
                rGroup.addMember(pInfo);
                pInfo.setRestartGroup(rGroup);
            }
            logger.log(Level.INFO, new Format("constructed {0}", rGroup) );
        }

        // Get the current node's list of types, layouts, and roles and
        // set the autostart flag on all processes configured for those
        // types, layouts, and roles
        String[] thisNodeTypes = ConfigDeployUtil.getStringArray("node.type");
        String[] thisNodeLayouts = 
                     ConfigDeployUtil.getStringArray("node.layout");
        String[] thisNodeRoles = ConfigDeployUtil.getStringArray("node.role");

        for(String type : thisNodeTypes) {
            for(String layout : thisNodeLayouts) {
                for(String role : thisNodeRoles) {
                    ProcessInfo pInfo = new ProcessInfo(type, layout, role);
                    RoleInfo rInfo = roles.get(pInfo);
                    if(rInfo == null) {
                        throw new InitializationException
                                      ("no role info for configured process "
                                       +"info - "+pInfo);
                    }
                    for( String pTag : rInfo.getProcessSet() ) {
                        processes.get(pTag).setAutoStart(true);
                    }
                }
            }
        }

        // Handle wild cards
        for(RoleInfo rInfo : roles.values()) {
            String type   = rInfo.getType();
            String layout = rInfo.getLayout();
            String role   = rInfo.getRole();
            if( ("*".equals(type))   ||
                ("*".equals(layout)) ||
                ("*".equals(role)) )
            {
                for( String pTag : rInfo.getProcessSet() ) {
                    processes.get(pTag).setAutoStart(true);
                }
            }
        }
    }


    private static String getProcessDefinitionsPath() {
        String rootPath = "/opt/bigdata";//real installation
        String appHome = System.getProperty("appHome");//pstart
        String appDotHome = System.getProperty("app.home");//pom.xml

        if(appHome != null) {
            rootPath = appHome;
        } else if(appDotHome != null) {
            rootPath = appDotHome + File.separator 
                               + "dist" + File.separator + "bigdata";
        }
        String relPath = "var" + File.separator + "config" 
                               + File.separator + "jini"
                               + File.separator + "boot";
        return rootPath + File.separator + relPath;
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

    private void cleanupOnExit() {
        if(executor != null) {
            executor.shutdownNow();
            this.executor = null;
        }

        if(scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            this.scheduledExecutor = null;
        }

        if(bootMgr != null) {
            bootMgr.terminate();
            this.bootMgr = null;
        }

        Util.cleanupOnExit(innerProxy, serverExporter, joinMgr, sdm, ldm);
        this.serverExporter = null;
        this.outerProxy = null;
        this.adminProxy = null;
    }
}
