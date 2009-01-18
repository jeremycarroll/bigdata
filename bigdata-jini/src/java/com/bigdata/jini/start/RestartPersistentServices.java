package com.bigdata.jini.start;

import java.net.UnknownHostException;
import java.nio.channels.FileLock;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.config.AbstractHostConstraint;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.JiniUtil;
import com.bigdata.zookeeper.ZLock;

/**
 * Task restarts persistent physical services that should be running on this
 * host but which are not discoverable using jini (not found when we query for
 * their {@link ServiceID}) and apparently disconnected from zookeeper (their
 * ephemeral znode is not found in the {@link BigdataZooDefs#MASTER_ELECTION}
 * container). Service restarts are logged. An error is logged if a service can
 * not be restarted. It is an error if the {@link ZooKeeper} client is not
 * connected. It is an error if no {@link ServiceRegistrar}s are joined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RestartPersistentServices implements Callable<Void> {

    protected static final Logger log = Logger.getLogger(RestartPersistentServices.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * A fatal error (jini registrars not joined, zookeeper client is
     * disconnected, etc).
     */
    static transient final String ERR_WILL_NOT_RESTART_SERVICES = "Will not restart services";
    
    protected final JiniFederation fed;
    
    protected final ZooKeeper zookeeper;
    
    final MonitorCreatePhysicalServiceLocksTask monitorCreatePhysicalServiceLocksTask;
    
    public RestartPersistentServices(
            final JiniFederation fed,
            final MonitorCreatePhysicalServiceLocksTask monitorCreatePhysicalServiceLocksTask) {
        
        if(fed == null)
            throw new IllegalArgumentException();
    
        if(monitorCreatePhysicalServiceLocksTask == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.zookeeper = fed.getZookeeper();
        
        this.monitorCreatePhysicalServiceLocksTask = monitorCreatePhysicalServiceLocksTask;
        
    }
    
    public Void call() throws Exception {

        /*
         * Make sure that the zookeeper client is connected (of course it could
         * disconnect at any time).
         */
        {
            final ZooKeeper.States state = zookeeper.getState();
            switch (state) {
            default:
                log.error(ERR_WILL_NOT_RESTART_SERVICES
                        + " : zookeeper not connected: state=" + state);
                return null;
            case CONNECTED:
                break;
            }
        }

        /*
         * Make sure that we are joined with at least one jini registrar.
         */
        if (fed.getDiscoveryManagement().getRegistrars().length == 0) {
            
            log.error(ERR_WILL_NOT_RESTART_SERVICES
                    + " : not joined with any service registrars.");
            
            return null;
            
        }
        
        // root znode for the federation.
        final String zroot = fed.getZooConfig().zroot;

        // znode for configuration metadata.
        final String zconfig = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        // these are the ServiceConfigurations.
        final List<String> serviceConfigZNodes = zookeeper.getChildren(zconfig, false);
        
        for (String serviceConfigZNode : serviceConfigZNodes) {

            /*
             * Get the service configuration. We will need it iff we restart an
             * instance of this service type.
             */
            final ManagedServiceConfiguration serviceConfig = (ManagedServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(zconfig + "/"
                            + serviceConfigZNode, false, new Stat()));

            /*
             * The children are the logical service instances for that service
             * type.
             */
            final List<String> logicalServiceZNodes = zookeeper.getChildren(
                    zconfig + "/" + serviceConfigZNode, false);

            for (String logicalServiceZNode : logicalServiceZNodes) {

                final String logicalServiceZPath = zconfig + "/"
                        + logicalServiceZNode;

                /*
                 * The children are the physical service instances for that
                 * logical service.
                 */
                final List<String> physicalServiceZNodes = zookeeper
                        .getChildren(logicalServiceZPath, false);
                
                for (String physicalServiceZNode : physicalServiceZNodes) {

                    final String physicalServiceZPath = logicalServiceZPath
                            + "/" + physicalServiceZNode;

                    final Entry[] attributes = (Entry[]) SerializerUtil
                            .deserialize(zookeeper.getData(
                                    physicalServiceZPath, false, new Stat()));

                    monitorCreatePhysicalServiceLocksTask.restartIfNotRunning(
                            serviceConfig, logicalServiceZPath,
                            physicalServiceZPath, attributes);
                    
                }

            }

        }

        return null;

    }
}
