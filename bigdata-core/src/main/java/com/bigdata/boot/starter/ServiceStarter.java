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
package com.bigdata.boot.starter;

import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.tool.ClassServer;
import net.jini.admin.Administrable;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;
import net.jini.config.EmptyConfiguration;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.bigdata.boot.BootAgent;
import com.bigdata.boot.AgentListener;

public class ServiceStarter {

    protected static final Logger logger =
                           Logger.getLogger(ServiceStarter.class.getName());

    /** List whose elements are the descriptors of the services to start. */
    protected final List<ServiceDescriptor> serviceDescriptorList;

    /**
     * List whose elements are references to the created services; that is,
     * the objects returned from the ServiceDescriptor create() method.
     */
    protected final List<Object> createdServiceList =
                                     new LinkedList<Object>();

    public static void main(String[] args) {

        List<ServiceDescriptor> serviceDescriptors =
                                    new ArrayList<ServiceDescriptor>();

        // Usage: java ServiceStarter configFile [configfile2] [-- [args...]]
        try {
            int i;
            for(i=0; i<args.length; i++) {
                if( "--help".equals(args[i]) ) {
                    System.out.println
                       ("Usage: ServiceStarter <configFile> [configFile] ...");
                    System.exit(0);
                } else if( "--".equals(args[i]) ) {
                    break;
                }

                // Read config file
                Configuration config = 
                    ConfigurationProvider.getInstance(new String[] {args[i]});

                    // Add the descriptors to the list
                    ServiceDescriptor[] sdArray =
                        (ServiceDescriptor[])config.getEntry
                                                ( "com.sun.jini.start",
                                                  "serviceDescriptors",
                                                  ServiceDescriptor[].class );
                    serviceDescriptors.addAll(Arrays.asList(sdArray));
            }
            ServiceStarter app = new ServiceStarter(serviceDescriptors);
            app.run();
            System.exit(0);
        } catch(Throwable t) {
            t.printStackTrace();
            System.exit(255);
        }
    }

    public ServiceStarter(List<ServiceDescriptor> serviceDescriptorList) {
        this.serviceDescriptorList = serviceDescriptorList;
    }

    public void run() throws java.lang.Exception {

        logger.log(Level.FINE, "Connecting to the boot launcher");

        BootAgent bootAgent = BootAgent.getInstance();
        bootAgent.registerListener(new ShutdownListener());

        try {
            logger.log(Level.FINE, 
                       "Creating services from the service descriptors");
            BootLifeCycle lifeCycle = new BootLifeCycle();
            for(ServiceDescriptor desc : serviceDescriptorList) {

                if(desc instanceof NonActivatableServiceDescriptor) {

                    // Only NonActivatable services use the LifeCycle object
                    ((NonActivatableServiceDescriptor)desc).setLifeCycle
                                                                 (lifeCycle);
                }

                Object created = desc.create(EmptyConfiguration.INSTANCE);
                synchronized(createdServiceList) {
                    createdServiceList.add(created);
                }
            }

            logger.log(Level.FINE,
                       "Sending 'init complete' message to launcher");
            bootAgent.initComplete();

            logger.log(Level.FINE, "Waiting for shutdown signal");
            waitForServicesToUnregister(0);

            logger.log(Level.FINE, 
                       "Sending 'shutting down' message to launcher");
            try {
                bootAgent.shuttingDown();
            } catch(IOException e) { /* swallow */ }

        } catch(Exception e) {
            logger.log(Level.FINE, "exception ["+e+"] - shutting down all "
                       +"services in the VM");
            shutdownAllServices();
            try {
                waitForServicesToUnregister(5000);
            } catch(InterruptedException e2) { /* swallow */ }

            if( !(e instanceof InterruptedException) ) throw e;
        }
    }

    /**
     * Through the DestroyAdmin interface, tells each service to shut down.
     */
    protected void shutdownAllServices() {

        List<Object> createdListCopy;

        synchronized(createdServiceList) {
            createdListCopy=new ArrayList<Object>(createdServiceList);
        }

        for(Object created : createdListCopy) {
            try {
                if(created instanceof NonActivatableServiceDescriptor.Created)
                {
                    if( !shutdownOneService
                          ((NonActivatableServiceDescriptor.Created)created) )
                    {
                        // Remove this service from createdServiceList because
                        // we have no idea how to shut it down.
                        synchronized(createdServiceList) {
                            createdServiceList.remove(created);
                            createdServiceList.notifyAll();
                        }
                    }
                } else {
                    logger.log(Level.WARNING, "cannot shut down service - "
                               +"service created using unsupported service "
                               +"descriptor type [{0}]", created.getClass());
                }
            } catch(Throwable t) {
                logger.log(Level.WARNING, 
                           "exception during service shut down", t);
            }
        }
    }

    protected boolean shutdownOneService
                          (NonActivatableServiceDescriptor.Created created)
                              throws RemoteException
    {
        // 1st determine if service impl itself implements the DestroyAdmin
        if( created.impl instanceof DestroyAdmin ) {
            ((DestroyAdmin)created.impl).destroy();
            return true;
        }

        // Retrieve the admin proxy from either the service itself or
        // through the service proxy.
        Object adminProxy = null;
        if( created.impl instanceof Administrable ) {
            adminProxy = ((Administrable)created.impl).getAdmin();
        } else if( (created.proxy != null) &&
                   (created.proxy instanceof Administrable) )
        {
            adminProxy = ((Administrable)created.proxy).getAdmin();
        }

        if( (adminProxy != null) && (adminProxy instanceof DestroyAdmin) ) {
            ((DestroyAdmin)adminProxy).destroy();
            return true;
        }

        try {
            // DestroyAdmin didn't work above, it may be the class server,
            // which provides a terminate method
            if(created.impl instanceof ClassServer) {
                ((ClassServer)created.impl).terminate();
                return true;
            }
        } catch(NoClassDefFoundError e) { /* swallow */ }

        logger.log(Level.WARNING, "cannot shut down service [{0}]",
                   created.impl.getClass());
        return false;
    }

    /**
     * Blocks until all services have unregistered through the LifeCycle
     * object. Specifying a timeout of 0 will wait forever for the
     * services to unregister.
     */
    protected void waitForServicesToUnregister(long timeout)
                       throws InterruptedException
    {
        long endTime = (timeout > 0) ?
                       (System.currentTimeMillis() + timeout) : Long.MAX_VALUE;

        synchronized(createdServiceList) {
            while( !createdServiceList.isEmpty() ) {
                long timeLeft = endTime - System.currentTimeMillis();
                if(timeLeft <= 0) break;
                createdServiceList.wait(timeLeft);
            }
        }
    }

    private class ShutdownListener implements AgentListener {

        public void beginShutdown() {
            logger.log(Level.INFO, "shut down command received");

            shutdownAllServices();
            // The services will eventually unregister through the LifeCycle
            // interface and the main loop will continue.
        }
    }

    // Implementation of the LifeCycle interface
    private class BootLifeCycle implements LifeCycle {
        public boolean unregister(Object impl) {
            // Find and remove the impl from createdServiceList. Compare with
            // the impl field using only reference equality (==)
            synchronized(createdServiceList) {
                Iterator<Object> itr = createdServiceList.iterator();
                while( itr.hasNext() ) {
                    Object created = itr.next();
                    if( (created instanceof 
                            NonActivatableServiceDescriptor.Created) &&
            (((NonActivatableServiceDescriptor.Created)created).impl == impl) )
                    {
                        itr.remove();
                        createdServiceList.notifyAll();
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
