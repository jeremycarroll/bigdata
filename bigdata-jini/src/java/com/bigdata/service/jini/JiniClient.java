/**

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
/*
 * Created on Mar 24, 2007
 */

package com.bigdata.service.jini;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationFile;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.discovery.LookupDiscovery;

import com.bigdata.service.AbstractScaleOutClient;
import com.bigdata.util.NV;
import com.bigdata.zookeeper.ZookeeperClientConfig;

/**
 * A client capable of connecting to a distributed bigdata federation using
 * JINI.
 * <p>
 * Clients are configured using a Jini service configuration file. The name of
 * that file is passed to {@link #newInstance(String[])}.  The configuration 
 * must be consistent with the configuration of the federation to which you
 * wish to connect.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniClient extends AbstractScaleOutClient {

    /**
     * Options understood by the {@link JiniClient}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractScaleOutClient.Options {
        
        /**
         * The timeout in milliseconds that the client will await the discovery
         * of a service if there is a cache miss (default
         * {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
         * 
         * @see DataServicesClient
         */
        String CACHE_MISS_TIMEOUT = JiniClient.class.getName()
                + ".cacheMissTimeout";
        
        String DEFAULT_CACHE_MISS_TIMEOUT = "" + (2 * 1000);
        
    }
    
    /**
     * The federation and <code>null</code> iff not connected.
     */
    private JiniFederation fed = null;

    synchronized public boolean isConnected() {
        
        return fed != null;
        
    }
    
    /**
     * Note: Immediate shutdown can cause odd exceptions to be logged. Normal
     * shutdown is recommended unless there is a reason to force immediate
     * shutdown.
     * 
     * @param immediateShutdown
     *            When <code>true</code> the shutdown is <em>abrubt</em>.
     *            You can expect to see messages about interrupted IO such as
     * 
     * <pre>
     * java.rmi.MarshalException: error marshalling arguments; nested exception is: 
     *     java.io.IOException: request I/O interrupted
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:785)
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *     at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *     at $Proxy5.notify(Ljava.lang.String;Ljava.util.UUID;Ljava.lang.String;[B)V(Unknown Source)
     * </pre>
     * 
     * These messages may be safely ignored if they occur during immediate
     * shutdown.
     */
    synchronized public void disconnect(final boolean immediateShutdown) {
        
        if (fed != null) {

            if(immediateShutdown) {
                
                fed.shutdownNow();
                
            } else {
                
                fed.shutdown();
                
            }
            
        }
        
        fed = null;

    }

    synchronized public JiniFederation getFederation() {

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    synchronized public JiniFederation connect() {

        if (fed == null) {

            fed = new JiniFederation(this, jiniConfig, zooConfig);

        }

        return fed;

    }

    /**
     * The jini configuration which will be used to connect to the federation.
     */
    private final JiniConfig jiniConfig;

    /**
     * The zookeeper configuration.
     */
    private final ZookeeperClientConfig zooConfig;

    /**
     * 
     * @param jiniConfig
     */
    protected JiniClient(final JiniConfig jiniConfig, final ZookeeperClientConfig zooConfig) {

        super(jiniConfig.properties);

        this.jiniConfig = jiniConfig;
        
        this.zooConfig = zooConfig;

    }
    
    /**
     * Helper class for passing pre-extracted Jini configuration information to
     * the {@link JiniFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class JiniConfig {
        
        final Configuration config;
        final String[] groups;
        final LookupLocator[] lookupLocators;
        final Properties properties;

        public JiniConfig(Configuration config, String[] groups,
                LookupLocator[] lookupLocators, 
                Properties properties) {

            this.config = config;
            
            this.groups = groups;

            this.lookupLocators = lookupLocators;
            
            this.properties = properties;
            
            if(INFO) {
                
                log.info(toString());
                
            }
            
        }

        public String toString() {
            
            return "JiniConfig"//
                    + "{ groups="
                    + (groups == null ? "N/A" : "" + Arrays.toString(groups))//
                    + ", locators="
                    + (lookupLocators == null ? "N/A" : ""
                            + Arrays.toString(lookupLocators))//
                    + ", properties="+properties
                    + "}";
            
        }
        
        /**
         * Return the configuration data for the client.
         * <p>
         * This helper method reads {@link Configuration} data from the file(s)
         * named by <i>args</i>, reads the <i>properties</i> file named in the
         * {@value #CLIENT_LABEL} section of the {@link Configuration} file, and
         * returns the configured client.
         * 
         * @param args
         *            The command line arguments.
         * 
         * @return The configuration data for the client.
         * 
         * @throws ConfigurationException
         *             if there is a problem reading the jini configuration for
         *             the client.
         * @throws IOException
         *             if there is a problem reading the optional properties
         *             file (a properties file may be specified as part of the
         *             configuration).
         */
        static public JiniConfig readConfiguration(final Configuration config)
                throws ConfigurationException, IOException {

            final String[] groups;
            final LookupLocator[] lookupLocators;
            final Properties properties;

            /*
             * Extract how the client will discover services from the
             * Configuration.
             */
            groups = (String[]) config
                    .getEntry(AbstractServer.ADVERT_LABEL, "groups",
                            String[].class, LookupDiscovery.ALL_GROUPS/* default */);

            /*
             * Note: multicast discovery is used regardless if
             * LookupDiscovery.ALL_GROUPS is selected above. That is why there
             * is no default for the lookupLocators. The default "ALL_GROUPS"
             * means that the lookupLocators are ignored.
             */

            lookupLocators = (LookupLocator[]) config.getEntry(
                    AbstractServer.ADVERT_LABEL, "unicastLocators",
                    LookupLocator[].class, null/* default */);

            {

                /*
                 * Extract the name of the optional properties file.
                 */

                final File propertyFile = (File) config.getEntry(
                        AbstractServer.SERVICE_LABEL, "propertyFile",
                        File.class, null/* defaultValue */);

                if (propertyFile != null) {

                    /*
                     * Read the properties file.
                     */

                    properties = getProperties(propertyFile);

                } else {

                    /*
                     * Start with an empty properties map.
                     */

                    properties = new Properties();

                }

            }

            {

                /*
                 * Read the optional [properties] array.
                 * 
                 * @todo this could be replaced by explicit use of the java
                 * identifier corresponding to the Option and simply collecting
                 * all such properties into a Properties object using their
                 * native type (as reported by the ConfigurationFile).
                 */

                final NV[] tmp = (NV[]) config.getEntry(
                        AbstractServer.SERVICE_LABEL, "properties", NV[].class,
                        new NV[] {}/* defaultValue */);

                for (NV nv : tmp) {

                    if (INFO)
                        log.info(nv.toString());

                    properties.setProperty(nv.getName(), nv.getValue());

                }

            }

            return new JiniConfig(config, groups, lookupLocators, properties);

        }

    }

    /**
     * Conditionally installs a {@link SecurityManager}, reads
     * {@link Configuration} data from the file(s) named by <i>args</i>, reads
     * the <i>properties</i> file named in the {@value #CLIENT_LABEL} section
     * of the {@link Configuration} file, and returns the configured client.
     * 
     * @param args
     *            The command line arguments.
     * 
     * @return The new client.
     * 
     * @throws RuntimeException
     *             if there is a problem: reading the jini configuration for the
     *             client; reading the properties for the client; starting
     *             service discovery, etc.
     */
    public static JiniClient newInstance(final String[] args) {

        // set the security manager.
        setSecurityManager();

        try {

            // Obtain the configuration object.
            final ConfigurationFile config = (ConfigurationFile) ConfigurationProvider
                    .getInstance(args);

            // read all the configuration data and the properties file.
            final JiniConfig jiniConfig = JiniConfig.readConfiguration(config);

            // read the zookeeper client configuration.
            final ZookeeperClientConfig zooConfig = ZookeeperClientConfig.readConfiguration(config);

            // return the client.
            return new JiniClient(jiniConfig, zooConfig);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the client can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    static protected void setSecurityManager() {

        SecurityManager sm = System.getSecurityManager();
        
        if (sm == null) {

            System.setSecurityManager(new SecurityManager());
         
            if (INFO)
                log.info("Set security manager");

        } else {

            if (INFO)
                log.info("Security manager already in place: " + sm.getClass());

        }

    }
    
    /**
     * Read and return the content of the properties file.
     * 
     * @param propertyFile
     *            The properties file.
     * 
     * @throws IOException
     */
    static protected Properties getProperties(final File propertyFile)
            throws IOException {

        if(INFO) {
            
            log.info("Reading properties: file="+propertyFile);
            
        }
        
        final Properties properties = new Properties();

        InputStream is = null;

        try {

            is = new BufferedInputStream(new FileInputStream(propertyFile));

            properties.load(is);

            if(INFO) {
                
                log.info("Read properties: " + properties);
                
            }
            
            return properties;

        } finally {

            if (is != null)
                is.close();

        }

    }
    
}
