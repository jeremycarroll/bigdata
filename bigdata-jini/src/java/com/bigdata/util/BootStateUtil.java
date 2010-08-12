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

package com.bigdata.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.config.Config;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;
import net.jini.core.lookup.ServiceID;

/**
 * Utility class that an entity (client or service) can use to recover its
 * <i>boot state</i>. 
 * <p>
 * To use this class, an entity would typically instantiate this class
 * during initialization of the entity (for example, during service startup),
 * passing to the constructor the desired <code>Configuration</code>.
 * <p>
 * With respect to the state that this class recovers, this class 
 * recognizes two modes in which an entity may be run: <i>persistent</i>
 * mode and <i>transient</i> mode. This class considers an entity to be
 * persistent if the <i>boot state</i> of the entity (that is, the state
 * recovered by this class) is persisted/recovered when the entity is
 * created/started/re-started, and the entity is configured to run in
 * persistent mode. In all other cases, the entity is considered transient.
 * <p>
 * Each entity's implementation can be configured to run in either 
 * persistent mode or transient mode. A deployer indicates that an entity
 * is persistent or transient by including or excluding the configuration
 * entry named <code>persistenceDirectory</code> in the implementation's
 * configuration file. The presence of such an entry will cause the entity
 * to run in persistent mode, and the absence of that entry will cause the
 * entity to run in transient mode. 
 */
public class BootStateUtil {

    private Class     entityImplType;
    private Logger    logger;

    private File      persistenceDir = null;
    private UUID      proxyId = null;
    private ServiceID serviceId = null;
    private String    stateKey = null;

    public BootStateUtil(final Configuration config,
                         final String        componentName,
                         final Class         entityImplType,
                               Logger        logger)
                                              throws ConfigurationException,
                                                     IOException,
                                                     ClassNotFoundException
    {
        if(config == null) {
            throw new NullPointerException("config null");
        }
        if(componentName == null) {
            throw new NullPointerException("componentName null");
        }
        if(entityImplType == null) {
            throw new NullPointerException("entityImplType null");
        }

        this.entityImplType = entityImplType;
        this.logger = (logger == null ?
                        Logger.getLogger(this.getClass()) :
                        logger);
        this.logger.log(Level.DEBUG, entityImplType+" in boot state recovery");

        // PERSISTENCE DIRECTORY - get the directory where the entity
        // persists it's boot state. If no configuration entry exists
        // for the entity's persistence directory, then it is assumed
        // the entity will be executing in transient mode.
        try {
            String persistDirStr = (String)Config.getNonNullEntry(config,
                componentName, "persistenceDirectory", String.class);
            this.persistenceDir = new File(persistDirStr);
        } catch(net.jini.config.NoSuchEntryException e) {
            this.persistenceDir = null;// No configuration setting. That's ok.
        }

        ServiceID defaultServiceId;
        try {
            defaultServiceId = (ServiceID)Config.getNonNullEntry(config,
                componentName, "defaultServiceId", ServiceID.class);
        } catch(net.jini.config.NoSuchEntryException e) {
            defaultServiceId = null;// No configuration setting. That's ok.
        }
        recoverBootState(defaultServiceId);
    }


    public BootStateUtil(File      persistenceDir, 
                         Class     entityImplType,
                         ServiceID defaultServiceId)
                             throws IOException, ClassNotFoundException
    {
        if(entityImplType == null) {
            throw new NullPointerException("entityImplType null");
        }
        this.entityImplType = entityImplType;
        this.logger = Logger.getLogger(this.getClass());
        this.persistenceDir = persistenceDir;

        recoverBootState(defaultServiceId);
    }

    /**
     * Returns the entity's unique <i>proxy id</i> that is generated/recoverd
     * as part of the boot state maintained by this class.
     */
    public UUID getProxyId() {
        return proxyId;
    }

    /** 
     * Returns the entity's unique <i>service id</i> that is generated
     * from the entity's proxy id, and which is part of the boot state
     * maintained by this class.
     */
    public ServiceID getServiceId() {
        return serviceId;
    }

    /**
     * Returns the <code>String</code> representing the path of the 
     * directory in which the entity's <i>boot state</i> is located.
     * If the value returned is <code>null</code>, then the entity
     * was configured to run in <i>transient</i> mode.
     */
    public String getPersistenceDirectory() {
        return (persistenceDir != null) ? persistenceDir.toString() : null;
    }

    /** 
     * Returns the <code>true</code> if the entity was configured to
     * run in <i>persistent</i> mode; <code>false</code> otherwise.
     */
    public boolean isPersistent() {
        return (persistenceDir != null);
    }

    /** 
     * If the entity is currently configured to run in <i>persistent</i>
     * mode, returns the name-based key under which an entity's (non-boot)
     * state was persisted during previous runs of the entity. If the
     * entity is currently configured to run in <i>transient</i> mode,
     * a non-<code>null</code>, randomly-generated key value is returned.
     */
    public String getStateKey() {
        return stateKey;
    }

    /** 
     * Returns the <code>Class</code> type of the entity whose boot state
     * is associated with the current instance of this utility.
     */
    public Class getType() {
        return entityImplType;
    }


    /* Performs the actual retrieval of the entity's boot state. This
     * method is called only once, in this utility's constructor.
     * It recovers the entity's boot state from local persistence storage,
     * from which the entity's proxy ID and well-known, persisted state key
     * is retrieved. Using the recovered proxy ID, the service ID
     * is reconstructed.
     */
    private void recoverBootState(ServiceID defaultServiceId)
        throws IOException, ClassNotFoundException
    {
        if(persistenceDir != null) {
            // If the requested directory does not exist, create it
            if(!(persistenceDir.exists() ? persistenceDir.isDirectory()
                                        : persistenceDir.mkdirs()) )
            {
                throw new IOException("could not create persistence "
                                      +"directory ["+persistenceDir+"]");
            }

            // BOOT STATE - retrieve/store
            File bootStateFd = new File(persistenceDir, "boot.state");
            logger.log(Level.DEBUG,"path to boot state - "+bootStateFd);
            try {
                FileInputStream fis = new FileInputStream(bootStateFd);
                ClassLoaderObjectInputStream ois = 
                        new ClassLoaderObjectInputStream
                         (new BufferedInputStream(fis),
                          ((Thread.currentThread()).getContextClassLoader()));
                try {
                    BootState bootState = (BootState)ois.readObject();
                    Class bootType = bootState.getType();
                    if( !entityImplType.equals(bootType) ) {
                        throw new IOException
                                     ("boot state from wrong implementation: "
                                      +"expected - "+entityImplType
                                      +", retrieved - "+bootType);
                    }
                    proxyId = bootState.getProxyId();
                    if( proxyId == null ) {
                        throw new IOException
                                       ("boot state contains null proxyId");
                    }
                    serviceId = 
                            new ServiceID(proxyId.getMostSignificantBits(),
                                          proxyId.getLeastSignificantBits());
                    stateKey = bootState.getKey();
                } finally {
                    ois.close();
                }
            } catch(FileNotFoundException e) {/* FIRST EVER STARTUP */
                logger.log(Level.DEBUG,
                           "create initial boot state in - "+bootStateFd);
                serviceId = createServiceId(defaultServiceId);
                proxyId = new UUID(serviceId.getMostSignificantBits(),
                                   serviceId.getLeastSignificantBits());
                stateKey = proxyId.toString();
                BootState bootState = new BootStateV0( entityImplType,
                                                       proxyId,
                                                       stateKey );
                ObjectOutputStream oos = 
                        new ObjectOutputStream
                                ( new BufferedOutputStream
                                         (new FileOutputStream(bootStateFd)) );
                try {
                    oos.writeObject(bootState);
                } finally {
                    oos.close();
                }
            }
            /* ========================================================== */

            logger.log(Level.DEBUG, "PERSISTENT "+entityImplType);
            logger.log(Level.DEBUG, "proxyId   = "+proxyId);
            logger.log(Level.DEBUG, "serviceId = "+serviceId);
            logger.log(Level.DEBUG, "stateKey  = "+stateKey);
        } else {/* TRANSIENT */
            serviceId = createServiceId(defaultServiceId);
            proxyId = new UUID(serviceId.getMostSignificantBits(),
                               serviceId.getLeastSignificantBits());
            stateKey = (UUID.randomUUID()).toString();//random string

            logger.log(Level.DEBUG, "TRANSIENT "+entityImplType);
            logger.log(Level.DEBUG, "proxyId   = "+proxyId);
            logger.log(Level.DEBUG, "serviceId = "+serviceId);
            logger.log(Level.DEBUG, "stateKey  = "+stateKey);
        }
        logger.log(Level.DEBUG, 
                   entityImplType+" boot state recovery complete");
    }

    /**
     * Creates a service id for the service. If a default service id is
     * specified then it is returned, otherwise a randomly-generated
     * service ID is returned.
     */ 
    private ServiceID createServiceId(ServiceID defaultServiceId) {
        if(defaultServiceId != null) {
            return defaultServiceId;
        } else {
            UUID uuid = UUID.randomUUID();
            return new ServiceID(uuid.getMostSignificantBits(),
                                 uuid.getLeastSignificantBits());
        }
    }

    /**
     * Subclass of <code>java.io.ObjectInputStream</code> that overrides the
     * behavior of the <code>ObjectInputStream#resolveClass</code> method by
     * allowing one to specify a non-<code>null</code> <code>ClassLoader</code>
     * to be used when loading the local class equivalent of the stream class
     * description that is input to this class' <code>resolveClass</code>
     * implementation.
     *
     * @see java.io.ObjectInputStream
     */
    public final static class ClassLoaderObjectInputStream
                                  extends ObjectInputStream
    {
        private ClassLoader loader;

        /** 
         * @see java.io.ObjectInputStream
         *
         * @throws java.lang.NullPointerException when <code>null</code> is
         *         input for the <code>loader</code> parameter; otherwise uses
         *         the given <code>loader</code> when <code>resolveClass</code>
         *         is invoked to perform class resolution.
         */
        public ClassLoaderObjectInputStream(InputStream in, ClassLoader loader)
                                               throws IOException
        {
            super(in);
            if(loader == null) throw new NullPointerException("null loader");
            this.loader = loader;
        }

        /** 
         * @see java.io.ObjectInputStream#resolveClass
         */
        protected Class<?> resolveClass(ObjectStreamClass desc)
                             throws IOException, ClassNotFoundException
        {
            try {
                return Class.forName(desc.getName(), false, loader);
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }

    /* Data structure that encapsulates the entity's boot state, and
     * which is persisted for recovery on restart/re-initialization. 
     * An interface is specified here to support evolution of new
     * versions of BootState.
     */
    interface BootState {
        Class  getType();
        UUID   getProxyId();
        String getKey();
    }

    private final static class BootStateV0 implements BootState, Serializable {

        private static final long serialVersionUID = 1L;

        private final Class  type;
        private final UUID   proxyId;
        private final String key;

        public BootStateV0(Class type, UUID proxyId, String key) {
            this.type    = type;
            this.proxyId = proxyId;
            this.key     = key;
        }

        public Class  getType() { return type; }
        public UUID   getProxyId() { return proxyId; }
        public String getKey() { return key; }

    }

}
