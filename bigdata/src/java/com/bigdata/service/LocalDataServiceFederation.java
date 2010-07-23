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
 * Created on Apr 1, 2008
 */

package com.bigdata.service;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.ResourceLockService;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.EmbeddedFederation.EmbeddedTransactionServiceImpl;
import com.bigdata.service.ndx.DataServiceIndex;
import com.bigdata.util.InnerCause;

//BTM
import com.bigdata.loadbalancer.EmbeddedLoadBalancer;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration provides a view of a local {@link DataService} as if it were a
 * federation. The {@link LocalDataServiceFederation} runs its own embedded
 * {@link AbstractTransactionService} and {@link LoadBalancerService} to support
 * its embedded {@link DataService}. Since there is no {@link MetadataService},
 * overflow processing is disabled.
 * 
 * @see LocalDataServiceClient
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the client or service.
 * @todo The EDS/LDS should use their own options in their own namespace to
 *       specify the data directory for the federation. Ditto for the
 *       "transient" or "createTempFile" properties. Everything is namespaced
 *       now and the overridden semantics of
 *       com.bigdata.journal.Options.CREATE_TEMP_FILE and StoreManager#DATA_DIR
 *       are just getting us into trouble. Look at all uses of these options in
 *       the unit tests and decouple them from the journal's options.
 */
public class LocalDataServiceFederation<T> extends AbstractFederation<T> {

    /** The top-level directory spanning the persistent state of each service. */
    private final File dataDir;
    private final AbstractTransactionService abstractTransactionService;
    private final ResourceLockService resourceLockManager = new ResourceLockService();
//BTM    private final LoadBalancerService loadBalancerService;
private final EmbeddedLoadBalancer loadBalancerService;
    private final LocalDataServiceImpl dataService;

//    /**
//     * There are no preconditions for a service start.
//     */
//    @Override
//    protected boolean awaitPreconditions(long timeout, TimeUnit unit)
//            throws InterruptedException {
//
//        return true;
//
//    }

    public LocalDataServiceFederation(final LocalDataServiceClient<T> client) {
        
        super(client);

//BTM
final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

        final Properties properties = client.getProperties();

        if (properties.getProperty(Options.DATA_DIR) == null)
            throw new IllegalArgumentException("Required: " + Options.DATA_DIR);

        dataDir = new File(properties.getProperty(Options.DATA_DIR));

        // indexCache
        indexCache = new DataServiceIndexCache(this, client
                .getIndexCacheCapacity(), client.getIndexCacheTimeout());

        {

            final Properties p = new Properties(properties);

            // specify the data directory for the txService.
            p.setProperty(EmbeddedTransactionServiceImpl.Options.DATA_DIR,
                    new File(dataDir, "txService").toString());

            abstractTransactionService = new AbstractEmbeddedTransactionService(
                    UUID.randomUUID(), p) {

                public LocalDataServiceFederation getFederation() {

                    return LocalDataServiceFederation.this;

                }
            }.start();

        }

        {
            
            final Properties p = new Properties(properties);

            // set the directory for the log files.
//BTM            p.setProperty(LoadBalancerService.Options.LOG_DIR, new File(
//BTM                    dataDir, "lbs").toString());
p.setProperty(EmbeddedLoadBalancer.Options.LOG_DIR, new File(dataDir, "lbs").toString());

            /*
             * Note: This will expose the counters for the local data service.
             */
//BTM*** For now, move the creation of the loadBalancerService to a
//BTM*** point below AFTER the creation of the dataService, so that the
//BTM*** dataServiceMap can be populated with the created dataService
//BTM*** and then passed into the EmbeddedLoadBalancer so that the
//BTM*** EmbeddedLoadBalancer can "discover" the created DataService.
//BTM*** But once the DataService is converted to a smart proxy model
//BTM*** and the LocalDataServiceImpl instantiated below registers with
//BTM*** the lookup service, the dataServiceMap can be removed and a
//BTM*** non-null SDM can be used by the loadBalancerService to actually
//BTM*** discover that DataService.

//BTM            loadBalancerService = new AbstractEmbeddedLoadBalancerService(UUID
//BTM                    .randomUUID(), p) {
//BTM
//BTM                public LocalDataServiceFederation getFederation() {
//BTM
//BTM                    return LocalDataServiceFederation.this;
//BTM
//BTM                }
//BTM
//BTM            }.start();
        }

        {
            
            final Properties p = new Properties(properties);

            // set the directory for the data service.
            p.setProperty(Options.DATA_DIR, new File(dataDir, "ds").toString());

            /*
             * Note: The embedded data service does not support scale-out
             * indices. Use an embedded or distributed federation for that.
             * 
             * @todo the UUID of the data service might be best persisted with
             * the data service in case anything comes to rely on it, but as far
             * as I can tell nothing does or should.
             */

            // Disable overflow.
            p.setProperty(Options.OVERFLOW_ENABLED, "false");

            // create the embedded data service.
            dataService = new LocalDataServiceImpl(p).start();

//BTM*** remove after LocalDataServiceImpl/shard.ServiceImpl/EmbeddedDataService
//BTM*** is converted to smart proxy?
Map<UUID, DataService> dataServiceMap = new HashMap<UUID, DataService>();
dataServiceMap.put(dataService.getServiceUUID(), dataService);
loadBalancerService = new EmbeddedLoadBalancer(UUID.randomUUID(),
                                               hostname,
                                               null,//SDM - replace with real SDM after conversion to smart proxy?
p.getProperty(EmbeddedLoadBalancer.Options.LOG_DIR),
//BTM*** LocalDataServiceFederation.this,
//BTM*** remove after EmbeddedDataService is converted to smart proxy?
                                               dataServiceMap,
                                               p);

        }

        // notify service joins.
        {

//BTM            final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

            loadBalancerService.join(dataService.getServiceUUID(), dataService
                    .getServiceIface(), 
//BTM
dataService.getServiceName(),
hostname);

            loadBalancerService.join(dataService.getServiceUUID(),
                    loadBalancerService.getServiceIface(), 
//BTM
dataService.getServiceName(),
hostname);

        }
        
    }
    
    /**
     * The embedded {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class LocalDataServiceImpl extends AbstractEmbeddedDataService {
        
        LocalDataServiceImpl(Properties properties) {
            
            super(UUID.randomUUID(), properties);
            
        }

        @Override
        public LocalDataServiceFederation getFederation() {

            return LocalDataServiceFederation.this;
            
        }
        
        @Override
        public LocalDataServiceImpl start() {
            
            return (LocalDataServiceImpl) super.start();
            
        }
        
    }

    /**
     * Extended for type-safe return.
     */
    public LocalDataServiceClient<T> getClient() {

        return (LocalDataServiceClient<T>) super.getClient();
        
    }
    
    /**
     * Returns an array containing one element - the {@link UUID} of the local
     * {@link IDataService}.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {
        
        assertOpen();
        
        return new UUID[]{dataService.getServiceUUID()};
        
    }
    
    /**
     * Returns the local {@link IDataService}.
     */
    public LocalDataServiceImpl getAnyDataService() {

        assertOpen();
        
        return dataService;
        
    }

    /**
     * Return the local {@link DataService}.
     */
    public LocalDataServiceImpl getDataService() {
        
        assertOpen();
        
        return dataService;
        
    }

    /**
     * Return the {@link UUID} that identifies the local {@link IDataService}.
     */
    public UUID getDataServiceUUID() {

        assertOpen();
        
        return dataService.getServiceUUID();
        
    }
    
    /**
     * Registers an index that does not support scale-out.
     */
    public void registerIndex(IndexMetadata metadata) {
        
        assertOpen();
        
        try {

            registerIndex(metadata,getDataServiceUUID());
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Registers an index that does not support scale-out.
     */
    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {
        
        assertOpen();

        try {

            dataService.registerIndex(metadata.getName(), metadata);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
        return metadata.getIndexUUID();
    }

    /**
     * Since key-range partitioned indices are not supported this method will
     * log a warning and register the index on the local {@link IDataService} as
     * an unpartitioned index.
     */
    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        log.warn("key-range partitioned indices not supported: "+metadata.getName());
        
        registerIndex(metadata);
        
        return metadata.getIndexUUID();
        
    }

    private final DataServiceIndexCache indexCache;
    
    protected DataServiceIndexCache getIndexCache() {
        
        return indexCache;
        
    }
    
    public void dropIndex(String name) {

        assertOpen();

        try {
            
            dataService.dropIndex(name);
            
            getIndexCache().dropIndexFromCache(name);
            
        } catch (Exception e) {

            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Strengthens the return type.
     */
    public DataServiceIndex getIndex(String name, long timestamp) {

        return (DataServiceIndex) super.getIndex(name, timestamp);
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always since the metadata index is not used.
     */
    public IMetadataService getMetadataService() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always
     */
    public IMetadataIndex getMetadataIndex(String name, long timestamp) {

        throw new UnsupportedOperationException();
        
    }

//BTM    public ILoadBalancerService getLoadBalancerService() {
public LoadBalancer getLoadBalancerService() {

        assertOpen();

        return loadBalancerService;
        
    }
    
    public ITransactionService getTransactionService() {

        assertOpen();

        return abstractTransactionService;
        
    }

    public IResourceLockService getResourceLockService() {
        
        assertOpen();
        
        return resourceLockManager;
        
    }
    
    /**
     * Returns the embedded data service IFF the given serviceUUID is
     * the UUID for the embedded data service and <code>null</code>
     * otherwise.
     */
    public IDataService getDataService(final UUID serviceUUID) {

        assertOpen();
        
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        if (getDataServiceUUID().equals(serviceUUID)) {
            
            return dataService;
            
        }

        return null; 
        
    }

    /**
     * Return <code>false</code>.
     */
    public boolean isScaleOut() {
        
        return false;
        
    }
    
    /**
     * Return <code>false</code>.
     */
    public boolean isDistributed() {
        
        return false;
        
    }
    
    public boolean isStable() {
        
        assertOpen();
        
        return dataService.getResourceManager().getLiveJournal().isStable();

    }
    
    /**
     * Extended to shutdown the embedded services.
     */
    synchronized public void shutdown() {
        
        super.shutdown();

        indexCache.shutdown();
        
        if (dataService != null) {

            dataService.shutdown();

//            dataService = null;

        }

        if (loadBalancerService != null) {

            loadBalancerService.shutdown();

//            loadBalancerService = null;

        }

        if (abstractTransactionService != null) {

            abstractTransactionService.shutdown();

//            abstractTransactionService = null;

        }
        
    }
    
    /**
     * Extended to shutdown the embedded services.
     */
    synchronized public void shutdownNow() {

        super.shutdownNow();

        indexCache.shutdown();

        if (dataService != null) {

            dataService.shutdownNow();

//            dataService = null;

        }

        if (loadBalancerService != null) {

            loadBalancerService.shutdownNow();

//            loadBalancerService = null;

        }

        if (abstractTransactionService != null) {

            abstractTransactionService.shutdownNow();

//            abstractTransactionService = null;

        }
        
    }

    /**
     * Destroys the embedded services and disconnects from the federation.
     */
    public void destroy() {

        super.destroy();
        
        indexCache.shutdown();

        if (dataService != null) {

            dataService.destroy();
            
        }
        
        if (loadBalancerService != null) {

            loadBalancerService.destroy();

//            loadBalancerService = null;

        }

        if (abstractTransactionService != null) {

            abstractTransactionService.destroy();
        
//            abstractTransactionService = null;
            
        }

        if (dataDir.exists() && !dataDir.delete()) {

            log.warn("Could not delete: " + dataDir);
            
        }

    }

    public long getLastCommitTime() {

        assertOpen();
        
        return dataService.getResourceManager().getLiveJournal()
                .getRootBlockView().getLastCommitTime();

    }
    
    /**
     * Concrete implementation for a {@link LocalDataServiceFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DataServiceIndexCache extends AbstractIndexCache<DataServiceIndex> {

        private final LocalDataServiceFederation fed;
        
        /**
         * @param fed
         * @param capacity
         */
        protected DataServiceIndexCache(LocalDataServiceFederation fed,
                int capacity, long timeout) {

            super(capacity, timeout);

            if (fed == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;

        }

        @Override
        protected DataServiceIndex newView(String name, long timestamp) {
            
            try {

                // test for existence.
                fed.getDataService().getIndexMetadata(name, timestamp);

            } catch (Exception ex) {

                if (InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {

                    // No such index.
                    return null;

                }

                throw new RuntimeException(ex);
                
            }
            
            // exists, so create view.
            /*
             * @todo this double-fetches the IndexMetadata. optimize per
             * IndexCache variant of this class.
             */
            return new DataServiceIndex(fed, name, timestamp);
            
        }
        
    }

    public IDataService getDataServiceByName(final String name) {

        if(name==null) throw new IllegalArgumentException();
        
        if (dataService.getServiceName().equals(name)) {

            return dataService;

        } else {

            return null;

        }

    }

}
