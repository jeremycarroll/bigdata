package com.bigdata.service;

import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.service.ndx.IScaleOutClientIndex;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IScaleOutIndexStore;
import com.bigdata.resources.ILocalResourceManagement;

/**
 * Concrete implementation for {@link IClientIndex} views.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 */
public class IndexCache extends AbstractIndexCache<IScaleOutClientIndex>{

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    private final AbstractScaleOutFederation fed;
//BTM - PRE_CLIENT_SERVICE    
//BTM - PRE_CLIENT_SERVICE    public IndexCache(final AbstractScaleOutFederation fed, final int capacity, final long timeout) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        super(capacity, timeout);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (fed == null)
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE        
//BTM - PRE_CLIENT_SERVICE        this.fed = fed;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    }

    private IBigdataDiscoveryManagement discoveryManager;
    private ILocalResourceManagement localResourceManager;
    private IScaleOutIndexStore indexStore;
    private int defaultRangeQueryCapacity;
    private boolean batchApiOnly;
    private long taskTimeout;
    private int maxParallelTasksPerRequest;
    private int maxStaleLocatorRetries;

    public IndexCache
               (final IBigdataDiscoveryManagement discoveryManager,
                final ILocalResourceManagement localResourceManager,
                final IScaleOutIndexStore indexStore,
                final int capacity,
                final long timeout,
                      int defaultRangeQueryCapacity,
                      boolean batchApiOnly,
                      long taskTimeout,
                      int maxParallelTasksPerRequest,
                      int maxStaleLocatorRetries)
    {

        super(capacity, timeout);

        if (discoveryManager == null) {
            throw new NullPointerException("null discoveryManager");
        }
        if (localResourceManager == null) {
            throw new NullPointerException("null localResourceManager");
        }
        if (indexStore == null) {
            throw new NullPointerException("null indexStore");
        }
        this.discoveryManager = discoveryManager;
        this.localResourceManager = localResourceManager;
        this.indexStore = indexStore;
        this.defaultRangeQueryCapacity = defaultRangeQueryCapacity;
        this.batchApiOnly = batchApiOnly;
        this.taskTimeout = taskTimeout;
        this.maxParallelTasksPerRequest = maxParallelTasksPerRequest;
        this.maxStaleLocatorRetries = maxStaleLocatorRetries;
    }
//BTM - PRE_CLIENT_SERVICE - END
    
    @Override
    protected ClientIndexView newView(final String name, final long timestamp) {
        
//BTM - PRE_CLIENT_SERVICE  final IMetadataIndex mdi = fed.getMetadataIndex(name, timestamp);
        final IMetadataIndex mdi =
                  indexStore.getMetadataIndex(name, timestamp);

        // No such index.
        if (mdi == null) {

            if (INFO)
                log.info("name=" + name + " @ " + timestamp
                        + " : is not registered");
//BTM
log.warn("\nIndexCache.newView >>>> META_DATA INDEX = NULL\n");
            return null;

        }

        // Index exists.
//BTM - PRE_CLIENT_SERVICE   return new ClientIndexView(fed, name, timestamp, mdi);
        return new ClientIndexView(discoveryManager,
                                   localResourceManager,
                                   indexStore,
                                   name,
                                   timestamp,
                                   mdi,
                                   defaultRangeQueryCapacity,
                                   batchApiOnly,
                                   taskTimeout,
                                   maxParallelTasksPerRequest,
                                   maxStaleLocatorRetries);
    }
 
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    protected void dropIndexFromCache(String name) {
//BTM - PRE_CLIENT_SERVICE        // drop the index from the cache.
//BTM - PRE_CLIENT_SERVICE        super.dropIndexFromCache(name);
//BTM - PRE_CLIENT_SERVICE        
//BTM - PRE_CLIENT_SERVICE        // and drop the metadata index from its cache as well.
//BTM - PRE_CLIENT_SERVICE        fed.getMetadataIndexCache().dropIndexFromCache(name);        
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE - because of different packages (com.bigdata.service vs com.bigdata.journal),
//BTM - PRE_CLIENT_SERVICE - changed to public so that ScaleOutIndexManager can call dropIndexFromCache()
//BTM - PRE_CLIENT_SERVICE - MetadataIndexCache it maintains
//BTM - PRE_CLIENT_SERVICE
    public void dropIndexFromCache(String name,
                                   MetadataIndexCache metadataIndexCache)
    {
        if (name == null) {
            throw new NullPointerException("null name");
        }
        if (metadataIndexCache == null) {
            throw new NullPointerException("null metadataIndexCache");
        }
        // drop the index from the cache.
        super.dropIndexFromCache(name);
        
        // and drop the metadata index from its cache as well.
        metadataIndexCache.dropIndexFromCache(name);
        
    }
//BTM - PRE_CLIENT_SERVICE - END
}
