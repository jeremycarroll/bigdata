package com.bigdata.service;

import java.util.concurrent.ExecutionException;

import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.util.InnerCause;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IIndexManager;

/**
 * Concrete implementation for {@link IMetadataIndex} views.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public class MetadataIndexCache extends AbstractIndexCache<IMetadataIndex>{

    /**
     * Text for an exception thrown when the metadata service has not been
     * discovered.
     */
    protected static transient final String ERR_NO_METADATA_SERVICE = "Metadata service";
    
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE//BTM - BEGIN
//BTM - PRE_CLIENT_SERVICE//BTM    private final AbstractScaleOutFederation fed;
//BTM - PRE_CLIENT_SERVICE//BTM    public MetadataIndexCache(final AbstractScaleOutFederation fed,
//BTM - PRE_CLIENT_SERVICE//BTM            final int capacity, final long timeout) {
//BTM - PRE_CLIENT_SERVICE//BTM        
//BTM - PRE_CLIENT_SERVICE//BTM        super(capacity, timeout);
//BTM - PRE_CLIENT_SERVICE//BTM
//BTM - PRE_CLIENT_SERVICE//BTM        if (fed == null)
//BTM - PRE_CLIENT_SERVICE//BTM            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE//BTM        
//BTM - PRE_CLIENT_SERVICE//BTM        this.fed = fed;
//BTM - PRE_CLIENT_SERVICE//BTM
//BTM - PRE_CLIENT_SERVICE//BTM    }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    private final IBigdataFederation fed;
//BTM - PRE_CLIENT_SERVICE    private final MetadataIndexCachePolicy metadataIndexCachePolicy;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    public MetadataIndexCache
//BTM - PRE_CLIENT_SERVICE               (final IBigdataFederation fed,
//BTM - PRE_CLIENT_SERVICE                final MetadataIndexCachePolicy metadataIndexCachePolicy,
//BTM - PRE_CLIENT_SERVICE                final int capacity,
//BTM - PRE_CLIENT_SERVICE                final long timeout)
//BTM - PRE_CLIENT_SERVICE    {
//BTM - PRE_CLIENT_SERVICE        super(capacity, timeout);
//BTM - PRE_CLIENT_SERVICE        if (fed == null) {
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException("null federation");
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE        this.fed = fed;
//BTM - PRE_CLIENT_SERVICE        this.metadataIndexCachePolicy = 
//BTM - PRE_CLIENT_SERVICE            (metadataIndexCachePolicy == null ? 
//BTM - PRE_CLIENT_SERVICE                 MetadataIndexCachePolicy.CacheAll 
//BTM - PRE_CLIENT_SERVICE                 : metadataIndexCachePolicy);
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    public MetadataIndexCache
//BTM - PRE_CLIENT_SERVICE               (final AbstractScaleOutFederation fed,
//BTM - PRE_CLIENT_SERVICE                final int capacity,
//BTM - PRE_CLIENT_SERVICE                final long timeout)
//BTM - PRE_CLIENT_SERVICE    {
//BTM - PRE_CLIENT_SERVICE        this((IBigdataFederation)fed, fed.metadataIndexCachePolicy,
//BTM - PRE_CLIENT_SERVICE             capacity, timeout);
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE//BTM - END

    private final IBigdataDiscoveryManagement discoveryManager;
    private final IIndexManager indexManager;
    private final MetadataIndexCachePolicy metadataIndexCachePolicy;

    public MetadataIndexCache
               (final IBigdataDiscoveryManagement discoveryManager,
                final IIndexManager indexManager,
                final MetadataIndexCachePolicy metadataIndexCachePolicy,
                final int capacity,
                final long timeout)
    {
        super(capacity, timeout);
        if (discoveryManager == null) {
            throw new IllegalArgumentException("null discoveryManager");
        }
        if (indexManager == null) {
            throw new IllegalArgumentException("null indexManager");
        }
        this.discoveryManager = discoveryManager;
        this.indexManager = indexManager;
        this.metadataIndexCachePolicy = 
            (metadataIndexCachePolicy == null ? 
                 MetadataIndexCachePolicy.CacheAll 
                 : metadataIndexCachePolicy);
    }
//BTM - PRE_CLIENT_SERVICE - END

    @Override
    protected IMetadataIndex newView(String name, long timestamp) {
        
        final MetadataIndexMetadata mdmd = getMetadataIndexMetadata(
                name, timestamp);
        
        // No such index.
        if (mdmd == null) {
//BTM
log.warn("\nMetadataIndexCache.newView >>>> META_DATA INDEX META_DATA = NULL\n");
            return null;
        }
                
//BTM        switch (fed.metadataIndexCachePolicy) {
switch (metadataIndexCachePolicy) {

        case NoCache: { 
        
//BTM - PRE_CLIENT_SERVICE  return new NoCacheMetadataIndexView(fed, name, timestamp, mdmd);
            return new NoCacheMetadataIndexView
                           (discoveryManager, name, timestamp, mdmd);
        }

        case CacheAll: {

            if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {

                /*
                 * A class that is willing to update its cache if the client
                 * discovers stale locators.
                 */
                
//BTM - PRE_CLIENT_SERVICE  return new CachingMetadataIndex(fed, name, timestamp, mdmd);
                return new CachingMetadataIndex
                               (discoveryManager, indexManager,
                                name, timestamp, mdmd);

            } else {

                /*
                 * A class that caches all the locators. This is used for
                 * historical reads since the locators can not become stale.
                 */
                
//BTM - PRE_CLIENT_SERVICE  return new CacheOnceMetadataIndex(fed, name, timestamp, mdmd);
                return new CacheOnceMetadataIndex
                               (discoveryManager, indexManager,
                                name, timestamp, mdmd);
            }
        }
        default:
//BTM            throw new AssertionError("Unknown option: " + fed.metadataIndexCachePolicy);
throw new AssertionError("Unknown option: " + metadataIndexCachePolicy);
        }
        
    }
    
    /**
     * Return the metadata for the metadata index itself.
     * <p>
     * Note: This method always reads through!
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param timestamp
     * 
     * @return The metadata for the metadata index or <code>null</code>
     *         iff no scale-out index is registered by that name at that
     *         timestamp.
     */
    protected MetadataIndexMetadata getMetadataIndexMetadata(final String name,
            final long timestamp) {

//BTM        final IMetadataService mds = fed.getMetadataService();
//BTM - PRE_CLIENT_SERVICE final ShardLocator mds = fed.getMetadataService(); //BTM - IDATA_SERVICE TO SHARD_SERVICE
        final ShardLocator mds = discoveryManager.getMetadataService(); //BTM - FOR_CLIENT_SERVICE

        if (mds == null)
            throw new NoSuchService(ERR_NO_METADATA_SERVICE);

        MetadataIndexMetadata mdmd = null;
        try {

            // @todo test cache for this object as of that timestamp?
//BTM - IDATA_SERVICE TO SHARD_SERVICE
            mdmd = 
            (MetadataIndexMetadata) ((ShardManagement)mds).getIndexMetadata
                      (MetadataService.getMetadataIndexName(name), timestamp);

            assert mdmd != null;

        } catch( NoSuchIndexException ex ) {
            
            return null;
        
        } catch (ExecutionException ex) {
            
            if (InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {

                // per API.
                return null;
                
            }
            
            throw new RuntimeException(ex);
            
        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
        if (mdmd == null) {

            // No such index.
            
            return null;

        }
        
        return mdmd;

    }
    
}