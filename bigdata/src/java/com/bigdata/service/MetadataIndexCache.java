package com.bigdata.service;

import java.util.concurrent.ExecutionException;

import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.util.InnerCause;

//BTM
import com.bigdata.journal.IIndexStore;

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
    
//BTM - BEGIN
//BTM    private final AbstractScaleOutFederation fed;
//BTM    public MetadataIndexCache(final AbstractScaleOutFederation fed,
//BTM            final int capacity, final long timeout) {
//BTM        
//BTM        super(capacity, timeout);
//BTM
//BTM        if (fed == null)
//BTM            throw new IllegalArgumentException();
//BTM        
//BTM        this.fed = fed;
//BTM
//BTM    }

    private final IBigdataFederation fed;
    private final MetadataIndexCachePolicy metadataIndexCachePolicy;

    public MetadataIndexCache
               (final IBigdataFederation fed,
                final MetadataIndexCachePolicy metadataIndexCachePolicy,
                final int capacity,
                final long timeout)
    {
        super(capacity, timeout);
        if (fed == null) {
            throw new IllegalArgumentException("null federation");
        }
        this.fed = fed;
        this.metadataIndexCachePolicy = 
            (metadataIndexCachePolicy == null ? 
                 MetadataIndexCachePolicy.CacheAll 
                 : metadataIndexCachePolicy);
    }

    public MetadataIndexCache
               (final AbstractScaleOutFederation fed,
                final int capacity,
                final long timeout)
    {
        this((IBigdataFederation)fed, fed.metadataIndexCachePolicy,
             capacity, timeout);
    }
//BTM - END

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
        
            return new NoCacheMetadataIndexView(fed, name, timestamp, mdmd);
            
        }

        case CacheAll: {

            if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {

                /*
                 * A class that is willing to update its cache if the client
                 * discovers stale locators.
                 */
                
                return new CachingMetadataIndex(fed, name, timestamp, mdmd);

            } else {

                /*
                 * A class that caches all the locators. This is used for
                 * historical reads since the locators can not become stale.
                 */
                
                return new CacheOnceMetadataIndex(fed, name, timestamp, mdmd);
                
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
final ShardLocator mds = fed.getMetadataService();

//BTM - BEGIN - IDATA_SERVICE TO SHARD_SERVICE
//BTM IDataService remoteShardMgr = null;
//BTM ShardManagement shardMgr = null;
//BTM if(mds instanceof IDataService) {
//BTM log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: mds INSTANCE OF IDataService ["+mds+"]\n");
//BTM     remoteShardMgr = (IDataService)mds;
//BTM } else if(mds instanceof ShardManagement) {
//BTM log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: mds IS INSTANCE OF ShardManagement ["+mds+"]\n");
//BTM     shardMgr = (ShardManagement)mds;
//BTM }else {
//BTM log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: mds NOT instance of ShardManagement or IDataService ["+mds+"]\n");
//BTM }
//BTM log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: remoteShardMgr="+remoteShardMgr+", shardMgr="+shardMgr+"\n");
//BTM - END - IDATA_SERVICE TO SHARD_SERVICE

        if (mds == null)
            throw new NoSuchService(ERR_NO_METADATA_SERVICE);

        MetadataIndexMetadata mdmd = null;
        try {

            // @todo test cache for this object as of that timestamp?
//BTM - BEGIN - IDATA_SERVICE TO SHARD_SERVICE
//BTM if(remoteShardMgr != null) {
//BTM             mdmd = (MetadataIndexMetadata) remoteShardMgr.getIndexMetadata(
//BTM                             MetadataService.getMetadataIndexName(name),
//BTM                             timestamp);
//BTM } else if(shardMgr != null) {
//BTM log.warn("\nMetadataIndexCache.getMetadataIndexMetadata >>>> CALLING shardMgr.getIndexMetadata <<<<\n");
//BTM             mdmd = (MetadataIndexMetadata) shardMgr.getIndexMetadata(
//BTM                             MetadataService.getMetadataIndexName(name),
//BTM                             timestamp);
//BTM }

mdmd = (MetadataIndexMetadata) ((ShardManagement)mds).getIndexMetadata(MetadataService.getMetadataIndexName(name), timestamp);

//BTM - END - IDATA_SERVICE TO SHARD_SERVICE

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