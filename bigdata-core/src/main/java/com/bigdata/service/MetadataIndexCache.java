package com.bigdata.service;

import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IMetadataIndex;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.btree.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.util.InnerCause;

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
    
    private final AbstractScaleOutFederation fed;

    public MetadataIndexCache(final AbstractScaleOutFederation fed,
            final int capacity, final long timeout) {
        
        super(capacity, timeout);

        if (fed == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;

    }

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
                
        switch (fed.metadataIndexCachePolicy) {

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
            throw new AssertionError("Unknown option: "
                    + fed.metadataIndexCachePolicy);
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
IDataService remoteShardMgr = null;
ShardManagement shardMgr = null;
if(mds instanceof IDataService) {
log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: mds INSTANCE OF IDataService ["+mds+"]\n");
    remoteShardMgr = (IDataService)mds;
} else if(mds instanceof ShardManagement) {
log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: mds IS INSTANCE OF ShardManagement ["+mds+"]\n");
    shardMgr = (ShardManagement)mds;
}else {
log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: mds NOT instance of ShardManagement or IDataService ["+mds+"]\n");
}
log.warn("\nMetadataIndexCache.getMetadataIndexMetadata: remoteShardMgr="+remoteShardMgr+", shardMgr="+shardMgr+"\n");

        if (mds == null)
            throw new NoSuchService(ERR_NO_METADATA_SERVICE);

        MetadataIndexMetadata mdmd = null;
        try {

            // @todo test cache for this object as of that timestamp?
if(remoteShardMgr != null) {
            mdmd = (MetadataIndexMetadata) remoteShardMgr.getIndexMetadata(
                            MetadataService.getMetadataIndexName(name),
                            timestamp);
} else if(shardMgr != null) {
log.warn("\nMetadataIndexCache.getMetadataIndexMetadata >>>> CALLING shardMgr.getIndexMetadata <<<<\n");
            mdmd = (MetadataIndexMetadata) shardMgr.getIndexMetadata(
                            MetadataService.getMetadataIndexName(name),
                            timestamp);
}
            
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