package com.bigdata.service;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.RangeCountProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.service.ndx.RawDataServiceTupleIterator;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.metadata.EmbeddedShardLocator;
import java.rmi.Remote;

/**
 * An implementation that performs NO caching. All methods read through to the
 * remote metadata index. Basically, this hides the RMI requests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NoCacheMetadataIndexView implements IMetadataIndex {

//BTM    final private AbstractScaleOutFederation fed;
//BTM - PRE_CLIENT_SERVICE final private IBigdataFederation fed;
    final private IBigdataDiscoveryManagement discoveryManager;

    final private String name;

    final private long timestamp;

    final private MetadataIndexMetadata mdmd;

//BTM - PRE_CLIENT_SERVICE    private ShardManagement shardMgr = null; //BTM - IDATA_SERVICE TO SHARD_SERVICE

    public MetadataIndexMetadata getIndexMetadata() {

        return mdmd;
        
    }

//BTM    protected IMetadataService getMetadataService() {
    protected ShardLocator getMetadataService() {

//BTM - PRE_CLIENT_SERVICE        return fed.getMetadataService();
        return discoveryManager.getMetadataService();

    }

    /**
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     */
//BTM    public NoCacheMetadataIndexView(AbstractScaleOutFederation fed,
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICEpublic NoCacheMetadataIndexView(IBigdataFederation fed,
//BTM - PRE_CLIENT_SERVICE            String name, long timestamp, MetadataIndexMetadata mdmd) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        if (fed == null)
//BTM - PRE_CLIENT_SERVICE            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE
    public NoCacheMetadataIndexView
               (IBigdataDiscoveryManagement discoveryManager,
                String name,
                long timestamp,
                MetadataIndexMetadata mdmd)
    {
        if (discoveryManager == null) {
            throw new NullPointerException("null discoveryManager");
        }
//BTM - PRE_CLIENT_SERVICE - END
        if (name == null)
            throw new IllegalArgumentException();
        if (mdmd == null)
            throw new IllegalArgumentException();

//BTM - PRE_CLIENT_SERVICE        this.fed = fed;
        this.discoveryManager = discoveryManager;

        this.name = name;

        this.timestamp = timestamp;

        this.mdmd = mdmd;

//BTM - PRE_CLIENT_SERVICE        ShardLocator mds = getMetadataService();//BTM
//BTM - PRE_CLIENT_SERVICE        if(mds == null) return;                 //BTM
//BTM - PRE_CLIENT_SERVICE        this.shardMgr = (ShardManagement)mds;   //BTM - IDATA_SERVICE TO SHARD_SERVICE
    }
    
    // @todo re-fetch if READ_COMMITTED or UNISOLATED? it's very unlikely to change.
    public IndexMetadata getScaleOutIndexMetadata() {

        return mdmd;
        
    }

    // this could be cached easily, but its only used by the unit tests.
    public PartitionLocator get(byte[] key) {

        try {

            return getMetadataService().get(name, timestamp, key);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    // harder to cache - must look for "gaps"
    public PartitionLocator find(byte[] key) {

        try {

            return getMetadataService().find(name, timestamp, key);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    public long rangeCount() {

        return rangeCount(null, null);
        
    }

    // only used by unit tests.
    public long rangeCount(final byte[] fromKey, final byte[] toKey) {

        final IIndexProcedure proc = new RangeCountProcedure(
                false/* exact */, false/*deleted*/, fromKey, toKey);

//BTM        final Long rangeCount;
        Long rangeCount = null; //BTM
        try {
//BTM            rangeCount = (Long) getMetadataService().submit(timestamp,
//BTM                    MetadataService.getMetadataIndexName(name), proc).get();

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  String indexName = MetadataService.getMetadataIndexName(name);
            String indexName = null;
            ShardLocator mds = getMetadataService();
            ShardManagement shardMgr = (ShardManagement)mds;
            if (mds instanceof Remote) {
                indexName = MetadataService.getMetadataIndexName(name);
            } else {
                indexName = EmbeddedShardLocator.getMetadataIndexName(name);
            }
            if(indexName == null) {
                throw new NullPointerException
                              ("NoCacheMetadataIndexView.rangeCount "
                               +"[null indexName]");
            }
//BTM - PRE_CLIENT_SERVICE - END

//BTM - IDATA_SERVICE TO SHARD_SERVICE - BEGIN
            rangeCount =
                (Long) shardMgr.submit(timestamp, indexName, proc).get();
            if(rangeCount == null) { //BTM - BEGIN
                throw new NullPointerException
                              ("NoCacheMetadataIndexView.rangeCount "
                               +"[null range count]");
            }
//BTM - IDATA_SERVICE TO SHARD_SERVICE - END
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rangeCount.longValue();
    }

    public long rangeCountExact(final byte[] fromKey, final byte[] toKey) {

        final IIndexProcedure proc = new RangeCountProcedure(
                true/* exact */, false/*deleted*/, fromKey, toKey);

//BTM        final Long rangeCount;
        Long rangeCount = null; //BTM
        try {

//BTM            rangeCount = (Long) getMetadataService().submit(timestamp,
//BTM                    MetadataService.getMetadataIndexName(name), proc).get();

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  String indexName = MetadataService.getMetadataIndexName(name);
            String indexName = null;
            ShardLocator mds = getMetadataService();
            ShardManagement shardMgr = (ShardManagement)mds;
            if (mds instanceof Remote) {
                indexName = MetadataService.getMetadataIndexName(name);
            } else {
                indexName = EmbeddedShardLocator.getMetadataIndexName(name);
            }
            if(indexName == null) {
                throw new NullPointerException
                              ("NoCacheMetadataIndexView.rangeCountExact "
                               +"[null indexName]");
            }
//BTM - PRE_CLIENT_SERVICE - END

//BTM - IDATA_SERVICE TO SHARD_SERVICE - BEGIN
            rangeCount =
                (Long) shardMgr.submit(timestamp, indexName, proc).get();
            if(rangeCount == null) { //BTM - BEGIN
                throw new NullPointerException
                              ("NoCacheMetadataIndexView.rangeCountExact "
                               +"[null range count]");
            }
//BTM - IDATA_SERVICE TO SHARD_SERVICE - END
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rangeCount.longValue();
    }
    
    public long rangeCountExactWithDeleted(final byte[] fromKey, final byte[] toKey) {

        final IIndexProcedure proc = new RangeCountProcedure(
                true/* exact */, true/*deleted*/, fromKey, toKey);

//BTM        final Long rangeCount;
        Long rangeCount = null; //BTM
        try {

//BTM            rangeCount = (Long) getMetadataService().submit(timestamp,
//BTM                    MetadataService.getMetadataIndexName(name), proc).get();

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  String indexName = MetadataService.getMetadataIndexName(name);
            String indexName = null;
            ShardLocator mds = getMetadataService();
            ShardManagement shardMgr = (ShardManagement)mds;
            if (mds instanceof Remote) {
                indexName = MetadataService.getMetadataIndexName(name);
            } else {
                indexName = EmbeddedShardLocator.getMetadataIndexName(name);
            }
            if(indexName == null) {
                throw new NullPointerException
                       ("NoCacheMetadataIndexView.rangeCountExactWithDeleted "
                        +"[null indexName]");
            }
//BTM - PRE_CLIENT_SERVICE - END

//BTM - IDATA_SERVICE TO SHARD_SERVICE - BEGIN
            rangeCount =
                (Long) shardMgr.submit(timestamp, indexName, proc).get();
            if(rangeCount == null) { //BTM - BEGIN
                throw new NullPointerException
                       ("NoCacheMetadataIndexView.rangeCountExactWithDeleted "
                        +"[null range count]");
            }
//BTM - IDATA_SERVICE TO SHARD_SERVICE - END
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rangeCount.longValue();
    }
    
    public ITupleIterator rangeIterator() {
        
        return rangeIterator(null,null);
        
    }
    
    public ITupleIterator rangeIterator(final byte[] fromKey, final byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                IRangeQuery.DEFAULT, null/* filter */);

    }

    /**
     * Note: Since this view is read-only this method forces the use of
     * {@link ITx#READ_COMMITTED} IFF the timestamp for the view is
     * {@link ITx#UNISOLATED}. This produces the same results on read and
     * reduces contention for the {@link WriteExecutorService}. This is
     * already done automatically for anything that gets run as an index
     * procedure, so we only have to do this explicitly for the range
     * iterator method.
     */
    // not so interesting to cache, but could cache the iterator results on the scale-out index.
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilterConstructor filter) {

        return new RawDataServiceTupleIterator(getMetadataService(),//
                MetadataService.getMetadataIndexName(name), //
                (timestamp==ITx.UNISOLATED?ITx.READ_COMMITTED:timestamp),//
                true, // read-consistent semantics.
                fromKey,//
                toKey,//
                capacity,//
                flags, //
                filter
        );

    }

    /**
     * NOP since nothing is cached.
     */
    public void staleLocator(PartitionLocator locator) {
        
    }

}
