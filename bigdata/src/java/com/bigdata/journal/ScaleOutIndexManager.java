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

package com.bigdata.journal;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.journal.TransactionService;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.service.IndexCache;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.MetadataIndexCache;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.config.LogUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import com.bigdata.zookeeper.ZooResourceLockService;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

// Note: IIndexManager sub-classes IIndexStore

public class ScaleOutIndexManager implements IScaleOutIndexManager {

    private IBigdataDiscoveryManagement discoveryMgr;
    private ILocalResourceManagement localResources;
    private ZooKeeperAccessor zkAccessor;
    private List<ACL> zkAcl;
    private String zkRoot;
    private Logger logger;
    private Properties properties;

    private IndexCache indexCache;
    private MetadataIndexCache metadataIndexCache;
    private GlobalRowStoreHelper globalRowStoreHelper;
    private GlobalFileSystemHelper globalFileSystemHelper;
    private TemporaryStoreFactory tempStoreFactory;
    private DefaultResourceLocator resourceLocator;
    private ZooResourceLockService resourceLockService;
    private long lastKnownCommitTime;

    private IConcurrencyManager concurrencyMgr;//WARN: circular ref

    public ScaleOutIndexManager
               (IBigdataDiscoveryManagement discoveryMgr,
                ILocalResourceManagement localResourceMgr,
                ZooKeeperAccessor zookeeperAccessor,
                List<ACL> zookeeperAcl,
                String zookeeperRoot,
                int indexCacheSize,
                long indexCacheTimeout,
                MetadataIndexCachePolicy metadataIndexCachePolicy,
                int resourceLocatorCacheSize,
                long resourceLocatorCacheTimeout,
                int defaultRangeQueryCapacity,
                boolean batchApiOnly,
                long taskTimeout,
                int maxParallelTasksPerRequest,
                int maxStaleLocatorRetries,
                Logger logger,
                Properties properties)
    {
        this.discoveryMgr = discoveryMgr;
        this.localResources = localResourceMgr;
        this.zkAccessor = zookeeperAccessor;
        this.zkAcl = zookeeperAcl;
        this.zkRoot = zookeeperRoot;
        this.logger = (logger == null ? 
                       LogUtil.getLog4jLogger((this.getClass()).getName()) :
                       logger);
        this.properties = (Properties)properties.clone();

        this.indexCache = 
            new IndexCache(this.discoveryMgr,
                           this.localResources,
                           this,
                           indexCacheSize,
                           indexCacheTimeout,
                           defaultRangeQueryCapacity,
                           batchApiOnly,
                           taskTimeout,
                           maxParallelTasksPerRequest,
                           maxStaleLocatorRetries);

        this.metadataIndexCache = new MetadataIndexCache
                                          (this.discoveryMgr,
                                           this,
                                           metadataIndexCachePolicy,
                                           indexCacheSize,
                                           indexCacheTimeout);
        this.globalRowStoreHelper = new GlobalRowStoreHelper(this);
        this.globalFileSystemHelper = new GlobalFileSystemHelper(this);
        this.tempStoreFactory = new TemporaryStoreFactory(this.properties);
        this.resourceLocator = 
            new DefaultResourceLocator(this, null, resourceLocatorCacheSize,
                                       resourceLocatorCacheTimeout);
        if ( (zkAccessor != null) && (zkAcl != null) && (zkRoot != null) ) {
            this.resourceLockService = 
                new ZooResourceLockService(zkAccessor, zkAcl, zkRoot);
        }
    }

    // Required by IScaleOutIndexStore

    public ClientIndexView getIndex(String name, long timestamp) {
        IRangeQuery index = indexCache.getIndex(name, timestamp);
        if ( logger.isDebugEnabled() ) {
            logger.debug("ScaleOutIndexManager.getIndex: [name="+name+", "
                         +"timestamp="+timestamp+", indexCache="+indexCache
                         +", index="+index+"]");
        }
        return (ClientIndexView)index;
    }

    public IMetadataIndex getMetadataIndex(String name, long timestamp) {
        return metadataIndexCache.getIndex(name, timestamp);
    }

    public Iterator<PartitionLocator> locatorScan(final String name,
                                                  final long timestamp,
                                                  final byte[] fromKey,
                                                  final byte[] toKey,
                                                  final boolean reverseScan)
    {
        if (logger.isDebugEnabled()) {
            logger.debug("ScaleOutIndexManager.getIndex: "
                         +"querying metadata index "
                         +"[name="+name+", timestamp="+timestamp
                         +", reverseScan="+reverseScan
                         +", fromKey="+BytesUtil.toString(fromKey)
                         +", toKey="+BytesUtil.toString(toKey));
        }

        final IMetadataIndex mdi = getMetadataIndex(name, timestamp);
        final ITupleIterator<PartitionLocator> itr;

        // the values are the locators (keys are not required).
        final int flags = IRangeQuery.VALS;

        if (reverseScan) {//reverse locator scan

            // The first locator visited will be the first index
            // partition whose leftSeparator is LT the optional
            // toKey. (If the toKey falls on an index partition
            // boundary then we use the prior index partition).

            itr = mdi.rangeIterator(fromKey,
                                    toKey,
                                    0, //capacity
                                    flags | IRangeQuery.REVERSE,
                                    null); //filter
        } else {//forward locator scan

            // Note: The scan on the metadata index needs to start
            //       at the index partition in which the fromKey
            //       would be located. Therefore, when the fromKey
            //       is specified, we replace it with the leftSeparator
            //       of the index partition which would contain that
            //       fromKey.

            final byte[] _fromKey = 
                fromKey == null ? null
                                : mdi.find(fromKey).getLeftSeparatorKey();

            itr = mdi.rangeIterator(_fromKey,
                                    toKey,
                                    0, //capacity
                                    flags,
                                    null); // filter
        }

        return new Striterator(itr).addFilter
               (new Resolver()
                    {
                        private static final long serialVersionUID =
                                                      7874887729130530971L;
                        @Override
                        protected Object resolve(Object obj) { 
                            final ITuple<PartitionLocator> tuple =
                                      (ITuple<PartitionLocator>) obj;
                            return tuple.getObject();                
                        }
                    });
    }

    // Required by IIndexStore

    public SparseRowStore getGlobalRowStore() {
        return globalRowStoreHelper.getGlobalRowStore();
    }

    public BigdataFileSystem getGlobalFileSystem() {
        return globalFileSystemHelper.getGlobalFileSystem
                                          (concurrencyMgr, discoveryMgr);
    }

    public TemporaryStore getTempStore() {
        return tempStoreFactory.getTempStore(concurrencyMgr, discoveryMgr);
    }

    public IResourceLocator getResourceLocator() {
        return resourceLocator;
    }

    public ExecutorService getExecutorService() {
        return localResources.getThreadPool();
    }

    public IResourceLockService getResourceLockService() {
        return resourceLockService;
    }

    public long getLastCommitTime() {
        TransactionService txnService = discoveryMgr.getTransactionService();
        if (txnService != null) {
            try {
                lastKnownCommitTime = txnService.getLastCommitTime();
            } catch (IOException e) {
                logger.error("timestamp service not reachable", e);
                // fall through - report the lastKnownCommitTime.
            }
        }
        return lastKnownCommitTime;
    }

    public void destroy() {
        tempStoreFactory.closeAll();
    }

    // Required IIndexManager

    public void registerIndex(IndexMetadata metadata) {
        registerIndex(metadata, null);
    }

    public void dropIndex(String name) {
        ShardLocator mds = discoveryMgr.getMetadataService();
        try {
            if (mds != null) {
                mds.dropScaleOutIndex(name);
            } else {
                logger.warn("could not drop scale-out index "
                            +"[name="+name+"] - shard locator "
                            +"unreachable");
            }
            indexCache.dropIndexFromCache(name, metadataIndexCache);
            if (logger.isDebugEnabled()) {
                logger.info("dropped scale-out index [name="+name+"]");
            }
        } catch (Exception e) {
            logger.error("while dropping scale-out index - "+e, e);
            throw new RuntimeException( e );//preserve original logic
        }
    }

    // Additional public methods not specified by an interface

    /**
     * Register a scale-out index and assign the initial index partition
     * to the shard service having the given <code>UUID</code>.
     * 
     * @param metadata
     *            The metadata template used to create component indices
     *            for <code>BTree</code> instances of the scale-out
     *            index being managed. Note that this parameter also
     *            specifies the name of the scale-out index.
     *
     * @param dataServiceUUID
     *            The <code>UUID</code> of the shard service onto which
     *            the initial index partition will be mapped. The value
     *            input for this parameter may be <code>null</code>. If
     *            <code>null</code> is input for this parameter, then
     *            the following criteria is applied when determining
     *            the shard service onto which the index is mapped:
     *            <ul>
     *              <li> If the given <code>metadata</code> parameter
     *                   specifies an initial shard service onto which
     *                   to map the index, then that shard service will
     *                   be used.
     *              <li> If the given <code>metadata</code> parameter does
     *                   not specify an initial shard service, and a load
     *                   balancer can be found, then the index partition
     *                   will be mapped onto an under-utilized shard service
     *                   as identified by the discovered load balancer.
     *              <li> If the given <code>metadata</code> parameter does
     *                   not specify an initial shard service, and a load
     *                   balancer can not be found, then a randomly selected
     *                   (implementation-dependent) shard service will be
     *                   used for the mapping.
     *            </ul>
     * 
     * @return The UUID of the registered index.
     * 
     * @see IndexMetadata.Options#INITIAL_DATA_SERVICE
     * 
     * @todo change to void return
     *
     * (Adapted from AbstractFederation).
     */
    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {

        UUID dsUUID = dataServiceUUID;

        if (dsUUID == null) {
            // Find the UUID of the shard (data) service with which to
            // register the named index by doing the following:
            //
            // 1. retrieve the desired UUID from the given IndexMetadata
            //    and, if non-null, register the index with the shard
            //    (data) service associated with that UUID
            // 2. if the UUID from the IndexMetadata is null, then request
            //    a reference to an under-utilized shard (data) service
            //    from the load balancer, and register the index with
            //    that service
            // 3. if the load balancer is not available, then choose any
            //    shard (data) service that is available and register
            //    the index with that service

            dsUUID = metadata.getInitialDataServiceUUID();//1. retrieve UUID
            
            if (dsUUID == null) {//try load balancer
                //2. ask lbs for under-utilized shard
                LoadBalancer lbs = discoveryMgr.getLoadBalancerService();
                if (lbs != null) {
                    try {
                        dsUUID = lbs.getUnderUtilizedDataService();
                    } catch (Exception e) {
                        logger.warn(e);
                    }
                }
                if(dsUUID == null) {//3. try any data service
                    dsUUID = (discoveryMgr.getDataServiceUUIDs(1))[0];
                }
                if(dsUUID == null) {
                    logger.warn
                        ("no shard service available for index registration");
                    return null;
                }
            }
        }
        byte[][] separatorKeys = new byte[][] { new byte[] {} };
        UUID[] dsUUIDArray = new UUID[] { dsUUID };
 
        return registerIndex(metadata, separatorKeys, dsUUIDArray);
    }

    /**
     * Register and statically partition a scale-out index.
     * 
     * @param metadata 
     *            The metadata template used to create component indices
     *            for <code>BTree</code> instances of the scale-out
     *            index being managed. Note that this parameter also
     *            specifies the name of the scale-out index.
     *
     * @param separatorKeys
     *            The array of separator keys. Each separator key is
     *            interpreted as an <em>unsigned byte[]</em> array. The
     *            first element of this parameter MUST be an empty byte[],
     *            and all elements of the parameter MUST be in sorted order.
     *
     * @param dataServiceUUIDs
     *            The array of shard services onto which each partition
     *            defined by a separator key will be mapped. The value
     *            input for this parameter may be <code>null</code>.
     *            When a non-<code>null</code> value is input, the number
     *            of elements in this parameter MUST equal the number of\
     *            elements of the <code>separatorKeys</code> parameter,
     *            and all elements of this parameter must be
     *            non-<code>null</code>. When <code>null</code> is input
     *            for this parameter, the index partitions will be
     *            auto-assigned to the discovered shard services.
     * 
     * @return The UUID of the scale-out index.
     * 
     * @todo change to void return
     *
     * (Adapted from AbstractFederation).
     */
    public UUID registerIndex(IndexMetadata metadata,
                              byte[][] separatorKeys,
                              UUID[] dataServiceUUIDs)
    {
        if (metadata == null) {
            throw new NullPointerException("null index metadata");
        }
        if (metadata.getName() == null) {
            throw new NullPointerException("null index name");
        }
        try {
            ShardLocator mds = discoveryMgr.getMetadataService();
            return mds.registerScaleOutIndex
                           (metadata, separatorKeys, dataServiceUUIDs);
        } catch (Exception e) {
            logger.error("on index registration", e);
            throw new RuntimeException(e);
        }
    }

    // NOTE: the following method is needed because the methods
    //       required by IIndexStore (getGlobalFileSystem and
    //       getTempStore) call GlobalFileSystemHelper.getGlobalFileSystem
    //       and TemporaryStoreFactory.getTempStore respectively; both
    //       of which require that an IConcurrencyManager be supplied.
    //       But because the ConcurrencyManager class must be constructed
    //       with an IIndexManager, this class is typically created
    //       prior to the constructor of the concurrency manager so
    //       the instance of this class that is created can be supplied
    //       to the ConcurrencyManager's constructor. Thus, after
    //       constructing this class and then the ConcurrencyManager,
    //       the method below should be invoked with the newly
    //       created ConcurrencyManager so that this class has a
    //       reference to that ConcurrencyManager so that that reference
    //       can be supplied to the GlobalFileSystemHelper.getGlobalFileSystem
    //       and TemporaryStoreFactory.getTempStore methods.
    //
    //       At some point, the Concurrency Manager class should be
    //       changed so that this circular reference no longer
    //       exists.

    public void setConcurrencyManager(IConcurrencyManager concurrencyManager) {
        this.concurrencyMgr = concurrencyManager;
    }
}
