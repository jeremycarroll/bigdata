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

package com.bigdata.metadata;

import static com.bigdata.metadata.Constants.*;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IMetadataIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TransactionService;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IService;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.Service;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.httpd.AbstractHTTPD;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

//NOTE: replace IBigdataFederation with IIndexStore when
//      StoreManager.getResourceLocator is changed to no longer
//      call getFederation
//
// IBigdata extends IIndexManager and IFederationDelegate
// IIndexManager extends IIndexStore
public class EmbeddedIndexStore<T> implements IBigdataFederation<T> {

    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedIndexStore.class).getName());

    private UUID serviceUUID;
    private LookupCache txnCache;
    private LookupCache remoteTxnCache;
    private TransactionService embeddedTxnService;//for embedded fed tests
    private DefaultResourceLocator resourceLocator;

    EmbeddedIndexStore(UUID serviceUUID,
                       LookupCache txnCache,
                       LookupCache remoteTxnCache,
                       TransactionService embeddedTxnService)
    {
        this.serviceUUID = serviceUUID;
        this.txnCache = txnCache;
        this.remoteTxnCache = remoteTxnCache;
        this.embeddedTxnService = embeddedTxnService;
        this.resourceLocator = 
            new DefaultResourceLocator(this, null, 20, 60L*1000L);
    }

    // Required by ONLY IIndexStore

    public BigdataFileSystem getGlobalFileSystem() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getGlobalFileSystem");
    }

    public TemporaryStore getTempStore() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getTempStore");
    }

    public IResourceLocator getResourceLocator() {
        return resourceLocator;
    }

    public IResourceLockService getResourceLockService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getResourceLockService");
    }

    // Required by BOTH IBigdataFederation, IIndexStore

    public IClientIndex getIndex(String name, long timestamp) {
logger.warn("\n>>>> EmbeddedIndexStore.getIndex <<<<\n");
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getIndex");
    }

    public SparseRowStore getGlobalRowStore() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getGlobalRowStore");
    }

    public ExecutorService getExecutorService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getExecutorService");
    }

    public long getLastCommitTime() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getLastCommitTime");
    }

    public void destroy() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.destroy");
    }

    // Required by BOTH IBigdataFederation, IIndexManager

    public void registerIndex(IndexMetadata metadata) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.dropIndex");
    }

    public void dropIndex(String name) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.dropIndex");
    }

    // Required by ONLY IFederationDelegate

    public T getService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getService");
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public Class getServiceIface() {
        return SERVICE_TYPE;
    }

    public UUID getServiceUUID() {
        return serviceUUID;
    }

    public void reattachDynamicCounters() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.reattachDynamicCounters");
    }

    public boolean isServiceReady() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isServiceReady");
    }

    public void didStart() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.didStart");
    }

    public void serviceJoin(IService service, UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.serviceJoin");
    }

    public void serviceJoin(Service service, UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.serviceJoin");
    }

    public void serviceLeave(UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.serviceLeave");
    }

    public AbstractHTTPD newHttpd(final int httpdPort,
                                  final CounterSet counterSet)
                             throws IOException
    {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.newHttpd");
    }

    // Required by ONLY IBigdataFederation

    public IBigdataClient<T> getClient() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getClient");
    }

    public String getHttpdURL() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getHttpdURL");
    }

    public TransactionService getTransactionService() {
        if(remoteTxnCache != null) {
            ServiceItem txnItem = remoteTxnCache.lookup(null);
            if( (txnItem == null) && (txnCache != null) ) {
                txnItem = txnCache.lookup(null);
            }
            if(txnItem != null) return (TransactionService)txnItem.service;
        }
        return embeddedTxnService;
    }

    public LoadBalancer getLoadBalancerService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getLoadBalancerService");
    }

    public ShardLocator getMetadataService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getMetadataService");
    }

    public CounterSet getCounterSet() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getCounterSet");
    }

    public CounterSet getHostCounterSet() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getHostCounterSet");
    }

    public CounterSet getServiceCounterSet() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getServiceCounterSet");
    }

    public String getServiceCounterPathPrefix() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getServiceCounterPathPrefix");
    }

    public UUID[] getDataServiceUUIDs(int maxCount) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getDataServiceUUIDs");
    }

    public IDataService[] getDataServices(UUID[] uuid) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getDataServices");
    }

    public IDataService getDataService(UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getDataService");
    }

    public IDataService getAnyDataService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getAnyDataService");
    }

    public IDataService getDataServiceByName(String name) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getDataServiceByName");
    }

    public IMetadataIndex getMetadataIndex(String name, long timestamp) {
logger.warn("\n>>>> EmbeddedIndexStore.getMetadataIndex <<<<\n");
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getMetadataIndex");
    }

    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.registerIndex");
    }

    public UUID registerIndex(IndexMetadata metadata,
                              byte[][] separatorKeys,
                              UUID[] dataServiceUUIDs)
    {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.registerIndex");
    }

    public boolean isScaleOut() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isScaleOut");
    }

    public boolean isDistributed() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isDistributed");
    }

    public boolean isStable() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isStable");
    }
}
