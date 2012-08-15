/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 25, 2012
 */

package com.bigdata.rdf.sparql.ast.cache;


import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.view.FusedView;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.htree.HTree;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.IDataService;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.NT;

/**
 * A connection to a local, remote, or distributed caching layer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CacheConnectionImpl implements ICacheConnection {

    private static transient final Logger log = Logger
            .getLogger(CacheConnectionImpl.class);
    
//    public interface Options {
//
//        /**
//         * The maximum amount of native memory which will be used to cache
//         * solution sets (default is 1/2 of the value reported by
//         * {@link Runtime#maxMemory()}).
//         * <p>
//         * Note: The {@link MemoryManager} backing the cache can use up to 4TB
//         * of RAM.
//         * <p>
//         * Note: Once the cache is full, solution sets will be expired according
//         * to the cache policy until the native memory demand has fallen below
//         * this threshold before a new solution set is added to the cache.
//         */
//        String MAX_MEMORY = SparqlCache.class.getName() + ".maxMemory";
//
//        final long DEFAULT_MAX_MEMORY = Runtime.getRuntime().maxMemory() / 2;
//
//    }
    
    private final QueryEngine queryEngine;
    
    /**
     * The backing store for cached data.
     * 
     * TODO Note: A slight twist on the design would allow us to cache in both
     * main memory and on a backing {@link RWStore} (DISK). However, it would be
     * worth while to migrate records to the {@link RWStore} only if they were
     * expensive to compute and we had a reasonable expectation of reuse before
     * they would be invalidated by an update. In practice, it is probably
     * better to hash partition the cache.
     * <p>
     * A hash partitioned cache design could proceed readily with splitting the
     * cache results between two nodes when adding a node. Any such approach
     * presumes a circular hash function such as is common in distributed row
     * stores, etc.
     */
    private final InnerCacheJournal cacheStore;
    
    /**
     * The {@link DescribeServiceFactory} tracks changes via an
     * {@link IChangeLog} registered with each update connection and is
     * responsible for cache invalidation.
     */
    private DescribeServiceFactory describeServiceFactory;

    /**
     * TODO Hack enables the DESCRIBE cache.
     */
    private boolean enableDescribeCache = QueryHints.DEFAULT_DESCRIBE_CACHE;
    
    /**
     * TODO Hack enables the SOLUTIONS cache.
     */
    private boolean enableSolutionsCache = QueryHints.DEFAULT_SOLUTION_SET_CACHE;
    
    private IIndexManager getLocalIndexManager() {
        
        return queryEngine.getIndexManager();
        
    }
    
    private ConcurrencyManager getConcurrencyManager() {

        /*
         * Note: I have commented this out on the QueryEngine and
         * FederatedQueryEngine until after the 1.2.0 release.
         */
//        return queryEngine.getConcurrencyManager();
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * 
     * Note: A distributed cache fabric could be accessed from any node in a
     * cluster. That means that this could be the {@link Journal} -or- the
     * {@link IndexManager} inside the {@link IDataService} and provides direct
     * access to {@link FusedView}s (aka shards).
     * 
     * @param queryEngine
     *            The {@link QueryEngine}.
     */
    public CacheConnectionImpl(final QueryEngine queryEngine) {

        if (queryEngine == null)
            throw new IllegalArgumentException();

        this.queryEngine = queryEngine;
        
        /*
         * TODO Setup properties from Journal or Federation (mainly the maximum
         * amount of RAM to use, but we can not limit that if we are using this
         * for to store named solution sets rather than as a cache).
         * 
         * TODO Setup an expire thread or a priority heap for expiring named
         * solution sets from the cache.
         */
        final Properties properties = new Properties();

        /*
         * Note: The cache will be backed by ByteBuffer objects allocated on the
         * native process heap (Zero GC).
         */
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.MemStore.name());

        // Start small, grow as required.
        properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, ""
                + (1 * Bytes.megabyte));

//        properties.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE,
//                "true");
//
////        properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
////                "false");
////
////        properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
////                "false");
////
////        properties.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);

        this.cacheStore = new InnerCacheJournal(properties);
        
    }
    
    @Override
    public void init() {

        if (enableDescribeCache) {

            describeServiceFactory = new DescribeServiceFactory();

        }

    }
    
    @Override
    public void close() {

//        cacheMap.clear();
        
        cacheStore.destroy();

    }

    /**
     * {@link CacheConnectionImpl} is used with a singleton pattern managed by the
     * {@link CacheConnectionFactory}. It will be torn down automatically it is no
     * longer reachable. This behavior depends on not having any hard references
     * back to the {@link QueryEngine}.
     */
    @Override
    protected void finalize() throws Throwable {
        
        close();
        
        super.finalize();
        
    }

    /**
     * Return the store backing the named solution sets.
     * 
     * @return The backing store.
     */
    protected AbstractJournal getStore() {
        
        return cacheStore;
        
    }

    public ISolutionSetCache getSparqlCache(final String namespace,
            final long timestamp) {

        if (!enableSolutionsCache) {

            // Not enabled.
            return null;

        }

        if (namespace == null)
            throw new IllegalArgumentException();

        final NT nt = new NT(namespace,timestamp);

        synchronized (sparqlCaches) {

            ISolutionSetCache tmp = sparqlCaches.get(nt);

            if (tmp == null) {

                tmp = new SolutionSetCache(this, namespace, timestamp);

                sparqlCaches.put(nt, tmp, false/* dirty */);

            }

            return tmp;
            
        }
        
    }

    /**
     * Canonicalizing mapping for the {@link ISolutionSetCache} views.
     * 
     * TODO Review choices for the class used to cache the {@link ISolutionSetCache}
     * views and the configuration for that cache class.
     */
    private WeakValueCache<NT, ISolutionSetCache> sparqlCaches = new WeakValueCache<NT, ISolutionSetCache>(
            new LRUCache<NT, ISolutionSetCache>(10));
    
    /**
     * 
     * @return The DESCRIBE cache for that view -or- <code>null</code> if the
     *         DESCRIBE cache is not enabled.
     * 
     * @see QueryHints#DESCRIBE_CACHE
     */
    @Override
    public IDescribeCache getDescribeCache(final String namespace,
            final long timestamp) {

        if (!enableDescribeCache) {

            // Not enabled.
            return null;

        }

        if (namespace == null)
            throw new IllegalArgumentException();
        
        /*
         * Resolve the DESCRIBE cache for this KB namespace using ATOMIC pattern
         * (locking).
         * 
         * Note: We do not need to use a canonicalizing mapping here since the
         * entire state of the DESCRIBE cache is contained by the HTree. The
         * caller just needs to use appropriate synchronization when writing on
         * a mutable HTree. The DescribeCache implementation takes care of that.
         * 
         * FIXME The DESCRIBE cache will never become materialized if you are
         * only running queries (versus mutations) against a given KB instance.
         * This is because the HTree is only created when we have a mutable
         * view.
         */

        HTree htree;
        synchronized (this) {

            final String name = namespace + ".describeCache";

            htree = cacheStore.getHTree(name);

            if (htree == null) {

                if (TimestampUtility.isReadOnly(timestamp)) {

                    // Cache is not pre-existing.
                    return null;

                }
                
                final HTreeIndexMetadata metadata = new HTreeIndexMetadata(
                        name, UUID.randomUUID());

                metadata.setRawRecords(true/* rawRecords */);

                metadata.setMaxRecLen(0/* maxRecLen */);
                
                cacheStore.registerIndex(metadata);

            }
            
            htree = cacheStore.getHTree(name);

        }
        
        return new DescribeCache(htree);
        
    }
    
    /*
     * END OF DESCRIBE CACHE SUPPORT
     */

    /**
     * The {@link InnerCacheJournal} provides the backing store for transient
     * named solution sets.
     */
    private class InnerCacheJournal extends AbstractJournal {

        protected InnerCacheJournal(final Properties properties) {

            super(properties);

//            /*
//             * TODO Report out performance counters for the cache.
//             */
//            if (getBufferStrategy() instanceof DiskOnlyStrategy) {
//
//                ((DiskOnlyStrategy) getBufferStrategy())
//                        .setStoreCounters(getStoreCounters());
//
//            } else if (getBufferStrategy() instanceof WORMStrategy) {
//
//                ((WORMStrategy) getBufferStrategy())
//                        .setStoreCounters(getStoreCounters());
//
//            }
 
        }

        public String toString() {
            
            /*
             * Note: Should not depend on any state that might be unreachable,
             * e.g., because the store is not open, etc.
             */
            
            final IRootBlockView rootBlock = getRootBlockView();
            
            return getClass().getName()
                    + "{file="
                    + getFile()
                    + ", open="
                    + InnerCacheJournal.this.isOpen()
                    + (rootBlock != null ? ", uuid="
                            + getRootBlockView().getUUID() : "") + "}";
            
        }
        
//        /**
//         * Note: Exposed for the {@link DataService} which needs this for its
//         * 2-phase commit protocol.
//         */
//        public long commitNow(final long commitTime) {
//            
//            return super.commitNow(commitTime);
//            
//        }
        
//        /**
//         * Exposed for {@link StoreManger#getResourcesForTimestamp(long)} which
//         * requires access to the {@link CommitRecordIndex} for the
//         * lastCommitTime on the historical journals.
//         * <p>
//         * Note: This always returns a distinct index object. The code relies on
//         * this fact to avoid contention with the live {@link CommitRecordIndex}
//         * for the live journal.
//         */
//        public CommitRecordIndex getCommitRecordIndex(final long addr) {
//            
//            return super.getCommitRecordIndex(addr);
//            
//        }

        public AbstractLocalTransactionManager getLocalTransactionManager() {

            return (AbstractLocalTransactionManager) getConcurrencyManager()
                    .getTransactionManager();

        }

        public SparseRowStore getGlobalRowStore() {
            
            return getLocalIndexManager().getGlobalRowStore();
            
        }

        public SparseRowStore getGlobalRowStore(final long timestamp) {
            
            return getLocalIndexManager().getGlobalRowStore(timestamp);
            
        }

        public BigdataFileSystem getGlobalFileSystem() {
            
            return getLocalIndexManager().getGlobalFileSystem();
            
        }
        
        @SuppressWarnings("rawtypes")
        public DefaultResourceLocator getResourceLocator() {
            
            return (DefaultResourceLocator) getLocalIndexManager()
                    .getResourceLocator();
            
        }
        
        public ExecutorService getExecutorService() {
            
            return getLocalIndexManager().getExecutorService();
            
        }
        
        public IResourceLockService getResourceLockService() {

            return getLocalIndexManager().getResourceLockService();
            
        }

        public TemporaryStore getTempStore() {
            
            return getLocalIndexManager().getTempStore();
            
        }

        public ScheduledFuture<?> addScheduledTask(Runnable task,
                long initialDelay, long delay, TimeUnit unit) {

            return getLocalIndexManager().addScheduledTask(task, initialDelay,
                    delay, unit);
        
        }

        public boolean getCollectPlatformStatistics() {
            return getLocalIndexManager().getCollectPlatformStatistics();
        }

        public boolean getCollectQueueStatistics() {
            return getLocalIndexManager().getCollectQueueStatistics();
        }

        public int getHttpdPort() {
            return getLocalIndexManager().getHttpdPort();
        }

    } // class CacheJournal

}
