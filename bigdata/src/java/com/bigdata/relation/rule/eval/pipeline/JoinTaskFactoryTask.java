package com.bigdata.relation.rule.eval.pipeline;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;
import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
//BTM - PRE_FRED_3481 import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.AbstractDistributedFederation;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
//BTM - PRE_FRED_3481 import com.bigdata.service.DataServiceCallable;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.Session;
import com.bigdata.service.proxy.ClientAsynchronousIterator;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.IKeyOrder;

//BTM
import com.bigdata.resources.ResourceManager;
import com.bigdata.util.Util;

//BTM - PRE_FRED_3481
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.service.IDataServiceCallable;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IScaleOutIndexStore;
import com.bigdata.resources.ILocalResourceManagement;
import java.rmi.server.ExportException;

/**
 * A factory for {@link DistributedJoinTask}s. The factory either creates a new
 * {@link DistributedJoinTask} or returns the pre-existing
 * {@link DistributedJoinTask} for the given {@link JoinMasterTask} instance (as
 * identified by its {@link UUID}), <i>orderIndex</i>, and <i>partitionId</i>.
 * When the desired join task pre-exists, factory will invoke
 * {@link DistributedJoinTask#addSource(IAsynchronousIterator)} and specify the
 * {@link #sourceItrProxy} as another source for that join task.
 * <p>
 * The use of a factory pattern allows us to concentrate all
 * {@link DistributedJoinTask}s which target the same tail predicate and index
 * partition for the same rule execution instance onto the same
 * {@link DistributedJoinTask}. The concentrator effect achieved by the factory
 * only matters when the fan-out is GT ONE (1).
 * 
 * @todo The factory semantics requires something like a "session" concept on
 *       the {@link DataService}. However, it could also be realized by a
 *       canonicalizing mapping of {masterProxy, orderIndex, partitionId} onto
 *       an object that is placed within a weak value cache.
 * 
 * @todo Whenever a {@link DistributedJoinTask} is interrupted or errors it must
 *       make sure that the entry is removed from the session (it could also
 *       interrupt/cancel the remaining {@link DistributedJoinTask}s for the
 *       same {masterInstance}, but we are already doing that in a different
 *       way.)
 * 
 * @todo We need to specify the failover behavior when running query or mutation
 *       rules. The simplest answer is that the query or closure operation fails
 *       and can be retried.
 *       <P>
 *       When retried a different data service instance could take over for the
 *       failed instance. This presumes some concept of "affinity" for a data
 *       service instance when locating a join task. If there are replicated
 *       instances of a data service, then affinity would be the tendency to
 *       choose the same instance for all join tasks with the same master,
 *       orderIndex, and partitionId. That might be more efficient since it
 *       allows aggregation of binding sets that require the same access path
 *       read. However, it might be more efficient to distribute the reads
 *       across the failover instances - it really depends on the workload.
 *       <p>
 *       Ideally, a data service failure would be handled by restarting only
 *       those parts of the operation that had failed. This means that there is
 *       some notion of idempotent for the operation. For at least the RDF
 *       database, this may be achievable. Failure during query leading to
 *       resend of some binding set chunks to a new join task could result in
 *       overgeneration of results, but those results would all be duplicates.
 *       If that is acceptable, then this approach could be considered "safe".
 *       Failure during mutation (aka closure) is even easier for RDF as
 *       redundant writes on an index still lead to the same fixed point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
//BTM - PRE_FRED_3481 public class JoinTaskFactoryTask extends DataServiceCallable<Future> {
public class JoinTaskFactoryTask implements IDataServiceCallable<Future> {

    /**
     * 
     */
    private static final long serialVersionUID = -2637166803787195001L;
    
    protected static final transient Logger log = Logger.getLogger(JoinTaskFactoryTask.class);

    /**
     * @deprecated This is only used by a logging statement.
     */
    final String scaleOutIndexName;
    
    final IRule rule;

    final IJoinNexusFactory joinNexusFactory;

    final int[] order;

    final int orderIndex;

    final int partitionId;

    final UUID masterUUID;
    
    final IJoinMaster masterProxy;

    final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
    
    final IKeyOrder[] keyOrders;
    
    /**
     * A list of variables required for each tail, by tailIndex. Used to filter 
     * downstream variable binding sets.  
     */
    final IVariable[][] requiredVars;

//    /**
//     * Set by the {@link DataService} which recognized that this class
//     * implements the {@link IDataServiceCallable}.
//     */
//    private transient DataService dataService;
//    
//    public void setDataService(DataService dataService) {
//        
//        this.dataService = dataService;
//        
//    }

    /**
     * Set by {@link #call()} to the federation instance available on the
     * {@link DataService}.
     */
//BTM - PRE_CLIENT_SERVICE     private transient AbstractScaleOutFederation fed;
    
    @Override
    public String toString() {

        return getClass().getSimpleName() + "{ orderIndex=" + orderIndex
                + ", partitionId=" + partitionId + "}";
        
    }
    
    /**
     * 
     * @param scaleOutIndexName
     * @param rule
     * @param joinNexusFactory
     * @param order
     * @param orderIndex
     * @param partitionId
     * @param masterProxy
     * @param masterUUID
     *            (Avoids RMI to obtain this later).
     * @param sourceItrProxy
     * @param nextScaleOutIndexName
     */
    public JoinTaskFactoryTask(final String scaleOutIndexName,
            final IRule rule, final IJoinNexusFactory joinNexusFactory,
            final int[] order, final int orderIndex, final int partitionId,
            final IJoinMaster masterProxy,
            final UUID masterUUID,
            final IAsynchronousIterator<IBindingSet[]> sourceItrProxy,
            final IKeyOrder[] keyOrders,
            final IVariable[][] requiredVars) {
        
        if (scaleOutIndexName == null)
            throw new IllegalArgumentException();
        if (rule == null)
            throw new IllegalArgumentException();
        final int tailCount = rule.getTailCount();
        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        if (order == null)
            throw new IllegalArgumentException();
        if (order.length != tailCount)
            throw new IllegalArgumentException();
        if (orderIndex < 0 || orderIndex >= tailCount)
            throw new IllegalArgumentException();
        if (partitionId < 0)
            throw new IllegalArgumentException();
        if (masterProxy == null)
            throw new IllegalArgumentException();
        if (masterUUID == null)
            throw new IllegalArgumentException();
        if (sourceItrProxy == null)
            throw new IllegalArgumentException();
        if (keyOrders == null || keyOrders.length != order.length)
            throw new IllegalArgumentException();
        if (requiredVars == null)
            throw new IllegalArgumentException();

        this.scaleOutIndexName = scaleOutIndexName;
        this.rule = rule;
        this.joinNexusFactory = joinNexusFactory;
        this.order = order;
        this.orderIndex = orderIndex;
        this.partitionId = partitionId;
        this.masterProxy = masterProxy;
        this.masterUUID = masterUUID;
        this.sourceItrProxy = sourceItrProxy;
        this.keyOrders = keyOrders;
        this.requiredVars = requiredVars;
        
    }

    /**
     * Either starts a new {@link DistributedJoinTask} and returns its
     * {@link Future} or returns the {@link Future} of an existing
     * {@link DistributedJoinTask} for the same
     * {@link DistributedJoinMasterTask} instance, <i>orderIndex</i>, and
     * <i>partitionId</i>.
     * 
     * @return (A proxy for) the {@link Future} of the
     *         {@link DistributedJoinTask}.
     */
//BTM - PRE_FRED_3481    public Future call() throws Exception {
//BTM - PRE_CLIENT_SERVICE     public Future startDataTask(IIndexManager indexManager,
//BTM - PRE_CLIENT_SERVICE                                 ResourceManager resourceManager,
//BTM - PRE_CLIENT_SERVICE                                 IConcurrencyManager concurrencyManager,
//BTM - PRE_CLIENT_SERVICE                                 final Session session,
//BTM - PRE_CLIENT_SERVICE                                 String hostname,
//BTM - PRE_CLIENT_SERVICE                                 String serviceName) {
//BTM - PRE_CLIENT_SERVICE 
    public Future startDataTask(IIndexManager indexManager,
                                ResourceManager resourceManager,
                                IConcurrencyManager concurrencyManager,
                                ILocalResourceManagement localResourceManager,
                                IBigdataDiscoveryManagement discoveryManager)
    {
        
//        if (dataService == null)
//            throw new IllegalStateException();

//BTM - PRE_FRED_3481        this.fed = (AbstractScaleOutFederation) getFederation();
//BTM - PRE_CLIENT_SERVICE   this.fed = (AbstractScaleOutFederation) indexManager;

        /*
         * Start the iterator using our local thread pool in order to avoid
         * having it start() with a new Thread().
         * 
         * Note: This MUST be done before we create the join task or the
         * iterator will create its own Thread.
         */
        if (sourceItrProxy instanceof ClientAsynchronousIterator) {

//BTM - PRE_CLIENT_SERVICE  ((ClientAsynchronousIterator) sourceItrProxy).start(fed.getExecutorService());
            ((ClientAsynchronousIterator) sourceItrProxy).start(localResourceManager.getThreadPool());

        }
        
        final String namespace = getJoinTaskNamespace(masterUUID, orderIndex,
                partitionId);
        
        final Future<Void> joinTaskFuture;

//BTM        final Session session = getDataService().getSession();
//BTM - PRE_FRED_3481 final Session session = getSession();
//BTM - PRE_CLIENT_SERVICE
        final Session session = localResourceManager.getSession();//BTM - POST_CLIENT_SERVICE
        
        /*
         * @todo this serializes all requests for a new join task on this data
         * service. However, we only need to serialize requests for the same
         * [uuid, orderIndex, partitionId]. A NamedLock on [namespace] would do
         * exactly that.
         * 
         * Note: The DistributedJoinTask will remove itself from the session
         * when it completes (regardless of success or failure). It does not
         * obtain a lock on the session but instead relies on addSource(itr) to
         * reject new sources until it can be removed from the session.
         */ 
        synchronized (session) {

            // lookup task for that key in the session.
            DistributedJoinTask joinTask = (DistributedJoinTask) session
                    .get(namespace);

            if (joinTask != null) {

                if (joinTask.addSource(sourceItrProxy)) {

                    // use the existing join task.
                    joinTaskFuture = joinTask.futureProxy;

                } else {

                    /*
                     * Create a new join task (the old one has decided that it
                     * will not accept any new sources).
                     */

                    // new task.
//BTM - PRE_FRED_3481       joinTask = newJoinTask();
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  joinTask = newJoinTask(indexManager, resourceManager, session, hostname, serviceName);
                    joinTask = newJoinTask(indexManager,
                                           resourceManager,
                                           concurrencyManager,
                                           localResourceManager,
                                           discoveryManager);
//BTM - PRE_CLIENT_SERVICE - END

                    // put into the session.
                    session.put(namespace, joinTask);

                    // submit task and note its future.
//BTM - PRE_FRED_3481       joinTaskFuture = submit(joinTask);
//BTM - PRE_CLIENT_SERVICE  joinTaskFuture = submit(joinTask, fed);
                    joinTaskFuture = 
                        submit(joinTask, indexManager, localResourceManager);

                }

            } else {

                /*
                 * There is no join task in the session so we create one now.
                 */
                
                // new task.
//BTM - PRE_FRED_3481       joinTask = newJoinTask();
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  joinTask = newJoinTask(indexManager, resourceManager, session, hostname, serviceName);
                    joinTask = newJoinTask(indexManager,
                                           resourceManager,
                                           concurrencyManager,
                                           localResourceManager,
                                           discoveryManager);
//BTM - PRE_CLIENT_SERVICE - END


                // put into the session.
                session.put(namespace, joinTask);

                // submit task and note its future.
//BTM - PRE_FRED_3481       joinTaskFuture = submit(joinTask);
//BTM - PRE_CLIENT_SERVICE  joinTaskFuture = submit(joinTask, fed);
                joinTaskFuture = 
                    submit(joinTask, indexManager, localResourceManager);
                
            }
            
        }

        return joinTaskFuture;
        
    }

//BTM - PRE_FRED_3481       protected DistributedJoinTask newJoinTask() {
//BTM - PRE_CLIENT_SERVICE  protected DistributedJoinTask newJoinTask(IIndexManager indexManager,
//BTM - PRE_CLIENT_SERVICE                                            ResourceManager resourceManager,
//BTM - PRE_CLIENT_SERVICE                                            Session session,
//BTM - PRE_CLIENT_SERVICE                                            String hostname,
//BTM - PRE_CLIENT_SERVICE                                            String serviceName) {
//BTM - PRE_CLIENT_SERVICE
    protected DistributedJoinTask newJoinTask
                         (IIndexManager indexManager,
                          ResourceManager resourceManager,
                          IConcurrencyManager concurrencyManager,
                          ILocalResourceManagement localResourceManager,
                          IBigdataDiscoveryManagement discoveryManager)
    {
        final DistributedJoinTask task;
        {

            /*
             * Note: This wrapper class passes getIndex(name,timestamp) to the
             * IndexManager for the DataService, which is the class that knows
             * how to assemble the index partition view.
             */
//BTM            final IIndexManager indexManager = new DelegateIndexManager(getDataService());
//BTM - PRE_FRED_3481       final IIndexManager indexManager = new DelegateIndexManager(getIndexManager(), getResourceManager());
//BTM - PRE_CLIENT_SERVICE  final IIndexManager delegateIndexManager = new DelegateIndexManager(indexManager, resourceManager);
                 final IIndexManager delegateIndexManager =
                        new DelegateIndexManager(indexManager,
                                                 resourceManager,
                                                 localResourceManager);

//BTM - PRE_FRED_3481        task = new DistributedJoinTask(/*scaleOutIndexName,*/ rule,
//BTM - PRE_FRED_3481                joinNexusFactory.newInstance(indexManager), order,
//BTM - PRE_FRED_3481                orderIndex, partitionId, fed, masterProxy, masterUUID,
//BTM - PRE_FRED_3481 //BTM                sourceItrProxy, keyOrders, getDataService(), requiredVars);
//BTM - PRE_FRED_3481 sourceItrProxy, keyOrders, getSession(), getHostname(), getServiceName(), requiredVars);
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE   task = new DistributedJoinTask(/*scaleOutIndexName,*/
//BTM - PRE_CLIENT_SERVICE                                  rule,
//BTM - PRE_CLIENT_SERVICE                                  joinNexusFactory.newInstance(delegateIndexManager),
//BTM - PRE_CLIENT_SERVICE                                  order,
//BTM - PRE_CLIENT_SERVICE                                  orderIndex,
//BTM - PRE_CLIENT_SERVICE                                  partitionId,
//BTM - PRE_CLIENT_SERVICE                                  fed,
//BTM - PRE_CLIENT_SERVICE                                  masterProxy,
//BTM - PRE_CLIENT_SERVICE                                  masterUUID,
//BTM - PRE_CLIENT_SERVICE                                  sourceItrProxy,
//BTM - PRE_CLIENT_SERVICE                                  keyOrders,
//BTM - PRE_CLIENT_SERVICE                                  session,
//BTM - PRE_CLIENT_SERVICE                                  hostname,
//BTM - PRE_CLIENT_SERVICE                                  serviceName,
//BTM - PRE_CLIENT_SERVICE                                  requiredVars);
//BTM - PRE_CLIENT_SERVICE
            task = new DistributedJoinTask
                           (rule,
                            joinNexusFactory.newInstance(delegateIndexManager,
                                                         concurrencyManager,
                                                         discoveryManager),
                            order,
                            orderIndex,
                            partitionId,
                            indexManager,
                            localResourceManager,
                            discoveryManager,
                            concurrencyManager,
                            masterProxy,
                            masterUUID,
                            sourceItrProxy,
                            keyOrders,
                            requiredVars);
//BTM - PRE_CLIENT_SERVICE = END
        }

        return task;

    }
   
//BTM - PRE_FRED_3481       protected Future<Void> submit(final DistributedJoinTask task) {
//BTM - PRE_CLIENT_SERVICE  protected Future<Void> submit(final DistributedJoinTask task, IBigdataFederation federation) {
    protected Future<Void> submit(final DistributedJoinTask task,
                                  IIndexManager indexMgr,
                                  ILocalResourceManagement localResources)
    {

        if (log.isDebugEnabled())
            log.debug("Submitting new JoinTask: orderIndex=" + orderIndex
                    + ", partitionId=" + partitionId + ", indexName="
                    + scaleOutIndexName);

//BTM - PRE_FRED_3481        Future<Void> joinTaskFuture = getFederation().getExecutorService().submit(task);
//BTM - PRE_CLIENT_SERVICE   Future<Void> joinTaskFuture = federation.getExecutorService().submit(task);
        Future<Void> joinTaskFuture = localResources.getThreadPool().submit(task);

//BTM - PRE_CLIENT_SERVICE        if (fed.isDistributed()) {
//BTM - PRE_CLIENT_SERVICE            // create a proxy for the future.
//BTM - PRE_CLIENT_SERVICE            joinTaskFuture = ((AbstractDistributedFederation) fed).getProxy(joinTaskFuture);
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE
        if (indexMgr instanceof IScaleOutIndexStore) {
            // scaleout ==> distributed, create a proxy for the future.
            try {
                joinTaskFuture = Util.wrapFuture(joinTaskFuture);
            } catch(ExportException e) {// maintain original behavior?
                throw new RuntimeException
                           ("JoinTaskFactoryTask.submit: "
                            +"failed on export of future wrapper", e);
            }
        }

        task.futureProxy = joinTaskFuture;
        
        return joinTaskFuture;

    }

    /**
     * 
     * @param masterUUID
     *            The master UUID should be cached locally by the JoinTask so
     *            that invoking this method does not require RMI.
     * @param orderIndex
     * @param partitionId
     * @return
     */
    static public String getJoinTaskNamespace(final UUID masterUUID,
            final int orderIndex, final int partitionId) {

        return masterUUID + "/" + orderIndex + "/" + partitionId;

    }

    /**
     * The index view that we need for the {@link DistributedJoinTask} is on the
     * {@link IndexManager} class, not the live {@link ManagedJournal}. Looking
     * on the live journal we will only see the mutable {@link BTree} and not
     * the entire index partition view. However, {@link IndexManager} does not
     * implement {@link IIndexManager} or even {@link IIndexStore}. Therefore
     * this class was introduced. It passes most of the methods on to the
     * {@link IBigdataFederation} but {@link #getIndex(String, long)} is
     * delegated to {@link IndexManager#getIndex(String, long)} which is the
     * method that knows how to create the index partition view.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * 
     * @todo While this class solves our problem I do not know whether or not
     *       this class should this class have more visibility? The downside is
     *       that it is a bit incoherent how it passes along one method to the
     *       {@link IndexManager}, most methods to the
     *       {@link IBigdataFederation} and disallows {@link #dropIndex(String)}
     *       and {@link #registerIndex(IndexMetadata)} in an attempt to stay out
     *       of trouble. That may be enough reason to keep it private.
     */
    static class DelegateIndexManager implements IIndexManager {
        
//BTM - BEGIN
//BTM        private final DataService dataService;
//BTM        
//BTM        public DelegateIndexManager(final DataService dataService) {
//BTM            
//BTM            if (dataService == null)
//BTM                throw new IllegalArgumentException();
//BTM            
//BTM            this.dataService = dataService;
//BTM            
//BTM        }
//BTM
        private final IIndexManager indexManager;
        private final ResourceManager resourceManager;
        private final ILocalResourceManagement localResources;

        public DelegateIndexManager
                   (IIndexManager indexManager,
                    ResourceManager resourceManager,
                    ILocalResourceManagement localResourceManager)
        {
            if (indexManager == null) {
                throw new IllegalArgumentException("null indexManager");
            }
            if (resourceManager == null) {
                throw new IllegalArgumentException("null resourceManager");
            }
            if (localResourceManager == null) {
                throw new IllegalArgumentException
                              ("null localResourceManager");
            }
            this.indexManager = indexManager;
            this.resourceManager = resourceManager;
            this.localResources = localResourceManager;
        }
//BTM - END

        /**
         * Delegates to the {@link IndexManager}.
         */
        public IIndex getIndex(final String name, final long timestamp) {
//BTM            return dataService.getResourceManager().getIndex(name, timestamp);
            return resourceManager.getIndex(name, timestamp);
        }

        /**
         * Not allowed.
         */
        public void dropIndex(final String name) {
            
            throw new UnsupportedOperationException();
            
        }

        /**
         * Not allowed.
         */
        public void registerIndex(IndexMetadata indexMetadata) {

            throw new UnsupportedOperationException();
            
        }

        public void destroy() {
            
            throw new UnsupportedOperationException();
            
        }

        public ExecutorService getExecutorService() {
            
//BTM            return dataService.getFederation().getExecutorService();
//BTM - PRE_CLIENT_SERVICE return ((IIndexStore)indexManager).getExecutorService();
            return localResources.getThreadPool();
        }

        public BigdataFileSystem getGlobalFileSystem() {

//BTM            return dataService.getFederation().getGlobalFileSystem();
            return ((IIndexStore)indexManager).getGlobalFileSystem();
            
        }

        public SparseRowStore getGlobalRowStore() {

//BTM            return dataService.getFederation().getGlobalRowStore();
            return ((IIndexStore)indexManager).getGlobalRowStore();
            
        }

        public long getLastCommitTime() {

//BTM            return dataService.getFederation().getLastCommitTime();
            return ((IIndexStore)indexManager).getLastCommitTime();
            
        }

        public IResourceLocator getResourceLocator() {

//BTM            return dataService.getFederation().getResourceLocator();
            return ((IIndexStore)indexManager).getResourceLocator();
            
        }

        public IResourceLockService getResourceLockService() {

//BTM            return dataService.getFederation().getResourceLockService();
            return ((IIndexStore)indexManager).getResourceLockService();
            
        }

        public TemporaryStore getTempStore() {

//BTM            return dataService.getFederation().getTempStore();
            return ((IIndexStore)indexManager).getTempStore();

        }

    }

}
