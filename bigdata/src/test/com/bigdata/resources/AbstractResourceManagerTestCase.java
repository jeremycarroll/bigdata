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
 * Created on Feb 22, 2008
 */

package com.bigdata.resources;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IResourceLockService;
//BTM import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.IndexPartitionCause;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.AbstractTransactionService;
//BTM import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
//BTM import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IService;
import com.bigdata.service.Session;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.httpd.AbstractHTTPD;

//BTM
//BTM - PRE_CLIENT_SERVICE import com.bigdata.event.EventQueue;
import com.bigdata.journal.TransactionService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.Service;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardService;
import com.bigdata.service.ShutdownAdmin;
import com.sun.jini.admin.DestroyAdmin;
import net.jini.admin.Administrable;

//BTM - PRE_FRED_3481
import com.bigdata.event.EventQueue;
import com.bigdata.service.Event;
import com.bigdata.service.IDataServiceCallable;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//BTM - FOR_CLIENT_SERVICE           
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.event.EventQueueSender;
import com.bigdata.jini.BigdataDiscoveryManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.service.Session;
import com.bigdata.service.ndx.ScaleOutIndexCounters;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.config.NicUtil;
import com.bigdata.util.config.ConfigDeployUtil;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Base class for {@link ResourceManager} test suites that can use normal
 * startup and shutdown.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractResourceManagerTestCase extends
        AbstractResourceManagerBootstrapTestCase {

    /**
     * 
     */
    public AbstractResourceManagerTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractResourceManagerTestCase(String arg0) {
        super(arg0);
    }

    /**
     * Forces the use of persistent journals so that we can do overflow
     * operations and the like.
     */
    public Properties getProperties() {
        
        final Properties properties = new Properties( super.getProperties() );
        
        // Note: test requires data on disk.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());
        
        // Disable index copy - overflow will always cause an index segment build.
        properties.setProperty(Options.COPY_INDEX_THRESHOLD,"0");
        
        return properties;
        
    }
    
//BTM    protected IMetadataService metadataService;
protected ShardLocator metadataService;
    protected ResourceManager resourceManager;
    protected ConcurrencyManager concurrencyManager;
    private AbstractTransactionService txService;
    protected AbstractLocalTransactionManager localTransactionManager;
    private ExecutorService executorService; 
    private IBigdataFederation fed;

    /**
     * Setup test fixtures.
     */
    public void setUp() throws Exception {
        
        super.setUp();

        metadataService = new MockMetadataService();
        
        final Properties properties = getProperties();

//BTM - FOR_CLIENT_SERVICE - BEGIN - moved these lines up from below for new ResourceManager constructor
        executorService = Executors.newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());
        fed = new MockFederation();
//BTM - FOR_CLIENT_SERVICE - END

//BTM - PRE_CLIENT_SERVICE - BEGIN - use new MyResourceManager ------------
//BTM - PRE_CLIENT_SERVICE         resourceManager = new ResourceManager(properties) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             final private UUID dataServiceUUID = UUID.randomUUID();
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             @Override
//BTM - PRE_CLIENT_SERVICE             public IBigdataFederation getFederation() {
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE                 return fed;
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             @Override
//BTM - PRE_CLIENT_SERVICE //BTM            public DataService getDataService() {
//BTM - PRE_CLIENT_SERVICE public ShardService getDataService() {
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE                 throw new UnsupportedOperationException();
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE             @Override
//BTM - PRE_CLIENT_SERVICE             public UUID getDataServiceUUID() {
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE                 return dataServiceUUID;
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         };

        resourceManager = new MyResourceManager
                                  ( (IIndexManager)fed,
                                    (ILocalResourceManagement)fed,
                                    properties );
//BTM - PRE_CLIENT_SERVICE - END -----------------------------------------

        txService = new MockTransactionService(properties){

        	@Override
            protected void setReleaseTime(long releaseTime) {
                
                super.setReleaseTime(releaseTime);

				if (log.isInfoEnabled())
					log
							.info("Propagating new release time to the resourceManager: releaseTime="
									+ releaseTime
									+ ", releaseAge="
									+ getMinReleaseAge());

                // propagate the new release time to the resource manager.
                resourceManager.setReleaseTime(releaseTime);
                
            }

        }.start();
        
        localTransactionManager = new MockLocalTransactionManager(txService);
        
        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        resourceManager.setConcurrencyManager(concurrencyManager);
        
        assertTrue( resourceManager.awaitRunning() );
        
//BTM - PRE_CLIENT_SERVICE - BEGIN - moved these lines above for new ResourceManager constructor
//BTM - PRE_CLIENT_SERVICE        executorService = Executors.newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());
//BTM - PRE_CLIENT_SERVICE        
//BTM - PRE_CLIENT_SERVICE        fed = new MockFederation();
//BTM - PRE_CLIENT_SERVICE - END
        
    }

    public void tearDown() throws Exception {

        if(executorService != null)
            executorService.shutdownNow();

        if (fed != null)
            fed.destroy();
        
//BTM        if (metadataService != null)
//BTM            metadataService.destroy();
if (metadataService != null) {
    if(metadataService instanceof IService) {
        ((IService)metadataService).destroy();
    } else if(metadataService instanceof Administrable) {
        Object serviceAdmin  = ((Administrable)metadataService).getAdmin();
        if(serviceAdmin instanceof DestroyAdmin) {
            ((DestroyAdmin)serviceAdmin).destroy();
        }
    } else if(metadataService instanceof ShutdownAdmin) {
        ((ShutdownAdmin)metadataService).shutdownNow();
    } else if(metadataService instanceof IServiceShutdown) {
        ((IServiceShutdown)metadataService).shutdownNow();
    }
}

        if (resourceManager != null)
            resourceManager.shutdownNow();
        
        if (concurrencyManager != null)
            concurrencyManager.shutdownNow();

        if (localTransactionManager != null)
            localTransactionManager.shutdownNow();

        if (txService != null) {
            txService.destroy();
        }
        
    }

    /**
     * A minimal implementation of the shard locator service - only those methods
     * actually used by the {@link ResourceManager} are implemented. This avoids
     * conflicts with the {@link ResourceManager} instance whose behavior we are
     * trying to test.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
//BTM    protected static class MockMetadataService implements IMetadataService {
protected static class MockMetadataService implements ShardLocator {

        private AtomicInteger partitionId = new AtomicInteger(0);
       
        private final Session session = new Session();
        
        public int nextPartitionId(String name) throws IOException, InterruptedException, ExecutionException {
            return partitionId.incrementAndGet();
        }

        public UUID registerScaleOutIndex(IndexMetadata metadata, byte[][] separatorKeys, UUID[] dataServices) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public void dropScaleOutIndex(String name) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public UUID getServiceUUID() throws IOException {
            throw new UnsupportedOperationException();
        }

        public String getStatistics() throws IOException {
            throw new UnsupportedOperationException();
        }

        public void registerIndex(String name, IndexMetadata metadata) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public IndexMetadata getIndexMetadata(String name,long timestamp) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void dropIndex(String name) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public ResultSet rangeIterator(long tx, String name, byte[] fromKey, byte[] toKey, int capacity, int flags, IFilterConstructor filter) throws InterruptedException, ExecutionException, IOException {
            throw new UnsupportedOperationException();
        }

        public Future submit(long tx, String name, IIndexProcedure proc) {
            throw new UnsupportedOperationException();
        }

        public IBlock readBlock(IResourceMetadata resource, long addr) {
            throw new UnsupportedOperationException();
        }

        public void splitIndexPartition(String name,
                PartitionLocator oldLocator, PartitionLocator[] newLocators)
                throws IOException, InterruptedException, ExecutionException {

            log.info("Split index partition: name=" + name + ", oldLocator="
                    + oldLocator + " into " + Arrays.toString(newLocators));

        }

        public void joinIndexPartition(String name,
                PartitionLocator[] oldLocators, PartitionLocator newLocator)
                throws IOException, InterruptedException, ExecutionException {

            log.info("Join index partitions: name=" + name + ", oldLocators="
                    + Arrays.toString(oldLocators) + " into " + newLocator );
            
        }

        public void moveIndexPartition(String name,
                PartitionLocator oldLocator, PartitionLocator newLocator)
                throws IOException, InterruptedException, ExecutionException {

            log.info("Move index partition: name=" + name + ", oldLocator="
                    + oldLocator + " to " + newLocator);
            
        }

        public PartitionLocator get(String name, long timestamp, byte[] key) throws InterruptedException, ExecutionException, IOException {

            return null;
        }

        public PartitionLocator find(String name, long timestamp, byte[] key) throws InterruptedException, ExecutionException, IOException {

            return null;
        }

        public void forceOverflow(boolean immediate,boolean compactingMerge) throws IOException {
            
            throw new UnsupportedOperationException();
            
        }
        
        public boolean isOverflowActive() throws IOException {
            
            throw new UnsupportedOperationException();
            
        }

        public long getAsynchronousOverflowCounter() throws IOException {

            throw new UnsupportedOperationException();
            
        }

        public void destroy() {

        }

//BTM - PRE_FRED_3481       public Future<? extends Object> submit(Callable<? extends Object> proc) {
       public <T> Future<T> submit(IDataServiceCallable<T> proc) {

            return null;
        }

        public String getHostname() throws IOException {

            return null;
        }

        public Class getServiceIface() throws IOException {

            return null;
        }

        public String getServiceName() throws IOException {

            return null;
        }

        public boolean purgeOldResources(long timeout, boolean truncateJournal) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            return false;
        }

        public void setReleaseTime(long releaseTime) {
            // TODO Auto-generated method stub
            
        }

        public void abort(long tx) throws IOException {

            throw new UnsupportedOperationException();
            
        }

        public long singlePhaseCommit(long tx) throws InterruptedException,
                ExecutionException, IOException {

            throw new UnsupportedOperationException();

        }

        public void prepare(long tx, long revisionTime)
                throws InterruptedException, ExecutionException, IOException {

            throw new UnsupportedOperationException();

        }

        public Session getSession() {
            return session;
        }

    }

    /**
     * A minimal implementation of only those methods actually utilized by the
     * {@link ResourceManager} during the unit tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    protected class MockFederation implements IBigdataFederation<MockMetadataService> {
    protected class MockFederation
                        implements IBigdataFederation<MockMetadataService>,
                                   ILocalResourceManagement
{
//BTM - PRE_CLIENT_SERVICE - END

        private final MockMetadataService metadataService = new MockMetadataService();
        private final MockSendEventsTask mockSendEventsTask = new MockSendEventsTask();//BTM

        public MockMetadataService getService() {
            
            return metadataService;
            
        }

        public void destroy() {

            
        }

        public void dropIndex(String name) {

            
        }

//BTM        public IDataService getAnyDataService() {
public ShardService getAnyDataService() {

            return null;
        }

        public IBigdataClient getClient() {

            return null;
        }

        public String getServiceCounterPathPrefix() {

            return null;
        }

        public CounterSet getCounterSet() {

            return null;
        }

//BTM        public IDataService getDataService(UUID serviceUUID) {
public ShardService getDataService(UUID serviceUUID) {

            return null;
        }

        public UUID[] getDataServiceUUIDs(int maxCount) {

            return null;
        }

        public ExecutorService getExecutorService() {
            
            return executorService;
            
        }

        public SparseRowStore getGlobalRowStore() {

            return null;
        }

        public IClientIndex getIndex(String name, long timestamp) {

            return null;
        }

        public IKeyBuilder getKeyBuilder() {

            return null;
        }

        public LoadBalancer getLoadBalancerService() {

            return null;
        }

        public IMetadataIndex getMetadataIndex(String name, long timestamp) {

            return null;
        }

//BTM        public IMetadataService getMetadataService() {
public ShardLocator getMetadataService() {

            return metadataService;
            
        }

//BTM        public ITransactionService getTransactionService() {
public TransactionService getTransactionService() {

            return txService;
        
        }

        public boolean isDistributed() {

            return false;
        }

        public boolean isScaleOut() {

            return false;
        }

        public boolean isStable() {

            return false;
        }

        public long getLastCommitTime() {

            return 0;
        }

        public void registerIndex(IndexMetadata metadata) {

            
        }

        public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {

            return null;
        }

        public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys, UUID[] dataServiceUUIDs) {

            return null;
        }

        public IResourceLocator getResourceLocator() {

            return null;
        }

        public IResourceLockService getResourceLockService() {

            return null;
        }

        public BigdataFileSystem getGlobalFileSystem() {

            return null;
        }

        public TemporaryStore getTempStore() {

            return null;
        }

        public String getHttpdURL() {

            return null;
        }

        public CounterSet getServiceCounterSet() {

            return null;
        }

//BTM        public IDataService getDataServiceByName(String name) {
public ShardService getDataServiceByName(String name) {
            // TODO Auto-generated method stub
            return null;
        }

//BTM        public IDataService[] getDataServices(UUID[] uuid) {
public ShardService[] getDataServices(UUID[] uuid) {
            // TODO Auto-generated method stub
            return null;
        }

        public void didStart() {
            
        }

        public Class getServiceIface() {
            return getClass();
        }

        public String getServiceName() {
            return getClass().getName();
        }

        public UUID getServiceUUID() {
            return serviceUUID;
        }
        private final UUID serviceUUID = UUID.randomUUID();

        public boolean isServiceReady() {
            return true;
        }

        public AbstractHTTPD newHttpd(int httpdPort, CounterSet counterSet) throws IOException {
            return null;
        }

        public void reattachDynamicCounters() {
        }

        public void serviceJoin(IService service, UUID serviceUUID) {
        }

        public void serviceLeave(UUID serviceUUID) {
        }

        public CounterSet getHostCounterSet() {
            // TODO Auto-generated method stub
            return null;
        }

//BTM - FOR_CLIENT_SERVICE - END ----------------------------------------------
        public void serviceJoin(Service service, UUID serviceUUID) { }

        // Required by ILocalResourceManagement

        public String getHostname() {
            try {
                return ( NicUtil.getIpAddress
                             ("default.nic", 
                              ConfigDeployUtil.getString
                                  ("node.serviceNetwork"),
                              false) );
            } catch(Throwable t) {
                return "UNKNOWN";
            }
        }

        public ExecutorService getThreadPool() {
            return this.getExecutorService();
        }

        public ScheduledExecutorService getScheduledExecutor() {
            return null;
        }

        public Session getSession() {
            return null;
        }
        public ScaleOutIndexCounters getIndexCounters(String name) {
            return null;
        }
        public void reattachDynamicCounters
                                 (ResourceManager resourceMgr,
                                  IConcurrencyManager concurrencyMgr)
        {
            //no-op
        }
        public CounterSet getServiceCounterSet(boolean addCounters) {
            return null;
        }
        public TaskCounters getTaskCounters() {
            return null;
        }
        public EventQueueSender getEventQueueSender() {
            return mockSendEventsTask;
        }
        public void terminate(long timeout) {
            //no-opp
        }

//BTM - PRE_CLIENT_SERVICE        private class MockSendEventsTask implements EventQueue, Runnable {
//BTM - PRE_CLIENT_SERVICE            final private BlockingQueue<Event> eventQueue = 
//BTM - PRE_CLIENT_SERVICE                              new LinkedBlockingQueue<Event>();
//BTM - PRE_CLIENT_SERVICE            public MockSendEventsTask() { }
//BTM - PRE_CLIENT_SERVICE            public void queueEvent(Event e) {
//BTM - PRE_CLIENT_SERVICE                eventQueue.add(e);
//BTM - PRE_CLIENT_SERVICE            }
//BTM - PRE_CLIENT_SERVICE            public void run() {
//BTM - PRE_CLIENT_SERVICE                try {
//BTM - PRE_CLIENT_SERVICE                    final LinkedList<Event> c = new LinkedList<Event>();
//BTM - PRE_CLIENT_SERVICE                    eventQueue.drainTo(c);
//BTM - PRE_CLIENT_SERVICE                    for (Event e : c) {
//BTM - PRE_CLIENT_SERVICE                        log.debug("sent event to load balancer");
//BTM - PRE_CLIENT_SERVICE                    }
//BTM - PRE_CLIENT_SERVICE                } catch (Throwable t) {
//BTM - PRE_CLIENT_SERVICE                    log.warn(getServiceName(), t);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                }
//BTM - PRE_CLIENT_SERVICE            }
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        public EventQueue getEventQueue() {
//BTM - PRE_CLIENT_SERVICE            return mockSendEventsTask;
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE
        private class MockSendEventsTask implements EventQueueSender {
            final private BlockingQueue<Event> eventQueue = 
                              new LinkedBlockingQueue<Event>();
            public MockSendEventsTask() { }
            public void queueEvent(Event e) {
                eventQueue.add(e);
            }
            public void run() {
                try {
                    final LinkedList<Event> c = new LinkedList<Event>();
                    eventQueue.drainTo(c);
                    for (Event e : c) {
                        log.debug("sent event to load balancer");
                    }
                } catch (Throwable t) {
                    log.warn(getServiceName(), t);

                }
            }
        }//end class MockSendEventsTask

    }//end class MockFederation

    protected static class MyResourceManager extends ResourceManager {

        final private UUID dataServiceUUID = UUID.randomUUID();

        private IBigdataDiscoveryManagement discoveryMgr;
        private IIndexManager indexMgr;
        private ILocalResourceManagement localResourceMgr;

        MyResourceManager(Properties properties) {
            this(null, null, properties);
        }

        MyResourceManager(IIndexManager indexManager,
                          ILocalResourceManagement localResourceManager,
                          Properties properties)
        {
            super(properties);
            this.discoveryMgr = new BigdataDiscoveryManager();
            this.indexMgr = indexManager;
            this.localResourceMgr = localResourceManager;
        }

        @Override
        public IBigdataDiscoveryManagement getDiscoveryManager() {
            return discoveryMgr;
        }

        @Override
        public ILocalResourceManagement getLocalResourceManager() {
            return localResourceMgr;
        }

        @Override
        public IIndexManager getIndexManager() {
            return indexMgr;
        }
            
        @Override
        public ShardService getDataService() {
            throw new UnsupportedOperationException();
        }
            
        @Override
        public UUID getDataServiceUUID() {
            return dataServiceUUID;
        }
    }//end class MyResourceManager

//BTM - FOR_CLIENT_SERVICE - END ----------------------------------------------



    /**
     * Utility method to register an index partition on the {@link #resourceManager}.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    protected void registerIndex(String name) throws InterruptedException, ExecutionException {
        
        final IndexMetadata indexMetadata = new IndexMetadata(name, UUID.randomUUID());
        {

            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            // must be an index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(
                    0, // partitionId
                    -1, // not a move.
                    new byte[] {}, // leftSeparator
                    null, // rightSeparator
                    new IResourceMetadata[] {//
                            resourceManager.getLiveJournal().getResourceMetadata(), //
                    }, //
                    IndexPartitionCause.register(resourceManager)
//                    ,"" // history
                    ));

            // submit task to register the index and wait for it to complete.
            concurrencyManager.submit(
                    new RegisterIndexTask(concurrencyManager, name,
                            indexMetadata)).get();

        }

    }
    
    /**
     * Test helper.
     * 
     * @param expected
     * @param actual
     */
    protected void assertSameResources(IRawStore[] expected, Set<UUID> actual) {
        
        if(log.isInfoEnabled()) {
            
            log.info("\nexpected=" + Arrays.toString(expected) + "\nactual="
                    + actual);
            
        }
        
        // copy to avoid side-effects.
        final Set<UUID> tmp = new HashSet<UUID>(actual);
        
        for(int i=0; i<expected.length; i++) {

            final UUID uuid = expected[i].getResourceMetadata().getUUID();
            
            assertFalse(tmp.isEmpty());

            if(!tmp.remove(uuid)) {
                
                fail("Expecting "+expected[i].getResourceMetadata());
                
            }
            
        }

        assertTrue(tmp.isEmpty());
        
    }

}
