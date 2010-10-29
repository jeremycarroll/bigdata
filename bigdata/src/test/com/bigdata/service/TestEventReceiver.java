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
 * Created on Mar 15, 2009
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase2;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TransactionService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.service.EventReceiver.EventBTree;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.ibm.icu.impl.LinkedHashMap;

//BTM
//BTM - PRE_CLIENT_SERVICE import com.bigdata.event.EventQueue;
import com.bigdata.service.Event;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.event.EventQueueSender;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.service.Session;
import com.bigdata.service.ndx.ScaleOutIndexCounters;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for the {@link EventReceiver}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEventReceiver extends TestCase2 {

    /**
     * 
     */
    public TestEventReceiver() {
    }

    /**
     * @param arg0
     */
    public TestEventReceiver(String arg0) {
        super(arg0);
    }
    
    /**
     * Subclass overrides {@link #sendEvent()} to send to a configured
     * {@link EventReceiver} on the {@link MockFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static class MyEvent extends Event {

        /**
         * 
         */
        private static final long serialVersionUID = 3987546249519888387L;

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE private transient IBigdataFederation fed;
        private transient ILocalResourceManagement localResourceMgr;
        private transient IEventReceivingService eventReceiver;
//BTM - PRE_CLIENT_SERVICE - END

        /**
         * @param fed
         * @param resource
         * @param majorEventType
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE //BTM        public MyEvent(IBigdataFederation fed, EventResource resource, Object majorEventType) {
//BTM - PRE_CLIENT_SERVICE public MyEvent(IBigdataFederation fed,
//BTM - PRE_CLIENT_SERVICE                EventQueue eventQueue,
//BTM - PRE_CLIENT_SERVICE                Class serviceIface,
//BTM - PRE_CLIENT_SERVICE                String serviceName,
//BTM - PRE_CLIENT_SERVICE                UUID serviceUUID,
//BTM - PRE_CLIENT_SERVICE                EventResource resource,
//BTM - PRE_CLIENT_SERVICE                Object majorEventType) {
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE //BTM            super(fed, resource, majorEventType);
//BTM - PRE_CLIENT_SERVICE                  super(eventQueue, serviceIface, serviceName, serviceUUID, resource, majorEventType);
//BTM - PRE_CLIENT_SERVICE                  this.fed = fed;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM        protected MyEvent(final IBigdataFederation fed, final EventResource resource, final Object majorEventType, final Object minorEventType, final Map<String, Object> details) {
//BTM - PRE_CLIENT_SERVICE         protected MyEvent(final IBigdataFederation fed,
//BTM - PRE_CLIENT_SERVICE                           final EventQueue eventQueue,
//BTM - PRE_CLIENT_SERVICE                           final Class serviceIface,
//BTM - PRE_CLIENT_SERVICE                           final String serviceName,
//BTM - PRE_CLIENT_SERVICE                           final UUID serviceUUID,
//BTM - PRE_CLIENT_SERVICE                           final EventResource resource,
//BTM - PRE_CLIENT_SERVICE                           final Object majorEventType,
//BTM - PRE_CLIENT_SERVICE                           final Object minorEventType,
//BTM - PRE_CLIENT_SERVICE                           final Map<String,
//BTM - PRE_CLIENT_SERVICE                           Object> details) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM            super(fed, resource, majorEventType, minorEventType, details);
//BTM - PRE_CLIENT_SERVICE             super(eventQueue, serviceIface, serviceName, serviceUUID, resource, majorEventType, minorEventType, details);
//BTM - PRE_CLIENT_SERVICE             this.fed = fed;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE //BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         @Override
//BTM - PRE_CLIENT_SERVICE         public MyEvent newSubEvent(Object minorEventType) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM            return new MyEvent(this.fed, this.resource, this.majorEventType, minorEventType, this.details);
//BTM - PRE_CLIENT_SERVICE             return new MyEvent(this.fed,
//BTM - PRE_CLIENT_SERVICE                                this.eventQueue,
//BTM - PRE_CLIENT_SERVICE                                this.serviceIface,
//BTM - PRE_CLIENT_SERVICE                                this.serviceName,
//BTM - PRE_CLIENT_SERVICE                                this.serviceUUID,
//BTM - PRE_CLIENT_SERVICE                                this.resource,
//BTM - PRE_CLIENT_SERVICE                                this.majorEventType,
//BTM - PRE_CLIENT_SERVICE                                minorEventType,
//BTM - PRE_CLIENT_SERVICE                                this.details);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         /**
//BTM - PRE_CLIENT_SERVICE          * Serializes and then de-serializes the event and sends the
//BTM - PRE_CLIENT_SERVICE          * de-serialized reference to the {@link EventReceiver}. This simulates
//BTM - PRE_CLIENT_SERVICE          * RMI where the receiver always has a different instance for each
//BTM - PRE_CLIENT_SERVICE          * message received.
//BTM - PRE_CLIENT_SERVICE          */
//BTM - PRE_CLIENT_SERVICE         protected void sendEvent() throws IOException {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             final byte[] data = SerializerUtil.serialize(this);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             final MyEvent e = (MyEvent) SerializerUtil.deserialize(data);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             ((MockFederation) fed).eventReceiver.notifyEvent(e);
//BTM - PRE_CLIENT_SERVICE             
//BTM - PRE_CLIENT_SERVICE         }
        public MyEvent(ILocalResourceManagement localResourceManager,
                       IEventReceivingService eventReceiver,
                       EventResource resource,
                       Object majorEventType)
        {
            super(localResourceManager.getEventQueueSender(),
                  localResourceManager.getServiceIface(),
                  localResourceManager.getServiceName(),
                  localResourceManager.getServiceUUID(),
                  resource,
                  majorEventType);
            this.localResourceMgr = localResourceManager;
            this.eventReceiver = eventReceiver;
        }

        protected MyEvent(final ILocalResourceManagement localResourceManager,
                          final IEventReceivingService eventReceiver,
                          final EventResource resource,
                          final Object majorEventType,
                          final Object minorEventType,
                          final Map<String,
                          Object> details)
        {
            super(localResourceManager.getEventQueueSender(),
                  localResourceManager.getServiceIface(),
                  localResourceManager.getServiceName(),
                  localResourceManager.getServiceUUID(),
                  resource,
                  majorEventType,
                  minorEventType,
                 details);
            this.localResourceMgr = localResourceManager;
            this.eventReceiver = eventReceiver;
        }

        @Override
        public MyEvent newSubEvent(Object minorEventType) {
            return new MyEvent(this.localResourceMgr,
                               this.eventReceiver,
                               this.resource,
                               this.majorEventType,
                               minorEventType,
                               this.details);
        }

        /**
         * Serializes and then de-serializes the event and sends the
         * de-serialized reference to the {@link EventReceiver}. This simulates
         * RMI where the receiver always has a different instance for each
         * message received.
         */
        protected void sendEvent() throws IOException {
            final byte[] data = SerializerUtil.serialize(this);
            final MyEvent e = (MyEvent) SerializerUtil.deserialize(data);
            (this.eventReceiver).notifyEvent(e);
        }
//BTM - PRE_CLIENT_SERVICE - END

    }//end class MyEvent
    
    /**
     * Test dispatch using both {@link Event#start()} and {@link Event#end()}.
     * 
     * @throws InterruptedException
     */
    public void test_start_end() throws InterruptedException {

        final EventBTree eventBTree = EventBTree.createTransient();
        
        final EventReceiver eventReceiver = new EventReceiver(
                1000/* eventHistoryMillis */, eventBTree);

        final IBigdataFederation fed = new MockFederation(eventReceiver);
        
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM        final Event e = new MyEvent(fed, new EventResource("testIndex"), "testEventType");
//BTM - PRE_CLIENT_SERVICE final Event e = new MyEvent(fed,
//BTM - PRE_CLIENT_SERVICE                             fed.getEventQueue(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceIface(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceName(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceUUID(),
//BTM - PRE_CLIENT_SERVICE                             new EventResource("testIndex"),
//BTM - PRE_CLIENT_SERVICE                             "testEventType");
        final Event e = new MyEvent( (ILocalResourceManagement)fed,
                                     ((MockFederation)fed).getService(),
                                     new EventResource("testIndex"),
                                     "testEventType" );
//BTM - PRE_CLIENT_SERVICE - END

        assertEquals(0, eventReceiver.eventCache.size());

        e.start();

        /*
         * Verify that the event is now in the receiver's cache.
         */
        
        assertEquals(1, eventReceiver.eventCache.size());
        
        assertTrue(eventReceiver.eventCache.containsKey(e.eventUUID));
        
        assertFalse(eventReceiver.eventCache.get(e.eventUUID).complete);
        
        try {

            // wait a bit so that endTime != startTime.
            Thread.sleep(200/* ms */);

        } finally {

            e.end();

        }

        // the event is still in the cache.
        assertEquals(1, eventReceiver.eventCache.size());
        
        /*
         * Verify the event as recorded in the B+Tree.
         */
        
        assertEquals(1L, eventReceiver.rangeCount(e.startTime, e.startTime + 1));

        assertTrue(eventReceiver.rangeIterator(e.startTime, e.startTime + 1)
                .hasNext());
        
        final Event t = eventReceiver.rangeIterator(e.startTime,
                e.startTime + 1).next();

        assertTrue(t.complete);

        assertEquals(e.eventUUID, t.eventUUID);

        assertEquals(e.startTime, t.startTime);
        
        assertEquals(e.endTime, t.endTime);

        /*
         * Verify that the receiver will purge the event from its cache.
         */
        
        // wait until the event is old enough to be purged.
        Thread.sleep(eventReceiver.eventHistoryMillis);
        
        // request purge of old events.
        eventReceiver.pruneHistory(System.currentTimeMillis());
        
        // event is no longer in the cache.
        assertEquals(0, eventReceiver.eventCache.size());
        assertFalse(eventReceiver.eventCache.containsKey(e.eventUUID));
        
    }

    /**
     * Test dispatch using only {@link Event#end()} (instantaneous events).
     * 
     * @throws InterruptedException
     */
    public void test_endOnly() throws InterruptedException {

        final EventBTree eventBTree = EventBTree.createTransient();
        
        final EventReceiver eventReceiver = new EventReceiver(
                1000/* eventHistoryMillis */, eventBTree);

        final IBigdataFederation fed = new MockFederation(eventReceiver);
        
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE //BTM        final Event e = new MyEvent(fed, new EventResource("testIndex"), "testEventType");
//BTM - PRE_CLIENT_SERVICE final Event e = new MyEvent(fed,
//BTM - PRE_CLIENT_SERVICE                             fed.getEventQueue(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceIface(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceName(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceUUID(),
//BTM - PRE_CLIENT_SERVICE                             new EventResource("testIndex"),
//BTM - PRE_CLIENT_SERVICE                             "testEventType");
        final Event e = new MyEvent( (ILocalResourceManagement)fed,
                                     ((MockFederation)fed).getService(),
                                     new EventResource("testIndex"),
                                     "testEventType" );
//BTM - PRE_CLIENT_SERVICE - END

        assertEquals(0, eventReceiver.eventCache.size());

        e.end();

        assertEquals(e.startTime, e.endTime);

        assertTrue(e.complete);

        /*
         * Verify that the event is in the receiver's cache.
         */

        assertEquals(1, eventReceiver.eventCache.size());
        
        assertTrue(eventReceiver.eventCache.containsKey(e.eventUUID));
        
        assertTrue(eventReceiver.eventCache.get(e.eventUUID).complete);

        /*
         * Verify the event as recorded in the B+Tree.
         */
        
        assertEquals(1L, eventReceiver.rangeCount(e.startTime, e.startTime+1));
        
        assertTrue(eventReceiver.rangeIterator(e.startTime, e.startTime + 1)
                .hasNext());
        
        final Event t = eventReceiver.rangeIterator(e.startTime,
                e.startTime + 1).next();

        assertTrue(t.complete);

        assertEquals(e.eventUUID, t.eventUUID);

        assertEquals(e.startTime, t.startTime);
        
        assertEquals(e.endTime, t.endTime);

        /*
         * Verify that the receiver will purge the event from its cache.
         */
        
        // wait until the event is old enough to be purged.
        Thread.sleep(eventReceiver.eventHistoryMillis);
        
        // request purge of old events.
        eventReceiver.pruneHistory(System.currentTimeMillis());
        
        // event is no longer in the cache.
        assertEquals(0, eventReceiver.eventCache.size());
        assertFalse(eventReceiver.eventCache.containsKey(e.eventUUID));
   
    }
    
    /**
     * Pumps a bunch of events through and then verifies that the start events
     * are correlated with the end events and that the size of the
     * {@link LinkedHashMap} is bounded by the #of incomplete events no older
     * than the configured eventHistoryMillis.
     * 
     * @throws InterruptedException
     */
    public void test_purgesHistory() throws InterruptedException {

        final long eventHistoryMillis = 1000L;
        
        final EventBTree eventBTree = EventBTree.createTransient();

        final EventReceiver eventReceiver = new EventReceiver(
                eventHistoryMillis, eventBTree);

        final IBigdataFederation fed = new MockFederation(eventReceiver);

        final Random r = new Random();
        
        final long begin = System.currentTimeMillis();
        long elapsed;
        int nevents = 0;
        while ((elapsed = System.currentTimeMillis() - begin) < eventHistoryMillis / 2) {
            
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE //BTM            final Event e = new MyEvent(fed, new EventResource("testIndex"), "testEventType");
//BTM - PRE_CLIENT_SERVICE final Event e = new MyEvent(fed,
//BTM - PRE_CLIENT_SERVICE                             fed.getEventQueue(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceIface(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceName(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceUUID(),
//BTM - PRE_CLIENT_SERVICE                             new EventResource("testIndex"),
//BTM - PRE_CLIENT_SERVICE                             "testEventType");
            final Event e = new MyEvent( (ILocalResourceManagement)fed,
                                         ((MockFederation)fed).getService(),
                                         new EventResource("testIndex"),
                                         "testEventType" );
//BTM - PRE_CLIENT_SERVICE - END

            if (r.nextDouble() < .2) {

                // instantaneous event.
                e.end();

            } else {

                /*
                 * event with duration.
                 */
                e.start();

                try {

                    Thread
                            .sleep(r.nextInt((int) eventHistoryMillis / 10)/* ms */);

                } finally {

                    e.end();

                }

            }

            nevents++;

        }

        // all the events should be in the cache.
        assertEquals(nevents, eventReceiver.eventCache.size());
        
        // sleep until the events should be expired.
        Thread.sleep(eventHistoryMillis);
        
        // prune the event cache using the specified timestamp.
        eventReceiver.pruneHistory(System.currentTimeMillis());

        // should be empty.
        assertEquals(0, eventReceiver.eventCache.size());
        
    }

    /**
     * Unit tests verifies that the APIs are thread-safe.
     * 
     * @throws InterruptedException
     * @throws ExecutionException 
     */
    public void test_threadSafe() throws InterruptedException, ExecutionException {

        final long eventHistoryMillis = 1000L;
        
        final EventBTree eventBTree = EventBTree.createTransient();

        final EventReceiver eventReceiver = new EventReceiver(
                eventHistoryMillis, eventBTree);

        final IBigdataFederation fed = new MockFederation(eventReceiver);

        final ExecutorService exService = Executors
                .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

        final int nthreads = 10;
        final int nevents = 100;
        try {

            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

            for (int i = 0; i < nthreads; i++) {

                tasks.add(new EventFactory(fed, nevents));

            }

            tasks.add( new EventConsumer(eventReceiver) );
            
            final List<Future<Void>> futures = exService.invokeAll(tasks);

            for(Future f : futures) {
                
                // verify no errors.
                f.get();
                
            }

        } finally {

            exService.shutdownNow();
            
        }
        
    }
    
    /**
     * Generates events.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class EventFactory implements Callable<Void> {
        
        private final IBigdataFederation fed;
        private final int nevents;

        public EventFactory(final IBigdataFederation fed,
                final int nevents) {

            this.fed = fed;

            this.nevents = nevents;
            
        }

        public Void call() throws Exception {

            final Random r = new Random();

            for (int i = 0; i < nevents; i++) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE //BTM                final Event e = new MyEvent(fed, new EventResource("testIndex"), "testEventType");
//BTM - PRE_CLIENT_SERVICE final Event e = new MyEvent(fed,
//BTM - PRE_CLIENT_SERVICE                             fed.getEventQueue(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceIface(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceName(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceUUID(),
//BTM - PRE_CLIENT_SERVICE                             new EventResource("testIndex"),
//BTM - PRE_CLIENT_SERVICE                             "testEventType");
                final Event e = new MyEvent( (ILocalResourceManagement)fed,
                                             ((MockFederation)fed).getService(),
                                             new EventResource("testIndex"),
                                             "testEventType" );
//BTM - PRE_CLIENT_SERVICE - END

                if (r.nextDouble() < .2) {

                    // instantaneous event.
                    e.end();

                } else {

                    /*
                     * event with duration.
                     */
                    e.start();

                    try {

                        Thread.sleep(r.nextInt(100)/* ms */);

                    } catch (InterruptedException ex) {

                        break;
                        
                    } finally {

                        e.end();

                    }

                }

            }

            return null;
            
        }
        
    }

    /**
     * Reports on events (places a concurrent read load on the B+Tree on which
     * the events are stored).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static private class EventConsumer implements Callable<Void> {
       
        final IEventReportingService eventReportingService;
        
        public EventConsumer(final IEventReportingService eventReportingService) {
            
            this.eventReportingService = eventReportingService;
            
        }
        
        public Void call() throws Exception {
            
            for (int i = 0; i < 50; i++) {

                eventReportingService.rangeCount(0L, Long.MAX_VALUE);

                final Iterator<Event> itr = eventReportingService
                        .rangeIterator(0L, Long.MAX_VALUE);

                while (itr.hasNext()) {

                    itr.next();

                }

                Thread.sleep(100/* ms */);

            }

            return null;
            
        }
        
    }
    
    /**
     * Mock federation to support the unit tests in the outer class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    static class MockFederation implements IBigdataFederation<IEventReceivingService> {
    static class MockFederation 
                     implements IBigdataFederation<IEventReceivingService>,
                                ILocalResourceManagement
    {
//BTM - PRE_CLIENT_SERVICE - END

        private final IEventReceivingService eventReceiver;
        private final UUID serviceUUID = UUID.randomUUID();
//BTM
private final MockSendEventsTask mockSendEventsTask = new MockSendEventsTask();

        MockFederation(final IEventReceivingService eventReceiver) {
            
            this.eventReceiver = eventReceiver;
            
        }
        
        public IEventReceivingService getService() {
            
            return eventReceiver;
            
        }
        
        public Class getServiceIface() {
        
            return IEventReceivingService.class;
//            return TestEventReceiver.class;
            
        }

        public String getServiceName() {
  
            return IEventReceivingService.class.getName();
            
//            return TestEventReceiver.class.getName();
            
        }

        public UUID getServiceUUID() {
            
            return serviceUUID;
            
        }

        /*
         * unused methods.
         */
        
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

        public CounterSet getCounterSet() {
            return null;
        }

//BTM        public IDataService getDataService(UUID serviceUUID) {
public ShardService getDataService(UUID serviceUUID) {
            return null;
        }

//BTM        public IDataService getDataServiceByName(String name) {
public ShardService getDataServiceByName(String name) {
            return null;
        }

        public UUID[] getDataServiceUUIDs(int maxCount) {
            return null;
        }

//BTM        public IDataService[] getDataServices(UUID[] uuid) {
public ShardService[] getDataServices(UUID[] uuid) {
            return null;
        }

        public ExecutorService getExecutorService() {
            return null;
        }

        public SparseRowStore getGlobalRowStore() {
            return null;
        }

        public String getHttpdURL() {
            return null;
        }

        public IClientIndex getIndex(String name, long timestamp) {
            return null;
        }

        public long getLastCommitTime() {
            return 0;
        }

        public LoadBalancer getLoadBalancerService() {
            return null;
        }

        public IMetadataIndex getMetadataIndex(String name, long timestamp) {
            return null;
        }

        public ShardLocator getMetadataService() {
            return null;
        }

        public String getServiceCounterPathPrefix() {
            return null;
        }

        public CounterSet getServiceCounterSet() {
            return null;
        }

        public TransactionService getTransactionService() {
            return null;
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

        public void registerIndex(IndexMetadata metadata) {
              
        }

        public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {
            return null;
        }

        public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys, UUID[] dataServiceUUIDs) {
            return null;
        }

        public BigdataFileSystem getGlobalFileSystem() {
            return null;
        }

        public IResourceLocator getResourceLocator() {
            return null;
        }

        public IResourceLockService getResourceLockService() {
            return null;
        }

        public TemporaryStore getTempStore() {
            return null;
        }

        public void didStart() {
              
        }

        public boolean isServiceReady() {
            return false;
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
            return null;
        }

//BTM - BEGIN -----------------------------------------------------------------
        public void serviceJoin(Service service, UUID serviceUUID) { }

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        public EventQueue getEventQueue() {
//BTM - PRE_CLIENT_SERVICE            return mockSendEventsTask;
//BTM - PRE_CLIENT_SERVICE        }
        public EventQueueSender getEventQueueSender() {
            return mockSendEventsTask;
        }

        // Required by ILocalResourceManagement
        public void terminate(long timeout) {
            //no-opp
        }
        public TaskCounters getTaskCounters() {
            return null;
        }
        public CounterSet getServiceCounterSet(boolean addCounters) {
            return null;
        }
        public void reattachDynamicCounters
                                 (ResourceManager resourceMgr,
                                  IConcurrencyManager concurrencyMgr)
        {
            //no-op
        }
        public ScaleOutIndexCounters getIndexCounters(String name) {
            return null;
        }
        public Session getSession() {
            return null;
        }
        public ExecutorService getThreadPool() {
            return null;
        }
        public ScheduledExecutorService getScheduledExecutor() {
            return null;
        }
        public String getHostname() {
            return "UNKNOWN-TESTING";
        }
//BTM - PRE_CLIENT_SERVICE - END

//BTM - END -------------------------------------------------------------------
        
    }

//BTM - BEGIN -----------------------------------------------------------------

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    static class MockSendEventsTask implements EventQueue, Runnable {
//BTM - PRE_CLIENT_SERVICE        final private BlockingQueue<Event> eventQueue = 
//BTM - PRE_CLIENT_SERVICE                          new LinkedBlockingQueue<Event>();
//BTM - PRE_CLIENT_SERVICE        public MockSendEventsTask() { }
//BTM - PRE_CLIENT_SERVICE        public void queueEvent(Event e) {
//BTM - PRE_CLIENT_SERVICE            eventQueue.add(e);
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE        public void run() {
//BTM - PRE_CLIENT_SERVICE            try {
//BTM - PRE_CLIENT_SERVICE                final LinkedList<Event> c = new LinkedList<Event>();
//BTM - PRE_CLIENT_SERVICE                eventQueue.drainTo(c);
//BTM - PRE_CLIENT_SERVICE                for (Event e : c) {
//BTM - PRE_CLIENT_SERVICE                    log.debug("sent event to load balancer");
//BTM - PRE_CLIENT_SERVICE                }
//BTM - PRE_CLIENT_SERVICE            } catch (Throwable t) {
//BTM - PRE_CLIENT_SERVICE                log.warn(IEventReceivingService.class.getName(), t);
//BTM - PRE_CLIENT_SERVICE            }
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE    }
    static class MockSendEventsTask implements EventQueueSender {
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
                log.warn(IEventReceivingService.class.getName(), t);
            }
        }
    }
//BTM - PRE_CLIENT_SERVICE - END

//BTM - END -------------------------------------------------------------------

}
