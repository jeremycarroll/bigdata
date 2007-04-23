/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.scaleup.PartitionMetadata;

import net.jini.core.lookup.ServiceID;

/**
 * Test ability to launch, register, discover and use a {@link MetadataService}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMetadataServer0 extends AbstractServerTestCase {

    /**
     * 
     */
    public TestMetadataServer0() {
    }

    /**
     * @param arg0
     */
    public TestMetadataServer0(String arg0) {
        super(arg0);
    }

    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    /**
     * Must be started by the test.
     */
    DataServer dataServer0;

    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {
        
        /*
         * Start up a data server before the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer1 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer1.config" });

        new Thread() {

            public void run() {
                
                dataServer1.run();
                
            }
            
        }.start();

        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config" });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

    }
    
    /**
     * destroy the test services.
     */
    public void tearDown() throws Exception {
        
        metadataServer0.destroy();

        dataServer1.destroy();
        
        if(dataServer0!=null) {
            
            destroyDataServer0();
            
        }
        
    }

    /**
     * Start data service 0.
     */
    protected void startDataServer0() {

        assert dataServer0 == null;
        
        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config" });

        new Thread() {

            public void run() {

                dataServer0.run();

            }

        }.start();

    }
    
    /**
     * Destroy data server 0.
     */
    protected void destroyDataServer0() {

        assert dataServer0 != null;
        
        System.err.println("Destroying DataServer0");

        dataServer0.destroy();

        dataServer0 = null;
        
    }
    
    /**
     * Test the ability to discover the {@link MetadataService} and the ability
     * of the {@link MetadataServer} to track {@link DataService}s.
     * <p>
     * Note: We start a data service both before and after the metadata server
     * and verify that both wind up in the service cache and that the metadata
     * server itself does not wind up in the cache since it should be excluded
     * by the service item filter.
     */
    public void test_serverRunning() throws Exception {

        // wait for the service to be ready.
        ServiceID dataService1ID = getServiceID(dataServer1);

        // wait for the service to be ready.
        ServiceID metadataServiceID = getServiceID(metadataServer0);

        // get proxy for this metadata service.
        final IMetadataService metadataServiceProxy = (IMetadataService) lookupDataService(metadataServiceID);

        assertNotNull("service not discovered", metadataServiceProxy);

        /*
         * Start a data service and verify that the metadata service will
         * discover it.
         */

        ServiceID dataService0ID = null;

        try {

            startDataServer0();

            /*
             * wait until we get the serviceID as an indication that the data
             * service is running.
             */

            dataService0ID = getServiceID(dataServer0);

            /*
             * verify that both data services were discovered by the metadata
             * server.
             */

            System.err.println("Sleeping");

            Thread.sleep(500);

            assertNotNull(metadataServer0.getDataServiceByID(dataService0ID));

            assertNotNull(metadataServer0.getDataServiceByID(dataService1ID));

            assertEquals("#dataServices", 2, metadataServer0
                    .getDataServiceCount());

        } finally {

            /*
             * Destroy one of the data services and verify that the metadata
             * server notices this event.
             */
            destroyDataServer0();

            if (dataService0ID != null) {

                System.err.println("Sleeping");

                Thread.sleep(500);

                assertEquals("#dataServices", 1, metadataServer0
                        .getDataServiceCount());

                assertNull(metadataServer0.getDataServiceByID(dataService0ID));

                assertNotNull(metadataServer0
                        .getDataServiceByID(dataService1ID));

            }

        }

    }

    /**
     * Registers a scale-out index and pre-partitions it to have data on each
     * of two {@link DataService} instances.
     */
    public void test_registerScaleOutIndex() throws Exception {

        // wait for the service to be ready.
        ServiceID dataService1ID = getServiceID(dataServer1);

        // wait for the service to be ready.
        ServiceID metadataServiceID = getServiceID(metadataServer0);

        // get proxy for this metadata service.
        final IMetadataService metadataServiceProxy = (IMetadataService) lookupDataService(metadataServiceID);

        assertNotNull("service not discovered", metadataServiceProxy);

        /*
         * wait until we get the serviceID as an indication that the data
         * service is running.
         */

        startDataServer0();

        // wait for the service to be ready.
        ServiceID dataService0ID = getServiceID(dataServer0);

        // lookup proxy for dataService0
        final IDataService dataService0Proxy = lookupDataService(dataService0ID); 

        try {
            /*
             * This should fail since the index was never registered.
             */
            dataService0Proxy.rangeCount(IDataService.UNISOLATED, "xyz", null,
                    null);
            
        } catch (ExecutionException ex) {
            
            System.err.println("cause="+ex.getCause());
            
            assertTrue(ex.getCause() instanceof IllegalStateException);
            
            log.info("Ignoring expected exception: " + ex);
            
        }

        //
        assertNotNull(metadataServiceProxy.getUnderUtilizedDataService());

        /*
         * register a scale-out index.
         */
        final String indexName = "testIndex";
        
        UUID indexUUID = metadataServiceProxy.registerIndex(indexName);
        
        log.info("Registered scale-out index: indexUUID="+indexUUID);
        
        /*
         * @todo request the partition for the scale-out index, figure out the
         * data service for that partition, and make sure that an index was
         * created on that data service for the partition.
         */

        // @todo encapsulate in method to generate metadata index name.
        byte[] val = metadataServiceProxy.lookup(IDataService.UNISOLATED,
                MetadataService.getMetadataName(indexName), new byte[] {});
        
        dataService0Proxy.rangeCount(IDataService.UNISOLATED, indexName, null,
                null);
        
    }
    
}
