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
 * Created on Sep 26, 2007
 */

package com.bigdata.service.mapReduce;

import com.bigdata.service.AbstractServerTestCase;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.DataServer;
import com.bigdata.service.MetadataServer;
import com.bigdata.service.mapReduce.Master.MapReduceServiceDiscoveryManager;

/**
 * Test suite for {@link Master}.
 * 
 * <pre>
 *     
 *     VM ARGS: -server -Xms1g -Xmx1g -XX:MaxDirectMemorySize=256M
 *     
 *     JDK 1.5.0_07
 *     
 *     
 *     Master: countKeywords: bufferMode=Transient; 1 data service.
 *     map( m=100, ntasks=416, nretried=0, success=100%, elapsed=28110ms )
 *     reduce( n=2, ntasks=2, nretried=0, success=100%, elapsed=5754ms )
 *     
 *     Master: countKeywords: bufferMode=Disk; forceOnCommit=No; 1 data service;
 *     map( m=100, ntasks=416, nretried=0, success=100%, elapsed=35618ms )
 *     reduce( n=2, ntasks=2, nretried=0, success=100%, elapsed=6312ms )
 *     
 *     
 *     EmbeddedMaster: countKeywords: bufferMode=Transient; 1 data service.
 *     map( m=100, ntasks=416, nretried=0, success=100%, elapsed=18206ms )
 *     reduce( n=2, ntasks=2, nretried=0, success=100%, elapsed=1677ms )
 *     
 *     EmbeddedMaster: countKeywords: bufferMode=Disk; forceOnCommit=No; 1 data service.
 *     map( m=100, ntasks=416, nretried=0, success=100%, elapsed=26063ms )
 *     reduce( n=2, ntasks=2, nretried=0, success=100%, elapsed=2267ms )
 *    
 * </pre>
 * 
 * @todo work through groupCommit - a lot of the latency is small conconcurrent
 *       writes on the data service when N>1, and the rest is sync to disk.
 * 
 * @todo improve performance by buffering map outputs across map tasks, but be
 *       sure to flush with the cancelJob() at the end of the map operation (it
 *       would make sense to have a cancelJob(boolean abort | flush) for this
 *       reason since we do not need to flush on abort).
 * 
 * @todo consider alternative design where the map outputs are fully buffered
 *       locally (on 1 file per output partition or just multiplexed onto one
 *       file) and send to the reduce host when the map operation completes (or
 *       read by the reduce host, but be careful to flush the data before
 *       sending it - if multiplexed, then how to split out the partitions. Raw
 *       file writes will be by far the fastest, leaving the reduce task to sort
 *       each map tasks output data and then merge sort the partitions). Does
 *       this improve the total throughput? Note that this test is already on a
 *       single host so this is really a network stack vs no network stack
 *       question.
 * 
 * @todo Do we ever need to sync to disk for temporary stores? Is it worth
 *       provisioning separate data services specifically for temporary data?
 * 
 * @todo You can directly compare the same map/reduce job here and with
 *       {@link TestEmbeddedMaster} to measure the cost of RPC -- just make sure
 *       that the job parameters are the same (M, N, #tasks, etc). The data
 *       service properties for the Master are in DataServer0.properties.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMaster extends AbstractServerTestCase {

    /**
     * 
     */
    public TestMaster() {
        super();
    }

    /**
     * @param arg0
     */
    public TestMaster(String arg0) {
        super(arg0);
    }

    /**
     * Starts in {@link #setUp()}.
     */
    MapServer mapServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    ReduceServer reduceServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    /**
     * Starts in {@link #setUp()}.
     */
    MapReduceServiceDiscoveryManager serviceDiscoveryManager;
    
    /**
     * Starts up a metadata service, a data service, a map service and a reduce
     * service.
     */
    public void setUp() throws Exception {

        super.setUp();
        
        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config"
                        });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config"
                        });

        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();

        client = new BigdataClient(
                new String[] { "src/resources/config/standalone/Client.config"
                        });

        mapServer0 = new MapServer(
                new String[] { "src/resources/config/standalone/MapServer0.config"
                      });
        
        reduceServer0 = new ReduceServer(
                new String[] { "src/resources/config/standalone/ReduceServer0.config"
                      });
        
        // Wait until all the services are up.
        getServiceID(metadataServer0);
        getServiceID(dataServer0);
        getServiceID(mapServer0);
        getServiceID(reduceServer0);
        
        // verify that the client has/can get the metadata service.
        assertNotNull("metadataService", client.getMetadataService());

        serviceDiscoveryManager = new MapReduceServiceDiscoveryManager(client);
        
    }
    
    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {

        serviceDiscoveryManager.terminate();

        if (mapServer0 != null) {
            
            mapServer0.destroy();

            mapServer0 = null;
            
        }
        
        if (reduceServer0 != null) {
            
            reduceServer0.destroy();

            reduceServer0 = null;
            
        }
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }

        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
        super.tearDown();
        
    }

    public void testCountKeywords() {
        
        MapReduceJob job = new CountKeywordJob(100/* m */, 10/* n */);
//      MapReduceJob job = new CountKeywordJob(1/* m */, 1/* n */);

        // non-zero to submit no more than this many map inputs.
//        job.setMaxMapTasks( 10 );

        // the timeout for a map task.
//        job.setMapTaskTimeout(2*1000/*millis*/);

        // the timeout for a reduce task.
//        job.setReduceTaskTimeout(2*1000/*millis*/);

        // the maximum #of times a map task will be retried (zero disables retry).
//        job.setMaxMapTaskRetry(3);

        // the maximum #of times a reduce task will be retried (zero disables retry).
//        job.setMaxReduceTaskRetry(3);

        /*
         * Run the map/reduce operation.
         */

        AbstractMaster master = new Master(job, client, serviceDiscoveryManager);
        
        master.run(.9d, .9d);
        
        System.out.println(master.status());
        
        assertTrue("map problem?",master.getMapPercentSuccess()>0.9);

        assertTrue("reduce problem?", master.getReducePercentSuccess()>0.9);

    }
    
}
