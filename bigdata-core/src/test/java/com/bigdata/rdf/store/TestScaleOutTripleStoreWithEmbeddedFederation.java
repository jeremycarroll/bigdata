/*

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
/*
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import com.bigdata.journal.ITx;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.EmbeddedFederation;

/**
 * Proxy test suite for {@link ScaleOutTripleStore} running against an
 * {@link EmbeddedFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestScaleOutTripleStoreWithEmbeddedFederation extends AbstractTestCase {

    /**
     * 
     */
    public TestScaleOutTripleStoreWithEmbeddedFederation () {
    }

    /**
     * Properties used by tests in the file and in this proxy suite.
     */
    @Override
    public Properties getProperties() {

        final Properties properties = new Properties( super.getProperties() );

//         Note: this reduces the disk usage at the expense of memory usage.
//        properties.setProperty(EmbeddedBigdataFederation.Options.BUFFER_MODE,
//                BufferMode.Transient.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

//        properties.setProperty(Options.CREATE_TEMP_FILE,"true");

//        properties.setProperty(Options.DELETE_ON_EXIT,"true");

        properties.setProperty(DataService.Options.OVERFLOW_ENABLED,"false");
        
        // disable platform statistics collection.
        properties.setProperty(
                EmbeddedClient.Options.COLLECT_PLATFORM_STATISTICS, "false");

        /*
         * Note: there are also properties to control the #of data services
         * created in the embedded federation.
         */
        
        return properties;

    }

    /**
     * An embedded federation is setup and torn down per unit test.
     */
    EmbeddedClient client;

    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    @Override
    public void setUp(final ProxyTestCase testCase) throws Exception {
    
        super.setUp(testCase);

        final File dataDir = new File(this.getClass().getName());

        if (dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete(dataDir);

        }

        final Properties properties = new Properties(getProperties());
        
//        // Note: directory named for the unit test (name is available from the
//        // proxy test case).
//        properties.setProperty(EmbeddedClient.Options.DATA_DIR, testCase
//                .getName());

        // new client
        client = new EmbeddedClient(properties);
        
        // connect.
        client.connect();
        
    }
    
    @Override
    public void tearDown(final ProxyTestCase testCase) throws Exception {

        if (client != null) {

            if (client.isConnected()) {

                // destroy the federation under test.
                client.getFederation().destroy();

            }

            /*
             * Note: Must clear the reference or junit will cause the federation
             * to be retained.
             */
            client = null;

        }
        
    }
    
    private AtomicInteger inc = new AtomicInteger();
    
    protected AbstractTripleStore getStore(final Properties properties) {
    
        // Note: distinct namespace for each triple store created on the federation.
        final String namespace = "test"+inc.incrementAndGet();
   
        final AbstractTripleStore store = new ScaleOutTripleStore(client
                .getFederation(), namespace, ITx.UNISOLATED, properties);

        store.create();
        
        return store;
        
    }
 
    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed, or if the store can not
     *                be re-opened, e.g., from failure to obtain a file lock,
     *                etc.
     */
    protected AbstractTripleStore reopenStore(final AbstractTripleStore store) {

        final String namespace = store.getNamespace();
        
        // Note: properties we need to re-start the client.
        final Properties properties = new Properties(client.getProperties());
        
        // Note: also shutdown the embedded federation.
        client.disconnect(true/*immediateShutdown*/);

        // Turn this off now since we want to re-open the same store.
        properties.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE, "false");

        // The data directory for the embedded federation.
        final File file = ((EmbeddedFederation) ((ScaleOutTripleStore) store)
                .getIndexManager()).getDataDir();

        // Set the file property explicitly.
        properties.setProperty(EmbeddedClient.Options.DATA_DIR, file.toString());

        // new client.
        client = new EmbeddedClient( properties );
     
        // connect.
        client.connect();
        
        // Obtain view on the triple store.
        return new ScaleOutTripleStore(client.getFederation(), namespace,
                ITx.UNISOLATED,
                store.getProperties()
//                client.getProperties()
                ).init();
        
    }

}
