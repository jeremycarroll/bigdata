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
/*
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rdf.store.LocalTripleStoreWithEmbeddedDataService.Options;

/**
 * Proxy test suite for {@link LocalTripleStoreWithEmbeddedDataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link TestScaleOutTripleStoreWithLocalDataServiceFederation}
 */
public class TestLocalTripleStoreWithEmbeddedDataService extends AbstractTestCase {

    /**
     * 
     */
    public TestLocalTripleStoreWithEmbeddedDataService() {
    }

    public TestLocalTripleStoreWithEmbeddedDataService(String name) {
        super(name);
    }
    
    public static Test suite() {

        final TestLocalTripleStoreWithEmbeddedDataService delegate = new TestLocalTripleStoreWithEmbeddedDataService(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Local Triple Store With Embedded Data Service Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */

        // ...
        
        /*
         * Proxied test suite for use only with the LocalConcurrentTripleStore.
         */

//        suite.addTestSuite(TestLocalTripleStoreTransactionSemantics.class);

        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */

        suite.addTest(TestTripleStoreBasics.suite());
        
        return suite;
        
    }

    /**
     * The directory under which the persistent data for the unit test is
     * stored.
     */
    File testDir = null;
    
    public Properties getProperties() {

        /*
         * Note: nothing needs to be overriden. This test corresponds to the
         * default configuration for the LocalTripleStore.
         */

        Properties properties = new Properties(super.getProperties());

        if (testDir == null) {
            
            try {

                testDir = File.createTempFile("embeddedTripleStore","");
                
                /* delete the plain file since we want a directory by this name. */
                testDir.delete();

            } catch (IOException e) {

                throw new RuntimeException(e);

            }
            
        }
        
        properties.setProperty(Options.DATA_DIR, testDir.toString());

        /*
         * Turn off these options that were defaulted in our super class.
         */
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        properties.setProperty(Options.DELETE_ON_EXIT,"false");
        
        return properties;
        
    }
    
    protected AbstractTripleStore getStore() {
        
        return new LocalTripleStoreWithEmbeddedDataService( getProperties() );
        
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
    protected AbstractTripleStore reopenStore(AbstractTripleStore store) {
        
        // close the store.
        store.close();
        
        if (!store.isStable()) {
            
            throw new UnsupportedOperationException("The backing store is not stable");
            
        }
        
//        // Note: clone to avoid modifying!!!
//        Properties properties = (Properties)getProperties().clone();
//        
//        // Turn this off now since we want to re-open the same store.
//        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//        
//        // The backing file that we need to re-open.
//        File file = ((LocalTripleStoreWithEmbeddedDataService)store).getFile();
//        
//        assertNotNull(file);
//        
//        // Set the file property explictly.
//        properties.setProperty(Options.FILE,file.toString());
        
        return new LocalTripleStoreWithEmbeddedDataService( getProperties() );
        
    }

}
