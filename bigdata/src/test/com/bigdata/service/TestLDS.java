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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.util.Properties;

import com.bigdata.journal.AbstractIndexManagerTestCase;
import com.bigdata.journal.ProxyTestCase;
import com.bigdata.resources.ResourceManager;

/**
 * Delegate for {@link ProxyTestCase}s for services running against an
 * {@link LocalDataServiceFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLDS extends
        AbstractIndexManagerTestCase<LocalDataServiceFederation> {

    /**
     * 
     */
    public TestLDS() {
        super();
    }

    /**
     * @param name
     */
    public TestLDS(String name) {
        super(name);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
//        // Note: uses transient mode for tests.
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
//                .toString());
        
        // when the data are persistent use the test to name the data directory.
        properties.setProperty(LocalDataServiceClient.Options.DATA_DIR,
                getName());
        
        // disable moves.
        properties.setProperty(ResourceManager.Options.MAXIMUM_MOVES_PER_TARGET,"0");
        
        return properties;
        
    }

//    private File dataDir;
//    
//    /**
//     * Data files are placed into a directory named by the test. If the
//     * directory exists, then it is removed before the federation is set up.
//     */
//    public void setUp() throws Exception {
//      
//        dataDir = new File( getName() );
//        
//        if(dataDir.exists() && dataDir.isDirectory()) {
//
//            recursiveDelete( dataDir );
//            
//        }
//
//    }
//    
//    public void tearDown() throws Exception {
//        
//        /*
//         * Optional cleanup after the test runs, but sometimes its helpful to be
//         * able to see what was created in the file system.
//         */
//        
//        if(true && dataDir.exists() && dataDir.isDirectory()) {
//
//            recursiveDelete( dataDir );
//            
//        }
//        
//    }
//    
//    /**
//     * Recursively removes any files and subdirectories and then removes the
//     * file (or directory) itself.
//     * 
//     * @param f
//     *            A file or directory.
//     */
//    private void recursiveDelete(File f) {
//        
//        if(f.isDirectory()) {
//            
//            File[] children = f.listFiles();
//            
//            for(int i=0; i<children.length; i++) {
//                
//                recursiveDelete( children[i] );
//                
//            }
//            
//        }
//        
//        System.err.println("Removing: "+f);
//        
//        if (!f.delete())
//            throw new RuntimeException("Could not remove: " + f);
//
//    }

    @Override
    protected LocalDataServiceFederation getStore(Properties properties) {

        return new LocalDataServiceClient(properties).connect();
    }

    @Override
    protected LocalDataServiceFederation reopenStore(LocalDataServiceFederation fed) {
        
        final Properties properties = fed.getClient().getProperties();
        
        fed.shutdown();
        
        return new LocalDataServiceClient(properties).connect();
                
    }
    
}
