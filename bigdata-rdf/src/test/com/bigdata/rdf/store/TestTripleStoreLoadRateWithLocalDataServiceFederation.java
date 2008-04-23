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

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.service.LocalDataServiceFederation;

/**
 * Variant of {@link TestTripleStoreLoadRateWithJiniFederation} that tests with
 * an {@link LocalDataServiceFederation} and therefore does not incur costs for
 * network IO and does not use scale-out indices (no data service).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreLoadRateWithLocalDataServiceFederation extends
        AbstractLocalDataServiceFederationTestCase {

    /**
     * 
     */
    public TestTripleStoreLoadRateWithLocalDataServiceFederation() {
        super();
    }

    /**
     * @param arg0
     */
    public TestTripleStoreLoadRateWithLocalDataServiceFederation(String arg0) {
        super(arg0);
    }

    /**
     * The triple store under test.
     */
    ScaleOutTripleStore store;

    public void setUp() throws Exception {

        super.setUp();

        // connect to the database.
        store = new ScaleOutTripleStore(client, "test", ITx.UNISOLATED);

    }

    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
        // Use the disk-backed store.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
        
        // name data directory for the unit test.
        properties.setProperty(com.bigdata.service.LocalDataServiceClient.Options.DATA_DIR, getName());
        
        // minimize overflow if we will be writing a large store.
        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*500);
        properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte*500);

        // turn off incremental truth maintenance.
        properties.setProperty(DataLoader.Options.CLOSURE,ClosureEnum.None.toString());

        // turn off text indexing.
//        properties.setProperty(Options.TEXT_INDEX,"false");

        // change the default port for httpd exposed by the load balancer. 
        properties.setProperty(com.bigdata.service.LoadBalancerService.Options.HTTPD_PORT,"8080");
        
        return properties;
        
    }
    
    public void test_loadNCIOncology() throws IOException {

        // load the data set.
//        store.getDataLoader().loadData("../rdf-data/nciOncology.owl", "", RDFFormat.RDFXML);
        store.getDataLoader().loadData("../rdf-data/Thesaurus.owl", "", RDFFormat.RDFXML);

        // compute the database at once closure.
//        store.getInferenceEngine().computeClosure(null/*focusStore*/);
        
    }

    public void test_U1() {
        
        new ConcurrentDataLoader(store, 10/*nthreads*/, 10000 /*bufferCapacity*/, new File("../rdf-data/lehigh/U1"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }
    
    /**
     * @todo setup an experiment driver to explore the parameter space for
     *       nthreads, buffer size, LUBM size, federation parameters, etc?
     */
    public void test_U10() {
        
        new ConcurrentDataLoader(store, 10/*nthreads*/, 100000 /*bufferCapacity*/, new File("../rdf-data/lehigh/U10"), new FilenameFilter(){

            public boolean accept(File dir, String name) {
                if(name.endsWith(".owl")) return true;
                return false;
            }
            
        });
        
    }

    /**
     * 
     * <dl>
     * <dt>-Dnthreads</dt>
     * <dd>#of threads to use.</dd>
     * <dt>-DbufferCapacity</dt>
     * <dd>Capacity of the statement buffers.</dd>
     * <dt>-Ddocuments.directory</dr>
     * <dd>The file or directory to be loaded (recursive processing).</dd>
     * </dl>
     * 
     * You must also specify
     * 
     * <pre>
     *   -Djava.security.policy=policy.all
     * </pre>
     * 
     * and probably want to specify
     * 
     * <pre>
     *  -Dcom.sun.jini.jeri.tcp.useNIO=true
     * </pre>
     * 
     * as well.
     * 
     * @todo support load of the ontology as well?
     * 
     * @todo make this part of the {@link ConcurrentDataLoader} and modify so
     *       that you can run against any pre-existing database, including a
     *       jini-based bigdata federation, or create a new one from the defined
     *       properties.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        final int nthreads = Integer.parseInt(System.getProperty("nthreads","20")); 
        
        final int bufferCapacity = Integer.parseInt(System.getProperty("bufferCapacity","100000")); 
        
        final String file = System.getProperty("documents.directory");
  
        if(file==null) throw new RuntimeException("Required property 'documents.directory' was not specified");
        
        TestTripleStoreLoadRateWithLocalDataServiceFederation test = new TestTripleStoreLoadRateWithLocalDataServiceFederation("test");
        
        test.setUp();
        
        new ConcurrentDataLoader(test.store, nthreads, bufferCapacity,
                new File(file), new FilenameFilter() {

            public boolean accept(File dir, String name) {
//                if(name.endsWith(".owl")) return true;
                return true;
//                return false;
            }
            
        });
        
        test.tearDown();
        
        System.out.println("Exiting normally.");
        
        System.exit(0);
        
    }

}
