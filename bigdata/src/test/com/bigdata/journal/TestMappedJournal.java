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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

/**
 * Test suite for {@link BufferMode#Mapped} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public class TestMappedJournal extends AbstractTestCase {

    public TestMappedJournal() {
        super();
    }

    public TestMappedJournal(String name) {
        super(name);
    }

    public static Test suite() {

        final TestMappedJournal delegate = new TestMappedJournal(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Mapped Journal Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class.
        suite.addTestSuite(TestMappedJournal.class);

        /*
         * Pickup the basic journal test suite. This is a proxied test suite, so
         * all the tests will run with the configuration specified in this test
         * class and its optional .properties file.
         */
        suite.addTest(TestJournalBasics.suite());

        return suite;

    }

    public Properties getProperties() {

        Properties properties = super.getProperties();

        properties.setProperty("bufferMode", BufferMode.Mapped.toString());

        return properties;

    }
    
    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Mapped}.
     * 
     * @throws IOException
     */
    public void test_create_mapped01() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();

        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            assertEquals("slotSize", 128, journal.slotSize);
            assertNotNull("slotMath", journal.slotMath);
            
            MappedBufferStrategy bufferStrategy = (MappedBufferStrategy) journal._bufferStrategy;
            
            assertEquals("file", filename, bufferStrategy.file.toString());
            assertEquals("initialExtent", Journal.DEFAULT_INITIAL_EXTENT,
                    bufferStrategy.getExtent());
            assertNotNull("raf", bufferStrategy.raf);
            assertEquals("bufferMode", BufferMode.Mapped, bufferStrategy.getBufferMode());
            assertNotNull("directBuffer", bufferStrategy.directBuffer);
            assertNotNull("mappedBuffer", bufferStrategy.mappedBuffer);
            assertEquals("mappedBuffer",bufferStrategy.directBuffer,bufferStrategy.directBuffer);
            assertEquals("", bufferStrategy.getExtent(), bufferStrategy.directBuffer
                    .capacity());
            
            journal.close();

        } finally {
            
            deleteTestJournalFile(filename);
            
        }

    }
    
}
