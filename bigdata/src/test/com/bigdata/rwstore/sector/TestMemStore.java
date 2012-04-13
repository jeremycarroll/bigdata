package com.bigdata.rwstore.sector;

import java.io.IOException;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.AbstractMRMWTestCase;
import com.bigdata.journal.AbstractMROWTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.journal.TestJournalBasics;
import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link MemStore}.
 * 
 * @author thompsonbry
 */
public class TestMemStore extends AbstractJournalTestCase {

	public TestMemStore() {
	}

	public TestMemStore(String name) {
		super(name);
	}

	public Properties getProperties() {

        final Properties properties = super.getProperties();

        properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);

        properties.setProperty(Options.BUFFER_MODE, BufferMode.MemStore
                .toString());
        
        /*
         * Make sure that we are not create a backing file. The CREATE_TEMP_FILE
         * option does not require a file name.  So, make sure there is no file
         * name and make sure that we CREATE_TEMP_FILE is not true.
         */
        {

            assertNull(properties.getProperty(Options.FILE));

            if (Boolean
                    .valueOf(properties.getProperty(Options.CREATE_TEMP_FILE,
                            Options.DEFAULT_CREATE_TEMP_FILE))) {

                properties.setProperty(Options.CREATE_TEMP_FILE, "false");
                
            }

        }

		// properties.setProperty(Options.BUFFER_MODE,
		// BufferMode.TemporaryRW.toString());

		// properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		// properties.setProperty(Options.FILE,
		// "/Volumes/SSDData/TestRW/tmp.rw");

		properties.setProperty(Options.DELETE_ON_EXIT, "true");

		// ensure history retention to force deferredFrees
		// properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,
		// "1"); // Non-zero
		
		// Set OVERWRITE_DELETE
		// properties.setProperty(RWStore.Options.OVERWRITE_DELETE, "true");

		return properties;

	}

	public static Test suite() {

        final TestMemStore delegate = new TestMemStore(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "MemStore Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // test suite for the IRawStore api.
        suite.addTestSuite( TestRawStore.class );

        // Note: test suite not used since there is no file channel to be closed by interrupts.
//        suite.addTestSuite( TestInterrupts.class );

        // test suite for MROW correctness.
        suite.addTestSuite( TestMROW.class );

        // test suite for MRMW correctness.
        suite.addTestSuite( TestMRMW.class );

        /*
         * Pickup the basic journal test suite. This is a proxied test suite, so
         * all the tests will run with the configuration specified in this test
         * class and its optional .properties file.
         */
        suite.addTest(TestJournalBasics.suite());

        return suite;

    }

    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Transient}.
     * 
     * @throws IOException
     */
    public void test_create_transient01() throws IOException {

		final IRawStore store = new MemStore(DirectBufferPool.INSTANCE, 1/* nbuffers */);
		try {
			assertFalse("isStable", store.isStable());
			assertTrue("isFullyBuffered", store.isFullyBuffered());
		} finally {
			store.destroy();
		}
    }
    
    /**
     * Test suite integration for {@link AbstractRawStoreTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
      */
    public static class TestRawStore extends AbstractRawStoreTestCase {
        
        public TestRawStore() {
            super();
        }

        public TestRawStore(String name) {
            super(name);
        }
        
		/** Note: Properties are not used. */
		public Properties getProperties() {

			return new Properties();

		}

		protected IRawStore getStore() {

			return new MemStore(DirectBufferPool.INSTANCE);

        }

    }

    /**
     * Test suite integration for {@link AbstractMROWTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class TestMROW extends AbstractMROWTestCase {
        
        public TestMROW() {
            super();
        }

        public TestMROW(String name) {
            super(name);
		}

		/** Note: Properties are not used. */
		public Properties getProperties() {

			return new Properties();

		}

		protected IRawStore getStore() {

			return new MemStore(DirectBufferPool.INSTANCE);

		}

    }

    /**
     * Test suite integration for {@link AbstractMRMWTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class TestMRMW extends AbstractMRMWTestCase {
        
        public TestMRMW() {
            super();
        }

        public TestMRMW(String name) {
            super(name);
        }

		/** Note: Properties are not used. */
		public Properties getProperties() {

			return new Properties();

		}

		protected IRawStore getStore() {

			return new MemStore(DirectBufferPool.INSTANCE);

        }

    }

}
