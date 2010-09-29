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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;
import com.bigdata.io.DirectBufferPool;

/**
 * Test suite for {@link BufferMode#DiskWORM} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestWORMStrategy extends AbstractJournalTestCase {

    public TestWORMStrategy() {
        super();
    }

    @Override
    public Properties getProperties() {

        final Properties properties = super.getProperties();

        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(Options.DELETE_ON_EXIT, "true");

        properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                + writeCacheEnabled);

        return properties;

    }
    
    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#DiskWORM}.
     * 
     * @throws IOException
     */
    @Test
    public void test_create_disk01() throws IOException {

        final Properties properties = getProperties();

        final Journal journal = new Journal(properties);

        try {

            final WORMStrategy bufferStrategy = (WORMStrategy) journal
                    .getBufferStrategy();

            assertTrue("isStable", bufferStrategy.isStable());
            assertFalse("isFullyBuffered", bufferStrategy.isFullyBuffered());
            // assertEquals(Options.FILE, properties.getProperty(Options.FILE),
            // bufferStrategy.file.toString());
            assertEquals(Options.INITIAL_EXTENT, Long
                    .parseLong(Options.DEFAULT_INITIAL_EXTENT), bufferStrategy
                    .getInitialExtent());
            assertEquals(Options.MAXIMUM_EXTENT,
                    0L/* soft limit for disk mode */, bufferStrategy
                            .getMaximumExtent());
            assertNotNull("raf", bufferStrategy.getRandomAccessFile());
            assertEquals(Options.BUFFER_MODE, BufferMode.DiskWORM, bufferStrategy
                    .getBufferMode());

        } finally {

            journal.destroy();

        }

    }
    
    /**
     * Unit test verifies that {@link Options#CREATE} may be used to initialize
     * a journal on a newly created empty file.
     * 
     * @throws IOException
     */
    @Test
    public void test_create_emptyFile() throws IOException {
        
        final File file = File.createTempFile(this.getClass().getName(), Options.JNL);

        final Properties properties = new Properties();

        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM.toString());

        properties.setProperty(Options.FILE, file.toString());

        properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                + writeCacheEnabled);

        final Journal journal = new Journal(properties);

        try {

            assertEquals(file, journal.getFile());

        } finally {

            journal.destroy();

        }

    }

    /**
     * Note: The write cache is allocated by the {@link WORMStrategy} from
     * the {@link DirectBufferPool} and should be released back to that pool as
     * well, so the size of the {@link DirectBufferPool} SHOULD NOT grow as we
     * run these tests. If it does, then there is probably a unit test which is
     * not tearing down its {@link Journal} correctly.
     */
//    /**
//     * Note: Since the write cache is a direct ByteBuffer we have to make it
//     * very small (or disable it entirely) when running the test suite or the
//     * JVM will run out of memory - this is exactly the same (Sun) bug which
//     * motivates us to reuse the same ByteBuffer when we overflow a journal
//     * using a write cache. Since small write caches are disallowed, we wind up
//     * testing with the write cache disabled!
//     */
    static final boolean writeCacheEnabled = true;
    
}
    
