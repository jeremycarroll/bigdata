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

import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

/**
 * Test suite for {@link BufferMode#Direct} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public class TestDirectJournal extends AbstractJournalTestCase {

    public TestDirectJournal() {
        super();
    }

    @Override
    public Properties getProperties() {

        Properties properties = super.getProperties();

        properties.setProperty(Options.DELETE_ON_EXIT,"true");

        properties.setProperty(Options.CREATE_TEMP_FILE,"true");

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

        return properties;

    }

    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Direct}.
     * 
     * @throws IOException
     */
    @Test
    public void test_create_direct01() throws IOException {

        final Properties properties = getProperties();

        Journal journal = new Journal(properties);

        DirectBufferStrategy bufferStrategy = (DirectBufferStrategy) journal.getBufferStrategy();

        assertTrue("isStable", bufferStrategy.isStable());
        assertTrue("isFullyBuffered", bufferStrategy.isFullyBuffered());
//        assertEquals(Options.FILE, properties.getProperty(Options.FILE),
//                bufferStrategy.file.toString());
        assertEquals(Options.INITIAL_EXTENT, Long.parseLong(Options.DEFAULT_INITIAL_EXTENT),
                bufferStrategy.getInitialExtent());
        assertEquals(Options.MAXIMUM_EXTENT, 0L /*soft limit for Direct buffer*/,
                bufferStrategy.getMaximumExtent());
        assertNotNull("raf", bufferStrategy.raf);
        assertEquals("bufferMode", BufferMode.Direct, bufferStrategy
                .getBufferMode());
        assertTrue("userExtent", bufferStrategy.getExtent() > bufferStrategy
                .getUserExtent());

        journal.destroy();

    }
    
}
