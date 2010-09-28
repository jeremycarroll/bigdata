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

package com.bigdata.journal;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;
import java.util.Properties;

/**
 * Test suite integration for {@link AbstractMRMWTestCase}.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestWORMStrategyMRMW extends AbstractMRMWTestCase {

    public TestWORMStrategyMRMW() {
        super();
    }

    protected IRawStore getStore() {
        final Properties properties = getProperties();
        properties.setProperty(Options.CREATE_TEMP_FILE, "true");
        properties.setProperty(Options.DELETE_ON_EXIT, "true");
        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM.toString());
        properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + TestWORMStrategy.writeCacheEnabled);
        /*
         * The following two properties are dialed way down in order to
         * raise the probability that we will observe the following error
         * during this test.
         *
         * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
         *
         * FIXME We should make the MRMW test harder and focus on
         * interleaving concurrent extensions of the backing store for both
         * WORM and R/W stores.
         */
        // Note: Use a relatively small initial extent.
        properties.setProperty(Options.INITIAL_EXTENT, "" + DirectBufferPool.INSTANCE.getBufferCapacity() * 1);
        // Note: Use a relatively small extension each time.
        properties.setProperty(Options.MINIMUM_EXTENSION, "" + (long) (DirectBufferPool.INSTANCE.getBufferCapacity() * 1.1));
        return new Journal(properties).getBufferStrategy();
    }
}
