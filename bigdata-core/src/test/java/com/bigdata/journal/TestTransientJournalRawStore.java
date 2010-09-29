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

/**
 * Test suite integration for {@link AbstractRawStoreTestCase}.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 *
 * @todo While the transient store uses root blocks there is no means
 * currently defined to re-open a journal based on a transient store
 * and hence it is not possible to extend
 * {@link AbstractRestartSafeTestCase}.
 */
public class TestTransientJournalRawStore extends AbstractBufferStrategyTestCase {

    public TestTransientJournalRawStore() {
        super();
    }

    protected BufferMode getBufferMode() {
        return BufferMode.Transient;
    }
    //        public Properties getProperties() {
    //
    //            Properties properties = super.getProperties();
    //
    //            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
    //
    //            return properties;
    //
    //        }
    //
    //        protected IRawStore getStore() {
    //
    //            return new Journal(getProperties());
    //
    //        }
    //        public Properties getProperties() {
    //
    //            Properties properties = super.getProperties();
    //
    //            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
    //
    //            return properties;
    //
    //        }
    //
    //        protected IRawStore getStore() {
    //
    //            return new Journal(getProperties());
    //
    //        }
}
