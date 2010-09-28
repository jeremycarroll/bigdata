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

package com.bigdata.resources;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IJournal;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.AbstractTransactionService;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * Test where the indices are copied during synchronous overflow processing
 * and where a non-zero [minReleaseAge] was specified.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReleaseResourcesWithCopy_NonZeroMinReleaseAge extends TestReleaseResources {

    public TestReleaseResourcesWithCopy_NonZeroMinReleaseAge() {
    }

    /**
     * This is the minimum release time that will be used for the test.
     * <P>
     * Note: 2000 is 2 seconds. This is what you SHOULD use for the test (it
     * can be a little longer if you run into problems).
     * <p>
     * Note: 200000 is 200 seconds. This can be used for debugging, but
     * always restore the value so that the test will run in a reasonable
     * time frame.
     */
    private final long MIN_RELEASE_AGE = 2000;

    @Override
    public Properties getProperties() {
        final Properties properties = new Properties(super.getProperties());
        properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, "" + MIN_RELEASE_AGE);
        return properties;
    }

    /**
     * Test where the index view is copied in its entirety onto the new
     * journal and the [minReleaseAge] is 2 seconds. In this case we have no
     * dependencies on the old journal, but the [minReleaseAge] is not
     * satisfied immediately so no resources are released during synchronous
     * overflow processing (assuming that synchronous overflow processing is
     * substantially faster than the [minReleaseAge]). We then wait until
     * the [minReleaseAge] has passed and force overflow processing again
     * and verify that the original journal was released while the 2nd and
     * 3rd journals are retained.
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test() throws IOException, InterruptedException, ExecutionException {
        // no overflow yet.
        assertEquals(0, resourceManager.getAsynchronousOverflowCount());
        // the initial journal.
        final AbstractJournal j0 = resourceManager.getLiveJournal();
        // the UUID for that journal.
        final UUID uuid0 = j0.getResourceMetadata().getUUID();
        // force overflow on the next commit.
        //            concurrencyManager.getWriteService().forceOverflow.set(true);
        resourceManager.forceOverflow.set(true);
        // disable asynchronous overflow processing to simplify the test environment.
        resourceManager.asyncOverflowEnabled.set(false);
        final String name = "A";
        // register the index - will also force overflow.
        registerIndex(name);
        // mark a timestamp after the 1st overflow event.
        final long begin = System.currentTimeMillis();
        // did overflow.
        assertEquals(1, resourceManager.getAsynchronousOverflowCount());
        // two journals.
        // @todo unit test needs to be updated to reflect purge in sync overflow.
        assertEquals(2, resourceManager.getManagedJournalCount());
        // no index segments.
        assertEquals(0, resourceManager.getManagedSegmentCount());
        // should have been closed against further writes but NOT deleted.
        assertTrue(resourceManager.openStore(uuid0).isReadOnly());
        // the new journal.
        final AbstractJournal j1 = resourceManager.getLiveJournal();
        // the UUID for that journal.
        final UUID uuid1 = j1.getResourceMetadata().getUUID();
        /*
         * Verify that the resources required for [A] after overflow are
         * exactly [j1].
         */
        assertSameResources(new IJournal[]{j1}, resourceManager.getResourcesForTimestamp(j1.getRootBlockView().getFirstCommitTime()));
        {
            final long elapsed = System.currentTimeMillis() - begin;
            final long delay = MIN_RELEASE_AGE - elapsed;
            System.err.println("Waiting " + delay + "ms to force an overflow");
            // wait until the minReleaseAge would be satisified.
            Thread.sleep(delay);
        }
        // force overflow on the next commit.
        //            concurrencyManager.getWriteService().forceOverflow.set(true);
        resourceManager.forceOverflow.set(true);
        // register another index - will force another overflow.
        registerIndex("B");
        // did overflow.
        assertEquals(2, resourceManager.getAsynchronousOverflowCount());
        // note: purge is not invoke on overflow anymore, so do it ourselves.
        resourceManager.purgeOldResources(100, false);
        // should have been closed no later than when it was deleted.
        assertFalse(j0.isOpen());
        // two journals (the original journal should have been deleted).
        assertEquals(2, resourceManager.getManagedJournalCount());
        // no index segments.
        assertEquals(0, resourceManager.getManagedSegmentCount());
        // should have been closed against further writes but NOT deleted.
        assertTrue(resourceManager.openStore(uuid1).isReadOnly());
        // verify unwilling to open the store with that UUID.
        try {
            resourceManager.openStore(uuid0);
            fail("Expecting exception: " + NoSuchStoreException.class);
        } catch (NoSuchStoreException ex) {
            log.warn("Expected exception: " + ex);
        }
    }
}
