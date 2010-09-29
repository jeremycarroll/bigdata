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
import com.bigdata.service.AbstractTransactionService;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * Test where the index view is copied in its entirety onto the new journal
 * and the [minReleaseAge] is ZERO(0). In this case we have no dependencies
 * on the old journal and it is released during overflow processing.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReleaseResourcesWithCopyImmediateRelease extends TestReleaseResources {

    public TestReleaseResourcesWithCopyImmediateRelease() {
    }

    @Override
    public Properties getProperties() {
        Properties properties = new Properties(super.getProperties());
        // Never retain history.
        properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, "0");
        return properties;
    }

    /**
     * Test creates an index whose view is initially defined by the initial
     * journal on the sole data service. An overflow of the journal is then
     * forced, which re-defines the view on the new journal. Since the index
     * is very small (it is empty), it is "copied" onto the new journal
     * rather than causing an index segment build to be scheduled. The
     * original journal should be released (deleted) during synchronous
     * overflow processing.
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
        // did overflow.
        assertEquals(1, resourceManager.getAsynchronousOverflowCount());
        //			/*
        //			 * Note: the old journal should have been closed for writes during
        //			 * synchronous overflow processing and deleted from the file system.
        //			 */
        //            assertTrue(j0.isOpen()); // still open
        //            assertTrue(j0.isReadOnly()); // but no longer accepts writes.
        //
        //            /*
        //             * Purge old resources. If the index was copied to the new journal
        //             * then there should be no dependency on the old journal and it
        //             * should be deleted.
        //             */
        //            {
        //
        //                final AbstractJournal liveJournal = resourceManager
        //                        .getLiveJournal();
        //
        //                final long lastCommitTime = liveJournal.getLastCommitTime();
        //
        //                final Set<UUID> actual = resourceManager
        //                        .getResourcesForTimestamp(lastCommitTime);
        //
        //                assertSameResources(new IRawStore[] {liveJournal}, actual);
        //
        //                // only retain the lastCommitTime.
        //                resourceManager.setReleaseTime(lastCommitTime - 1);
        //
        //            }
        //
        //            resourceManager
        //                    .purgeOldResources(1000/* ms */, false/*truncateJournal*/);
        // verify that the old journal is no longer open.
        assertFalse(j0.isOpen());
        // verify unwilling to open the store with that UUID.
        try {
            resourceManager.openStore(uuid0);
            fail("Expecting exception: " + NoSuchStoreException.class);
        } catch (NoSuchStoreException ex) {
            log.warn("Expected exception: " + ex);
        }
        final AbstractJournal j1 = resourceManager.getLiveJournal();
        // the UUID for that resource.
        final UUID uuid1 = j1.getResourceMetadata().getUUID();
        // verify willing to "open" the store with that UUID.
        assertTrue(j1 == resourceManager.openStore(uuid1));
        // one journal remaining.
        assertEquals(1, resourceManager.getManagedJournalCount());
        // no index segments.
        assertEquals(0, resourceManager.getManagedSegmentCount());
        /*
         * Verify that the resources required for [A] after overflow are
         * exactly [j1].
         */
        final Set<UUID> actualResourceUUIDs = resourceManager.getResourcesForTimestamp(j1.getRootBlockView().getFirstCommitTime());
        System.err.println("resources=" + actualResourceUUIDs);
        assertSameResources(new IJournal[]{j1}, actualResourceUUIDs);
    }
}
