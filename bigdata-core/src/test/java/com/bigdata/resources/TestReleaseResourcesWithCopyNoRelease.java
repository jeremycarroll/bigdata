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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * Test where the index view is copied in its entirety onto the new journal
 * but the {@link ResourceManager} is not permitted to release old resources
 * (it is configured as an immortal store).
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReleaseResourcesWithCopyNoRelease extends TestReleaseResources {

    public TestReleaseResourcesWithCopyNoRelease() {
    }

    @Override
    public Properties getProperties() {
        Properties properties = new Properties(super.getProperties());
        // Never release old resources.
        properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, AbstractTransactionService.Options.MIN_RELEASE_AGE_NEVER);
        return properties;
    }

    /**
     * Test creates an index whose view is initially defined by the initial
     * journal on the {@link ResourceManager}. An overflow of the journal is
     * then forced, which re-defines the view on the new journal. Since the
     * index is very small (it is empty), it is "copied" onto the new
     * journal rather than causing an index segment build to be scheduled.
     * The original journal should not be released.
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
        // the new journal.
        final AbstractJournal j1 = resourceManager.getLiveJournal();
        assertTrue(j0 != j1);
        final long createTime0 = j0.getResourceMetadata().getCreateTime();
        final long createTime1 = j1.getResourceMetadata().getCreateTime();
        assertTrue(createTime0 < createTime1);
        // verify can still open the original journal.
        assertTrue(j0 == resourceManager.openStore(j0.getResourceMetadata().getUUID()));
        // verify can still open the new journal.
        assertTrue(j1 == resourceManager.openStore(j1.getResourceMetadata().getUUID()));
        // 1 journals.
        assertEquals(2, resourceManager.getManagedJournalCount());
        // no index segments.
        assertEquals(0, resourceManager.getManagedSegmentCount());
        /*
         * Verify that the commit time index.
         */
        // for j0
        assertEquals(j1.getRootBlockView().getFirstCommitTime(), resourceManager.getCommitTimeStrictlyGreaterThan(createTime1));
        // for j1.
        assertEquals(j1.getRootBlockView().getFirstCommitTime(), resourceManager.getCommitTimeStrictlyGreaterThan(createTime1));
        /*
         * Verify that the resources required for [A] are {j0, j1} when the
         * probe commitTime is the timestamp when we registered [A] on [j0].
         * (j1 is also represented since we include all dependencies for all
         * commit points subsequent to the probe in order to ensure that we
         * do not accidently release dependencies required for more current
         * views of the index).
         */
        /*
         * Verify that the resources required for [A] are {j0, j1} when the
         * probe commitTime is the timestamp when we registered [A] on [j0].
         * (j1 is also represented since we include all dependencies for all
         * commit points subsequent to the probe in order to ensure that we
         * do not accidently release dependencies required for more current
         * views of the index).
         */
        {
            final long commitTime = j0.getRootBlockView().getFirstCommitTime();
            final Set<UUID> actual = resourceManager.getResourcesForTimestamp(commitTime);
            assertSameResources(new IJournal[]{j0, j1}, actual);
        }
        /*
         * Verify that the resources required for [A] after overflow are
         * exactly [j1].
         */
        /*
         * Verify that the resources required for [A] after overflow are
         * exactly [j1].
         */
        {
            final long commitTime = j1.getRootBlockView().getFirstCommitTime();
            final Set<UUID> actual = resourceManager.getResourcesForTimestamp(commitTime);
            assertSameResources(new IJournal[]{j1}, actual);
        }
    }
}
