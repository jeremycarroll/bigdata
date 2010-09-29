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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Runs all tests for all journal implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
       // test ability of the platform to synchronize writes to disk.
       TestRandomAccessFileSynchronousWrites.class,
       // test the ability to (de-)serialize the root addreses.
       TestCommitRecordSerializer.class,
       // test the root block api.
       TestRootBlockView.class,
       // @todo tests of the index used map index names to indices.
       TestName2Addr.class,
       // tests of the index used to access historical commit records
       TestCommitRecordIndex.class,
       // Test a scalable temporary store (uses the transient and disk-only
       // buffer modes).

       TestJournalBasics.class,


       // ==== Proxy Tests previously from TestTemporaryStore.suite()
           TestTemporaryStore.class,
           TestTemporaryStoreRawStore.class,
           TestTemporaryStoreInterrupts.class,
           TestTemporaryStoreMROW.class,
           TestTemporaryStoreMRMW.class,

       // ==== Proxy Tests previously from TestTransientJournal.suite()
           TestTransientJournal.class,
           TestTransientJournalRawStore.class,
           TestTransientJournalMROW.class,
           TestTransientJournalMRMW.class,

       // ==== Proxy Tests previously from TestDirectJournal.suite()

           //TestDirectJournal.class,
           //TestDirectJournalRawStore.class,
           //TestDirectJournalInterrupts.class,
           //TestDirectJournalMROW.class,
           //TestDirectJournalMRMW.class,

       /*
        * Note: The mapped journal is somewhat problematic and its tests are
        * disabled for the moment since (a) we have to pre-allocate large
        * extends; (b) it does not perform any better than other options; and
        * (c) we can not synchronously unmap or delete a mapped file which
        * makes cleanup of the test suites difficult and winds up spewing 200M
        * files all over your temp directory.
        */

        //TestMappedJournal.class,

        /*
         * Commented out since this mode is not used and there is an occasional
         * test failure in:
         * 
         * com.bigdata.journal.TestConcurrentJournal.test_concurrentReadersAreOk
         * 
         * This error is stochastic and appears to be restricted to
         * BufferMode#Direct. This is a journal mode based by a fixed capacity
         * native ByteBuffer serving as a write through cache to the disk. Since
         * the buffer can not be extended, that journal mode is not being
         * excercised by anything. If you like, I can deprecate the Direct
         * BufferMode and turn disable its test suite. (There is also a "Mapped"
         * BufferMode whose tests we are not running due to problems with Java
         * releasing native heap ByteBuffers and closing memory mapped files.
         * Its use is strongly discouraged in the javadoc, but it has not been
         * excised from the code since it might be appropriate for some
         * applications.)
         */
         //TestDiskJournal.class,

       // ==== Proxy Tests previously from TestWORMStrategy.suite()
           TestWORMStrategy.class,
           TestWORMStrategyRawStore.class,
           TestWORMStrategyInterrupts.class,
           TestWORMStrategyMROW.class,
           TestWORMStrategyMRMW.class,

           com.bigdata.rwstore.TestAll.class
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
    }
}
