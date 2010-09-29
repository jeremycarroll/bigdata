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
 * Aggregates all tests into something approximately increasing dependency
 * order. Most of the tests that are aggregated are proxied test suites and will
 * therefore run with the configuration of the test class running that suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see AbstractJournalTestCase
 * @see ProxyTestCase
 */
@RunWith(Suite.class)
@SuiteClasses( {
       // tests of creation, lookup, use, commit of named indices.
       TestNamedIndices.class,
       // verify that an index is restart-safe iff the journal commits.
       TestRestartSafe.class,
       // tests of the commit list for named indices.
       TestCommitList.class,
       // tests the ability to recover and find historical commit records.
       TestCommitHistory.class,
       // tests the ability to abort operations.
       TestAbort.class,
       // test ability to rollback a commit.
       TestRollbackCommit.class,
       // test behavior when journal is opened by two threads.
       TestDoubleOpen.class,
       // test compacting merge of a Journal.
       TestCompactJournal.class,

       /*
        * tests of transaction support.
        */
       TestTransactionSupport.class,

       /*
        * Tests of concurrent execution of readers, writers, and transactions
        * and group commit.
        */

       // test basics of the concurrent task execution.
       TestConcurrentJournal.class,
//        test tasks to add and drop named indices.
//        This has been commented out since the unit test has dated semantics.
//        TestAddDropIndexTask.class);
       // test writing on one or more unisolated indices and verify read back after the commit.
       TestUnisolatedWriteTasks.class,
       // stress test of throughput when lock contention serializes unisolated writers.
       StressTestLockContention.class,
       // stress test of group commit.
       StressTestGroupCommit.class,
       // stress tests of writes on unisolated named indices.
       StressTestConcurrentUnisolatedIndices.class,
       /*
        * Stress test of concurrent transactions.
        *
        * Note: transactions use unisolated operations on the live indices when
        * they commit and read against unisolated (but not live) indices so
        * stress tests written in terms of transactions cover a lot of
        * territory.
        *
        * @todo add correctness tests here.
        *
        * @todo we add known transaction processing benchmarks here.
        */
       StressTestConcurrentTx.class

       /*
        * Test suite for low-level data replication.
        *
        * @todo test basic replication here
        */
        //TestReplicatedStore.class,
        } )
public class TestJournalBasics {

    public TestJournalBasics() {
        super();
    }
}
