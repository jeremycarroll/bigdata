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
 * Created on Oct 1, 2007
 */

package com.bigdata.concurrent;

import java.util.Properties;

import com.bigdata.test.ExperimentDriver.Result;

/**
 * Stress tests where a {@link TxDag} is used to detect deadlock.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestNonBlockingLockManagerWithTxDag extends
        AbstractStressTestNonBlockingLockManager {

    /**
     * 
     */
    public StressTestNonBlockingLockManagerWithTxDag() {
        super();
    }

    public StressTestNonBlockingLockManagerWithTxDag(String name) {
        super(name);
    }

    /**
     * Test where each operation locks only a single resource (low concurrency
     * condition w/ 5 threads). There is no timeout. All tasks should run to
     * completion. Since modestly large number of resources (100) is used and
     * since each task declares a single resource lock, the maximum observed
     * concurrency should be the #of tasks in the thread pool.
     */
    public void test_singleResourceLocking_waitsFor_lowConcurrency5() throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 5;
        final int ntasks = 1000;
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // all tasks completed successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));

        // tasks were run in parallel.
        assertEquals("maxrunning", nthreads, Integer.parseInt(result
                .get("maxrunning")));
        
    }
    
    /**
     * Test where each operation locks only a single resource using a thread
     * pool with 20 core threads. Since there is no timeout, all tasks should
     * run to completion. The maximum observed concurrency SHOULD be equal to
     * the size of the thread pool.
     */
    public void test_singleResourceLocking_waitsFor_defaultConcurrency20()
            throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 20;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // all tasks complete successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));

        // tasks were run in parallel.
        assertEquals("maxrunning", nthreads, Integer.parseInt(result
                .get("maxrunning")));

    }
    
    /**
     * Test where each operation locks only a single resource (high concurrency
     * condition with 100 threads).
     */
    public void test_singleResourceLocking_waitsFor_highConcurrency100() throws Exception {

        final Properties properties = new Properties();
        
        final int nthreads = 100;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // all tasks complete successfully.
        assertEquals(ntasks, Integer.parseInt(result.get("nsuccess")));

        /*
         * Tasks were run in parallel with at least 50% of the threads executing
         * concurrently.
         */
        final int maxrunning = Integer.parseInt(result.get("maxrunning"));
        
        assertTrue("nthreads=" + nthreads + ", but maxrunning is only"
                + maxrunning, maxrunning >= nthreads / 2);

    }
    
    /**
     * Test where each operation locks only a single resource and there is only one
     * resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_waitsFor_lowConcurrency2() throws Exception {

        final Properties properties = new Properties();
        
        final int nthreads = 2;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // all tasks completed successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));

        // tasks were serialized.
        assertEquals("maxrunning", 1, Integer
                .parseInt(result.get("maxrunning")));
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_waitsFor_lowConcurrency5()
            throws Exception {

        final Properties properties = new Properties();
        
        // Note: Small ntasks since this case otherwise is slow.
        final int nthreads = 3;
        final int ntasks = 100;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0"); // Note: timeout==0 when debugging.
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");

        final Result result = doComparisonTest(properties);

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // all tasks completed successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));

        // tasks were serialized.
        assertEquals("maxrunning", 1, Integer
                .parseInt(result.get("maxrunning")));
               
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized and
     * where 10% of all tasks die a horrid death.
     */
    public void test_singleResourceLocking_serialized_waitsFor_lowConcurrency5_withTaskDeath()
            throws Exception {

        final Properties properties = new Properties();

        // Note: Small ntasks since this case otherwise is slow.
        final int nthreads = 3;
        final int ntasks = 100;
        final double percentTaskDeath = .1d;
        final double expectedErrorRate = percentTaskDeath;
        
        properties.setProperty(TestOptions.NTHREADS, ""+nthreads);
        properties.setProperty(TestOptions.NTASKS, ""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES, "1");
        properties.setProperty(TestOptions.MIN_LOCKS, "1");
        properties.setProperty(TestOptions.MAX_LOCKS, "1");
        // properties.setProperty(TestOptions.LOCK_TIMEOUT,"0"); // Note:
        // timeout==0 when debugging.
        properties.setProperty(TestOptions.PREDECLARE_LOCKS, "false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS, "false");
        properties.setProperty(TestOptions.PERCENT_TASK_DEATH, ""+percentTaskDeath);

        final Result result = doComparisonTest(properties);
        
        // tasks were serialized.
        assertEquals("maxrunning", 1, Integer
                .parseInt(result.get("maxrunning")));

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        final int nsuccess = Integer.parseInt(result.get("nsuccess"));
        
        final int nhorriddeath = Integer.parseInt(result.get("nhorriddeath"));

        // all tasks were either successfull or a died a horrid death.
        assertEquals(ntasks, nsuccess + nhorriddeath);

        /*
         * Verify that the observed error rate corresponds closely to the
         * specified error rate. It need not be exact since the actual #of tasks
         * scheduled to die is random.
         */
        final double actualErrorRate = nhorriddeath / (double) ntasks;
        
        if ((actualErrorRate < expectedErrorRate - .05)
                || (actualErrorRate > expectedErrorRate + .05)) {

            fail("error rate: expected=" + expectedErrorRate + ", actual="
                    + actualErrorRate);
            
        }
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_waitsFor_highConcurrency()
            throws Exception {

        final Properties properties = new Properties();
        
        // Note: Small ntasks since otherwise this case takes very long.
        final int nthreads = 100;
        final int ntasks = 100;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);
        
        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // Should have been serialized.
        assertEquals("maxrunning",1, Integer.parseInt(result.get("maxrunning")));

        // All tasks complete successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized. The
     * task timeout is non-zero, so long-running tasks will be cancelled. This
     * test stresses the logic in lock() that is responsible for backing out a
     * lock requests when a task is cancelled either while awaiting its locks or
     * while running.
     */
    public void test_singleResourceLocking_serialized_waitsFor_highConcurrency_lockTimeout()
            throws Exception {

        final Properties properties = new Properties();
        
        // Note: Small ntasks since otherwise this case takes very long.
        final int nthreads = 100;
        final int ntasks = 100;

        properties.setProperty(TestOptions.NTHREADS, "" + nthreads);
        properties.setProperty(TestOptions.NTASKS, "" + ntasks);
        properties.setProperty(TestOptions.NRESOURCES, "1");
        properties.setProperty(TestOptions.MIN_LOCKS, "1");
        properties.setProperty(TestOptions.MAX_LOCKS, "1");
        properties.setProperty(TestOptions.TASK_TIMEOUT, "1000");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS, "false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS, "false");

        final Result result = doComparisonTest(properties);

        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // Should have been serialized.
        assertEquals("maxrunning", 1, Integer
                .parseInt(result.get("maxrunning")));

        // Note: Timeouts should be expected. They will show up as cancelled
        // tasks.
        final int ntimeout = Integer.parseInt(result.get("ntimeout"));
        assertTrue("No timeouts?", ntimeout > 0);
        
    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks.
     * 
     * FIXME postcondition tests
     * 
     * FIXME In fact, predeclaring locks appears sufficient to avoid deadlocks
     * as long as we make issue the lock requests atomically for each task. This
     * means that TxDag would only be useful for 2PL, and this class
     * (NonBlockingLockManager) currently does not support 2PL (because it
     * couples the concepts of the task and the transaction together).
     */
    public void test_multipleResourceLocking_resources3_waitsFor_deadlocks_locktries3()
            throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 20;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS, ""+nthreads);
        properties.setProperty(TestOptions.NTASKS, ""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES, "100");
        properties.setProperty(TestOptions.MIN_LOCKS, "3");
        properties.setProperty(TestOptions.MAX_LOCKS, "3");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES, "3");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS, "false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS, "false");

        doComparisonTest(properties);

    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks. In fact, since we
     * have 10 resource locks for each operation and only 100 resources the
     * chances of a deadlock on any given operation are extremely high.
     * 
     * FIXME postcondition tests
     * 
     * FIXME In fact, predeclaring locks appears sufficient to avoid deadlocks
     * as long as we make issue the lock requests atomically for each task. This
     * means that TxDag would only be useful for 2PL, and this class
     * (NonBlockingLockManager) currently does not support 2PL (because it
     * couples the concepts of the task and the transaction together).
     */
    public void test_multipleResourceLocking_resources10_waitsFor_deadlocks_locktries10()
            throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 20;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS, ""+nthreads);
        properties.setProperty(TestOptions.NTASKS, ""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES, "100");
        properties.setProperty(TestOptions.MIN_LOCKS, "10");
        properties.setProperty(TestOptions.MAX_LOCKS, "10");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES, "10");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS, "false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS, "false");

        doComparisonTest(properties);
        
    }

}
