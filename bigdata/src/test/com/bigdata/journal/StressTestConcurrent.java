/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 18, 2007
 */

package com.bigdata.journal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.IIndex;

/**
 * Stress tests for concurrent transaction processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestConcurrent extends ProxyTestCase {

    public StressTestConcurrent() {
    }

    public StressTestConcurrent(String name) {
        super(name);
    }
    
    /**
     * A stress test with a small pool of concurrent clients.
     */
    public void test_concurrentClients() throws InterruptedException {

        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String name = "abc";
        
        {
            journal.registerIndex(name, new UnisolatedBTree(journal));
            
            journal.commit();
        }

        try {
        
            /*
             * FIXME This is Ok with one concurrent client, but it will fail
             * with more than one concurrent client. The underlying problem is
             * that we are not serializing transactions. Once a transaction
             * begins to prepare, concurrent transactions that seek to prepare
             * must block until the transaction either aborts or commits. This
             * can be implemented by a queue of committers. Note that concurrent
             * transactions may always abort while they are running. Also note
             * that a transaction that is read only does not need to synchronize
             * since it will have an empty write set. We could also detect
             * transactions with empty write sets (no touched indices) and
             * shortcut the prepare/commit for those transactions as well.
             */
            doConcurrentClientTest(journal,name,1,100);
        
        } finally {
        
            journal.close();
            
        }
        
    }

    /**
     * A stress test with a pool of concurrent clients.
     * 
     * @param journal
     *            The database.
     * 
     * @param name
     *            The name of the index on which the transactions will
     *            operation.
     * 
     * @param nclients
     *            The #of concurrent clients.
     * 
     * @param ntx
     *            The #of transactions to execute.
     * 
     * @todo can this also be a correctness test if we choose the
     *       read/write/delete operations carefully and maintain a ground truth
     *       index?
     */
    public void doConcurrentClientTest(Journal journal, String name, int nclients, int ntx) 
        throws InterruptedException
    {
        
        ExecutorService executorService = Executors.newFixedThreadPool(
                nclients, Executors.defaultThreadFactory());
        
        final long timeout = 5;
        
        Collection<Callable<Long>> tasks = new HashSet<Callable<Long>>(); 
        
        for(int i=0; i<ntx; i++) {
            
            tasks.add(new Task(journal, journal.newTx(), name));
            
        }

        /*
         * @todo this will fail since we are not serializing transactions in
         * prepare->commit.
         */
        List<Future<Long>> results = executorService.invokeAll(tasks, timeout, TimeUnit.SECONDS);
        
        /*
         * @todo validate results - all execute, valid commit times, no errors.
         * 
         * @todo if write-write conflicts can result, then those should be
         * acceptable errors and the task could just be retried.
         */

        Iterator<Future<Long>> itr = results.iterator();
        
        while(itr.hasNext()) {

            Future<Long> future = itr.next();
            
            assertFalse(future.isCancelled());

            try {

                assertNotSame(0L,future.get());
                
            } catch(ExecutionException ex ) {
                
                // @todo validation errors should be allowed here.
                
                fail("Not expecting: "+ex, ex);
                
            }
            
        }
        
    }
    
    // @todo change to IJournal
    public static class Task implements Callable<Long> {

        private final Journal journal;
        private final long tx;
        private final IIndex ndx;
        final Random r = new Random();
        
        public Task(Journal journal,long tx, String name) {

            this.journal = journal;
            
            this.tx = tx;

            this.ndx = journal.getIndex(name,tx);
            
        }

        /**
         * Executes random operations in the transaction.
         * 
         * @return The commit time of the transaction.
         */
        public Long call() throws Exception {
            
            // Random operations on the named index(s).
            
            for (int i = 0; i < 100; i++) {

                byte[] key = new byte[4];

                r.nextBytes(key);

                if (r.nextInt(100) > 10) {

                    byte[] val = new byte[5];

                    r.nextBytes(val);

                    ndx.insert(key, val);

                } else {

                    ndx.remove(key);

                }

            }

            // commit.
            return journal.commit(tx);
            
        }
        
    }
    
}
