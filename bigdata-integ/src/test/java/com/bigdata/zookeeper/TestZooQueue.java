/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 4, 2009
 */

package com.bigdata.zookeeper;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ZooQueue} test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZooQueue extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZooQueue() {
        
    }

    /**
     * Simple test populates a queue and then waits until it has been drained.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_queue() throws KeeperException, InterruptedException {
        
        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final ZooQueue<String> queue = new ZooQueue<String>(zookeeper, zroot,
                acl, Integer.MAX_VALUE/*capacity*/);

        final ReentrantLock lock = new ReentrantLock();

        final Condition consumerDone = lock.newCondition();

        Assert.assertEquals(0, queue.size());

        queue.add("1");

        Assert.assertEquals(1, queue.size());

        queue.add("2");

        Assert.assertEquals(2, queue.size());

        new ClientThread(Thread.currentThread(), lock) {

            public void run2() throws Exception {

                Assert.assertEquals(2, queue.size());

                Assert.assertEquals("1", queue.remove());

                Assert.assertEquals(1, queue.size());

                Assert.assertEquals("2", queue.remove());

                Assert.assertEquals(0, queue.size());

                lock.lock();
                try {
                    consumerDone.signal();
                } finally {
                    lock.unlock();
                }

            }

        }.start();

        lock.lock();
        try {
            consumerDone.await(500, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }

        Assert.assertEquals(0, queue.size());

    }

    @Test
    public void test_queueBlocks() throws KeeperException, InterruptedException {
        
        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final int capacity = 2;
        
        final ZooQueue<String> queue = new ZooQueue<String>(zookeeper, zroot,
                acl, capacity);

        final ReentrantLock unusedLock = new ReentrantLock();

        new ClientThread(Thread.currentThread(), unusedLock) {

            public void run2() throws Exception {

                Assert.assertEquals(2, queue.capacity());

                Assert.assertEquals(0, queue.size());
                
                queue.add("A");
                
                Assert.assertEquals(1, queue.size());
                
                queue.add("B");
                
                Assert.assertEquals(2, queue.size());

                log.info("Should block.");

                queue.add("C"); // should block.
                
                log.info("Producer done.");
                
            }

        }.start();
        
        int size;
        while ((size = queue.size()) < capacity) {

            log.info("size=" + size);
            
            Thread.sleep(10/* ms */);
            
        }

        log.info("Queue is at capacity: size=" + queue.size());

        /* Make sure that the producer is blocked.  If it is not blocked
         * then the producer will add another element and the queue will
         * be over capacity.
         */
        Thread.sleep(500/* ms */);

        // queue is still at capacity.
        Assert.assertEquals(capacity, queue.size());
        
        // take an item from the queue.
        Assert.assertEquals("A",queue.remove());
        
        // producer should now complete.
        Thread.sleep(50/* ms */);

        // queue is back at capacity.
        Assert.assertEquals(2, queue.size());
        
        /*
         * Now verify that we can detect when the queue becomes empty.
         */
        
        new ClientThread(Thread.currentThread(), unusedLock) {

            public void run2() throws Exception {

                Assert.assertEquals(2, queue.capacity());

                Assert.assertEquals(2, queue.size());
                
                Assert.assertEquals("B", queue.remove());
                
                Assert.assertEquals(1, queue.size());
                
                // Wait to give awaitEmpty() a chance in the main thread.
                Thread.sleep(500/*ms*/);
                
                Assert.assertEquals("C", queue.remove());
                
                Assert.assertEquals(0, queue.size());

                log.info("Consumer done.");
                
            }

        }.start();
       
        log.info("Will wait for queue to become empty");
        
        // not empty yet.
        Assert.assertNotSame(0, queue.size());

        queue.awaitEmpty();

        Assert.assertEquals(0, queue.size());

    }

}
