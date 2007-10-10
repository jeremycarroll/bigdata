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
 * Created on Oct 10, 2007
 */

package com.bigdata.journal;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ConcurrentJournal.Options;

/**
 * Test suite for {@link RegisterIndexTask} and {@link DropIndexTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAddDropIndexTask extends ProxyTestCase {

    /**
     * 
     */
    public TestAddDropIndexTask() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAddDropIndexTask(String arg0) {
        super(arg0);
    }

    /**
     * Test ability to create a {@link ConcurrentJournal}, submit an unisolated
     * write task that creates a named index shutdown the journal. The journal
     * is then re-opened and we verify that the registered index is restart
     * safe.  Finally we drop the index, close the journal and then re-open it
     * again to verify that the drop index operation was restart safe.
     */
    public void test_addDropIndex() {

        Properties properties = getProperties();
        
//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
//
//        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        Journal journal = new Journal(properties);
        
        final String name = "abc";

        final UUID indexUUID = UUID.randomUUID();
        
        /*
         * Run task to register a named index.
         */
        {
         
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
            
            Future<Object> future = journal.submit(new RegisterIndexTask(
                    journal, name, new UnisolatedBTree(journal, indexUUID)));

            try {

                log.info("Resolving future for task.");
                
                assertTrue( "Index already exists?", (Boolean)future.get() );

                log.info("Resolved future");
                
                /*
                 * This verifies that the write task did not return control to
                 * the caller until the write set of that task was committed.
                 * 
                 * @todo Since this is vital to the semantics of unisolated
                 * writes verify this more systematically.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore + 1, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * reopen (iff backed by stable store).
         */

        if(journal.isStable()) {

            journal.shutdown();

            properties.setProperty(Options.CREATE_TEMP_FILE,"false");
            properties.setProperty(Options.FILE,journal.getFile().toString());

            journal = new Journal(properties);
            
        }
        
        /*
         * Verify that the index was registered.
         */
        {

            final long commitCounterBefore = journal.getRootBlockView()
                    .getCommitCounter();

            Future<Object> future = journal.submit(new AbstractIndexTask(
                    journal, ITx.UNISOLATED, true/* readOnly */, name) {

                protected Object doTask() throws Exception {

                    /*
                     * Note: Throws an exception if the index is not registered.
                     */

                    IIndex ndx = getIndex(name);
                    
                    assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());
                    
                    return null;

                }
            });

            try {

                // return value should be [null] for this task.
                
                assertNull( future.get() );

                /*
                 * The commit counter MUST NOT have been changed since we ran a
                 * read-only task.
                 */
                
                assertEquals("commit counter changed?",
                        commitCounterBefore, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * Drop the index.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

            Future<Object> future = journal.submit(new DropIndexTask(journal,
                    name));
            
            try {

                // return is true iff the index was pre-existing (and therefore
                // was dropped).
                
                assertTrue( "Index did not exist?", (Boolean)future.get() );

                /*
                 * This verifies that the write task did not return control to
                 * the caller until the write set of that task was committed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore + 1, journal.getRootBlockView()
                                .getCommitCounter());
                
            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * Re-open the journal (iff backed by stable store). 
         */
        if(journal.isStable()) {
        
            journal.shutdown();
            
            journal = new Journal(properties);
            
        }
        
        /*
         * Verify that the index is gone.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

            Future<Object> future = journal.submit(new AbstractIndexTask(
                    journal, ITx.UNISOLATED, true/* readOnly */, name) {

                protected Object doTask() throws Exception {

                    /*
                     * Note: Throws an exception if the index is not registered.
                     */

                    try {

                        getIndex(name);
                        
                        fail("Expecting: "+NoSuchIndexException.class);
                        
                    } catch(NoSuchIndexException ex) {
                        
                        System.err.println("Ignoring expected exception: "+ex);
                        
                    }

                    return null;

                }
            });

            try {

                // return value should be [null] for this task.
                
                assertNull( future.get() );

                /*
                 * We ran a read-only task so the commit counter MUST NOT have
                 * been changed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        // shutdown again.
        journal.shutdown();
        
        /*
         * Delete the backing store to clean up after the test.
         */
        
        journal.delete();
        
    }

    /**
     * Test registers an index and then verifies that a second
     * {@link RegisterIndexTask} will return <code>false</code> since the
     * index already exists.
     */
    public void test_addDropIndex_twice() {

        Properties properties = new Properties();
        
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        Journal journal = new Journal(properties);
        
        final String name = "abc";
        
        /*
         * Run task to register a named index.
         */
        {
         
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
            
            Future<Object> future = journal.submit(new RegisterIndexTask(
                    journal, name, new UnisolatedBTree(journal, UUID
                            .randomUUID())));

            try {

                assertTrue( "Index already exists?", (Boolean)future.get() );
                
                /*
                 * This verifies that the write task did not return control to
                 * the caller until the write set of that task was committed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore + 1, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * Run another task to re-register the same named index.
         */
        {
         
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
            
            Future<Object> future = journal.submit(new RegisterIndexTask(
                    journal, name, new UnisolatedBTree(journal, UUID
                            .randomUUID())));

            try {

                assertFalse( "Index does not exist?", (Boolean)future.get() );
                
                /*
                 * This verifies that no commit was performed since no data was
                 * actually written on the store because the index was
                 * pre-existing.
                 */
         
                assertEquals("commit counter changed?", commitCounterBefore,
                        journal.getRootBlockView().getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * Now drop the index.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
            
            Future<Object> future = journal.submit(new DropIndexTask(journal,
                    name));

            try {

                // should return true if the index was dropped.
                assertTrue( "Index did not exist?", (Boolean)future.get() );
                
                /*
                 * Verify that a commit was performed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore + 1, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }
        
        /*
         * Now drop the index again.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
            
            Future<Object> future = journal.submit(new DropIndexTask(journal,
                    name));

            try {

                // should return false since the index does not exist.
                assertFalse("Index exists?", (Boolean)future.get() );
                
                /*
                 * Verify that a commit was NOT performed since no data was written by
                 * the task.
                 */
         
                assertEquals("commit counter changed?",
                        commitCounterBefore, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }
        
        journal.shutdown();
        
        journal.delete();
        
    }
    
}
