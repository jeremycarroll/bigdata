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
 * Created on Feb 3, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Level;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.SimpleEntry;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for restart-safety of {@link BTree}s backed by an
 * {@link IJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafe extends ProxyTestCase {

    /**
     * 
     */
    public TestRestartSafe() {
    }

    /**
     * @param name
     */
    public TestRestartSafe(String name) {
        super(name);
    }

//    public Properties getProperties() {
//
//        if (properties == null) {
//
//            properties = super.getProperties();
//
//            // we need to use a persistent mode of the journal (not transient).
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
//                    .toString());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//
//            properties.setProperty(Options.DELETE_ON_EXIT,"true");
//
//        }
//
//        return properties;
//
//    }
//
//    private Properties properties;
    
//    /**
//     * Re-open the same backing store.
//     * 
//     * @param store
//     *            the existing store.
//     * 
//     * @return A new store.
//     * 
//     * @exception Throwable
//     *                if the existing store is not closed, e.g., from failure to
//     *                obtain a file lock, etc.
//     */
//    protected Journal reopenStore(Journal store) {
//        
//        // close the store.
//        store.close();
//        
//        Properties properties = (Properties)getProperties().clone();
//        
//        // Turn this off now since we want to re-open the same store.
//        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//        
//        // The backing file that we need to re-open.
//        File file = store.getFile();
//        
//        assertNotNull(file);
//        
//        // Set the file property explictly.
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return new Journal( properties );
//        
//    }

    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor, Journal journal) {

        BTree btree = new BTree(journal, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);

        return btree;
            
    }

    /**
     * Test basic btree is restart safe, including a test of
     * {@link BTree#removeAll()}
     * 
     * @throws IOException
     */
    public void test_restartSafe01() throws IOException {

        Journal journal = new Journal(getProperties());

        try {
        
        final int m = 3;

        final long addr1;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[]{v5,v6,v7,v8,v3,v4,v2,v1};

        {
            
            final BTree btree = getBTree(m,journal);
    
            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };
            
            btree.insert(new BatchInsert(values.length, keys, values));
            
            assertTrue(btree.dump(Level.DEBUG,System.err));
    
            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());
    
            addr1 = btree.write();
            
            journal.commit();
            
        }
        
        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            final long addr2;
            {
                journal = reopenStore(journal);

                final BTree btree = BTree.load(journal, addr1);

                assertTrue(btree.dump(Level.DEBUG, System.err));

                // @todo verify in more detail.
                assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7,
                        v8 }, btree.entryIterator());

                // remove all entries by replacing the root node.

                btree.removeAll();

                assertTrue(btree.dump(Level.DEBUG, System.err));

                assertSameIterator(new Object[] {}, btree.entryIterator());

                addr2 = btree.write();

                journal.commit();
            }

            /*
             * restart, re-opening the same file.
             */
            {

                journal = reopenStore(journal);

                final BTree btree = BTree.load(journal, addr2);

                assertTrue(btree.dump(Level.DEBUG, System.err));

                assertSameIterator(new Object[] {}, btree.entryIterator());

            }

        }
        
        }

        finally {

            journal.closeAndDelete();
            
        }

    }
    
    /**
     * Test verifies that the {@link IIndexWithCounter} is restart-safe.
     */
    public void test_restartSafeCounter() {
       
        Journal journal = new Journal(getProperties());

        try {
        
        final int m = 3;

        final long addr1;
        {
            
            final BTree btree = getBTree(m,journal);
            
            assertEquals(0,btree.getCounter().get());
            assertEquals(0,btree.getCounter().inc());
            assertEquals(1,btree.getCounter().get());
            
            addr1 = btree.write();
            
            journal.commit();
            
        }
        
        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            journal = reopenStore(journal);
            
            final BTree btree = BTree.load(journal, addr1);

            // verify the counter.
            assertEquals(1,btree.getCounter().get());
            
        }

        } finally {

            journal.closeAndDelete();
            
        }
        
    }

    /**
     * Test verifies that classes which extend {@link BTree} are correctly
     * restored by {@link BTree#load(com.bigdata.rawstore.IRawStore, long)}.
     */
    public void test_restartSafeSubclass() {

        Journal journal = new Journal(getProperties());

        try {
        
        final int m = 3;

        final long addr1;

        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[] { v5, v6, v7, v8, v3, v4, v2, v1 };

        {

            final BTree btree = new MyBTree(journal, m, UUID.randomUUID(),
                    SimpleEntry.Serializer.INSTANCE);

            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };

            btree.insert(new BatchInsert(values.length, keys, values));

            assertTrue(btree.dump(Level.DEBUG, System.err));

            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());

            addr1 = btree.write();

            journal.commit();

        }

        /*
         * restart, re-opening the same file.
         */
        if(journal.isStable()){

            journal = reopenStore(journal);

            final MyBTree btree = (MyBTree) BTree.load(journal, addr1);

            assertTrue(btree.dump(Level.DEBUG, System.err));

            // @todo verify in more detail.
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.entryIterator());

        }

        } finally {

            journal.closeAndDelete();
            
        }

    }

    public static class MyBTree extends BTree {

        public MyBTree(IRawStore store, int branchingFactor, UUID indexUUID,
                IValueSerializer valSer) {
            
            super(store, branchingFactor, indexUUID, valSer);
            
        }
        
        /**
         * @param store
         * @param metadata
         */
        public MyBTree(IRawStore store, BTreeMetadata metadata) {
            super(store, metadata);
        }
        
    }
    
}
