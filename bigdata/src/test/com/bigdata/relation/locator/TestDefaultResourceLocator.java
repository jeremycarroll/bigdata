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
 * Created on Jul 9, 2008
 */

package com.bigdata.relation.locator;

import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase2;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.DaemonThreadFactory;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;

/**
 * Test suite for location relations, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo make this a proxy test case and run for {@link Journal} and
 *       {@link IBigdataFederation}
 */
public class TestDefaultResourceLocator extends TestCase2 {

    public TestDefaultResourceLocator() {
        super();
    }
    
    public TestDefaultResourceLocator(String name) {
        super(name);
    }

    public Properties getProperties() {
        
        Properties properties = super.getProperties();
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                .toString());
        
        return properties;
        
    }
    
    /**
     * @todo locate a relation.
     * 
     * @todo locate a relation, write on it, verify read-committed view not yet
     *       visible, then commit and verify the read committed view is visible.
     * 
     * @todo same as above, but the relation is on a temporary store that is
     *       added to the set of index managers against which relations are
     *       resolved.
     * 
     */
    public void test_locateRelation() {
        
        final Properties properties = getProperties();
        
        final Journal store = new Journal( properties );

        final ExecutorService executorService = Executors.newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());
        
        final String namespace = "test";
        
        try {

            // instantiate relation.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            MockRelation mockRelation = new MockRelation(store, namespace,
//BTM - PRE_CLIENT_SERVICE                    ITx.UNISOLATED, properties);
            MockRelation mockRelation =
                new MockRelation(store,//Journal -> AbstractJournal -> IBTreeManager -> IIndexManager
                                 store,//Journal -> IConcurrencyManager
                                 store.getDiscoveryManager(),//returns null
                                 namespace,
                                 ITx.UNISOLATED,
                                 properties);
//BTM - PRE_CLIENT_SERVICE - END
            
            /*
             * the index for the relation does not exist yet. we verify
             * that, and then create the index.
             */

            assertNull(mockRelation.getIndex());

            // create indices.
            mockRelation.create();

            assertNotNull(mockRelation.getIndex());

            {
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE                /*
//BTM - PRE_CLIENT_SERVICE                 * Should be locatable now since the writes on the global row
//BTM - PRE_CLIENT_SERVICE                 * store are unisolated (ah!, but only if you are using the
//BTM - PRE_CLIENT_SERVICE                 * concurrency control API)
//BTM - PRE_CLIENT_SERVICE                 */
//BTM - PRE_CLIENT_SERVICE                assertNotNull(store.getResourceLocator().locate(namespace,
//BTM - PRE_CLIENT_SERVICE                        ITx.UNISOLATED));
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                // a request for the unisolated view gives us the same instance.
//BTM - PRE_CLIENT_SERVICE                assertTrue(((MockRelation) store.getResourceLocator().locate(
//BTM - PRE_CLIENT_SERVICE                        namespace, ITx.UNISOLATED)) == mockRelation);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                /*
//BTM - PRE_CLIENT_SERVICE                 * the read-committed relation is also locatable since its in
//BTM - PRE_CLIENT_SERVICE                 * the global row store but it will not see the indices until
//BTM - PRE_CLIENT_SERVICE                 * (a) they have been created; and (b) there has been a commit
//BTM - PRE_CLIENT_SERVICE                 * of the journal.
//BTM - PRE_CLIENT_SERVICE                 */
//BTM - PRE_CLIENT_SERVICE                assertNotNull(store.getResourceLocator().locate(namespace,
//BTM - PRE_CLIENT_SERVICE                        ITx.READ_COMMITTED));
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                // a request for the read committed view is not the same
//BTM - PRE_CLIENT_SERVICE                // instance as the unisolated view.
//BTM - PRE_CLIENT_SERVICE                assertTrue(((MockRelation) store.getResourceLocator().locate(
//BTM - PRE_CLIENT_SERVICE                        namespace, ITx.READ_COMMITTED)) != mockRelation);
//BTM - PRE_CLIENT_SERVICE
                /*
                 * Should be locatable now since the writes on the global row
                 * store are unisolated (ah!, but only if you are using the
                 * concurrency control API)
                 */
                assertNotNull(store.getResourceLocator()
                                   .locate(store,//IIndexManager
                                           store,//IConcurrencyManager
                                           store.getDiscoveryManager(),
                                           namespace,
                                          ITx.UNISOLATED));

                // a request for the unisolated view gives us the same instance.
                assertTrue(((MockRelation) store.getResourceLocator() 
                               .locate(store,//IIndexManager
                                       store,//IConcurrencyManager
                                       store.getDiscoveryManager(),
                                       namespace,
                                       ITx.UNISOLATED)) == mockRelation);

                /*
                 * the read-committed relation is also locatable since its in
                 * the global row store but it will not see the indices until
                 * (a) they have been created; and (b) there has been a commit
                 * of the journal.
                 */
                assertNotNull(store.getResourceLocator()
                                   .locate(store,//IIndexManager
                                           store,//IConcurrencyManager
                                           store.getDiscoveryManager(),
                                           namespace,
                                           ITx.READ_COMMITTED));

                // a request for the read committed view is not the same
                // instance as the unisolated view.
                assertTrue(((MockRelation) store.getResourceLocator()
                               .locate(store,//IIndexManager
                                       store,//IConcurrencyManager
                                       store.getDiscoveryManager(),
                                       namespace,
                                       ITx.READ_COMMITTED)) != mockRelation);
//BTM - PRE_CLIENT_SERVICE - END
            }
            
            {
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                // a request for the unisolated view shows that the index
//BTM - PRE_CLIENT_SERVICE                // exists.
//BTM - PRE_CLIENT_SERVICE                assertNotNull(((MockRelation) store.getResourceLocator()
//BTM - PRE_CLIENT_SERVICE                        .locate(namespace, ITx.UNISOLATED)).getIndex());
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                // a request for the unisolated view gives us the same instance.
//BTM - PRE_CLIENT_SERVICE                assertTrue(((MockRelation) store.getResourceLocator().locate(
//BTM - PRE_CLIENT_SERVICE                        namespace, ITx.UNISOLATED)) == mockRelation);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                /*
//BTM - PRE_CLIENT_SERVICE                 * @todo The read-committed view still does not see the relation
//BTM - PRE_CLIENT_SERVICE                 * since there has not been a commit yet after the index was
//BTM - PRE_CLIENT_SERVICE                 * created.
//BTM - PRE_CLIENT_SERVICE                 */
//BTM - PRE_CLIENT_SERVICE                if(false) {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                assertNull(((MockRelation) store.getResourceLocator().locate(
//BTM - PRE_CLIENT_SERVICE                        namespace, ITx.READ_COMMITTED)));
//BTM - PRE_CLIENT_SERVICE            
//BTM - PRE_CLIENT_SERVICE                final MockRelation readCommittedView1 = (MockRelation) store
//BTM - PRE_CLIENT_SERVICE                        .getResourceLocator().locate(namespace,
//BTM - PRE_CLIENT_SERVICE                                ITx.READ_COMMITTED);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                // same view before a commit.
//BTM - PRE_CLIENT_SERVICE                assertTrue(readCommittedView1 == (MockRelation) store
//BTM - PRE_CLIENT_SERVICE                        .getResourceLocator().locate(namespace,
//BTM - PRE_CLIENT_SERVICE                                ITx.READ_COMMITTED));
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                
//BTM - PRE_CLIENT_SERVICE                // commit
//BTM - PRE_CLIENT_SERVICE                store.commit();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE                /*
//BTM - PRE_CLIENT_SERVICE                 * should be a new read-committed view
//BTM - PRE_CLIENT_SERVICE                 * 
//BTM - PRE_CLIENT_SERVICE                 * FIXME cache must be defeated for read-committed!!! at least
//BTM - PRE_CLIENT_SERVICE                 * if there HAS been a commit
//BTM - PRE_CLIENT_SERVICE                 */
//BTM - PRE_CLIENT_SERVICE                final MockRelation readCommittedView2 = (MockRelation) store
//BTM - PRE_CLIENT_SERVICE                        .getResourceLocator().locate(namespace,
//BTM - PRE_CLIENT_SERVICE                                ITx.READ_COMMITTED);
//BTM - PRE_CLIENT_SERVICE        
//BTM - PRE_CLIENT_SERVICE                // different view after a commit.
//BTM - PRE_CLIENT_SERVICE                assertTrue(readCommittedView1 != readCommittedView2);
//BTM - PRE_CLIENT_SERVICE                
//BTM - PRE_CLIENT_SERVICE                /*
//BTM - PRE_CLIENT_SERVICE                 * The index is now visible to the read committed view.
//BTM - PRE_CLIENT_SERVICE                 */
//BTM - PRE_CLIENT_SERVICE                assertNull(readCommittedView2.getIndex());
//BTM - PRE_CLIENT_SERVICE                
//BTM - PRE_CLIENT_SERVICE                // still not visible to the old view.
//BTM - PRE_CLIENT_SERVICE                assertNull(readCommittedView1.getIndex());
//BTM - PRE_CLIENT_SERVICE                
//BTM - PRE_CLIENT_SERVICE                // another request gives us the same view
//BTM - PRE_CLIENT_SERVICE                assertTrue(readCommittedView2 == (MockRelation) store
//BTM - PRE_CLIENT_SERVICE                        .getResourceLocator().locate(namespace,
//BTM - PRE_CLIENT_SERVICE                                ITx.READ_COMMITTED));
//BTM - PRE_CLIENT_SERVICE                
//BTM - PRE_CLIENT_SERVICE                }
                // a request for the unisolated view shows that the index
                // exists.
                assertNotNull(((MockRelation) store.getResourceLocator()
                                 .locate(store,//IIndexManager
                                         store,//IConcurrencyManager
                                         store.getDiscoveryManager(),
                                         namespace,
                                         ITx.UNISOLATED)).getIndex());

                // a request for the unisolated view gives us the same instance.
                assertTrue(((MockRelation) store.getResourceLocator()
                              .locate(store,//IIndexManager
                                      store,//IConcurrencyManager
                                      store.getDiscoveryManager(),
                                      namespace,
                                      ITx.UNISOLATED)) == mockRelation);

                /*
                 * @todo The read-committed view still does not see the relation
                 * since there has not been a commit yet after the index was
                 * created.
                 */
                if(false) {

                    assertNull(((MockRelation) store.getResourceLocator()
                                  .locate(store,//IIndexManager
                                          store,//IConcurrencyManager
                                          store.getDiscoveryManager(),
                                          namespace,
                                          ITx.READ_COMMITTED)));
            
                    final MockRelation readCommittedView1 =
                          (MockRelation) store.getResourceLocator()
                              .locate(store,//IIndexManager
                                      store,//IConcurrencyManager
                                      store.getDiscoveryManager(),
                                      namespace,
                                      ITx.READ_COMMITTED);

                    // same view before a commit.
                    assertTrue( readCommittedView1 ==
                                 (MockRelation) store.getResourceLocator()
                                      .locate(store,//IIndexManager
                                              store,//IConcurrencyManager
                                              store.getDiscoveryManager(),
                                              namespace,
                                              ITx.READ_COMMITTED) );

                
                    // commit
                    store.commit();

                    /*
                     * should be a new read-committed view
                     * 
                     * FIXME cache must be defeated for read-committed!!! at least
                     * if there HAS been a commit
                     */
                    final MockRelation readCommittedView2 =
                              (MockRelation) store.getResourceLocator()
                                   .locate(store,//IIndexManager
                                           store,//IConcurrencyManager
                                           store.getDiscoveryManager(),
                                           namespace,
                                           ITx.READ_COMMITTED);
        
                    // different view after a commit.
                    assertTrue(readCommittedView1 != readCommittedView2);
                
                    /*
                     * The index is now visible to the read committed view.
                     */
                    assertNull(readCommittedView2.getIndex());
                
                    // still not visible to the old view.
                    assertNull(readCommittedView1.getIndex());
                
                    // another request gives us the same view
                    assertTrue( readCommittedView2 ==
                                 (MockRelation) store.getResourceLocator()
                                      .locate(store,//IIndexManager
                                              store,//IConcurrencyManager
                                              store.getDiscoveryManager(),
                                              namespace,
                                              ITx.READ_COMMITTED) );
                
                }//endif(false)
//BTM - PRE_CLIENT_SERVICE - END

            }
            
        } finally {
            
            store.destroy();
            
            executorService.shutdownNow();
            
        }
        
    }

    private static class MockRelation extends AbstractRelation {

        final private String indexName = "foo";
        private IIndex ndx;
        
        /**
         * @param indexManager
         * @param namespace
         * @param timestamp
         * @param properties
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        public MockRelation(IIndexManager indexManager, String namespace, Long timestamp,
//BTM - PRE_CLIENT_SERVICE                Properties properties) {
//BTM - PRE_CLIENT_SERVICE            
//BTM - PRE_CLIENT_SERVICE            super(indexManager, namespace, timestamp, properties);
//BTM - PRE_CLIENT_SERVICE            
//BTM - PRE_CLIENT_SERVICE        }
        public MockRelation(IIndexManager indexManager,
                            IConcurrencyManager concurrencyManager,
                            IBigdataDiscoveryManagement discoveryManager,
                            String namespace,
                            Long timestamp,
                            Properties properties) {
            
            super(indexManager,
                  concurrencyManager,
                  discoveryManager,
                  namespace,
                  timestamp,
                  properties);
        }
//BTM - PRE_CLIENT_SERVICE - END

        public void create() {
            
            super.create();
            
            if (getIndex() == null) {

                getIndexManager().registerIndex(
                        new IndexMetadata(getNamespace() + indexName, UUID
                                .randomUUID()));

                log.info("Created index.");

                getIndex();
                
            }
            
        }

        public void destroy() {
            
            super.destroy();
            
            if (getIndex() != null) {

                getIndexManager().dropIndex(getNamespace() + indexName);

                log.info("Dropped index.");
                
            }

        }

        private IIndex getIndex() {

            if (ndx == null) {

                ndx = getIndex(getNamespace() + indexName);

            }

            if (ndx == null) {

                log.info("Index not found.");

            }

            return ndx;

        }
        
        @Override
        public String getFQN(IKeyOrder keyOrder) {
            // TODO Auto-generated method stub
            return null;
        }

        public long delete(IChunkedOrderedIterator itr) {
            // TODO Auto-generated method stub
            return 0;
        }

        public long insert(IChunkedOrderedIterator itr) {
            // TODO Auto-generated method stub
            return 0;
        }

        public IAccessPath getAccessPath(IPredicate predicate) {
            // TODO Auto-generated method stub
            return null;
        }

        public long getElementCount(boolean exact) {
            // TODO Auto-generated method stub
            return 0;
        }

        public Set getIndexNames() {
            // TODO Auto-generated method stub
            return null;
        }

        public Object newElement(IPredicate predicate, IBindingSet bindingSet) {
            // TODO Auto-generated method stub
            return null;
        }
        
        public Class<ISPO> getElementClass() {

            return null;

        }
    
    }
    
}
