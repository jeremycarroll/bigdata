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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.btree.BTree;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.locator.DefaultResourceLocator;

/**
 * A triple store based on the <em>bigdata</em> architecture. This class
 * offers extremely low latency for index operations. All indices are local
 * (in-process) objects and there are no concurrency controls, so point tests on
 * the indices are extremely efficient. Significant parallelism is achieved by
 * paying careful attention to the concurrency constraints imposed by the
 * {@link BTree} class (writers are single threaded, reads may be concurrent,
 * but not concurrent with a writer) and by using different views (unisolated vs
 * read-historical) of the indices when computing entailments or performing
 * high-level query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalTripleStore extends AbstractLocalTripleStore {

    protected final Journal store;

    /**
     * The backing embedded database.
     */
    public Journal getIndexManager() {
        
        return store;
        
    }
    
    /**
     * Delegates the operation to the backing store.
     */
    synchronized public long commit() {
     
        final long begin = System.currentTimeMillis();

        super.commit();
        
        final long commitTime= getIndexManager().commit();
        
        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled())
            log.info("commit: commit latency=" + elapsed + "ms");
        
        return commitTime;
    }

    public void abort() {
                
        super.abort();
        
        // discard the write sets.
        getIndexManager().abort();

    }
    
    public boolean isStable() {
        
        return store.isStable();
        
    }
    
    public boolean isReadOnly() {
        
        return super.isReadOnly() || store.isReadOnly();
        
    }

    public void close() {
        
        super.close();
        
        if(!isReadOnly()) {

            store.shutdown();
            
        }
        
    }
    
//    public void __tearDownUnitTest() {
//        
//        super.__tearDownUnitTest();
//
//        if(!isReadOnly()) {
//
//            store.destroy();
//            
//        }
//        
//    }

    /**
     * Options understood by the {@link LocalTripleStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractTripleStore.Options {
       
    }
    
    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     */
    public LocalTripleStore(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE - NOTE: the super class (AbstractLocalTripleStore --> AbstractTripleStore) now takes an
//BTM - PRE_CLIENT_SERVICE -       IConcurrencyManager and an IBigdataDiscoveryManagment instance for scale out.
//BTM - PRE_CLIENT_SERVICE -       It's not clear whether passing null for those parameters will be a problem 
//BTM - PRE_CLIENT_SERVICE -       not. Need to monitor the tests for NullPointerExceptions.
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        super(indexManager, namespace, timestamp, properties);
        super(indexManager, 
              null, //IConcurrencyManager
              null, //IBigdataDiscoveryManagement
              namespace, timestamp, properties);
//BTM - PRE_CLIENT_SERVICE - END

        store = (Journal) indexManager;
        
    }

    /**
     * Create or re-open a triple store using a local embedded database.
     */
	/* public */LocalTripleStore(final Properties properties) {

        /*
         * FIXME This should pass up the existing properties for the KB instance
         * when the KB instance is pre-existing.  Really though, you should first
         * obtain the Journal and then attempt to locate the KB and create it if
         * it does not exist.
         */
        this(new Journal(properties), "kb"/* namespace */, ITx.UNISOLATED, properties);
        
        /*
         * FIXME Modify this to use a row scan for the contained relations.
         * There is one other place where the same test is being used. The
         * reason for this test is that getSPORelation() tries to _locate_ the
         * relation, but that will fail if it does not exist. By using the ctor
         * and exists() we can test for pre-existence. However, the best route
         * is to perform a row scan when the container is created and then we
         * can just materialize the existing relations and create them if they
         * are not found.
         */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        if (!new SPORelation(getIndexManager(), getNamespace() + "."
//BTM - PRE_CLIENT_SERVICE                + SPORelation.NAME_SPO_RELATION, getTimestamp(), getProperties()).exists()) {
        if ( !new SPORelation(getIndexManager(),
                              getConcurrencyManager(),
                              getDiscoveryManager(),
                              getNamespace() + "."
                                  + SPORelation.NAME_SPO_RELATION,
                              getTimestamp(),
                              getProperties()).exists() )
        {
//BTM - PRE_CLIENT_SERVICE - END

            /*
             * If we could not find the SPO relation then presume that this is a
             * new KB and create it now.
             */
            
            create();

        } else {
        	
        	init();
        	
        }

    }

    /**
     * When using an {@link ITx#UNISOLATED} view, this store is NOT safe for
     * write operations concurrent with either readers or writers. However, it
     * does support concurrent readers for {@link ITx#READ_COMMITTED} and
     * read-historical views.
     */
    public boolean isConcurrent() {

        return getTimestamp() == ITx.UNISOLATED;
        
    }

}
