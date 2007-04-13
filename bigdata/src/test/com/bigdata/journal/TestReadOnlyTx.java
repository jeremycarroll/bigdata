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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;

/**
 * Test suite for fully isolated read-only transactions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReadOnlyTx extends ProxyTestCase {

    /**
     * 
     */
    public TestReadOnlyTx() {
    }

    /**
     * @param name
     */
    public TestReadOnlyTx(String name) {
        super(name);
    }

    /**
     * Test verifies that you can not write on a read-only transaction.
     */
    public void test_isReadOnly() {

        Journal journal = new Journal(getProperties());
        
        String name = "abc";
        
        final byte[] k1 = new byte[]{1};

        final byte[] v1 = new byte[]{1};

        {
            
            /*
             * register an index, write on the index, and commit the journal.
             */
            IIndex ndx = journal.registerIndex(name, new UnisolatedBTree(
                    journal, UUID.randomUUID()));
            
            ndx.insert(k1, v1);

            journal.commit();
            
        }
        
        {
            
            /*
             * create a read-only transaction, verify that we can read the
             * value written on the index but that we can not write on the
             * index.
             */
            
            final long tx1 = journal.newTx(IsolationEnum.ReadOnly);
            
            IIndex ndx = journal.getIndex(name,tx1);

            assertNotNull(ndx);
            
            assertEquals((byte[])v1,(byte[])ndx.lookup(k1));
         
            try {
                ndx.insert(k1,new byte[]{1,2,3});
                fail("Expecting: "+UnsupportedOperationException.class);
                } catch( UnsupportedOperationException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            journal.commit(tx1);
            
        }
        
        {
            
            /*
             * do it again, but this time we will abort the read-only
             * transaction.
             */
            
            final long tx1 = journal.newTx(IsolationEnum.ReadOnly);
            
            IIndex ndx = journal.getIndex(name,tx1);

            assertNotNull(ndx);
            
            assertEquals((byte[])v1,(byte[])ndx.lookup(k1));
         
            try {
                ndx.insert(k1,new byte[]{1,2,3});
                fail("Expecting: "+UnsupportedOperationException.class);
                } catch( UnsupportedOperationException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            journal.abort(tx1);
            
        }

        journal.closeAndDelete();
        
    }

}
