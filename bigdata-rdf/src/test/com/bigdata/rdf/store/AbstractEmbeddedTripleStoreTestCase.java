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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.journal.ITx;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.AbstractFederation;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractEmbeddedTripleStoreTestCase extends
        AbstractEmbeddedBigdataFederationTestCase {

    /**
     * 
     */
    public AbstractEmbeddedTripleStoreTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractEmbeddedTripleStoreTestCase(String arg0) {
        super(arg0);
    }

    /**
     * The triple store under test.
     */
    ScaleOutTripleStore store;

    protected Properties getProperties() {
    
        return new Properties(System.getProperties());
    
    }
    
    public void setUp() throws Exception {

        super.setUp();

        // connect to the database.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        store = new ScaleOutTripleStore(client.getFederation(), "test_",
//BTM - PRE_CLIENT_SERVICE                ITx.UNISOLATED, client.getProperties());
        AbstractFederation fed = (AbstractFederation)(client.getFederation());
        store = new ScaleOutTripleStore
                        ((IIndexManager)fed,
                         fed.getConcurrencyManager(),
                         (IBigdataDiscoveryManagement)fed,
                         "test_",
                         ITx.UNISOLATED,
                         client.getProperties());
//BTM - PRE_CLIENT_SERVICE - END

        
        store.create();
        
    }

    public void tearDown() throws Exception {

        super.tearDown();

    }

}
