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
 * Created on Dec 23, 2008
 */

package com.bigdata.journal;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
       // test of the RunState class.
       TestRunState.class,
       // Test suite for the transaction service using a mock client.
       TestTransactionService.class,
       // Isolation tests with a standalone database (Journal).
       // tests of read-write transactions and isolation.
       TestTx.class,
       // tests of read-only transactions.
       TestReadOnlyTx.class,
       // tests of transactions starting from the last committed state.
       TestReadCommittedTx.class
        } )
public class TestTransactionSupport extends ProxyTestCase {

    /**
     * 
     */
    public TestTransactionSupport() {
    }
}
