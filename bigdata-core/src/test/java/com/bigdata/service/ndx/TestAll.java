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
 * Created on Oct 14, 2006
 */

package com.bigdata.service.ndx;

import com.bigdata.service.TestBasicIndexStuff;
import com.bigdata.service.TestBasicIndexStuffRemote;
import com.bigdata.service.TestEmbeddedClient;
import com.bigdata.service.TestEmbeddedClientRemote;
import com.bigdata.service.TestRangeQuery;
import com.bigdata.service.TestRangeQueryRemote;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites in increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
        // test splitting client requests against index partition locators.
        TestSplitter.class,

        // client basics, including static partitioning of indices.
        TestEmbeddedClient.class,
        TestEmbeddedClientRemote.class,

        // test basic index operations.
        TestBasicIndexStuff.class,
        TestBasicIndexStuffRemote.class,

        // test range iterators (within and across index partitions).
        TestRangeQuery.class,
        TestRangeQueryRemote.class,

        // unit tests for the streaming index write API.
        com.bigdata.service.ndx.pipeline.TestAll.class
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
    }
}
