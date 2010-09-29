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
 * Created on Feb 4, 2007
 */

package com.bigdata;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites in increase dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
        // boot launcher tests
        com.bigdata.boot.BootComponentTest.class,
        com.bigdata.boot.launcher.ConfigReaderUnitTest.class,
        com.bigdata.process.ProcessConfigXmlHandlerTest.class,

        com.bigdata.cache.TestAll.class,
        com.bigdata.io.TestAll.class,
        com.bigdata.net.TestAll.class,
        com.bigdata.config.TestAll.class,
        com.bigdata.util.TestAll.class,
        com.bigdata.util.concurrent.TestAll.class,
        com.bigdata.striterator.TestAll.class,
        com.bigdata.counters.TestAll.class,
        com.bigdata.rawstore.TestAll.class,
        com.bigdata.btree.TestAll.class,
        com.bigdata.concurrent.TestAll.class,
        com.bigdata.journal.TestAll.class,
        com.bigdata.resources.TestAll.class,
        com.bigdata.mdi.TestAll.class,
        com.bigdata.service.TestAll.class,
        com.bigdata.sparse.TestAll.class,
        com.bigdata.search.TestAll.class,
        //com.bigdata.bfs.TestAll.class,
        com.bigdata.relation.TestAll.class,
        //com.bigdata.service.mapReduce.TestAll.class,

        // RDF
        com.bigdata.rdf.TestAll.class,
        com.bigdata.rdf.sail.TestAll.class
        } )
public class TestAll {
}
