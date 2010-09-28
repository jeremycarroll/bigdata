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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import com.bigdata.test.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test that are run for each {@link ITripleStore} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
        /*
         * Bootstrap test suites.
         */

        // make sure that the db can find the relations and they their container
        TestRelationLocator.class,

        // test suite for the LexiconRelation.
        com.bigdata.rdf.lexicon.TestAll.class,

        // test suite for the SPORelation.
        com.bigdata.rdf.spo.TestAll.class,

        /*
         * Tests at the RDF Statement level, requiring use of both the
         * LexiconRelation and the SPORelation.
         */

        // test adding terms and statements.
        TestTripleStore.class,

        // test the ISPO#isModified() API (low-level API).
        TestIsModified.class,

        // test adding terms and statements is restart safe.
        TestRestartSafe.class,

        // a stress test based on an issue observed for centos.
        StressTestCentos.class,

        // somewhat dated test of sustained insert rate on synthetic data.
        TestInsertRate.class,

        // test of the statement identifier semantics.
        TestStatementIdentifiers.class,

        // test suite for bulk filter of statements absent/present in the kb.
        TestBulkFilter.class,

        /*
         * test suite for the rio parser and data loading integration, including
         * support for statement identifiers and handling of blank nodes when
         * statement identifiers are NOT enabled.
         */
        com.bigdata.rdf.rio.TestAll.class,

        // magic sets support (still under development).
        com.bigdata.rdf.magic.TestAll.class
        } )
public class TestTripleStoreBasics extends Assert {
}
