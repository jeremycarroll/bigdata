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
package com.bigdata.rdf.lexicon;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
        TestComparators.class,

        TestSerialization.class,

        TestTermIdEncoder.class,

        TestTerm2IdTupleSerializer.class,

        TestId2TermTupleSerializer.class,

        // basic unit tests for adding terms to the lexicon.
        TestAddTerms.class,

        // test suite for the vocabulary models and their persistence.
        TestVocabulary.class,

        // test suite for the completion scan (prefix match for literals).
        TestCompletionScan.class,

        // test suite for the full-text indexer integration.
        TestFullTextIndex.class,

        // test suite for inlining
        TestInlining.class
      } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
    }
}
