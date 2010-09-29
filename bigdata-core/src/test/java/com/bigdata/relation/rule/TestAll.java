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
package com.bigdata.relation.rule;

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
        // test variable and constant impls.
        TestVar.class,
        TestConstant.class,

        // test predicate impls.
        TestPredicate.class,

        // @todo test binding set impls.
        TestBindingSet.class,

        // test ISlice() impl.
        TestSlice.class,

        // test suite for basic rule mechanisms.
         TestRule.class,

        // test suite for IProgram, but not program evaluation.
         TestProgram.class,

        // test suite for rule evaluation.
        com.bigdata.relation.rule.eval.TestAll.class

        // @todo test suite for XML interchange of programs.
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
        
    }
}
