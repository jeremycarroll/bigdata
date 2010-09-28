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
 * Created on Sep 26, 2008
 */

package com.bigdata.rdf.rules;

import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.ProxyTestCase;
import com.bigdata.relation.rule.IQueryOptions;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@link IQueryOptions#isDistinct()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@RunWith(Parameterized.class)
public class TestDistinct extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestDistinct(AbstractTestCase delegate) {
        setDelegate(delegate);
    }

    @Parameters
    public static Collection<Object[]> getDelegates() {
        return ProxyTestCase.getDelegateGroup4();
    };

    /**
     * @todo write unit tests for "distinct".
     * 
     * @todo Unit test where there are no results.
     * 
     * @todo Unit test where duplicates are correctly dropped.
     */
    @Test
    public void test_distinct() {
        
//        fail("write tests");
        
    }
    
}
