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

package  com.bigdata.rdf.rules;

import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.ProxyTestCase;
import com.bigdata.relation.rule.IQueryOptions;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@link IQueryOptions#getOrderBy()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@RunWith(Parameterized.class)
public class TestOrderBy extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestOrderBy(AbstractTestCase delegate) {
        setDelegate(delegate);
    }

    @Parameters
    public static Collection<Object[]> getDelegates() {
        return ProxyTestCase.getDelegateGroup4();
    };

    /**
     * @todo write unit tests for "orderBy".
     * 
     * @todo unit test where empty result set.
     * 
     * @todo unit test where duplicate records exit and verify that duplicates
     *       are retained.
     * 
     * @todo unit test for various ascending and descending key combinations,
     *       including keys with unicode values. verify that the ascending and
     *       descending orders are being computed correctly (basically, verify
     *       that we are generating the descending order by a correct
     *       perturbation of the generated sort key).
     * 
     * @todo native order by support has not been implemented yet.
     */
    @Test
    public void test_orderBy() {
        
//        fail("write tests");
        
    }
    
}
