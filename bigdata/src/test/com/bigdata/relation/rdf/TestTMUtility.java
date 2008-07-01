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
 * Created on Jun 24, 2008
 */

package com.bigdata.relation.rdf;

import java.util.List;

import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rule.AbstractRuleTestCase;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.MockRelationName;
import com.bigdata.relation.rule.Rule;

/**
 * Test suite for rule re-writes supporting truth maintenance for the RDF DB.
 * This does NOT test the execution of those rules or their role in truth
 * maintenance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTMUtility extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestTMUtility() {

    }

    /**
     * @param name
     */
    public TestTMUtility(String name) {

        super(name);
        
    }

    final private IRelationName database = new MockRelationName("database");

    final private IRelationName focusStore = new MockRelationName("focusStore");

    /**
     * Test mapping of a rule with a single predicate in the tail across two
     * relations for truth maintenance. This will produce a single new rule.
     * That rule will read from the focusStore rather than the database.
     */
    public void test_rewrite1() {

        final Rule r = new TestRuleRdfs04a(database);

        log.info(r.toString());

        final List<IRule> rules = TMUtility.INSTANCE.mapRuleForTruthMaintenance(
                r, focusStore);

        assertEquals(1, rules.size());

        {

            IRule r0 = rules.get(0);

            log.info(r0.toString());

            assertTrue(r0.getTail(0).getRelationName() == focusStore);
            
        }
        
    }

    /**
     * Test mapping of a rule with a two predicates in the tail across two
     * relations for truth maintenance. This will produce 2 new rules.
     * <p>
     * In the first rule, the first tail will read from the focusStore while the
     * second tail reads from the fused view of the focusStore and the database.
     * <p>
     * In the second rule, the first tail will read from the fused view while
     * the second tail will read from the focusStore.
     */
    public void test_rewrite2() {

        final Rule r = new TestRuleRdfs9(database);

        log.info(r.toString());

        final List<IRule> rules = TMUtility.INSTANCE.mapRuleForTruthMaintenance(
                r, focusStore);

        assertEquals(2, rules.size());

        {
            IRule r0 = rules.get(0);

            log.info(r0.toString());

            // 1st tail.
            assertTrue(r0.getTail(0).getRelationName() == focusStore);

            // 2nd tail
            assertTrue(r0.getTail(1).getRelationName() instanceof SPORelationView);

            SPORelationView fusedView = (SPORelationView)r0.getTail(1).getRelationName();

            assertTrue(fusedView.getDatabase() == database);

            assertTrue(fusedView.getFocusStore() == focusStore);
            
        }

        {

            IRule r1 = rules.get(1);

            log.info(r1.toString());

            // 1st tail.
            assertTrue(r1.getTail(0).getRelationName() instanceof SPORelationView);

            SPORelationView fusedView = (SPORelationView)r1.getTail(0).getRelationName();

            assertTrue(fusedView.getDatabase() == database);

            assertTrue(fusedView.getFocusStore() == focusStore);
            
            // 2nd tail
            assertTrue(r1.getTail(1).getRelationName() == focusStore);

        }

    }

}
