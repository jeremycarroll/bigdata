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
package com.bigdata.rdf.rules;

import com.bigdata.rdf.spo.TestSPORelation;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@RunWith(Suite.class)
@SuiteClasses( {
        // test suite for the axiom models and their persistence.
        com.bigdata.rdf.axioms.TestAll.class,

        /*
         * test ability to insert, update, or remove elements from a relation
         * and the ability to select the right access path given a predicate for
         * that relation and query for those elements (we have to test all this
         * stuff together since testing query requires us to have some data in
         * the relation).
         */
        TestSPORelation.class,

        /*
         * test suite for modelsEqual().
         */
        TestModelsEqual.class,

        /*
         * test forward chainer rules.
         */

        // test suite for rule re-writes for RDF DB truth maintenance.
        TestTMUtility.class,

        // test suite for rule re-writes.
        TestMappedProgram.class,

        // test suite for writing, reading, chasing and retracting justifications.
        TestJustifications.class,

        // test suite for distinct term scan
        TestDistinctTermScan.class,

        // test suite for rdf1.
        TestRuleRdf01.class,

        // test that rdfs3 does not let literals into the subject.
        TestRuleRdfs03.class,

        // test suite for rdfs4.
        TestRuleRdfs04.class,

        // Note: rdfs 2, 3, 7, and 9 use the same base class.
        TestRuleRdfs07.class,

        // Note: rdfs 6, 8, 10, 12, and 13 use the same base class.
        TestRuleRdfs10.class,

        // Note: rdfs 5 and 11 use the same base class.
        TestRuleRdfs11.class,

        // test suite for the "match" rule (entity matching).
        TestMatch.class,

        // Note: fast closure rules using the same base class.
        TestRuleFastClosure_11_13.class,

        // Note: fast closure rules using the same base class.
        TestRuleFastClosure_3_5_6_7_9.class,

        // owl:sameAs rules.
        TestRuleOwlSameAs.class,

        // test owl:equivalentClass
        TestRuleOwlEquivalentClass.class,

        // test owl:equivalentProperty
        TestRuleOwlEquivalentProperty.class,

        // test owl:transitiveProperty
        TestRuleOwlTransitiveProperty.class,

        // test owl:inverseOf
        TestRuleOwlInverseOf.class,

        // test owl:hasValue
        TestRuleOwlHasValue.class,

        /*
         * Test for optionals and for query options applied during native rule
         * and program evaluation.
         *
         * Note: These features MUST also be tested at the Sesame integration
         * level. Tests at that level test the correct transform of the query
         * operation tree into an IRule or IProgram and the correct handling of
         * the generated (typically asynchronous) iterator reading on the
         * generated solutions.
         */

        // test suite for optionals.
        TestOptionals.class,

        // test suite for SLICE (OFFSET + LIMIT).
        TestSlice.class,

        // test suite for DISTINCT.
        TestDistinct.class,

        // test suite for ORDER_BY.
        TestOrderBy.class,

        // test suite for UNION, including interaction with SLICE, DISTINCT, and ORDER_BY.
        TestUnion.class,

        /*
         * Tests for computing closure.
         */

        // test suite for fix point closure of some rule sets (full and fast).
        TestDatabaseAtOnceClosure.class,

        // compare two means of computing owl:sameAs for equivalence.
        TestCompareFullAndFastClosure.class,

        /*
         * Test entailments that are computed at query time rather than when the
         * data are loaded into the store.
         *
         * Note: These are sometimes referred to as "backchained" rules, but in
         * fact they are either highly specialized code, e.g., for the (x
         * rdf:Type: rdfs:Resource) entailments, or an application of the
         * relevant rules using the forward chainer once the rules have been
         * constrained by the triple pattern (similar to magic sets but less
         * general).
         */

        // test suite for backward chaining of (?x rdf:type rdfs:Resource).
        TestBackchainTypeResourceIterator.class ,

//        // test suite for owl:sameAs {2,3} (constained forward evaluation).
//        TestBackchainOwlSameAs.class ,

        // test suite for owl:sameAs {2,3} (backward chaining).
        TestBackchainOwlSameAsPropertiesIterator.class ,

        // test suite for RDF specific rule evaluation support.
        com.bigdata.rdf.internal.constraints.TestAll.class,

        /*
         * Test truth maintenance. This examines the incremental assertion and
         * retraction of statements buffered in a [focusStore] against the
         * database.  Database-at-once closure is tested above.
         */

        // test suite for basic TM mechanism encapsulated by this class.
        TestTruthMaintenance.class
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
    }
}
