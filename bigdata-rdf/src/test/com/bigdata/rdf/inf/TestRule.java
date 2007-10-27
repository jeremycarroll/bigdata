/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 26, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Test suite for basic {@link Rule} mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRule extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRule() {
        super();
    }

    /**
     * @param name
     */
    public TestRule(String name) {
        super(name);
    }

    /**
     * Test the singleton factory for {@link Var}s.
     */
    public void test_variableSingletonFactory() {
        
        Var u = Rule.var("u");

        // same instance.
        assertTrue(u == Rule.var("u"));
        
        // different instance.
        assertTrue(u != Rule.var("x"));
        
    }
    
    /**
     * Verify construction of a simple rule.
     */
    public void test_ctor() {
        
        AbstractTripleStore store = getStore();
        
        try {
        
            InferenceEngine inf = new InferenceEngine(store);
        
            Var u = Rule.var("u");
            
            Triple head = new Triple(u,inf.rdfsSubClassOf,inf.rdfsResource);
            
            Pred[] body = new Pred[] { new Triple(u, inf.rdfType, inf.rdfsClass) };
            
            Rule r = new MyRule(inf, head, body);

            // write out the rule on the console.
            System.err.println(r.toString());

            // check bindings -- should be Ok.
            assertTrue(r.checkBindings());
            
            // verify body[0] is not fully bound.
            assertFalse(r.isFullyBound(0));
            
            // verify you can overwrite a variable in the tail.
            r.set(u, 1);
            assertTrue(r.checkBindings()); // no complaints.
            assertTrue(r.isFullyBound(0)); // is fully bound.
            r.resetBindings(); // restore the bindings.
            assertTrue(r.checkBindings()); // verify no complaints.
            assertFalse(r.isFullyBound(0)); // no longer fully bound.

            /*
             * verify no binding for u.
             */
            assertEquals(NULL,r.get(u));
            assertEquals(NULL,r.get(Rule.var("u")));
            
            try {
                r.get(Rule.var("v"));
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
            
            // set a binding and check things out.
            r.resetBindings(); // clean slate.
            assertEquals(NULL,r.get(u)); // no binding.
            assertEquals(NULL,r.get(Rule.var("u"))); // no binding.
            r.set(u, inf.rdfsClass.id);
            assertTrue(r.checkBindings()); // verify we did not overwrite constants.
            // write on the console.
            System.err.println(r.toString(true));
            // verify [u] is now bound.
            assertEquals(inf.rdfsClass.id,r.get(u));
            assertEquals(inf.rdfsClass.id,r.get(Rule.var("u")));
            // verify [u] is now bound.
            assertEquals(inf.rdfsClass.id,r.get(u));
            assertEquals(inf.rdfsClass.id,r.get(Rule.var("u")));
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }

    /**
     * Test the ability to compute the variables shared between two {@link Pred}s
     * in a {@link Rule}.
     */
    public void test_getSharedVars() {
        
        AbstractTripleStore store = getStore();
        
        try {

            InferenceEngine inf = new InferenceEngine(store);

            Rule r = new MyRulePattern1(inf);
            
            Set<Var> shared = r.getSharedVars(0, 1);

            assertTrue(shared.contains(Rule.var("u")));

            assertFalse(shared.contains(Rule.var("v")));

            assertEquals(1,shared.size());

        } finally {
            
            store.closeAndDelete();
            
        }
    }
    
    /**
     * Test the ability to obtain the access path given the {@link Pred}.
     */
    public void test_getAccessPath() {
        
        AbstractTripleStore store = getStore();
        
        try {

            InferenceEngine inf = new InferenceEngine(store);

            Rule r = new MyRulePattern1(inf);

            // (u rdfs:subClassOf x)
            assertEquals(KeyOrder.POS,r.getAccessPath(0).getKeyOrder());

            // (v rdfs:subClassOf u)
            assertEquals(KeyOrder.POS,r.getAccessPath(1).getKeyOrder());
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * Test the ability to choose the more selective access path, that the
     * selected path changes as predicates become bound, and that the resulting
     * entailment reflects the current variable bindings.
     */
    public void test_getMoreSelectiveAccessPath() {
        
        AbstractTripleStore store = getStore();
        
        try {

            InferenceEngine inf = new InferenceEngine(store);

            URI U1 = new URIImpl("http://www.foo.org/U1");
            URI U2 = new URIImpl("http://www.foo.org/U2");
            URI V1 = new URIImpl("http://www.foo.org/V1");
            URI V2 = new URIImpl("http://www.foo.org/V2");
            URI X1 = new URIImpl("http://www.foo.org/X1");
//            URI X2 = new URIImpl("http://www.foo.org/X2");

            // body[0]                  body[1]          -> head
            // (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
            Rule r = new MyRulePattern1(inf);

            /*
             * Obtain the access paths corresponding to each predicate in the
             * body of the rule. Each access path is parameterized by the triple
             * pattern described by the corresponding predicate in the body of
             * the rule.
             * 
             * Note: even when using the same access paths the range counts CAN
             * differ based on what constants are bound in each predicate and on
             * what positions are variables.
             * 
             * Note: When there are shared variables the range count generally
             * will be different after those variable(s) become bound.
             */
            for (int i = 0; i < r.body.length; i++) {

                assertEquals(0, r.getAccessPath(i).rangeCount());

            }
            
            /*
             * Add some data into the store where it is visible to those access
             * paths and notice the change in the range count.
             */
            StatementBuffer buffer = new StatementBuffer(store,100/*capacity*/,true/*distinct*/);

            // (u rdf:subClassOf x)
            buffer.add(U1, URIImpl.RDFS_SUBCLASSOF, X1);
            
            // (v rdf:type u)
            buffer.add(V1, URIImpl.RDF_TYPE, U1);
            buffer.add(V2, URIImpl.RDF_TYPE, U2);
            
            buffer.flush();
            
            store.dumpStore();
            
            assertEquals(3,store.getStatementCount());
            
            // (u rdf:subClassOf x)
            assertEquals(1,r.getAccessPath(0).rangeCount());

            // (v rdf:type u)
            assertEquals(2,r.getAccessPath(1).rangeCount());

            /*
             * Now use the more selective of the two 1-bound triple patterns to
             * query the store.
             */

            // (u rdf:subClassOf x)
            assertEquals(0,r.getMostSelectiveAccessPath());
            
            /*
             * bind variables for (u rdf:subClassOf x) to known values from the
             * statement in the database that matches the predicate.
             */
            
            assertNotSame(NULL,store.getTermId(U1));
            r.set(Rule.var("u"), store.getTermId(U1));
            
            assertNotSame(NULL,store.getTermId(X1));
            r.set(Rule.var("x"), store.getTermId(X1));
            
            assertTrue(r.isFullyBound(0));
            
            // (v rdf:type u)
            assertEquals(1,r.getMostSelectiveAccessPath());

            // bind the last variable.
            assertNotSame(NULL,store.getTermId(V1));
            r.set(Rule.var("v"), store.getTermId(V1));

            assertTrue(r.isFullyBound(1));

            // verify no access path is recommended since the rule is fully bound.
            assertEquals(-1,r.getMostSelectiveAccessPath());

            SPOBuffer buffer2 = new SPOBuffer(store, null/* filter */,
                    1/* capacity */, false/* distinct */, true/* justified */);
            
            // emit the entailment
            r.emit(buffer2);
            
            assertEquals(1,buffer2.size());
            assertEquals(r.justify?1:0,buffer2.getJustificationCount());
            
            // verify bindings on the emitted entailment.
            SPO entailment = buffer2.get(0);
            assertEquals(entailment.s,store.getTermId(V1));
            assertEquals(entailment.p,store.getTermId(URIImpl.RDF_TYPE));
            assertEquals(entailment.o,store.getTermId(X1));
            
            if(r.justify) {
             
                // @todo verify the justification
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }
    }
    
    private static class MyRule extends Rule {

        public MyRule(InferenceEngine inf, Triple head, Pred[] body) {

            super(inf, head, body);

        }

        public RuleStats apply(RuleStats stats, SPOBuffer buffer) {

            return null;
            
        }

    }

    /**
     * this is rdfs9:
     * 
     * <pre>
     * (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class MyRulePattern1 extends Rule {
        
        public MyRulePattern1(InferenceEngine inf) {
            super(inf,//
                    new Triple(var("v"), inf.rdfType, var("x")), //
                    new Pred[] {//
                            new Triple(var("u"), inf.rdfsSubClassOf, var("x")),//
                            new Triple(var("v"), inf.rdfType, var("u")) //
                    });
        }

        public RuleStats apply(RuleStats stats, SPOBuffer buffer) {

            return null;
            
        }
    }
}
