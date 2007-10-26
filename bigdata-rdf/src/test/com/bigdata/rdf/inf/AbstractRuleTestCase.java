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
 * Created on Apr 18, 2007
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPOBuffer;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleTestCase extends AbstractInferenceEngineTestCase {

//    protected TempTripleStore tmpStore;
//    
//    final protected RuleStats stats = new RuleStats();
//    
//    final protected int capacity = 10;
//    
//    final protected boolean distinct = false;
//
//    protected SPOBuffer buffer;
//
//    protected AbstractTripleStore store;
//
//    protected InferenceEngine inferenceEngine;
//
//    public void setUp() throws Exception {
//
//        super.setUp();
//
//        store = getStore();
//      
//        inferenceEngine = new InferenceEngine(store);
//
//        tmpStore = new TempTripleStore(store.getProperties());
//    
//        buffer = new SPOBuffer(tmpStore,capacity,distinct);
//        
//    }
//    
//    public void tearDown() throws Exception {
//
//        if(this.store!=null) {
//            
//            this.store.closeAndDelete();
//            
//        }
//        
//        super.tearDown();
//        
//    }
    
    /**
     * 
     */
    public AbstractRuleTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRuleTestCase(String name) {
        super(name);
    }

    /**
     * Applies the rule, copies the new entailments into the store and checks
     * the expected #of inferences computed and new statements copied into the
     * store.
     * <p>
     * Invoke as <code>applyRule( store.{rule}, ..., ... )</code>
     * 
     * @param rule
     *            The rule, which must be one of those found on {@link #store}
     *            or otherwise configured so as to run with the {@link #store}
     *            instance.
     * 
     * @param expectedComputed
     *            The #of entailments that should be computed by the rule.
     */
    protected RuleStats applyRule(Rule rule, int expectedComputed) {
        
        RuleStats stats = new RuleStats();
        
        /*
         * Note: Choose a capacity large enough that all entailments will still
         * be in the buffer until we explicitly flush them to the store. This
         * let's us dump the entailments to the console below.
         */
        
        final int capacity = Math.max(expectedComputed, 1000);
        
        SPOBuffer buffer = new SPOBuffer(rule.db,
                rule.inf.doNotAddFilter/* filter */, capacity,
                false/* distinct */, rule.justify);
        
        // dump the database on the console.
        System.err.println("database::");
        rule.db.dumpStore();
        
        // apply the rule.
        rule.apply(stats, buffer);
        
        // dump entailments on the console.
        System.err.println("entailments:: (may duplicate statements in the database)");
        buffer.dump(rule.db/*used to resolve term identifiers*/);

        // flush entailments into the temporary store.
        buffer.flush();

        /*
         * Verify the #of entailments computed. 
         */
        assertEquals("numComputed",expectedComputed,stats.numComputed);
        
//        /*
//         * transfer the entailments from the temporary store to the primary
//         * store.
//         */
//        final int actualCopied = InferenceEngine.copyStatements(tmpStore,
//                store);
//        
//        assertEquals("#copied",expectedCopied,actualCopied);

        return stats;
        
    }
    
}
