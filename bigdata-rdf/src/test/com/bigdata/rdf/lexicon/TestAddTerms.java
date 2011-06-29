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
 * Created on Nov 6, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for adding terms to the lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAddTerms extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestAddTerms() {
        super();
       
    }

    /**
     * @param name
     */
    public TestAddTerms(String name) {
        super(name);
       
    }

//	/**
//	 * Verify that a {@link BlobIV#NullIV} was entered into the TERMS index when
//	 * the lexicon was created.
//	 */
//	public void test_NullIV() {
//    
//        final Properties properties = getProperties();
//        
//        // test w/o predefined vocab.
//        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
//                .getName());
//
//        // test w/o axioms - they imply a predefined vocab.
//        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
//        
//        // test w/o the full text index.
//        properties.setProperty(Options.TEXT_INDEX, "false");
//
//		// test w/o date time inlining (since this causes xsd:dateTime to be
//		// inserted into the TERMS index).
//        properties.setProperty(Options.INLINE_DATE_TIMES, "false");
//
//        // test w/o unicode inlining (since this causes xsd:string to be
//		// inserted into the TERMS index).
//        properties.setProperty(Options.MAX_INLINE_TEXT_LENGTH, "0");
//
//        final AbstractTripleStore store = getStore(properties);
//        
//        try {
//        	
//        	final TermsIndexHelper helper = new TermsIndexHelper();
//
////        	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
//        	
//        	final IIndex ndx = store.getLexiconRelation().getTermsIndex();
//
//			if (log.isInfoEnabled())
//				log
//						.info("dump:\n"
//								+ helper.dump(store.getLexiconRelation()
//										.getNamespace(), ndx));
//
////        	for (VTE vte : VTE.values()) {
////                
////                // Each VTE has an associated NullIV (mapped to a [null]).
////                assertNull(ndx.lookup(BlobIV.mockIV(vte).encode(
////                        keyBuilder.reset()).getKey()));
////                
////            }
////            
////            // Should be one entry for each type of NullIV.
////            assertEquals(4L, ndx.rangeCount());
////
////            // Verify we visit each of those NullIVs.
////            @SuppressWarnings("unchecked")
////            final ITupleIterator<BigdataValue> itr = ndx.rangeIterator();
////
////            while(itr.hasNext()) {
////                
////                final ITuple<BigdataValue> tuple = itr.next();
////                
////                assertTrue(tuple.isNull());
////                
////                // The tuple is deserialized as a [null] reference.
////                assertNull(tuple.getObject());
////
////            }
//			
//        } finally {
//        	
//        	store.__tearDownUnitTest();
//        	
//        }
//    	
//    }
    
    public void test_addTerms() {

        final Properties properties = getProperties();
        
        // test w/o predefined vocab.
        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
                .getName());

        // test w/o axioms - they imply a predefined vocab.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // test w/o the full text index.
        properties.setProperty(Options.TEXT_INDEX, "false");

        // test w/o inlining
        properties.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");

        AbstractTripleStore store = getStore(properties);
        
        try {

            final Collection<BigdataValue> terms = new HashSet<BigdataValue>();

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            terms.add(f.asValue(RDF.TYPE));
            terms.add(f.asValue(RDF.PROPERTY));
            terms.add(f.createLiteral("test"));
            terms.add(f.createLiteral("test", "en"));
            terms.add(f.createLiteral("10", f
                    .createURI("http://www.w3.org/2001/XMLSchema#int")));

            terms.add(f.createLiteral("12", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            terms.add(f.createLiteral("12.", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            terms.add(f.createLiteral("12.0", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

            terms.add(f.createLiteral("12.00", f
                    .createURI("http://www.w3.org/2001/XMLSchema#float")));

			if (store.getLexiconRelation().isStoreBlankNodes()) {
	            /*
	             * Note: Blank nodes will not round trip through the lexicon unless
	             * the "told bnodes" is enabled.
	             */
				terms.add(f.createBNode());
				terms.add(f.createBNode("a"));
			}

			final Map<IV, BigdataValue> ids = doAddTermsTest(store, terms);

			if (store.isStable()) {

				store.commit();

				store = reopenStore(store);

				// verify same reverse mappings.

                final Map<IV, BigdataValue> ids2 = store.getLexiconRelation()
                        .getTerms(ids.keySet());

                assertEquals(ids.size(),ids2.size());
                
                for (IV id : ids.keySet()) {

                    assertEquals("Id mapped to a different term? : termId="
                            + id, ids.get(id), ids2.get(id));

                }

            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }

    /**
     * The "told bnodes" mode uses the blank node ID as specified rather than
     * assigning one based on a UUID. For this case, we need to store the blank
     * nodes in the reverse index (T2ID) so we can translate a blank node back
     * to a specific identifier.
     */
    public void test_toldBNodes() {

        final Properties properties = getProperties();
        
        // test w/o predefined vocab.
        properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class
                .getName());

        // test w/o axioms - they imply a predefined vocab.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // test w/o the full text index.
        properties.setProperty(Options.TEXT_INDEX, "false");

        // this is the "told bnodes" mode.
        properties.setProperty(Options.STORE_BLANK_NODES, "true");

        AbstractTripleStore store = getStore(properties);
        
        try {

            if (!store.isStable()) {

                /*
                 * We need a restart safe store to test this since otherwise a
                 * term cache could give us a false positive.
                 */

                return;
                
            }

            final Collection<BigdataValue> terms = new HashSet<BigdataValue>();

            // lookup/add some values.
            final BigdataValueFactory f = store.getValueFactory();

            terms.add(f.createBNode());
            terms.add(f.createBNode("a"));

            final Map<IV, BigdataValue> ids = doAddTermsTest(store, terms);

            if (store.isStable()) {
                
                store.commit();
                
                store = reopenStore(store);

                // verify same reverse mappings.

                final Map<IV, BigdataValue> ids2 = store.getLexiconRelation()
                        .getTerms(ids.keySet());

                assertEquals(ids.size(),ids2.size());
                
                for (IV id : ids.keySet()) {

                    assertEquals("Id mapped to a different term? : termId="
                            + id, ids.get(id), ids2.get(id));

                }

            }

        } finally {
            
            store.__tearDownUnitTest();
            
        }

    }

    /**
     * FIXME Write unit test which inserts and verifies support for very large
     * objects (otherwise this class does not test the BLOBS index and does not
     * verify support for large objects).
     * 
     * FIXME We should also explicitly test bnode support for the BLOBS index
     * (it is already unit tested separately at the {@link TermsIndexHelper}
     * level).
     */
    public void test_blobs() {
        
        fail("write test");
        
    }

    /**
     * @param store
     * @param terms
     * @return
     */
    private Map<IV, BigdataValue> doAddTermsTest(
            final AbstractTripleStore store,
            final Collection<BigdataValue> terms) {

        final int size = terms.size();

        final BigdataValue[] a = terms.toArray(new BigdataValue[size]);
        
        // resolve term ids.
        store
                .getLexiconRelation()
                .addTerms(a, size, false/* readOnly */);

        // populate map w/ the assigned term identifiers.
        final Collection<IV> ids = new ArrayList<IV>();

        for(BigdataValue t : a) {
            
            ids.add(t.getIV());
            
        }
        
        /*
         * Resolve the assigned term identifiers against the lexicon.
         */
        final Map<IV, BigdataValue> tmp = store.getLexiconRelation()
                .getTerms(ids);
        
        assertEquals(size,tmp.size());

        /*
         * Verify that the lexicon reports the same RDF Values for those term
         * identifiers (they will be "equals()" to the values that we added to
         * the lexicon).
         */
        for(BigdataValue expected : a) {

            assertNotSame("Did not assign term identifier? : " + expected,
                    null, expected.getIV());

            final BigdataValue actual = tmp.get(expected.getIV());

            if (actual == null) {

                fail("Lexicon does not have value: termId="
                        + expected.getIV() + ", term=" + expected);
                
            }
            
            assertEquals("Id mapped to a different term? id="
                    + expected.getIV(), expected, actual);

        }
        
        return tmp;

    }
        
}
