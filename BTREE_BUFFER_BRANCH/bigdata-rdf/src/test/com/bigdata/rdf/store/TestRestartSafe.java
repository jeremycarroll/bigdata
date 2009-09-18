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
 * Created on Feb 5, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.BNode;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rules.InferenceEngine.Options;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test restart safety for the various indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafe extends AbstractTripleStoreTestCase {

    public TestRestartSafe() {
    }

    public TestRestartSafe(String name) {
        super(name);
    }
    
    public void test_restartSafe() throws IOException {

        final Properties properties = super.getProperties();
        
        // override the default vocabulary.
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());

        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {
        
            /*
             * setup the database.
             */
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI x = f.createURI("http://www.foo.org/x");
            final BigdataURI y = f.createURI("http://www.foo.org/y");
            final BigdataURI z = f.createURI("http://www.foo.org/z");

            final BigdataURI A = f.createURI("http://www.foo.org/A");
            final BigdataURI B = f.createURI("http://www.foo.org/B");
            final BigdataURI C = f.createURI("http://www.foo.org/C");

            final BigdataURI rdfType = f.asValue(RDF.TYPE);

            final BigdataURI rdfsSubClassOf = f.asValue(RDFS.SUBCLASSOF);

            final BigdataLiteral lit1 = f.createLiteral("abc");
            final BigdataLiteral lit2 = f.createLiteral("abc", A);
            final BigdataLiteral lit3 = f.createLiteral("abc", "en");

            final BigdataBNode bn1 = f.createBNode(UUID.randomUUID()
                    .toString());
            final BigdataBNode bn2 = f.createBNode("a12");

            store.addStatement(x, rdfType, C);
            store.addStatement(y, rdfType, B);
            store.addStatement(z, rdfType, A);

            store.addStatement(B, rdfsSubClassOf, A);
            store.addStatement(C, rdfsSubClassOf, B);

            final long x_id = store.getTermId(x);
            assertTrue(x_id != NULL);
            final long y_id = store.getTermId(y);
            assertTrue(y_id != NULL);
            final long z_id = store.getTermId(z);
            assertTrue(z_id != NULL);
            final long A_id = store.getTermId(A);
            assertTrue(A_id != NULL);
            final long B_id = store.getTermId(B);
            assertTrue(B_id != NULL);
            final long C_id = store.getTermId(C);
            assertTrue(C_id != NULL);
            final long rdfType_id = store.getTermId(rdfType);
            assertTrue(rdfType_id != NULL);
            final long rdfsSubClassOf_id = store.getTermId(rdfsSubClassOf);
            assertTrue(rdfsSubClassOf_id != NULL);

            final long lit1_id = store.addTerm(lit1);
            assertTrue(lit1_id != NULL);
            final long lit2_id = store.addTerm(lit2);
            assertTrue(lit2_id != NULL);
            final long lit3_id = store.addTerm(lit3);
            assertTrue(lit3_id != NULL);

            final long bn1_id = store.addTerm(bn1);
            assertTrue(bn1_id != NULL);
            final long bn2_id = store.addTerm(bn2);
            assertTrue(bn2_id != NULL);

            final boolean storeBlankNodes = store.getLexiconRelation()
                    .isStoreBlankNodes();

            assertEquals("#terms", 8 + 3 + (storeBlankNodes ? 2 : 0), store
                    .getTermCount());
            assertEquals("#uris", 8, store.getURICount());
            assertEquals("#lits", 3, store.getLiteralCount());
            if (storeBlankNodes) {
                assertEquals("#bnodes", 2, store.getBNodeCount());
            } else {
                assertEquals("#bnodes", 0, store.getBNodeCount());
            }

            store.commit();

            assertEquals(x_id, store.getTermId(x));
            assertEquals(y_id, store.getTermId(y));
            assertEquals(z_id, store.getTermId(z));
            assertEquals(A_id, store.getTermId(A));
            assertEquals(B_id, store.getTermId(B));
            assertEquals(C_id, store.getTermId(C));
            assertEquals(rdfType_id, store.getTermId(rdfType));
            assertEquals(rdfsSubClassOf_id, store.getTermId(rdfsSubClassOf));

            for (String fqn : store.getSPORelation().getIndexNames()) {
                assertEquals("statementCount", 5, store.getSPORelation()
                        .getIndex(fqn).rangeCount(null, null));
            }
            assertTrue(store.hasStatement(x, rdfType, C));
            assertTrue(store.hasStatement(y, rdfType, B));
            assertTrue(store.hasStatement(z, rdfType, A));
            assertTrue(store.hasStatement(B, rdfsSubClassOf, A));
            assertTrue(store.hasStatement(C, rdfsSubClassOf, B));

            assertEquals("#terms", 8 + 3 + (storeBlankNodes ? 2 : 0), store
                    .getTermCount());
            assertEquals("#uris", 8, store.getURICount());
            assertEquals("#lits", 3, store.getLiteralCount());
            if (storeBlankNodes) {
                assertEquals("#bnodes", 2, store.getBNodeCount());
            } else {
                assertEquals("#bnodes", 0, store.getBNodeCount());
            }

            if (log.isInfoEnabled())
                log.info("\n" + store.dumpStore());

            store.commit();

            if (store.isStable()) {

                store = reopenStore(store);

                if (log.isInfoEnabled())
                    log.info("\n" + store.dumpStore());

                assertEquals(x_id, store.getTermId(x));
                assertEquals(y_id, store.getTermId(y));
                assertEquals(z_id, store.getTermId(z));
                assertEquals(A_id, store.getTermId(A));
                assertEquals(B_id, store.getTermId(B));
                assertEquals(C_id, store.getTermId(C));
                assertEquals(rdfType_id, store.getTermId(rdfType));
                assertEquals(rdfsSubClassOf_id, store.getTermId(rdfsSubClassOf));

                for (String fqn : store.getSPORelation().getIndexNames()) {
                    assertEquals("statementCount", 5, store.getSPORelation()
                            .getIndex(fqn).rangeCount(null, null));
                }
//                assertEquals("statementCount", 5, store.getSPORelation().getSPOIndex()
//                        .rangeCount(null, null));
//                assertEquals("statementCount", 5, store.getSPORelation().getPOSIndex()
//                        .rangeCount(null, null));
//                assertEquals("statementCount", 5, store.getSPORelation().getOSPIndex()
//                        .rangeCount(null, null));
                assertTrue(store.hasStatement(x, rdfType, C));
                assertTrue(store.hasStatement(y, rdfType, B));
                assertTrue(store.hasStatement(z, rdfType, A));
                assertTrue(store.hasStatement(B, rdfsSubClassOf, A));
                assertTrue(store.hasStatement(C, rdfsSubClassOf, B));

                assertEquals("#terms", 8 + 3 + (storeBlankNodes ? 2 : 0), store
                        .getTermCount());
                assertEquals("#uris", 8, store.getURICount());
                assertEquals("#lits", 3, store.getLiteralCount());
                if (storeBlankNodes) {
                    assertEquals("#bnodes", 2, store.getBNodeCount());
                } else {
                    assertEquals("#bnodes", 0, store.getBNodeCount());
                }

                /*
                 * verify the terms can be recovered.
                 */
                assertEquals(x, store.getTerm(x_id));
                assertEquals(y, store.getTerm(y_id));
                assertEquals(z, store.getTerm(z_id));
                assertEquals(A, store.getTerm(A_id));
                assertEquals(B, store.getTerm(B_id));
                assertEquals(C, store.getTerm(C_id));

                assertEquals(rdfType, store.getTerm(rdfType_id));
                assertEquals(rdfsSubClassOf, store.getTerm(rdfsSubClassOf_id));

                assertEquals(lit1, store.getTerm(lit1_id));
                assertEquals(lit2, store.getTerm(lit2_id));
                assertEquals(lit3, store.getTerm(lit3_id));

                //            assertEquals(bn1, store.getTerm(bn1_id));
                //            assertEquals(bn2, store.getTerm(bn2_id));
                assertTrue(store.getTerm(bn1_id) != null);
                assertTrue(store.getTerm(bn1_id) instanceof BNode);
                assertTrue(store.getTerm(bn2_id) != null);
                assertTrue(store.getTerm(bn2_id) instanceof BNode);
                assertTrue(store.getTerm(bn1_id) != store.getTerm(bn2_id));

            }

        } finally {

            store.closeAndDelete();

        }
        
    }

}
