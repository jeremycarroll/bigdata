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
 * Created on Apr 10, 2008
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.UUID;

import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Test of the statement identifier semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStatementIdentifiers extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestStatementIdentifiers() {
    }

    /**
     * @param name
     */
    public TestStatementIdentifiers(String name) {
        super(name);
    }
    
    /**
     * Some validation of the logic for assigning statement identifiers to
     * explicit statements.
     */
    public void test_statementIdentifiers() {

        AbstractTripleStore store = getStore();
        
        try {
            
            if (!store.statementIdentifiers) {
                
                log.warn("Statement identifiers are not enabled");
                
                return;
                
            }

            _URI x = new _URI("http://www.foo.org/x");
            _URI y = new _URI("http://www.foo.org/y");
            _URI z = new _URI("http://www.foo.org/z");
    
            _URI A = new _URI("http://www.foo.org/A");
            _URI B = new _URI("http://www.foo.org/B");
            _URI C = new _URI("http://www.foo.org/C");
    
            _URI rdfType = new _URI(RDF.TYPE);
            _URI rdfsLabel = new _URI(RDFS.LABEL);
            _URI rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);
    
            _Literal lit1 = new _Literal("abc");
            _Literal lit2 = new _Literal("abc", A);
            _Literal lit3 = new _Literal("abc", "en");
    
            _BNode bn1 = new _BNode(UUID.randomUUID().toString());
            _BNode bn2 = new _BNode("a12");

            _Value[] terms = new _Value[] {
              
                    x,y,z,//
                    A,B,C,//
                    rdfType,//
                    rdfsLabel,//
                    rdfsSubClassOf,//
                    lit1,lit2,lit3,//
                    bn1,bn2//
                    
            };
            
            store.addTerms(terms, terms.length);
            
            SPO[] stmts = new SPO[] {

                new SPO(x.termId, rdfType.termId, C.termId, StatementEnum.Explicit),
                new SPO(y.termId, rdfType.termId, B.termId, StatementEnum.Explicit),
                new SPO(z.termId, rdfType.termId, A.termId, StatementEnum.Explicit),
                
                new SPO(A.termId, rdfsLabel.termId, lit1.termId, StatementEnum.Explicit),
                new SPO(B.termId, rdfsLabel.termId, lit2.termId, StatementEnum.Explicit),
                new SPO(C.termId, rdfsLabel.termId, lit3.termId, StatementEnum.Explicit),
                
                new SPO(B.termId, rdfsSubClassOf.termId, A.termId, StatementEnum.Explicit),
                new SPO(C.termId, rdfsSubClassOf.termId, B.termId, StatementEnum.Explicit),
                    
                new SPO(bn1.termId, rdfsLabel.termId, lit1.termId, StatementEnum.Explicit),
                new SPO(bn2.termId, rdfsLabel.termId, lit2.termId, StatementEnum.Explicit),

            };

            /*
             * Add the statements to the KB.
             * 
             * Note: sorts into SPO order as a side-effect.
             */
            store.addStatements(stmts, stmts.length);
            
            /*
             * Verify statement identifiers were assigned.
             */
            for(int i=0; i<stmts.length; i++) {
                
                final long sid = stmts[i].getStatementIdentifier();
                
                assertNotSame(NULL, sid);
                
                assertTrue(AbstractTripleStore.isStatement(sid));
                assertFalse(AbstractTripleStore.isLiteral(sid));
                assertFalse(AbstractTripleStore.isURI(sid));
                assertFalse(AbstractTripleStore.isBNode(sid));
                
                System.err.println(stmts[i].toString(store)+" ::: "+stmts[i].toString());
                
            }
            
            /*
             * Verify read back from the KB. Note that the SPO[] is already in
             * SPO order and we are reading in SPO order so the array and the
             * iterator should be aligned and should visit the same #of
             * statements (since there is no truth maintenance in this test
             * there are no entailments and a closed world assumption is Ok).
             */
            {

                final ISPOIterator itr = store.getAccessPath(KeyOrder.SPO).iterator();
                
                for(int i=0; i<stmts.length; i++) {
                    
                    assertTrue("i="+i, itr.hasNext());
                    
                    final SPO expected = stmts[i];
                    
                    final SPO actual = itr.next();

                    assertEquals("S @ i=" + i, expected.s, actual.s);

                    assertEquals("P @ i=" + i, expected.p, actual.p);

                    assertEquals("O @ i=" + i, expected.o, actual.o);

                    assertEquals("TYPE @ i=" + i, expected.type, actual.type);

                    assertEquals("SID @ i=" + i, expected
                            .getStatementIdentifier(), actual
                            .getStatementIdentifier());

                }

            }

            /*
             * Verify same statement identifiers assigned if we re-assert the
             * statements.
             * 
             * Note: this creates a new SPO[] with the same {s,p,o}:type data
             * but without the statement identifiers. The array is in the same
             * order (SPO order) as the original SPO[]. We then insert the new
             * SPO[] into the KB and compare the assigned statement identifiers
             * with the ones that were assigned above.
             */
            {

                SPO[] a = new SPO[stmts.length];
                
                for(int i=0; i<stmts.length; i++) {
                    
                    SPO spo = stmts[i];
                    
                    a[i] = new SPO(spo.s, spo.p, spo.o, spo.type);
                    
                }
                
                final int nwritten = store.addStatements(a, a.length);

                for(int i=0;i<stmts.length; i++) {
                    
                    assertEquals("sid @ i=" + i, stmts[i]
                            .getStatementIdentifier(), a[i]
                            .getStatementIdentifier());
                    
                }
                
                /*
                 * Note: no statements should have been written on the kb since
                 * they were all pre-existing. addStatements() should have just
                 * read the pre-existing statement identifiers out of the KB and
                 * assigned them to a[].
                 */
                assertEquals("#written",0,nwritten);
                
            }

            StatementIterator itr = store.getStatements(null, null, null);
            
            try {

                Writer w = new StringWriter();
                
                RDFXMLWriter rdfWriter = new RDFXMLWriter(w);
                
                rdfWriter.startRDF();

                while(itr.hasNext()) {
                
                    Statement stmt = itr.next();
                    
                    rdfWriter.handleStatement(stmt);
                    
                }
                
                rdfWriter.endRDF();
                
                System.err.println(w.toString());
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            } finally {
                
                try {
                    itr.close();
                } catch (SailException e) {
                    throw new RuntimeException(e);
                }
                
            }
            
            /*
             * Verify after restart.
             */
            if (store.isStable()) {

                // flush any changes.
                store.commit();

                // re-open the database.
                store = reopenStore(store);

                /*
                 * Verify read back from the KB. Note that the SPO[] is already
                 * in SPO order and we are reading in SPO order so the array and
                 * the iterator should be aligned and should visit the same #of
                 * statements (since there is no truth maintenance in this test
                 * there are no entailments and a closed world assumption is
                 * Ok).
                 */
                assertSameSPOArray(store, stmts, stmts.length);

                /*
                 * Verify same statement identifiers assigned if we re-assert
                 * the statements.
                 * 
                 * Note: this creates a new SPO[] with the same {s,p,o}:type
                 * data but without the statement identifiers. The array is in
                 * the same order (SPO order) as the original SPO[]. We then
                 * insert the new SPO[] into the KB and compare the assigned
                 * statement identifiers with the ones that were assigned above.
                 */
                {

                    SPO[] a = new SPO[stmts.length];

                    for (int i = 0; i < stmts.length; i++) {

                        SPO spo = stmts[i];

                        a[i] = new SPO(spo.s, spo.p, spo.o, spo.type);

                    }

                    final int nwritten = store.addStatements(a, a.length);

                    for (int i = 0; i < stmts.length; i++) {

                        assertEquals("sid @ i=" + i, stmts[i]
                                .getStatementIdentifier(), a[i]
                                .getStatementIdentifier());

                    }

                    /*
                     * Note: no statements should have been written on the kb
                     * since they were all pre-existing. addStatements() should
                     * have just read the pre-existing statement identifiers out
                     * of the KB and assigned them to a[].
                     */
                    assertEquals("#written", 0, nwritten);

                }
            }

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * Test creates a statement (a) and then a statement (b) about statement (a)
     * using the statement identifier for (a) and then retracts (a) and verifies
     * that (b) is also retracted.
     */
    public void test_retractionOfStatementsAboutStatements() {
        
        AbstractTripleStore store = getStore();

        try {

            if(!store.statementIdentifiers) {
                
                log.warn("Statement identifiers are not enabled");

                return;
                
            }

            _URI x = new _URI("http://www.foo.org/x");
            _URI y = new _URI("http://www.foo.org/y");
            _URI z = new _URI("http://www.foo.org/z");
    
            _URI A = new _URI("http://www.foo.org/A");
            _URI B = new _URI("http://www.foo.org/B");
            _URI C = new _URI("http://www.foo.org/C");
    
            _URI rdfType = new _URI(RDF.TYPE);
//            _URI rdfsLabel = new _URI(RDFS.LABEL);
            _URI dcCreator = new _URI("http://purl.org/dc/terms/creator");
    
            _Literal lit1 = new _Literal("bryan");
            _Literal lit2 = new _Literal("mike");
    
//            _BNode bn1 = new _BNode(UUID.randomUUID().toString());
//            _BNode bn2 = new _BNode("a12");

            _Value[] terms = new _Value[] {
              
                    x,y,z,//
                    A,B,C,//
                    rdfType,//
//                    rdfsLabel,//
                    dcCreator,//
                    lit1,lit2,//
//                    bn1,bn2//
                    
            };
            
            store.addTerms(terms, terms.length);
            
            SPO[] stmts1 = new SPO[] {

                new SPO(x.termId, rdfType.termId, A.termId, StatementEnum.Explicit),
                
            };
            
            assertEquals(1,store.addStatements(stmts1, stmts1.length));

            final long sid1 = stmts1[0].getStatementIdentifier();
            
            SPO[] stmts2 = new SPO[] {
              
                    new SPO(sid1, dcCreator.termId, lit1.termId, StatementEnum.Explicit),
                    
            };

            assertEquals(1,store.addStatements(stmts2, stmts2.length));

            assertEquals(2,store.getStatementCount(true/*exact*/));

            /*
             * Verify read back.
             */
            {
            
                SPO[] all = new SPO[] {
                  
                        stmts1[0],
                        stmts2[0]
                        
                };
            
                assertSameSPOArray(store, all, all.length);
                            
            }

            /*
             * Retract the original statement and verify that the statement
             * about that statement is also retracted.
             */
            {
                
                store.removeStatements(new SPOArrayIterator(stmts1,stmts1.length));

                assertSameSPOArray(store, new SPO[]{}, 0/*numStmts*/);

            }
            
        } finally {

            store.closeAndDelete();

        }
        
    }
    
    /**
     * Test creates a statement (a), a statement (b) about statement (a), and a
     * statement (c) about statement (b) and then retracts (a) and verifies that
     * both (b) and (c) are also retracted.
     */
    public void test_retractionOfStatementsAboutStatements2() {
        
        AbstractTripleStore store = getStore();

        try {

            if(!store.statementIdentifiers) {
                
                log.warn("Statement identifiers are not enabled");

                return;
                
            }

            _URI x = new _URI("http://www.foo.org/x");
            _URI y = new _URI("http://www.foo.org/y");
            _URI z = new _URI("http://www.foo.org/z");
    
            _URI A = new _URI("http://www.foo.org/A");
            _URI B = new _URI("http://www.foo.org/B");
            _URI C = new _URI("http://www.foo.org/C");
    
            _URI rdfType = new _URI(RDF.TYPE);
            _URI dcCreator = new _URI("http://purl.org/dc/terms/creator");
    
            _Literal lit1 = new _Literal("bryan");
            _Literal lit2 = new _Literal("mike");
    
            _Value[] terms = new _Value[] {
              
                    x,y,z,//
                    A,B,C,//
                    rdfType,//
                    dcCreator,//
                    lit1,lit2,//
                    
            };
            
            store.addTerms(terms, terms.length);
            
            /*
             * create the original statement.
             */
            SPO[] stmts1 = new SPO[] {

                new SPO(x.termId, rdfType.termId, A.termId, StatementEnum.Explicit),
                
            };
            
            // insert into the database.
            assertEquals(1,store.addStatements(stmts1, stmts1.length));
            
            // the statement identifier for the original stmt. 
            final long sid1 = stmts1[0].getStatementIdentifier();
            
            /*
             * create a statement about that statement.
             */
            
            SPO[] stmts2 = new SPO[] {
                    
                    new SPO(sid1, dcCreator.termId, lit1.termId, StatementEnum.Explicit),
                    
            };

            // insert the metadata statement into the database.
            assertEquals(1,store.addStatements(stmts2, stmts2.length));

            assertEquals(2,store.getStatementCount(true/*exact*/));

            // the stmt identifier for the statement about the original stmt.
            final long sid2 = stmts2[0].getStatementIdentifier();

            /*
             * create a statement about the statement about the original statement.
             */

            SPO[] stmts3 = new SPO[] {
                    
                    new SPO(sid2, dcCreator.termId, lit2.termId, StatementEnum.Explicit),
                    
            };

            // insert the metadata statement into the database.
            assertEquals(1,store.addStatements(stmts3, stmts3.length));

            assertEquals(3,store.getStatementCount(true/*exact*/));

            /*
             * Verify read back.
             */
            {
            
                SPO[] all = new SPO[] {
                  
                        stmts1[0],
                        stmts2[0],
                        stmts3[0],
                        
                };
                
                assertSameSPOArray(store, all, all.length);
                            
            }

            /*
             * Retract the original statement and verify that the statement
             * about the original statement and the statement about the
             * statement about the original statement are also retracted.
             */
            {
                
                store.removeStatements(new SPOArrayIterator(stmts1,stmts1.length));

                assertSameSPOArray(store, new SPO[]{}, 0/*numStmts*/);

            }
            
        } finally {

            store.closeAndDelete();

        }
        
    }
    
    /**
     * Verify read back from the KB. Note that the SPO[] MUST be in SPO order
     * and we are reading in SPO order so the array and the iterator should be
     * aligned and should visit the same #of statements (since there is no truth
     * maintenance in this test there are no entailments and a closed world
     * assumption is Ok).
     * 
     * @param store
     * @param all
     *            The expected statemetns (sorted into SPO order as a
     *            side-effect).
     * @param numStmts
     *            The #of statements <i>all</i>.
     */
    private void assertSameSPOArray(AbstractTripleStore store, SPO[] all, int numStmts) {
        
        Arrays.sort(all, 0, all.length, SPOComparator.INSTANCE);

        ISPOIterator itr = store.getAccessPath(KeyOrder.SPO).iterator();
        
        for(int i=0; i<all.length; i++) {
            
            assertTrue("i=" + i, itr.hasNext());

            final SPO expected = all[i];

            final SPO actual = itr.next();

            assertEquals("S @ i=" + i, expected.s, actual.s);

            assertEquals("P @ i=" + i, expected.p, actual.p);

            assertEquals("O @ i=" + i, expected.o, actual.o);

            assertEquals("TYPE @ i=" + i, expected.type,
                    actual.type);

            assertEquals("SID @ i=" + i, expected
                    .getStatementIdentifier(), actual
                    .getStatementIdentifier());                    
            
        }
        
        assertFalse("iterator is willing to visit more than " + numStmts
                + " statements", itr.hasNext());

    }
    
    /**
     * Test case for the interchange of the statement identifiers using a
     * private extension for the interchange of RDF/XML.
     * 
     * @throws SailException
     * @throws RDFHandlerException
     * 
     * @throws IOException 
     */
    public void test_rdfXmlInterchange() throws SailException, RDFHandlerException, IOException {

        AbstractTripleStore store = getStore();

        try {

            if (!store.statementIdentifiers) {

                log.warn("Statement identifiers are not enabled");

                return;

            }

            _URI x = new _URI("http://www.foo.org/x");
            _URI y = new _URI("http://www.foo.org/y");
            _URI z = new _URI("http://www.foo.org/z");

            _URI A = new _URI("http://www.foo.org/A");
            _URI B = new _URI("http://www.foo.org/B");
            _URI C = new _URI("http://www.foo.org/C");

            _URI rdfType = new _URI(RDF.TYPE);
            _URI rdfsLabel = new _URI(RDFS.LABEL);
            _URI rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);

            _Literal lit1 = new _Literal("abc");
            _Literal lit2 = new _Literal("abc", A);
            _Literal lit3 = new _Literal("abc", "en");

            _BNode bn1 = new _BNode(UUID.randomUUID().toString());
            _BNode bn2 = new _BNode("a12");

            _Value[] terms = new _Value[] {

            x, y, z,//
                    A, B, C,//
                    rdfType,//
                    rdfsLabel,//
                    rdfsSubClassOf,//
                    lit1, lit2, lit3,//
                    bn1, bn2 //

            };

            store.addTerms(terms, terms.length);

            SPO[] stmts = new SPO[] {

                    new SPO(x.termId, rdfType.termId, C.termId,
                            StatementEnum.Explicit),
                    new SPO(y.termId, rdfType.termId, B.termId,
                            StatementEnum.Explicit),
                    new SPO(z.termId, rdfType.termId, A.termId,
                            StatementEnum.Explicit),

                    new SPO(A.termId, rdfsLabel.termId, lit1.termId,
                            StatementEnum.Explicit),
                    new SPO(B.termId, rdfsLabel.termId, lit2.termId,
                            StatementEnum.Explicit),
                    new SPO(C.termId, rdfsLabel.termId, lit3.termId,
                            StatementEnum.Explicit),

                    new SPO(B.termId, rdfsSubClassOf.termId, A.termId,
                            StatementEnum.Explicit),
                    new SPO(C.termId, rdfsSubClassOf.termId, B.termId,
                            StatementEnum.Explicit),

                    new SPO(bn1.termId, rdfsLabel.termId, lit1.termId,
                            StatementEnum.Explicit),
                    new SPO(bn2.termId, rdfsLabel.termId, lit2.termId,
                            StatementEnum.Explicit),

            };

            /*
             * Add the statements to the KB.
             * 
             * Note: sorts into SPO order as a side-effect.
             */
            store.addStatements(stmts, stmts.length);

            final StatementIterator itr = store.getStatements(null, null, null);
            final String rdfXml;
            try {

                Writer w = new StringWriter();

                RDFXMLWriter rdfWriter = new RDFXMLWriter(w);

                rdfWriter.startRDF();

                while (itr.hasNext()) {

                    Statement stmt = itr.next();

                    rdfWriter.handleStatement(stmt);

                }

                rdfWriter.endRDF();

                rdfXml = w.toString();
                
            } finally {

                try {
                    itr.close();
                } catch (SailException e) {
                    throw new RuntimeException(e);
                }

            }

            // write the rdf/xml on the console.
            System.err.println(rdfXml);

            store.getDataLoader().loadData(new StringReader(rdfXml),
                    ""/* baseURL */, RDFFormat.RDFXML);

            /*
             * FIXME verify read-back of the graph with statement-level
             * provenance metadata.
             * 
             * FIXME There are a lot of ways in which reportStatement/3 gets
             * called in RDFXMLParser. I need to survey them all and decide
             * whether or not "context" (the variable set based on
             * bigdata:graph) will be correct ("" or the sid) or in there are
             * some uses, such as reification, where "context" should be
             * ignored.
             * 
             * FIXME BNodes must be unified with statements identifiers when
             * they appear in the context position and statement identifiers are
             * enabled. This should happen in addStatements if the statement has
             * context and statement identifiers are enabled. Also, if a BNode
             * appears in the context position then it must be the same instance
             * as all other instances of that bnode (this is handled by rio) and
             * it SHOULD NOT be assigned a BNode termId since we are going to
             * assign/resolve the statement identifier instead. This probably
             * means sorting all statements with statement identifiers in
             * dependency order (and using BNodes you could create cycles, which
             * would have to be illegal).
             * 
             * FIXME detect cycles in statement identifiers -- they are not
             * legal. you can not make a statement about itself and you can not
             * make a statement whose transitive closure via the statement
             * identifier describes itself. The statement identifier is not
             * knowable until the triple is specified.
             * 
             * FIXME When parsing RDF/XML, we must defer the insert of
             * statements with blank nodes in their subject or object position
             * until the end of the input stream since we can not tell whether
             * the blank node is in fact a statement identifier.  This issue can
             * only arise during the conversion of an RDF interchange syntax into
             * SPOs objects. Therefore it is addressed by the StatementBuffer and
             * NOT the AbstractTripleStore.
             * 
             * FIXME When parsing, _BNode references need to be preserved in a
             * cache until the end of the document in order to guarentee that
             * all BNodes with a given ID within a document are mapped onto the
             * same termId in the database. This means that we need something
             * like a scoped optimized value factory object. (The same
             * constraint probably obtains for any RDF Value to SPO conversion
             * context, which is always handled by a StatementBuffer.)
             */
            fail("Verify correct read of the same graph");
            
        } finally {

            store.closeAndDelete();

        }

    }

}
