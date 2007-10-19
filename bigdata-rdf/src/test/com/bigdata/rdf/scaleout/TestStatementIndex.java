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
 * Created on May 7, 2007
 */

package com.bigdata.rdf.scaleout;

import java.util.UUID;

import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TestTripleStore;

/**
 * 
 * @todo write tests for all methods that work with the statement indices.
 * 
 * @todo work through support for the triple store statement indices.
 * 
 * @todo tune performance of triple store with client-server divide.
 * 
 * @todo refactor to support a quad store (on a branch?).
 * 
 * @todo this duplicates tests found in {@link TestTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStatementIndex extends AbstractDistributedTripleStoreTestCase {
    
    /**
     * 
     */
    public TestStatementIndex() {
    }

    /**
     * @param arg0
     */
    public TestStatementIndex(String arg0) {
        super(arg0);
    }

    /**
     * Test of
     * {@link ITripleStore#addStatement(org.openrdf.model.Resource, org.openrdf.model.URI, org.openrdf.model.Value)}
     * and
     * {@link ITripleStore#containsStatement(org.openrdf.model.Resource, org.openrdf.model.URI, org.openrdf.model.Value)}.
     */
    public void test_statements() {
        _URI x = new _URI("http://www.foo.org/x");
        _URI y = new _URI("http://www.foo.org/y");
        _URI z = new _URI("http://www.foo.org/z");

        _URI A = new _URI("http://www.foo.org/A");
        _URI B = new _URI("http://www.foo.org/B");
        _URI C = new _URI("http://www.foo.org/C");

        _URI rdfType = new _URI(RDF.TYPE);

        _URI rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);

        _Literal lit1 = new _Literal("abc");
        _Literal lit2 = new _Literal("abc",A);
        _Literal lit3 = new _Literal("abc","en");

        _BNode bn1 = new _BNode(UUID.randomUUID().toString());
        _BNode bn2 = new _BNode("a12");
        
        store.addStatement(x, rdfType, C);
        store.addStatement(y, rdfType, B);
        store.addStatement(z, rdfType, A);

        store.addStatement(B, rdfsSubClassOf, A);
        store.addStatement(C, rdfsSubClassOf, B);

        final long x_id = store.getTermId(x); assertTrue(x_id!=NULL);
        final long y_id = store.getTermId(y); assertTrue(y_id!=NULL);
        final long z_id = store.getTermId(z); assertTrue(z_id!=NULL);
        final long A_id = store.getTermId(A); assertTrue(A_id!=NULL);
        final long B_id = store.getTermId(B); assertTrue(B_id!=NULL);
        final long C_id = store.getTermId(C); assertTrue(C_id!=NULL);
        final long rdfType_id = store.getTermId(rdfType); assertTrue(rdfType_id!=NULL);
        final long rdfsSubClassOf_id = store.getTermId(rdfsSubClassOf); assertTrue(rdfsSubClassOf_id!=NULL);
        
        final long lit1_id = store.addTerm(lit1); assertTrue(lit1_id!=NULL);
        final long lit2_id = store.addTerm(lit2); assertTrue(lit2_id!=NULL);
        final long lit3_id = store.addTerm(lit3); assertTrue(lit3_id!=NULL);
        
        final long bn1_id = store.addTerm(bn1); assertTrue(bn1_id!=NULL);
        final long bn2_id = store.addTerm(bn2); assertTrue(bn2_id!=NULL);

        assertEquals("#terms",8+3+2,store.getTermCount());
        assertEquals("#uris",8,store.getURICount());
        assertEquals("#lits",3,store.getLiteralCount());
        assertEquals("#bnodes",2,store.getBNodeCount());

        assertEquals(x_id,store.getTermId(x));
        assertEquals(y_id,store.getTermId(y));
        assertEquals(z_id,store.getTermId(z));
        assertEquals(A_id,store.getTermId(A));
        assertEquals(B_id,store.getTermId(B));
        assertEquals(C_id,store.getTermId(C));
        assertEquals(rdfType_id,store.getTermId(rdfType));
        assertEquals(rdfsSubClassOf_id,store.getTermId(rdfsSubClassOf));
        
        assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,null));
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));

        store.commit();
        
        assertEquals("statementCount", 5, store.getSPOIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getPOSIndex().rangeCount(null,null));
        assertEquals("statementCount", 5, store.getOSPIndex().rangeCount(null,null));
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));
        
        assertEquals("#terms",8+3+2,store.getTermCount());
        assertEquals("#uris",8,store.getURICount());
        assertEquals("#lits",3,store.getLiteralCount());
        assertEquals("#bnodes",2,store.getBNodeCount());

    }

}
