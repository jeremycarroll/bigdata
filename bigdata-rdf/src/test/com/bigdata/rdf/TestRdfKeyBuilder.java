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
 * Created on Jan 18, 2007
 */

package com.bigdata.rdf;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.objndx.BytesUtil;
import com.bigdata.objndx.KeyBuilder;

/**
 * Test suite for construction of variable length unsigned byte[] keys from RDF
 * {@link Value}s and statements.
 * 
 * @todo write test for sort key generated for each basic value type.
 * @todo write test for sort key generated for each well-known datatype uri.
 * @todo write test that sort keys for various value types are assigned to
 *       non-overlapping regions of the key space.
 * @todo write test for generate of statement keys.
 * @todo write code to generate permutations of statement keys and tests of that
 *       code.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRdfKeyBuilder extends TestCase2 {

    KeyBuilder keyBuilder = new KeyBuilder();
    RdfKeyBuilder fixture = new RdfKeyBuilder(keyBuilder);
    
    /**
     * 
     */
    public TestRdfKeyBuilder() {
    }

    /**
     * @param name
     */
    public TestRdfKeyBuilder(String name) {
        super(name);
    }

    public void test_uri() {
        
        String uri1 = "http://www.cognitiveweb.org";
        String uri2 = "http://www.cognitiveweb.org/a";
        String uri3 = "http://www.cognitiveweb.com/a";
        
        byte[] k1 = fixture.uri2key(uri1);
        byte[] k2 = fixture.uri2key(uri2);
        byte[] k3 = fixture.uri2key(uri3);

        System.err.println("k1("+uri1+") = "+BytesUtil.toString(k1));
        System.err.println("k2("+uri2+") = "+BytesUtil.toString(k2));
        System.err.println("k3("+uri3+") = "+BytesUtil.toString(k3));
        
        // subdirectory sorts after root directory.
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        
        // .com extension sorts before .org
        assertTrue(BytesUtil.compareBytes(k2, k3)>0);
        
    }
    
    public void test_plainLiteral() {

        String lit1 = "abc";
        String lit2 = "abcd";
        String lit3 = "abcde";
        
        byte[] k1 = fixture.plainLiteral2key(lit1);
        byte[] k2 = fixture.plainLiteral2key(lit2);
        byte[] k3 = fixture.plainLiteral2key(lit3);

        System.err.println("k1("+lit1+") = "+BytesUtil.toString(k1));
        System.err.println("k2("+lit2+") = "+BytesUtil.toString(k2));
        System.err.println("k3("+lit3+") = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_languageCodeLiteral() {
        
        String en = "en";
        String de = "de";
        
        String lit1 = "abc";
        String lit2 = "abc";
        String lit3 = "abce";
        
        byte[] k1 = fixture.languageCodeLiteral2key(en, lit1);
        byte[] k2 = fixture.languageCodeLiteral2key(de, lit2);
        byte[] k3 = fixture.languageCodeLiteral2key(de, lit3);

        System.err.println("k1(en:"+lit1+") = "+BytesUtil.toString(k1));
        System.err.println("k2(de:"+lit2+") = "+BytesUtil.toString(k2));
        System.err.println("k3(de:"+lit3+") = "+BytesUtil.toString(k3));
        
        // "en" sorts after "de".
        assertTrue(BytesUtil.compareBytes(k1, k2)>0);

        // en:abc != de:abc
        assertTrue(BytesUtil.compareBytes(k1, k2) != 0);
        
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_datatypeLiteral_xsd_int() {
        
        String datatype = XmlSchema.INTEGER;
        
        // Note: leading zeros are ignored in the xsd:int value space.
        String lit1 = "-4";
        String lit2 = "005";
        String lit3 = "6";
        
        byte[] k1 = fixture.datatypeLiteral2key(datatype,lit1);
        byte[] k2 = fixture.datatypeLiteral2key(datatype,lit2);
        byte[] k3 = fixture.datatypeLiteral2key(datatype,lit3);

        System.err.println("k1(int:"+lit1+") = "+BytesUtil.toString(k1));
        System.err.println("k2(int:"+lit2+") = "+BytesUtil.toString(k2));
        System.err.println("k3(int:"+lit3+") = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_blankNode() {
        
        String id1 = "_12";
        String id2 = "_abc";
        String id3 = "abc";
        
        byte[] k1 = fixture.blankNode2Key(id1);
        byte[] k2 = fixture.blankNode2Key(id2);
        byte[] k3 = fixture.blankNode2Key(id3);

        System.err.println("k1(bnodeId:"+id1+") = "+BytesUtil.toString(k1));
        System.err.println("k2(bnodeId:"+id2+") = "+BytesUtil.toString(k2));
        System.err.println("k3(bnodeId:"+id3+") = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_id2key() {
        
        long id1 = -1;
        long id2 = 0;
        long id3 = 1;
        
        byte[] k1 = fixture.id2key(id1);
        byte[] k2 = fixture.id2key(id2);
        byte[] k3 = fixture.id2key(id3);
        
        System.err.println("k1(termId:"+id1+") = "+BytesUtil.toString(k1));
        System.err.println("k2(termId:"+id2+") = "+BytesUtil.toString(k2));
        System.err.println("k3(termId:"+id3+") = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_statement() {
        
        byte[] k1 = fixture.statement2Key(1, 2, 3);
        byte[] k2 = fixture.statement2Key(2, 2, 3);
        byte[] k3 = fixture.statement2Key(2, 2, 4);
        
        System.err.println("k1(1,2,2) = "+BytesUtil.toString(k1));
        System.err.println("k2(2,2,3) = "+BytesUtil.toString(k2));
        System.err.println("k3(2,2,4) = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);

    }
    
}
