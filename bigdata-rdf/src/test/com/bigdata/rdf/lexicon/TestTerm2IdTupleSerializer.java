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
 * Created on Jan 18, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Locale;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DefaultKeyBuilderFactory;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer.LexiconKeyBuilder;

/**
 * Test suite for construction of variable length unsigned byte[] keys from RDF
 * {@link Value}s and statements.
 * 
 * @todo write test for sort key generated for each basic value type.
 * @todo write test for sort key generated for each well-known datatype uri.
 * @todo write test that sort keys for various value types are assigned to
 *       non-overlapping regions of the key space.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTerm2IdTupleSerializer extends TestCase2 {

    /**
     * Note: The key builder will wind up configured with the default
     * {@link Locale} unless that gets overriden by {@link #getProperties()}
     */
    final Term2IdTupleSerializer tupleSer = new Term2IdTupleSerializer(
            new DefaultKeyBuilderFactory(getProperties()));
    
    final LexiconKeyBuilder fixture = tupleSer.getLexiconKeyBuilder();
    
    /**
     * 
     */
    public TestTerm2IdTupleSerializer() {
    }

    /**
     * @param name
     */
    public TestTerm2IdTupleSerializer(String name) {
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
        
        String datatype = XMLSchema.INTEGER.toString();
        
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

    /**
     * Test verifies the ordering among URIs, Literals, and BNodes. This
     * ordering is important when batching terms of these different types into
     * the term index since you want to insert the type types according to this
     * order for the best performance.
     */
    public void test_termTypeOrder() {

        /*
         * one key of each type. the specific values for the types do not matter
         * since we are only interested in the relative order between those
         * types in this test.
         */
        
        byte[] k1 = fixture.uri2key("http://www.cognitiveweb.org");
        byte[] k2 = fixture.plainLiteral2key("hello world!");
        byte[] k3 = fixture.blankNode2Key("a12");
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
}
