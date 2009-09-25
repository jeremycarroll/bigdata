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
 * Created on Jun 21, 2008
 */

package com.bigdata.rdf.spo;

import java.util.Iterator;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for {@link SPOKeyOrder}. 
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOKeyOrder extends TestCase2 {

    /**
     * 
     */
    public TestSPOKeyOrder() {
    }

    /**
     * @param name
     */
    public TestSPOKeyOrder(String name) {
        super(name);
    }

    /**
     * Writes the serialized size of an instance on stdout. It is an amazing 61
     * bytes. You can serialize just the {@link SPOKeyOrder#index()} byte value
     * for an amazing 60 byte savings per instance.
     */
    public void test_serializationSize() {

        System.out.println("serializedSize="
                + SerializerUtil.serialize(SPOKeyOrder.SPO).length + " bytes");

    }

    /**
     * Unit tests verifies all of the triple key encoding orders. It encodes a
     * known key in each order in turn and verifies by decoding using the SPO
     * key order and checking against the expected permutation of the (s,p,o)
     * values.
     */
    public void test_keyOrder_triples() {

        final KeyBuilder keyBuilder = new KeyBuilder(4 * Bytes.SIZEOF_LONG);
        
        final long S = 1, P = 2, O = 3, C = 4;
        
        final SPO expected = new SPO(S, P, O, C);

        assertSPOCEquals(new SPO(S, P, O), SPOKeyOrder.SPO
                .decodeKey(SPOKeyOrder.SPO.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(P, O, S), SPOKeyOrder.SPO
                .decodeKey(SPOKeyOrder.POS.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(O, S, P), SPOKeyOrder.SPO
                .decodeKey(SPOKeyOrder.OSP.encodeKey(keyBuilder, expected)));

    }

    /**
     * Unit tests verifies all of the quad key encoding orders. It encodes a
     * known key in each order in turn and verifies by decoding using the SPOC
     * key order and checking against the expected permutation of the (s,p,o,c)
     * values.
     */
    public void test_keyOrder_quads() {

        final KeyBuilder keyBuilder = new KeyBuilder(4 * Bytes.SIZEOF_LONG);

        final long S = 1, P = 2, O = 3, C = 4;

        final SPO expected = new SPO(S, P, O, C);

        assertSPOCEquals(new SPO(S, P, O, C), SPOKeyOrder.SPOC
                .decodeKey(SPOKeyOrder.SPOC.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(P, O, C, S), SPOKeyOrder.SPOC
                .decodeKey(SPOKeyOrder.POCS.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(O, C, S, P), SPOKeyOrder.SPOC
                .decodeKey(SPOKeyOrder.OCSP.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(C, S, P, O), SPOKeyOrder.SPOC
                .decodeKey(SPOKeyOrder.CSPO.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(P, C, S, O), SPOKeyOrder.SPOC
                .decodeKey(SPOKeyOrder.PCSO.encodeKey(keyBuilder, expected)));

        assertSPOCEquals(new SPO(S, O, P, C), SPOKeyOrder.SPOC
                .decodeKey(SPOKeyOrder.SOPC.encodeKey(keyBuilder, expected)));

    }

    protected void assertSPOCEquals(final SPO e, final SPO a) {

        assertEquals("s", e.s(), a.s());
        
        assertEquals("p", e.p(), a.p());
        
        assertEquals("o", e.o(), a.o());
        
        assertEquals("c", e.c(), a.c());
        
    }
    
    /**
     * Verify that
     * {@link SPOKeyOrder#encodeKey(com.bigdata.btree.keys.IKeyBuilder, ISPO)}
     * and {@link SPOKeyOrder#decodeKey(byte[])} round trip correctly.
     */
    public void test_encodeDecode() {

        final KeyBuilder keyBuilder = new KeyBuilder(4 * Bytes.SIZEOF_LONG);

        for (int i = SPOKeyOrder.FIRST_QUAD_INDEX; i <= SPOKeyOrder.LAST_QUAD_INDEX; i++) {

            final SPOKeyOrder keyOrder = SPOKeyOrder.valueOf(i);

            final SPO expected = new SPO(1, 2, 3, 4);

            final byte[] key = keyOrder.encodeKey(keyBuilder, expected);

            final SPO actual = keyOrder.decodeKey(key);

            assertSPOCEquals(expected, actual);

        }

    }

    public void test_spoOnlyKeyOrder_iterator() {

        assertSameIteratorAnyOrder(new SPOKeyOrder[] { SPOKeyOrder.SPO },
                SPOKeyOrder.spoOnlyKeyOrderIterator());
        
    }
    
    public void test_spocOnlyKeyOrder_iterator() {

        assertSameIteratorAnyOrder(new SPOKeyOrder[] { SPOKeyOrder.SPOC },
                SPOKeyOrder.spocOnlyKeyOrderIterator());
        
    }
    
    public void test_tripleStoreKeyOrders_iterator() {

        assertSameIteratorAnyOrder(new SPOKeyOrder[] { SPOKeyOrder.SPO,
                SPOKeyOrder.POS, SPOKeyOrder.OSP }, SPOKeyOrder
                .tripleStoreKeyOrderIterator());

    }

    public void test_quadStoreKeyOrders_iterator() {

        assertSameIteratorAnyOrder(new SPOKeyOrder[] { SPOKeyOrder.SPOC,
                SPOKeyOrder.POCS, SPOKeyOrder.OCSP, SPOKeyOrder.CSPO,
                SPOKeyOrder.PCSO, SPOKeyOrder.SOPC, }, SPOKeyOrder
                .quadStoreKeyOrderIterator());

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIteratorAnyOrder(final Object[] expected,
            final Iterator actual) {

        assertSameIteratorAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIteratorAnyOrder(final String msg,
            final Object[] expected, final Iterator actual) {

        try {
        
        // Populate a map that we will use to realize the match and
        // selection without replacement logic.

        final int nrange = expected.length;

        java.util.Map range = new java.util.HashMap();

        for (int j = 0; j < nrange; j++) {

            range.put(expected[j], expected[j]);

        }

        // Do selection without replacement for the objects visited by
        // iterator.

        for (int j = 0; j < nrange; j++) {

            if (!actual.hasNext()) {

                fail(msg + ": Index exhausted while expecting more object(s)"
                        + ": index=" + j);

            }

            final Object actualObject = actual.next();

            if (range.remove(actualObject) == null) {

                fail("Object not expected" + ": index=" + j + ", object="
                        + actualObject);

            }

        }

        if (actual.hasNext()) {

            fail("Iterator will deliver too many objects.");

        }
        
        } finally {
            
            if(actual instanceof ICloseableIterator) {
                
                ((ICloseableIterator)actual).close();
                
            }
            
        }

    }

}
