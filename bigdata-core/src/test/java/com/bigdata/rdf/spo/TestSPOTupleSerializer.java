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
 * Created on Jul 8, 2008
 */

package com.bigdata.rdf.spo;

import com.bigdata.btree.AbstractTuple;
import com.bigdata.io.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.test.Assert;
import org.junit.Test;

/**
 * Test suite for {@link SPOTupleSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see TestSPO#test_valueEncodingRoundTrip()
 */
public class TestSPOTupleSerializer extends Assert {

    private TermId _1 = tid(1), _2 = tid(2), _3 = tid(3), _4 = tid(4);
    
    private TermId tid(long tid) {
        return new TermId(VTE.URI, tid);
    }
    
    /**
     * 
     */
    public TestSPOTupleSerializer() {
    }

    @Test
    public void test_statementOrder() {

        SPOTupleSerializer fixture = new SPOTupleSerializer(SPOKeyOrder.SPO);
        
        byte[] k_1 = fixture.serializeKey(new SPO(_1, _2, _3));
        byte[] k_2 = fixture.serializeKey(new SPO(_2, _2, _3));
        byte[] k_3 = fixture.serializeKey(new SPO(_2, _2, _4));
        
        System.err.println("k_1(_1,_2,_2) = "+BytesUtil.toString(k_1));
        System.err.println("k_2(_2,_2,_3) = "+BytesUtil.toString(k_2));
        System.err.println("k_3(_2,_2,_4) = "+BytesUtil.toString(k_3));
        
        assertTrue(BytesUtil.compareBytes(k_1, k_2)<0);
        assertTrue(BytesUtil.compareBytes(k_2, k_3)<0);

    }

    @Test
    public void test_encodeDecodeTriple() {

        doEncodeDecodeTest(new SPO(_1, _2, _3, StatementEnum.Axiom),
                SPOKeyOrder.SPO);

        doEncodeDecodeTest(new SPO(_1, _2, _3, StatementEnum.Explicit),
                SPOKeyOrder.POS);

        doEncodeDecodeTest(new SPO(_1, _2, _3, StatementEnum.Inferred),
                SPOKeyOrder.OSP);

    }
    
    @Test
    public void test_encodeDecodeTripleWithSID() {

        /*
         * Note: [sid] is a legal statement identifier.
         * 
         * Note: Only explicit statements may have statement identifiers.
         */
        final TermId sid = new TermId(VTE.STATEMENT, 1);

        {

            final SPO spo = new SPO(_3, _1, _2, StatementEnum.Explicit);

            spo.setStatementIdentifier(sid);

            doEncodeDecodeTest(spo, SPOKeyOrder.SPO);

        }

        {

            final SPO spo = new SPO(_3, _1, _2, StatementEnum.Explicit);

            spo.setStatementIdentifier(sid);

            doEncodeDecodeTest(spo, SPOKeyOrder.POS);

        }

        {

            final SPO spo = new SPO(_3, _1, _2, StatementEnum.Explicit);

            spo.setStatementIdentifier(sid);
            
            doEncodeDecodeTest(spo, SPOKeyOrder.OSP);
            
        }

    }

    @Test
    public void test_encodeDecodeQuad() {

        for (int i = SPOKeyOrder.FIRST_QUAD_INDEX; i <= SPOKeyOrder.LAST_QUAD_INDEX; i++) {
         
            final SPOKeyOrder keyOrder = SPOKeyOrder.valueOf(i);
            
            doEncodeDecodeTest(new SPO(_1, _2, _3, _4, StatementEnum.Axiom),
                    keyOrder);
            
            doEncodeDecodeTest(new SPO(_1, _2, _3, _4, StatementEnum.Inferred),
                    keyOrder);
            
            doEncodeDecodeTest(new SPO(_1, _2, _3, _4, StatementEnum.Explicit),
                    keyOrder);
            
        }

    }

    protected void doEncodeDecodeTest(final SPO expected, SPOKeyOrder keyOrder) {
        
        final SPOTupleSerializer fixture = new SPOTupleSerializer(
                keyOrder);

        // encode key
        final byte[] key = fixture.serializeKey(expected);

        // encode value.
        final byte[] val = fixture.serializeVal(expected);

        /*
         * verify decoding.
         */
        final TestTuple<SPO> tuple = new TestTuple<SPO>(IRangeQuery.KEYS
                | IRangeQuery.VALS) {

            public ITupleSerializer getTupleSerializer() {
                return fixture;
            }

        };

        // copy data into the test tuple.
        tuple.copyTuple(key, val);

        final SPO actual = tuple.getObject();

        if (!expected.equals(actual)) {
            
            fail("Expected: "+expected+", but actual="+actual);
            
        }
        
        // Note: equals() does not test the context position.
        assertEquals("c", expected.c(), actual.c()); 
        
        if (expected.hasStatementType()) {

            assertEquals("type", expected.getStatementType(), actual
                    .getStatementType());

        }

        assertEquals(expected.hasStatementIdentifier(), actual
                .hasStatementIdentifier());

        if(expected.hasStatementIdentifier()) {
            
            assertEquals("statementIdentifier", expected
                    .getStatementIdentifier(), actual.getStatementIdentifier());
            
        }

    }

    abstract private static class TestTuple<E> extends AbstractTuple<E> {

        public TestTuple(int flags) {
            
            super(flags);
            
        }

        public int getSourceIndex() {
            throw new UnsupportedOperationException();
        }

        // exposed for unit test.
        public void copyTuple(byte[] key, byte[] val) {
            
            super.copyTuple(key, val);
            
        }
    };
    
}
