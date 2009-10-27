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
 * Created on Sep 18, 2009
 */

package com.bigdata.rdf.spo;

import junit.framework.TestCase2;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.lexicon.ITermIdCodes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Test suite for the {@link SPO} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPO extends TestCase2 {

    /**
     * 
     */
    public TestSPO() {
    }

    /**
     * @param name
     */
    public TestSPO(String name) {
        super(name);
    }

    /**
     * Unit test for round-trip of the encoded value of an {@link ISPO},
     * including all states of override flag, all possible kinds of term
     * identifiers bound in the context position (NULL, uri, literal, bnode, or
     * statement), and all possible {@link StatementEnum} that are actually
     * stored in the database.
     */
    public void test_valueEncodingRoundTrip() {

        assertFalse(AbstractTripleStore.isStatement(IRawTripleStore.NULL));
        doValueRoundTripTest(IRawTripleStore.NULL);

        // Note: isURI() reports false for 0L so we must set a bit above the
        // mask to a non-zero value.
        assertTrue(AbstractTripleStore.isURI(1 << 2
                | ITermIdCodes.TERMID_CODE_URI));
        assertFalse(AbstractTripleStore
                .isStatement(ITermIdCodes.TERMID_CODE_URI));
        doValueRoundTripTest(ITermIdCodes.TERMID_CODE_URI);

        assertTrue(AbstractTripleStore
                .isLiteral(ITermIdCodes.TERMID_CODE_LITERAL));
        assertFalse(AbstractTripleStore
                .isStatement(ITermIdCodes.TERMID_CODE_LITERAL));
        doValueRoundTripTest(ITermIdCodes.TERMID_CODE_LITERAL);

        assertTrue(AbstractTripleStore.isBNode(ITermIdCodes.TERMID_CODE_BNODE));
        assertFalse(AbstractTripleStore
                .isStatement(ITermIdCodes.TERMID_CODE_BNODE));
        doValueRoundTripTest(ITermIdCodes.TERMID_CODE_BNODE);

        assertTrue(AbstractTripleStore
                .isStatement(ITermIdCodes.TERMID_CODE_STATEMENT));
        doValueRoundTripTest(ITermIdCodes.TERMID_CODE_STATEMENT);

    }

    /**
     * Test (de-)serialization of the encoded SPO byte[] value with and without
     * the override flag using {@link SPO#serializeValue(ByteArrayBuffer)} and
     * {@link SPO#decodeValue(ISPO, byte[])}.
     */
    protected void doValueRoundTripTest(final long c) {
        
        // test w/o override flag.
        boolean override = false;
        for (StatementEnum type : StatementEnum.values()) {
            
            if (type == StatementEnum.Backchained) {

                // skip this - it is not a real statement type.
                continue;
                
            }

            final byte[] val = SPO.serializeValue(new ByteArrayBuffer(),
                    override, type, c);

            final byte b = val[0];

            assertEquals(type, StatementEnum.decode(b));
            
            assertEquals(override, StatementEnum.isOverride(b));

            if (type == StatementEnum.Explicit
                    && AbstractTripleStore.isStatement(c)) {
                // Should have (en|de)coded [c] as as statement identifier.
                assertEquals(9, val.length);
                assertEquals(c, SPO.decodeValue(new SPO(0, 0, 0), val).c());
            } else {
                // Should not have (en|de)coded a statement identifier.
                assertEquals(1, val.length);
                assertEquals(IRawTripleStore.NULL, SPO.decodeValue(
                        new SPO(0, 0, 0), val).c());
            }

        }

        // test w/ override flag.
        override = true;
        for (StatementEnum type : StatementEnum.values()) {
            
            if (type == StatementEnum.Backchained) {

                // skip this - it is not a real statement type.
                continue;
                
            }

            final byte[] val = SPO.serializeValue(new ByteArrayBuffer(),
                    override, type, c);

            final byte b = val[0];

            assertEquals(type, StatementEnum.decode(b));

            assertEquals(override, StatementEnum.isOverride(b));
         
            if (type == StatementEnum.Explicit
                    && AbstractTripleStore.isStatement(c)) {
                // Should have (en|de)coded [c] as as statement identifier.
                assertEquals(9, val.length);
                assertEquals(c, SPO.decodeValue(new SPO(0, 0, 0), val).c());
            } else {
                // Should not have (en|de)coded a statement identifier.
                assertEquals(1, val.length);
                assertEquals(IRawTripleStore.NULL, SPO.decodeValue(
                        new SPO(0, 0, 0), val).c());
            }

        }

    }

}
