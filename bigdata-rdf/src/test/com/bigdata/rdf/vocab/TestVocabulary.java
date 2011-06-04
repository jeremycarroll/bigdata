/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 4, 2011
 */

package com.bigdata.rdf.vocab;

import java.util.Iterator;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Test suite for {@link BaseVocabulary}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVocabulary extends TestCase2 {

    /**
     * 
     */
    public TestVocabulary() {
    }

    /**
     * @param name
     */
    public TestVocabulary(String name) {
        super(name);
    }

    public void test_BasicVocabulary() {
        
        final RDFSVocabulary vocab = new RDFSVocabulary(getName());

        vocab.init();
        
        doRoundTripTest(vocab);

        // point tests for known value.
        assertNotNull(vocab.get(RDF.TYPE));
        assertEquals(RDF.TYPE, vocab.asValue(vocab.get(RDF.TYPE)));

        // point test for an unknown value.
        assertNull(vocab.get(new URIImpl("http://www.bigdata.com/unknown-uri")));

        /*
         * Verify self-consistent for all declared values.
         */
        {

            int nfound = 0;
            
            final Iterator<BigdataValue> itr = vocab.values();
            
            while(itr.hasNext()) {

                final BigdataValue v = itr.next();
            
                // The IV is cached on the Value.
                assertNotNull(v.getIV());

                // The Value is NOT cached on the IV.
                assertFalse(v.getIV().hasValue());
                
                // The Value is NOT cached on the IV.
                try {
                    v.getIV().getValue();
                    fail("Expecting: " + NotMaterializedException.class);
                } catch (NotMaterializedException ex) {
                    if (log.isInfoEnabled())
                        log.info("Ignoring expected exception: " + ex);
                }

                /*
                 * The IV attached to the Value can be used to lookup the Value
                 * in the Vocabulary.
                 */
                assertEquals(v, vocab.asValue(v.getIV()));

                /*
                 * The Value can be used to wrap it's cached IV.
                 */
                
                assertNotNull(vocab.getConstant(v));
                
                assertEquals(v.getIV(), vocab.getConstant(v).get());

                nfound++;
                
            }
            
            // The vocabulary size is consistent with its iterator.
            assertEquals(nfound, vocab.size());
            
        }
        
    }

    /**
     * Test (de-)serialization of a {@link Vocabulary}.
     */
    static void doRoundTripTest(final Vocabulary expected) {

        final byte[] data = SerializerUtil.serialize(expected);

        final Vocabulary actual = (Vocabulary) SerializerUtil.deserialize(data);

        assertSameVocabulary(expected, actual);

    }

    static public void assertSameVocabulary(final Vocabulary expected,
            final Vocabulary actual) {

        // same size.
        assertEquals("size", expected.size(), actual.size());

        /*
         * verify each value in expected is present with the same term
         * identifier in actual.
         */
        final Iterator<? extends Value> itre = expected.values();

        while (itre.hasNext()) {

            final Value value = itre.next();

            if (log.isInfoEnabled()) {

                log.info(value.toString());

            }

            // same assigned term identifier.
            assertEquals(expected.get(value), expected.get(value));

        }

    }

}
