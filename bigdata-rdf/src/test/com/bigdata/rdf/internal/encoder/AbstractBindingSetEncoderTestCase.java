/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 16, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.TestCase2;

import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Base class for {@link IBindingSetEncoder}and {@link IBindingSetDecoder} test
 * suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBindingSetEncoderTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractBindingSetEncoderTestCase() {
    }

    /**
     * @param name
     */
    public AbstractBindingSetEncoderTestCase(String name) {
        super(name);
    }

    /**
     * When <code>true</code>, {@link #doEncodeDecodeTest(IBindingSet)} will
     * also verify that the {@link IVCache} assertions were decoded.
     */
    protected boolean testCache = true;

    /**
     * The namespace for the {@link BigdataValueFactory}.
     * 
     */
    protected String namespace = getName();

    /**
     * The value factory for that namespace.
     */
    protected BigdataValueFactory valueFactory = BigdataValueFactoryImpl
            .getInstance(namespace);

    /**
     * A {@link TermId} whose {@link IVCache} is set.
     */
    protected TermId<BigdataLiteral> termId;

    /**
     * A {@link TermId} whose {@link IVCache} is set.
     */
    protected TermId<BigdataLiteral> termId2;

    /**
     * A {@link BlobIV} whose {@link IVCache} is set.
     */
    protected BlobIV<BigdataLiteral> blobIV;

    /**
     * The encoder under test.
     */
    protected IBindingSetEncoder encoder;
    
    /**
     * The decoder under test.
     */
    protected IBindingSetDecoder decoder;

    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        termId = new TermId<BigdataLiteral>(VTE.LITERAL, 12/* termId */);
        termId.setValue(valueFactory.createLiteral("abc"));

        termId2 = new TermId<BigdataLiteral>(VTE.LITERAL, 36/* termId */);
        termId2.setValue(valueFactory.createLiteral("xyz"));

        blobIV = new BlobIV<BigdataLiteral>(VTE.LITERAL, 912/* hash */,
                (short) 0/* collisionCounter */);
        blobIV.setValue(valueFactory.createLiteral("bigfoo"));

    }

    protected void tearDown() throws Exception {
        
        super.tearDown();
        
        // Clear references.
        encoder.release();
        encoder = null;
        decoder.release();
        decoder = null;
        valueFactory.remove();
        valueFactory = null;
        namespace = null;
        termId = null;
        blobIV = null;
        
    }

    protected void doEncodeDecodeTest(final IBindingSet expected) {

        doEncodeDecodeTest(expected, testCache);

    }

    protected void doEncodeDecodeTest(final IBindingSet expected,
            final boolean testCache) {

        final byte[] data = encoder.encodeSolution(expected);

        // Vector updates against the cache.
        encoder.flush();

        final Random r = new Random();
        
        if(r.nextBoolean()){

            // Decode.
            final IBindingSet actual = decoder.decodeSolution(data, 0/* off */,
                    data.length/* len */, true/* resolveCachedValues */);

            assertEquals(expected, actual, testCache);
        
        } else  {

            /*
             * Copy the record to be decoded to a different byte offset and the
             * re-decode the record. This allows us to check for correct
             * handling of the [off] argument by decodeSolution().
             */

            final int off2 = r.nextInt(20) + 1;

            // Decode from a different offset.
            final byte[] data2 = new byte[data.length + off2];

            System.arraycopy(data/* src */, 0/* srcPos */, data2/* dest */,
                    off2/* destPos */, data.length);

            final IBindingSet actual2 = decoder
                    .decodeSolution(data2, off2/* off */, data.length/* len */,
                            true/* resolveCachedValues */);

            assertEquals(expected, actual2, testCache);

        }
        
    }

    @SuppressWarnings("rawtypes")
    protected void assertEquals(final IBindingSet expected,
            final IBindingSet actual, final boolean testCache) {

        // check the binding set
        super.assertEquals(expected, actual);

        if (!testCache)
            return;

        final Iterator<Entry<IVariable, IConstant>> itr = expected.iterator();

        while (itr.hasNext()) {

            final Entry<IVariable, IConstant> e = itr.next();

            final IConstant c = e.getValue();

            final IV iv = (IV) c.get();

            if (iv.hasValue()) {

                final IVariable var = e.getKey();

                final IConstant c2 = actual.get(var);

                assertNotNull(c2);

                final IV iv2 = (IV) c2.get();

                assertEquals(iv, iv2);

                assertTrue(iv2.hasValue());

                assertEquals(iv.getValue(), iv2.getValue());

            }
            
        }
        
    }

    public void test_encodeEmpty() {

        final IBindingSet expected = new ListBindingSet();

        doEncodeDecodeTest(expected);

    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(
                new XSDNumericIV<BigdataLiteral>(12)));

        doEncodeDecodeTest(expected);
    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty2() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(
                new XSDNumericIV<BigdataLiteral>(12)));
        expected.set(Var.var("y"), new Constant<IV>(
                new FullyInlineURIIV<BigdataURI>(new URIImpl(
                        "http://www.bigdata.com"))));

        doEncodeDecodeTest(expected);

    }

    /**
     * Multiple solutions where a variable does not appear in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where a new variables appears in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions2() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the middle of the
     * sequence.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions3() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
        }

    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmptyWithCachedValue() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV>(termId));

        doEncodeDecodeTest(expected);
    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmptyWithCachedValues() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(termId));
        expected.set(Var.var("y"), new Constant<IV>(blobIV));

        doEncodeDecodeTest(expected);

    }

    /**
     * Multiple solutions where a variable does not appear in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValues() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where a new variables appears in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValues2() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the middle of the
     * sequence.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValues3() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Unit test of a solution with 3 bindings.
     */
    public void test_solutionWithThreeBindings1() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

        doEncodeDecodeTest(expected);
        
    }

    /**
     * Unit test of a solution with 3 bindings in a different order.
     */
    public void test_solutionWithThreeBindings2() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));

        doEncodeDecodeTest(expected);
        
    }

    protected BlobIV<BigdataLiteral> getVeryLargeLiteral() {
        
        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }

        final String s = sb.toString();

        final Random r = new Random();
        
        final int hash = r.nextInt();

        final short collisionCounter = (short) r.nextInt(12);

        final BlobIV<BigdataLiteral> blobIV2 = new BlobIV<BigdataLiteral>(
                VTE.LITERAL, hash, collisionCounter);

        blobIV2.setValue(valueFactory.createLiteral(s));

        return blobIV2;

    }

    /**
     * Unit test of a solution having a {@link BlobIV} with a
     * {@link BigdataLiteral} which is very large.
     */
    public void test_solutionWithVeryLargeObject() {

        final BlobIV<BigdataLiteral> blobIV2 = getVeryLargeLiteral();
        
        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV<?, ?>>(blobIV2));

        doEncodeDecodeTest(expected);
        
    }

    /**
     * Unit test of a solution having a {@link BlobIV} with a
     * {@link BigdataLiteral} which is very large plus a few other bindings.
     */
    public void test_solutionWithVeryLargeObject2() {

        final BlobIV<BigdataLiteral> blobIV2 = getVeryLargeLiteral();
        
        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV<?, ?>>(blobIV2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

        doEncodeDecodeTest(expected);
        
    }

    /**
     * Unit test of a solution having a {@link BlobIV} with a
     * {@link BigdataLiteral} which is very large plus a few other bindings
     * (different order from the test above).
     */
    public void test_solutionWithVeryLargeObject3() {

        final BlobIV<BigdataLiteral> blobIV2 = getVeryLargeLiteral();
        
        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(blobIV2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));

        doEncodeDecodeTest(expected);
        
    }

}
