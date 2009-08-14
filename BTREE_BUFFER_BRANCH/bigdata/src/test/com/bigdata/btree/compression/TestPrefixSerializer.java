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
 * Created on Sep 25, 2008
 */

package com.bigdata.btree.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableRaba;

import junit.framework.TestCase2;

/**
 * Test suite for {@link PrefixSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPrefixSerializer extends TestCase2 {

    /**
     * 
     */
    public TestPrefixSerializer() {
    }

    /**
     * @param arg0
     */
    public TestPrefixSerializer(String arg0) {
        super(arg0);
    }

    final Random r = new Random();

    public void test_prefixCompression_Empty() throws IOException {

        // capacity of the array.
        final int capacity = r.nextInt(10);

        doRandomRoundTripTest(0, capacity);

    }

    public void test_prefixCompression_1() throws IOException {

        doRandomRoundTripTest(1/* n */, 1/* capacity */);

        doRandomRoundTripTest(1/* n */, 2/* capacity */);

    }

    public void test_prefixCompression_2() throws IOException {

        doRandomRoundTripTest(2/* n */, 2/* capacity */);

        doRandomRoundTripTest(2/* n */, 3/* capacity */);

    }
    
    public void test_prefixCompressionOnceRandom() throws IOException {
        
        // #of elements.
        final int n = r.nextInt(100);

        // capacity of the array.
        final int capacity = n + r.nextInt(n); 
        
        doRandomRoundTripTest(n,capacity);
        
    }

    public void testStress() throws IOException {
        
        for (int i = 0; i < 100; i++) {

            // #of elements.
            final int n = r.nextInt(100) + 1;

            // capacity of the array.
            final int capacity = n + r.nextInt(n); 
            
            doRandomRoundTripTest(n,capacity);

        }

    }
    
    protected void doRandomRoundTripTest(final int n, final int capacity)
            throws IOException {
        
        assert capacity >= n;
        
        final byte[][] data = new byte[capacity][];
        
        for (int i = 0; i < n; i++) {
            
            data[i] = new byte[r.nextInt(512)];
            
            r.nextBytes(data[i]);
            
        }
        
        // put into sorted order.
        Arrays.sort(data, 0, n, UnsignedByteArrayComparator.INSTANCE);
        
        // layer on interface.
        final IRaba raba = new MutableRaba(
                0/* fromIndex */, n/* toIndex */, data);
        
        doRoundTripTest(raba);
        

    }
    
    public void doRoundTripTest(IRaba raba) throws IOException {

        final byte[] data;
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final DataOutputStream out = new DataOutputStream(baos);

            PrefixSerializer.INSTANCE.write(out, raba);

            out.flush();

            data = baos.toByteArray();

        }

        final IRaba raba2;
        {

//            final int n = raba.getKeyCount();
            
            final int capacity = raba.capacity();
            
            raba2 = new MutableRaba(0/* fromIndex */,
                    0/* toIndex */, new byte[capacity][]);

            final DataInput in = new DataInputStream(new ByteArrayInputStream(
                    data));

            PrefixSerializer.INSTANCE.read(in, raba2);

        }

        assertEquals(raba, raba2);

        if (log.isInfoEnabled()) {
        
            int nbytes = 0;
            
            for (byte[] a : raba) {
            
                nbytes += a.length;
            
            }
            
            /*
             * Note: this is for compression of sorted random data, you can
             * expect to do better for real data.
             */
            log.info("original: "+nbytes+", compressed: "+data.length);
            
        }
        
    }

    protected void assertEquals(IRaba expected,
            IRaba actual) {

        assertEquals("n", expected.size(), actual.size());

        assertEquals("capacity", expected.capacity(), actual.capacity());

        final int n = expected.size();

        for (int i = 0; i < n; i++) {

            assertEquals(expected.get(i), actual.get(i));

        }

    }

}
