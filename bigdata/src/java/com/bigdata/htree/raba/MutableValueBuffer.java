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
package com.bigdata.htree.raba;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import com.bigdata.btree.raba.AbstractRaba;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.htree.HTree;

/**
 * A flyweight mutable implementation for an {@link HTree} bucket page using a
 * backing <code>byte[][]</code>. Unlike the values in a B+Tree, the
 * {@link HTree} values need not be dense. Further, each bucket page is
 * logically divided into a set of buddy hash buckets. All operations therefore
 * take place within a buddy bucket. The buddy bucket is identified by its
 * offset and its extent is identified by the global depth of the bucket page.
 * <p>
 * Note: Because the slots are divided logically among the buddy buckets any
 * slot may have a non-<code>null</code> value and the {@link IRaba} methods as
 * implemented by this class DO NOT range check the index against
 * {@link #size()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Test suite. This should be pretty much a clone of the test
 *          suite for the {@link MutableKeyBuffer} in this package.
 */
public class MutableValueBuffer implements IRaba {

    /**
     * The #of defined values across the entire bucket page. 
     */
    public int nvalues;

    /**
     * An array containing the values. The size of the array is the maximum
     * capacity of the value buffer, which is <code>2^addressBits</code>.
     */
    public final byte[][] values;

    /**
     * Allocate a mutable value buffer capable of storing <i>capacity</i>
     * values.
     * 
     * @param capacity
     *            The capacity of the value buffer.
     */
    public MutableValueBuffer(final int capacity) {

        if (capacity <= 0)
            throw new IllegalArgumentException();

        if ((capacity & -capacity) != capacity) // e.g., not a power of 2.
            throw new IllegalArgumentException();

        nvalues = 0;
        
        values = new byte[capacity][];
        
    }

    /**
     * Constructor wraps an existing byte[][].
     * 
     * @param nvalues
     *            The #of defined values in the array.
     * @param values
     *            The array of values.
     */
    public MutableValueBuffer(final int nvalues, final byte[][] values) {

        assert nvalues >= 0;

        assert values != null;

        assert values.length == nvalues;

        this.nvalues = nvalues;

        this.values = values;

    }

    /**
     * Creates a new instance using a new array of values but sharing the value
     * references with the provided {@link MutableValueBuffer}.
     * 
     * @param src
     *            An existing instance.
     */
    public MutableValueBuffer(final MutableValueBuffer src) {

        assert src != null;

        this.nvalues = src.nvalues;

        // note: dimension to the capacity of the source.
        this.values = new byte[src.values.length][];

        // copy the values.
        for (int i = 0; i < src.values.length; i++) {

            // Note: copies the reference.
            this.values[i] = src.values[i];

        }

    }

    /**
     * Builds a mutable value buffer.
     * 
     * @param capacity
     *            The capacity of the new instance (this is based on the
     *            branching factor for the B+Tree).
     * @param src
     *            The source data.
     * 
     * @throws IllegalArgumentException
     *             if the capacity is LT the {@link IRaba#size()} of the
     *             <i>src</i>.
     * @throws IllegalArgumentException
     *             if the source is <code>null</code>.
     */
    public MutableValueBuffer(final int capacity, final IRaba src) {

        if (src == null)
            throw new IllegalArgumentException();

        if (capacity < src.capacity())
            throw new IllegalArgumentException();
        
        nvalues = src.size();

        assert nvalues >= 0;

        values = new byte[capacity][];

        int i = 0;
        for (byte[] a : src) {

            values[i++] = a;

        }
        
    }

    public String toString() {

        return AbstractRaba.toString(this);
        
    }

    /**
     * Returns a reference to the value at that index.
     */
    final public byte[] get(final int index) {

        return values[index];

    }

    final public int length(final int index) {

        final byte[] tmp = values[index];

        if (tmp == null)
            throw new NullPointerException();

        return tmp.length;

    }
    
    final public int copy(final int index, final OutputStream out) {

        final byte[] tmp = values[index];

        try {
            
            out.write(tmp, 0, tmp.length);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return tmp.length;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @return <code>true</code> iff the value at that index is <code>null</code>.
     */
    final public boolean isNull(final int index) {
        
        return values[index] == null;
                
    }
    
    final public boolean isEmpty() {
        
        return nvalues == 0;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This is the #of values in the bucket page (across all buddy buckets
     * on that page). Unless there is only one buddy bucket on the page, you
     * MUST explicitly scan a buddy bucket to determine the #of values in a
     * buddy bucket on the page.
     */
    final public int size() {

        return nvalues;

    }

    final public int capacity() {

        return values.length;
        
    }

    final public boolean isFull() {
        
        return nvalues == values.length;
        
    }
    
    /**
     * Mutable.
     */
    final public boolean isReadOnly() {
        
        return false;
        
    }

    /**
     * Instances are NOT searchable. Duplicates and <code>null</code>s ARE
     * permitted.
     * 
     * @returns <code>false</code>
     */
    final public boolean isKeys() {

        return false;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This iterator visits all values on the bucket page, including
     * <code>null</code>s.
     */
    public Iterator<byte[]> iterator() {

        return new Iterator<byte[]>() {

            int i = 0;
            
            public boolean hasNext() {
                return i < size();
            }

            public byte[] next() {
                return get(i++);
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };

    }

    /*
     * Mutation api. The contents of individual values are never modified. 
     */
    
    final public void set(final int index, final byte[] value) {

        assert value != null;
        assert values[index] == null;
        assert nvalues < values.length;
        
        values[index] = value;
        
        nvalues++;
        
    }

    /**
     * Remove a value in the buffer at the specified index, decrementing the #of
     * value in the buffer by one.
     * 
     * @param index
     *            The index in [0:{@link #capacity()}-1].
     * 
     * @return The #of values in the buffer.
     */
    final public int remove(final int index) {

        assert values[index] != null;
        assert nvalues > 0;
        
        values[index] = null;
        
        return --nvalues;
      
    }
    
    /**
     * This method is not supported. Values must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the value
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    final public int add(final byte[] value) {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. Values must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the value
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    final public int add(byte[] value, int off, int len) {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. Values must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the value
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    public int add(DataInput in, int len) throws IOException {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. 
     * 
     * @throws UnsupportedOperationException
     */
    public int search(byte[] searchKey) {
        
        throw new UnsupportedOperationException();
        
    }

}
