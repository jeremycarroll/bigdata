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
 * backing <code>byte[][]</code>. Unlike the keys in a B+Tree, the {@link HTree}
 * keys are NOT ordered and need not be dense. Further, each bucket page is
 * logically divided into a set of buddy hash buckets. All operations therefore
 * take place within a buddy bucket. The buddy bucket is identified by its
 * offset and its extent is identified by the global depth of the bucket page.
 * <p>
 * While the total #of non-null keys is reported by {@link #size()}, this is the
 * value for the bucket page as a whole. The {@link HTree} must explicitly
 * examine a buddy hash bucket and count the non-<code>null</code> keys in order
 * to know the "size" of a given buddy hash bucket.
 * <p>
 * Note: Because the slots are divided logically among the buddy buckets any
 * slot may have a non-<code>null</code> key and the {@link IRaba} methods as
 * implemented by this class DO NOT range check the index against
 * {@link #size()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableKeyBuffer implements IRaba {

    /**
     * The #of defined keys across the entire bucket page. The caller must
     * explicitly scan a buddy hash bucket in order to learn the #of non-
     * <code>null</code> keys (free slots) in that buddy hash bucket.
     */
    public int nkeys;

    /**
     * An array containing the keys. The size of the array is the maximum
     * capacity of the key buffer, which is <code>2^addressBits</code>.
     */
    final public byte[][] keys;

    /**
     * Must be <code>2^n</code> where <code>n</code> GT ZERO (0).
     */
    private static void checkCapacity(final int capacity) {
     
        if (capacity <= 1 || (capacity & -capacity) != capacity)
            throw new IllegalArgumentException(
                    "capacity must be 2^n where n is positive, not " + capacity);

    }
    
    /**
     * Allocate a mutable key buffer capable of storing <i>capacity</i> keys.
     * 
     * @param capacity
     *            The capacity of the key buffer.
     */
    public MutableKeyBuffer(final int capacity) {

        checkCapacity(capacity);

        nkeys = 0;
        
        keys = new byte[capacity][];
        
    }

    /**
     * Constructor wraps an existing byte[][].
     * 
     * @param nkeys
     *            The #of defined keys in the array.
     * @param keys
     *            The array of keys.
     */
    public MutableKeyBuffer(final int nkeys, final byte[][] keys) {

        if (keys == null)
            throw new IllegalArgumentException();

        if (nkeys < 0 || nkeys > keys.length)
            throw new IllegalArgumentException();

        checkCapacity(keys.length);
        
        this.nkeys = nkeys;

        this.keys = keys;

    }

    /**
     * Creates a new instance using a new byte[][] but sharing the byte[]
     * references with the caller's buffer.
     * 
     * @param src
     *            An existing instance.
     */
    public MutableKeyBuffer(final MutableKeyBuffer src) {

        if(src == null)
            throw new IllegalArgumentException();
        
        checkCapacity(src.capacity());
        
        this.nkeys = src.nkeys;

        // note: dimension to the capacity of the source.
        this.keys = new byte[src.keys.length][];

        // copy the keys.
        for (int i = 0; i < keys.length; i++) {

            // Note: copies the reference.
            this.keys[i] = src.keys[i];

        }

    }

    /**
     * Builds a mutable key buffer.
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
    public MutableKeyBuffer(final int capacity, final IRaba src) {

        if (src == null)
            throw new IllegalArgumentException();

        checkCapacity(capacity);
        
        if (capacity < src.capacity())
            throw new IllegalArgumentException();
        
        nkeys = src.size();

        keys = new byte[capacity][];

        int i = 0;
        for (byte[] a : src) {

            keys[i++] = a;

        }
        
    }

    public String toString() {

        return AbstractRaba.toString(this);
        
    }

    /**
     * Returns a reference to the key at that index.
     */
    final public byte[] get(final int index) {

        return keys[index];

    }

    final public int length(final int index) {

        final byte[] tmp = keys[index];

        if (tmp == null)
            throw new NullPointerException();

        return tmp.length;

    }
    
    final public int copy(final int index, final OutputStream out) {

        final byte[] tmp = keys[index];

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
     * @return <code>true</code> iff the key at that index is <code>null</code>.
     */
    final public boolean isNull(final int index) {
        
        return keys[index] == null;
                
    }
    
    final public boolean isEmpty() {
        
        return nkeys == 0;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This is the #of keys in the bucket page (across all buddy buckets
     * on that page). Unless there is only one buddy bucket on the page, you
     * MUST explicitly scan a buddy bucket to determine the #of keys in a buddy
     * bucket on the page.
     */
    final public int size() {

        return nkeys;

    }

    final public int capacity() {

        return keys.length;
        
    }

    final public boolean isFull() {
        
        return nkeys == keys.length;
        
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
     * This iterator visits all keys on the bucket page, including
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
     * Mutation api. The contents of individual keys are never modified. 
     */
    
    final public void set(final int index, final byte[] key) {

        assert key != null;
        assert keys[index] == null;
        assert nkeys < keys.length;
        
        keys[index] = key;
        
        nkeys++;
        
    }

    /**
     * Remove a key in the buffer at the specified index, decrementing the #of
     * keys in the buffer by one.
     * 
     * @param index
     *            The index in [0:{@link #capacity()}-1].
     * @param key
     *            The key.
     * 
     * @return The #of keys in the buffer.
     */
    final public int remove(final int index) {

        assert keys[index] != null;
        assert nkeys > 0;
        
        keys[index] = null;
        
        return --nkeys;
      
    }
    
    /**
     * This method is not supported. Keys must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the key
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    final public int add(final byte[] key) {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. Keys must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the key
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    final public int add(byte[] key, int off, int len) {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. Keys must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the key
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    public int add(DataInput in, int len) throws IOException {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. The keys in a buddy hash table are neither
     * ordered, dense, nor unique (duplicate keys are permitted). Search must be
     * performed by a scan of the non-<code>null</code> keys in a specific buddy
     * bucket and there can be multiple "matches" for a given key.
     * 
     * @throws UnsupportedOperationException
     */
    public int search(byte[] searchKey) {
        
        throw new UnsupportedOperationException();
        
    }

}
