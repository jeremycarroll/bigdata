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
 * Created on Apr 30, 2007
 */

package com.bigdata.btree;

import java.text.Collator;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * A class that may be used to form multi-component keys but which does not
 * support Unicode. An instance of this class is quite light-weight and SHOULD
 * be used when Unicode support is not required.
 * <p>
 * Note: Avoid any dependencies within this class on the ICU libraries so that
 * the code may run without those libraries when they are not required.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SuccessorUtil Compute the successor of a value before encoding it as a
 *      component of a key.
 * 
 * @see BytesUtil#successor(byte[]) Compute the successor of an encoded key.
 * 
 * @todo introduce a mark and restore feature for generating multiple keys that
 *       share some leading prefix. in general, this is as easy as resetting the
 *       len field to the mark. keys with multiple components could benefit from
 *       allowing multiple marks (the sparse row store is the main use case).
 * 
 * @todo Integrate support for ICU versioning into the client and perhaps into
 *       the index metadata so clients can discover which version and
 *       configuration properties to use when generating keys for an index.
 * 
 * @todo provide a synchronization factory for the keybuilder using a delegation
 *       model. KeyBuilder.synchronizedKeyBuilder():KeyBuilder? (Note that
 *       {@link #asSortKey(Object)} already handles the most common use cases
 *       and is thread-safe.)
 */
public class KeyBuilder implements IKeyBuilder {

    protected static final Logger log = Logger.getLogger(KeyBuilder.class);
    
    /**
     * The default capacity of the key buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 1024;
    
    /**
     * A non-negative integer specifying the #of bytes of data in the buffer
     * that contain valid data starting from position zero(0).
     */
    protected int len;
    
    /**
     * The key buffer. This is re-allocated whenever the capacity of the buffer
     * is too small and reused otherwise.
     */
    protected byte[] buf;
    
    /**
     * The object used to generate sort keys from Unicode strings (optional).
     * <p>
     * Note: When <code>null</code> the IKeyBuilder does NOT support Unicode
     * and the optional Unicode methods will all throw an
     * {@link UnsupportedOperationException}.
     */
    protected final UnicodeSortKeyGenerator sortKeyGenerator;
    
    /**
     * Creates a key builder with an initial buffer capacity of
     * <code>1024</code> bytes.
     */
    public KeyBuilder() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * Creates a key builder with the specified initial buffer capacity.
     * 
     * @param initialCapacity
     *            The initial capacity of the internal byte[] used to construct
     *            keys. When zero (0) the {@link #DEFAULT_INITIAL_CAPACITY} will
     *            be used.
     */
    public KeyBuilder(int initialCapacity) {
        
        this(0, createBuffer(initialCapacity));
        
    }
    
    /**
     * Create a buffer of the specified initial capacity.
     * 
     * @param initialCapacity
     *            The initial size of the buffer.
     * 
     * @return The byte[] buffer.
     * 
     * @exception IllegalArgumentException
     *                if the initial capacity is negative.
     */
    protected static byte[] createBuffer(int initialCapacity) {

        if(initialCapacity<0) {
            
            throw new IllegalArgumentException("initialCapacity must be non-negative");
            
        }
        
        final int capacity = initialCapacity == 0 ? DEFAULT_INITIAL_CAPACITY
                : initialCapacity;
        
        return new byte[capacity];
        
    }
    
    /**
     * Creates a key builder using an existing buffer with some data.
     * 
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     */
    /*public*/ KeyBuilder(int len, byte[] buf) {

        this( null /* no unicode support*/, len, buf );
        
    }    
    
    /**
     * Creates a key builder using an existing buffer with some data (designated
     * constructor).
     * 
     * @param sortKeyGenerator
     *            The object used to generate sort keys from Unicode strings
     *            (when <code>null</code> Unicode collation support is
     *            disabled).
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     */
    protected KeyBuilder(UnicodeSortKeyGenerator sortKeyGenerator, int len,
            byte[] buf) {
        
        if (len < 0)
            throw new IllegalArgumentException("len");

        if (buf == null)
            throw new IllegalArgumentException("buf");

        if (len > buf.length)
            throw new IllegalArgumentException("len>buf.length");

        this.len = len;

        this.buf = buf;

        this.sortKeyGenerator = sortKeyGenerator; // MAY be null.
        
    }
    
    final public int getLength() {
        
        return len;
        
    }

    /**
     * Sets the position to any non-negative length less than the current
     * capacity of the buffer.
     */
    final public void position(int pos) {
        
        if (len < 0 || len > buf.length) {

            throw new IndexOutOfBoundsException("pos=" + pos
                    + ", but capacity=" + buf.length);
            
        }

        len = pos;
        
    }
    
    final public IKeyBuilder append(int off, int len, byte[] a) {
        
        ensureFree(len);
        
        System.arraycopy(a, off, buf, this.len, len);
        
        this.len += len;
        
//        assert this.len <= buf.length;
        
        return this;
        
    }

    /**
     * Ensure that at least <i>len</i> bytes are free in the buffer. The
     * {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * <p>
     * This operation is equivilent to
     * 
     * <pre>
     * ensureCapacity(this.len + len)
     * </pre>
     * 
     * and the latter is often used as an optimization.
     * 
     * @param len
     *            The minimum #of free bytes.
     */
    final public void ensureFree(int len) {
        
        ensureCapacity(this.len + len );
        
    }

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total bytes.
     * The {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * 
     * @param capacity
     *            The minimum #of bytes in the buffer.
     */
    final public void ensureCapacity(int capacity) {
        
        if(capacity<0) throw new IllegalArgumentException();
//        assert capacity >= 0;
        
        final int overflow = capacity - buf.length;
        
        if(overflow>0) {
        
            /*
             * extend to at least the target capacity.
             */
            final byte[] tmp = new byte[capacity];
            
            // copy only the defined bytes.
            System.arraycopy(buf, 0, tmp, 0, this.len);
            
            buf = tmp;
            
        }

    }

    final public byte[] getKey() {
        
        byte[] tmp = new byte[this.len];
        
        System.arraycopy(buf, 0, tmp, 0, this.len);
        
        return tmp;
        
    }
    
    /*
     * The problem with this method is that it encourages us to reuse a key
     * buffer but the btree (at least when used as part of a local api) requires
     * that we donate the key buffer to the btree.
     */
//    /**
//     * Copy the key from the internal buffer into the supplied buffer.
//     * 
//     * @param b
//     *            A byte[].
//     * 
//     * @exception IndexOutOfBoundsException
//     *                if the supplied buffer is not large enough.
//     */
//    final public void copyKey(byte[] b) {
//    
//        System.arraycopy(this.buf, 0, b, 0, this.len);
//        
//    }
    
    final public IKeyBuilder reset() {
        
        len = 0;
        
        return this;
        
    }

    /*
     * Optional Unicode operations.
     */

    private final void assertUnicodeEnabled() {
        
        if (sortKeyGenerator == null)
            throw new UnsupportedOperationException();
        
    }

    final public boolean isUnicodeSupported() {

        if (sortKeyGenerator == null) return false;

        return true;
        
    }
    
    /**
     * The object responsible for generating sort keys from Unicode strings.
     * 
     * The {@link UnicodeSortKeyGenerator} -or- <code>null</code> if Unicode
     * is not supported by this {@link KeyBuilder} instance.
     */
    final public UnicodeSortKeyGenerator getSortKeyGenerator() {
        
        return sortKeyGenerator;
        
    }

    final public IKeyBuilder append(String s) {
        
        assertUnicodeEnabled();

        sortKeyGenerator.appendSortKey( this, s );
        
        return this;
        
    }

    /*
     * Note: Dropped from the API to minimize confusion.
     */
//    final public IKeyBuilder append(char[] v) {
//
//        return append(new String(v));
//        
//    }
    
    /*
     * Non-optional operations.
     */
    
    public IKeyBuilder appendASCII(String s) {
        
        int len = s.length();
        
        ensureFree(len);
        
        for(int j=0; j<len; j++) {
            
            char ch = s.charAt(j);
            
//            append((byte)(ch & 0xff));

            // lexiographic ordering as unsigned byte.
            
            int v = (byte)ch;
            
            if (v < 0) {

                v = v - 0x80;

            } else {
                
                v = v + 0x80;
                
            }
            
            buf[this.len++] = (byte)(v & 0xff);

        }
        
        return this;
        
    }

    /**
     * The default pad character (a space).
     * <p>
     * Note: Any character may be choosen as the pad character as long as it has
     * a one byte representation. In practice this means you can choose 0x20 (a
     * space) or 0x00 (a nul). This limit arises in
     * {@link #appendText(String, boolean, boolean)} which assumes that it can
     * write a pad character (or its successor) in one byte. 0xff will NOT work
     * since its successor is not defined within an bit string of length 8.
     * 
     * @todo make this a configuration option? if so then verify that the choice
     *       (and its successor) fit in 8 bits.
     */
    final public byte pad = 0x20;

    /**
     * Normalize the text by truncating to no more than {@link #maxlen}
     * characters and then stripping off trailing {@link #pad} characters.
     */
    /*public*/ String normalizeText(String text) {

        if (text.length() > maxlen) {

            /*
             * Truncate the encoded text field to maxlen characters to prevent
             * overflow. While the number of bytes generated by the resulting
             * encoding is variable, the order semantics will respect only the
             * 1st maxlen characters _regardless_ of how many bytes are required
             * to encode those characters.
             */

            text = text.substring(0, maxlen);

        }

        /*
         * Strip trailing pad "characters" from the text. This helps to ensure a
         * canonical representation. If we did not do this then text with
         * trailing pad characters would be encoded differently from text
         * without trailing pad characters - even though they are supposed to be
         * the "same".
         */
        {
            int npadded = 0;
            for (int i = text.length() - 1; i >= 0; i--) {
                if (text.charAt(i) == pad) {
                    npadded++;
                    continue;
                }
                break;
            }
            if (npadded > 0) {
                int begin = 0;
                int end = text.length() - npadded;
                text = text.substring(begin,end);
            }
        }

        return text;

    }
    
    public IKeyBuilder appendText(String text, final boolean unicode,
            final boolean successor) {

        // current length of the encoded key.
        final int pos = this.len;

        /*
         * Normalize the text by truncating to no more than [maxlen] characters
         * and then stripping off trailing pad characters.
         */
        
        text = normalizeText( text );
        
        /*
         * Encode the text as ASCII or Unicode as appropriate.
         */
        
        if(unicode) {

            append(text);
            
        } else {
            
            appendASCII(text);
            
        }

        // #of bytes in the encoded text field.
        final int encoded_len = this.len - pos;

        // #of characters (not bytes) in the text.
        final int textlen;
        
        if(successor) {

            if (encoded_len == 0) {

                /*
                 * Note: The successor of an empty string is not defined since
                 * it maps to an empty byte[] (an empty value space). However an
                 * empty string is semantically equivilent to all pad characters
                 * so we use the successor of a string containing a single pad
                 * character, which is equivilent to a string containing a
                 * single byte whose value is pad+1.
                 */
                
                append((byte)(pad+1));
                
                textlen = 1;
                
            } else {

                /*
                 * Note: This generates the successor of the encoded text by
                 * treading the encoded byte[] as a fixed length bit string and
                 * finding the successor of that bit-string. The bytes in the
                 * buffer are modified as a side-effect. A runtime exception is
                 * thrown if there is no successor to the bit string (this is
                 * not a plausible scenario for either ASCII or Unicode text as
                 * the encoding would have to be all 0xff bytes for the
                 * successor to not be defined).
                 */

                SuccessorUtil.successor(buf, pos, encoded_len);
                
                textlen = text.length();
                
            }
            
        } else {
            
            textlen = text.length();
            
        }

        if (textlen < maxlen) {

            // append a single pad byte.
            append(pad);
            
            // append the run length for the trailing pad characters.
            final int runLength = maxlen - textlen;

            append((short) runLength);
            
        }

        return this;
        
    }
    
    final public IKeyBuilder append(byte[] a) {
        
        return append(0, a.length, a);
        
    }
    
    final public IKeyBuilder append(double d) {
        
        // performance tweak.
        if (len + 8 > buf.length) ensureCapacity(len+8);
//        ensureFree(8);

        long v = Double.doubleToLongBits(d);
        
        // convert to twos-complement long.

        if (v < 0) {
            
            v = 0x8000000000000000L - v;

        }

        // delegate to append(long)
        return append( v );

    }

    final public IKeyBuilder append(float f) {

        // performance tweak.
        if (len + 4 > buf.length) ensureCapacity(len+4);
//        ensureFree(4);

        int v = Float.floatToIntBits(f);

        // convert to twos complement int.
        if (v < 0) {

            v = 0x80000000 - v;

        }

        // delegate to append(int)
        return append(v);

    }

    final public IKeyBuilder append(UUID uuid) {

        if (len + 16 > buf.length) ensureCapacity(len+16);

        append( uuid.getMostSignificantBits() );
        
        append( uuid.getLeastSignificantBits() );
        
        return this;
        
    }
    
    final public IKeyBuilder append(long v) {

        // performance tweak adds .3% on rdfs bulk load.
        if (len + 8 > buf.length) ensureCapacity(len+8);
//        ensureFree(8);
//        ensureCapacity( len + 8 );
        
        // lexiographic ordering as unsigned long integer.

        if (v < 0) {
            
            v = v - 0x8000000000000000L;

        } else {
            
            v = v + 0x8000000000000000L;
            
        }

        // big-endian.
        buf[len++] = (byte)(v >>> 56);
        buf[len++] = (byte)(v >>> 48);
        buf[len++] = (byte)(v >>> 40);
        buf[len++] = (byte)(v >>> 32);
        buf[len++] = (byte)(v >>> 24);
        buf[len++] = (byte)(v >>> 16);
        buf[len++] = (byte)(v >>>  8);
        buf[len++] = (byte)(v >>>  0);

        return this;
        
    }
    
    /**
     * Return the value that will impose the lexiographic ordering as an
     * unsigned long integer.
     * 
     * @param v
     *            The signed long integer.
     * 
     * @return The value that will impose the lexiograph ordering as an unsigned
     *         long integer.
     */
    static final /*public*/ long encode(long v) {

        if (v < 0) {
            
            v = v - 0x8000000000000000L;

        } else {
            
            v = v + 0x8000000000000000L;
            
        }

        return v;
        
    }

    final public IKeyBuilder append(int v) {

        // performance tweak.
        if (len + 4 > buf.length) ensureCapacity(len+4);
//        ensureFree(4);
        
        // lexiographic ordering as unsigned int.
        
        if (v < 0) {

            v = v - 0x80000000;

        } else {
            
            v = 0x80000000 + v;
            
        }

        // big-endian
        buf[len++] = (byte)(v >>> 24);
        buf[len++] = (byte)(v >>> 16);
        buf[len++] = (byte)(v >>>  8);
        buf[len++] = (byte)(v >>>  0);
                    
        return this;
        
    }

    final public IKeyBuilder append(short v) {

        // performance tweak.
        if (len + 2 > buf.length) ensureCapacity(len+2);
//        ensureFree(2);
        
        // lexiographic ordering as unsigned short.
        
        if (v < 0) {

            v = (short)(v - (short)0x8000);

        } else {
            
            v = (short) ((short)0x8000 + v);
            
        }

        // big-endian
        buf[len++] = (byte)(v >>>  8);
        buf[len++] = (byte)(v >>>  0);
                    
        return this;
        
    }
    
    /*
     * Note: this method has been dropped from the API to reduce the
     * possibility of confusion.  If you want Unicode semantics then use
     * append(String).  If you want ASCII semantics then use appendASCII().
     * If you want signed integer semantics then use append(short).
     */
//    final public IKeyBuilder append(char v) {
//
//        /*
//         * Note: converting to String first produces significantly larger keys
//         * which, more important, violate the sort order expectations for
//         * characters. For example, successor in the value space of 'z' is '{'.
//         * However, the sort key generated from the String representation of the
//         * character '{' is NOT ordered after the sort key generated from the
//         * String representation of the character 'z'.  Unicode wierdness.
//         */
//
//        return append((short) v);
//        
//    }

    final public IKeyBuilder append(final byte v) {

        // performance tweak
        if (len + 1 > buf.length) ensureCapacity(len+1);
        // ensureFree(1);

        // lexiographic ordering as unsigned byte.
        
        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }
        
        buf[len++] = (byte)(i & 0xff);
        
        return this;
        
    }

    /**
     * Return the value that will impose the lexiographic ordering as an
     * unsigned byte.
     * 
     * @param v
     *            The signed byte.
     * 
     * @return The value that will impose the lexiographic ordering as an
     *         unsigned byte.
     */
    final static /*public*/ byte encode(byte v) {

        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }
        
        byte tmp = (byte)(i & 0xff);
        
        return tmp;
        
    }
    
    final public IKeyBuilder appendNul() {

//        return append(0);
        
        // performance tweak.
        if (len + 1 > buf.length) ensureCapacity(len+1);
//        ensureFree(1);
        
        buf[len++] = (byte) 0;
        
        return this;
        
    }

    /*
     * static helper methods.
     */
    
    /**
     * Used to unbox an application key (convert it to an unsigned byte[]).
     */
    static private final IKeyBuilder _keyBuilder = newUnicodeInstance();

    /**
     * Utility method converts an application key to a sort key (an unsigned
     * byte[] that imposes the same sort order).
     * <p>
     * Note: This method is thread-safe.
     * <p>
     * Note: Strings are Unicode safe for the default locale. See
     * {@link Locale#getDefault()}. If you require a specific local or
     * different locals at different times or for different indices then you
     * MUST provision and apply your own {@link KeyBuilder}.
     * 
     * @param key
     *            An application key.
     * 
     * @return The unsigned byte[] equivilent of that key. This will be
     *         <code>null</code> iff the <i>key</i> is <code>null</code>.
     *         If the <i>key</i> is a byte[], then the byte[] itself will be
     *         returned.
     */
    public static final byte[] asSortKey(Object key) {
        
        if (key == null) {

            return null;
            
        }

        if (key instanceof byte[]) {

            return (byte[]) key;
            
        }

        /*
         * Synchronize on the keyBuilder to avoid concurrent modification of its
         * state.
         */

        synchronized (_keyBuilder) {
    
            _keyBuilder.reset();

            _keyBuilder.append( key );

            return _keyBuilder.getKey();
    
        }
    
    }

    public IKeyBuilder append(Object val) {
        
        if (val == null) {

            throw new IllegalArgumentException();
            
        }

        if(val instanceof byte[]) {

            append((byte[])val);

        } else if (val instanceof Byte) {
            
            append(((Byte) val).byteValue());

        } else if (val instanceof Character) {

            // append(((Character) val).charValue());

            throw new UnsupportedOperationException(
                    "Character is not supported.  Use Short or String depending on the semantics that you want.");

        } else if (val instanceof Short) {

            append(((Short) val).shortValue());

        } else if (val instanceof Integer) {

            append(((Integer) val).intValue());

        } else if (val instanceof Long) {

            append(((Long) val).longValue());

        } else if (val instanceof Float) {

            append(((Float) val).floatValue());

        } else if (val instanceof Double) {

            append(((Double) val).doubleValue());

        } else if (val instanceof String) {

            append((String) val);

        } else if (val instanceof UUID) {

            append(((UUID) val));

        } else {

            throw new UnsupportedOperationException("Can not encode key: "
                    + val.getClass());

        }

        return this;
        
    }
    
    /**
     * Converts an unsigned byte into a signed byte.
     * 
     * @param v
     *            The unsigned byte.
     *            
     * @return The corresponding signed value.
     */
    static public byte decodeByte(byte v) {

        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }

        return (byte)(i & 0xff);
        
    }

    /**
     * Encodes a double precision floating point value as an int64 value that
     * has the same total ordering (you can compare two doubles encoded by this
     * method and the long values will have the same ordering as the double
     * values). The method works by converting the double to the IEEE 754
     * floating-point "double format" bit layout using
     * {@link Double#doubleToLongBits(double)} and then converting the resulting
     * long into a two's complement number.
     * 
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     * 
     * @param d
     *            The double precision floating point value.
     * 
     * @return The corresponding long integer value that maintains the same
     *         total ordering.
     */
    public static long d2l(double d) {

        long aLong = Double.doubleToLongBits(d);
        
        if (aLong < 0) {
            
            aLong = 0x8000000000000000L - aLong;

        }
        
        return aLong;

    }

    /**
     * Encodes a floating point value as an int32 value that has the same total
     * ordering (you can compare two floats encoded by this method and the int
     * values will have the same ordering as the float values). The method works
     * by converting the float to the IEEE 754 floating-point "single format"
     * bit layout using {@link Float#floatToIntBits(float)} and then converting
     * the resulting int into a two's complement number.
     * 
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     * 
     * @param f
     *            The floating point value.
     * 
     * @return The corresponding integer value that maintains the same total
     *         ordering.
     */
    public static int f2i(float f) {

        int aInt = Float.floatToIntBits(f);
        
        if (aInt < 0) {
            
            aInt = 0x80000000 - aInt;

        }
        
        return aInt;

    }
    
    /**
     * Decodes a signed long value as encoded by {@link #append(long)}.
     * 
     * @param buf
     *            The buffer containing the encoded key.
     * @param off
     *            The offset at which to decode the key.
     *            
     * @return The signed long value.
     */
    static public long decodeLong(byte[] buf,int off) {

        long v = 0L;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 56;
        v += (0xffL & buf[off++]) << 48;
        v += (0xffL & buf[off++]) << 40;
        v += (0xffL & buf[off++]) << 32;
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;

        if (v < 0) {
            
            v = v + 0x8000000000000000L;

        } else {
            
            v = v - 0x8000000000000000L;
            
        }

        return v;
        
    }

    /**
     * Interface allows us to encapsulate differences between the ICU and JDK
     * libraries for generating sort keys from Unicode strings.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface UnicodeSortKeyGenerator {
        
        /**
         * The {@link Locale} used to configure this object.
         */
        public Locale getLocale();

        /**
         * Append a Unicode sort key to the {@link KeyBuilder}.
         * 
         * @param keyBuilder
         *            The {@link KeyBuilder}.
         * @param s
         *            The Unicode string.
         */
        public void appendSortKey(KeyBuilder keyBuilder, String s);
        
    }
    
    /**
     * Type safe enumeration for the strength.
     * <p>
     * Note: ICU and the JDK use different integer constants for the
     * #IDENTICAL strength
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum StrengthEnum {
        
        /**
         * See {@link Collator#PRIMARY}.
         */
        Primary,
        /**
         * See {@link Collator#SECONDARY}.
         */
        Secondary,
        /**
         * See {@link Collator#TERTIARY}.
         */
        Tertiary,
        /**
         * Note: this option is NOT supported by the JDK.
         */
        Quaternary,
        /**
         * See {@link Collator#IDENTICAL}.
         */
        Identical;
        
    }
    
    /**
     * Type safe enumeration for the decomposition mode.
     * <p>
     * Note: ICU and the JDK use different integer constants for the
     * decomposition modes!
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum DecompositionEnum {
        
        /**
         * See {@link Collator#NO_DECOMPOSITION}.
         */
        None,
        /**
         * See {@link Collator#FULL_DECOMPOSITION}.
         */
        Full,
        /**
         * See {@link Collator#CANONICAL_DECOMPOSITION}.
         */
        Canonical;
        
    }
    
    public static IKeyBuilder newInstance() {

        return newInstance(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    public static IKeyBuilder newInstance(int capacity) {
     
        return newInstance(capacity, false/* unicode */, false/* icu */,
                null/* locale */, null/* strength */, null/* decomposition mode */);
        
    }

    /**
     * Configuration options for the {@link KeyBuilder} factory methods. These
     * options should be specified as System properties using
     * <code>-Dproperty=value</code> on the command line.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {
       
        /**
         * Optional boolean property specifies whether or not the ICU library is
         * required. When <code>true</code> the ICU library is required and
         * MUST be available. When <code>false</code> the ICU library will NOT
         * be used regardless of whether or not it is available. When this
         * option is not specified the default behavior is to use the ICU
         * library IFF it is available on the CLASSPATH.
         * 
         * @todo Add option to support the JNI integration of ICU (native code
         *       for faster generation of sort keys).
         */
        public String ICU = "collator.icu";

        /**
         * Optional string -or- integer property whose value is the strength to
         * be set on the collator. When specified, the value must be either one
         * of the type-safe {@link StrengthEnum}s -or- one of those supported
         * by the ICU or JDK library, as appropriate. The following values are
         * shared by both libraries:
         * <dl>
         * <dt>0</dt>
         * <dd>{@link Collator#PRIMARY}</dd>
         * <dt>1</dt>
         * <dd>{@link Collator#SECONDARY}</dd>
         * <dt>2</dt>
         * <dd>{@link Collator#TERTIARY}</dd>
         * </dl>
         * The ICU library also supports
         * <dl>
         * <dt>3</dt>
         * <dd>Quaternary</dd>
         * </dl>
         * While both libraries define <strong>IDENTICAL</strong> they use
         * different values for this strength, hence the use of the type safe
         * enums is recommended.
         */
        public String STRENGTH = "collator.strength";

        /**
         * Optional string property whose value is one of the type safe
         * {@link DecompositionEnum}s. The default decomposition mode will be
         * overriden on the collator one is explicitly specified using this
         * property.
         * 
         * @see DecompositionEnum
         */
        public String DECOMPOSITION = "collator.decomposition";

        /**
         * This is the pre-defined System property that determines the language
         * for the default {@link Locale}.
         * 
         * @see Locale#setDefault(Locale)
         * 
         * @see <a
         *      href="http://java.sun.com/developer/technicalArticles/J2SE/locale/">http://java.sun.com/developer/technicalArticles/J2SE/locale/</a>
         */
        public String USER_LANGUAGE = "user.language";
        
        /**
         * This is the pre-defined System property that determines the country
         * for the default {@link Locale}.
         * 
         * @see <a
         *      href="http://java.sun.com/developer/technicalArticles/J2SE/locale/">http://java.sun.com/developer/technicalArticles/J2SE/locale/</a>
         */
        public String USER_COUNTRY = "user.country";
        
    }

    /**
     * Create a new Unicode capable instance configured using the
     * {@link System#getProperties() System properties}. In particular, the
     * {@link Locale} will be the value returned by {@link Locale#getDefault()}.
     * 
     * @see Options
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath. See {@link Options#ICU}.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICUJNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public static IKeyBuilder newUnicodeInstance() {

        return newUnicodeInstance(null);
        
    }

    static private String getProperty(Properties properties, String key) {
        
        return getProperty(properties,key,null);
        
    }

    static private String getProperty(Properties properties, String key, String def) {
        
        if(properties==null) {
        
            return System.getProperty(key,def);
            
        } else {
            
            if(def==null) {
                
                def = System.getProperty(key);
                
            }
            
            return properties.getProperty(key,def);
            
        }

    }
    
    /**
     * Create a new Unicode capable instance configured according to the
     * specified properties. Any properties NOT explicitly given will be
     * defaulted from the {@link System#getProperties() System properties}.
     * {@link Options#USER_LANGUAGE} and {@link Options#USER_COUNTRY} MAY be
     * overriden in the given <i>properties</i> such that they differ from the
     * values available from {@link System#getProperties()}.
     * 
     * @param properties
     *            The properties to be used (optional). When <code>null</code>
     *            the {@link System#getProperties() System properties} are used.
     * 
     * @see Options
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath. See {@link Options#ICU}.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICUJNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public static IKeyBuilder newUnicodeInstance(Properties properties) {
     
        final boolean icu_avail = isICUAvailable();

        log.warn("ICU library is" + (icu_avail ? "" : "not ") + " available.");
        
        /*
         * Allow the caller to require the ICU library. The default depends on
         * whether or not the ICU library is on the class path.
         */
        
        final boolean icu_explicit = getProperty(properties, Options.ICU) != null;

        final boolean icu = Boolean.parseBoolean(getProperty(properties,
                    Options.ICU, "" + icu_avail));
        
        if(icu && !icu_avail) {
            
            /*
             * The ICU library was required but was not located. Make sure that
             * the ICU JAR is on the classpath.
             * 
             * Note: If you are trying to use ICUJNI then that has to be
             * locatable as a native library. How you do this is different for
             * Windows and Un*x.
             */
            
            throw new UnsupportedOperationException(ICU_NOT_AVAILABLE);
            
        }
        
        if (!icu_explicit) {

            /*
             * Issue warning since the choice was made by default rather than
             * explicitly specified by a property.
             */
            
            log.warn("Defaulting to the " + (icu ? "ICU" : "JDK")
                    + " library for Unicode sort keys.");
            
        }
        
        Object strength = null;
        
        if(getProperty(properties,Options.STRENGTH)!=null) {

            String val = getProperty(properties,Options.STRENGTH);
            
            try {

                strength = StrengthEnum.valueOf(val);
                
            } catch(RuntimeException ex) {

                strength = Integer.parseInt(val);

            }
            
            log.info(Options.STRENGTH+"="+strength);
            
        }
        
        DecompositionEnum mode = null;

        if(getProperty(properties,Options.DECOMPOSITION)!=null) {
            
            mode = DecompositionEnum.valueOf(getProperty(properties,Options.DECOMPOSITION));

            log.info(Options.DECOMPOSITION+"="+mode);
            
        }

        return newInstance(DEFAULT_INITIAL_CAPACITY, true/* unicode */, icu,
                null/* locale */, strength, mode);

    }

    /**
     * Create a new instance that optionally supports Unicode sort keys.
     * 
     * @param capacity
     *            The initial capacity of the buffer. When zero (0) the
     *            {@link #DEFAULT_INITIAL_CAPACITY} will be used.
     * @param unicode
     *            When <code>true</code> Unicode sort keys will be supported.
     * @param icu
     *            When <code>true</code> the ICU library will be used.
     * @param locale
     *            When <code>null</code> the
     *            {@link Locale#getDefault() default locale} will be used.
     * @param strength
     *            Either an {@link Integer} or a {@link StrengthEnum} specifying
     *            the strength to be set on the collator object (optional). When
     *            <code>null</code> the default strength of the collator will
     *            not be overriden.
     * @param mode
     *            The decomposition mode to be set on the collator object
     *            (optional). When <code>null</code> the default decomposition
     *            mode of the collator will not be overriden.
     * 
     * @return The new instance.
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICUJNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public static IKeyBuilder newInstance(int capacity, boolean unicode,
            boolean icu, Locale locale, Object strength, DecompositionEnum mode) {
        
        if (unicode) {
            
            if (locale == null) {

                locale = Locale.getDefault();

                log.warn("Using default locale: " + locale.getDisplayName());

            }

            if (icu && !isICUAvailable()) {

                throw new UnsupportedOperationException(ICU_NOT_AVAILABLE);

            }
            
            // create the initial buffer.
            final byte[] buf = createBuffer(capacity);
            
            // the buffer is initially empty.
            final int len = 0;
            
            if(icu) {
                
                return new KeyBuilder(new ICUSortKeyGenerator(locale, strength,
                        mode), len, buf);

            } else {

                return new KeyBuilder(new JDKSortKeyGenerator(locale, strength,
                        mode), len, buf);
                
            }

        } else {
            
            return new KeyBuilder(capacity);
            
        }
       
    }
    
    /**
     * Text of the exception thrown when the ICU library is required but is not
     * available.
     */
    final public static String ICU_NOT_AVAILABLE = "The ICU library is not available.";
    
    /**
     * Figures out whether or not the ICU library is available.
     * 
     * @return <code>true</code> iff the ICU library is available.
     */
    public static boolean isICUAvailable() {
        
        boolean icu_avail;
        
        try {
        
            Class.forName("com.ibm.icu.text.RuleBasedCollator");
            
            icu_avail = true;
            
        } catch(Throwable t) {
            
            log.warn("ICU library is not available");
            
            icu_avail = false;
            
        }

        return icu_avail;
        
    }
    
}
