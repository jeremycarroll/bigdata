package com.bigdata.btree.raba.codec;

import it.unimi.dsi.fastutil.bytes.ByteArrayFrontCodedList;
import it.unimi.dsi.fastutil.bytes.CustomByteArrayFrontCodedList;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.compression.PrefixSerializer;
import com.bigdata.btree.raba.IRandomAccessByteArray;

/**
 * Class provides (de-)compression for logical byte[][]s based on front coding.
 * The data MUST be ordered. <code>null</code> values are not allowed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Reconcile with {@link PrefixSerializer} (stream-based). It would be
 *       best if they could use the same representation do we could just wrap
 *       the data with a {@link FrontCodedDecoder} without materializing the
 *       byte[][]. This may pose problems since the {@link PrefixSerializer}
 *       current includes the #of bytes in the front-coded representation as
 *       part of its record format.
 */
public class FrontCodedDataCoder implements IDataCoder, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -8195482077263147785L;

    protected static final Logger log = Logger
            .getLogger(FrontCodedDataCoder.class);

    private int ratio;

    /**
     * @param ratio
     *            The ratio as defined by {@link ByteArrayFrontCodedList}. For
     *            front-coding, compression trades directly for search
     *            performance. Every ratio byte[]s is fully coded. Binary search
     *            is used on the fully coded byte[]s and will identify a bucket
     *            <i>ratio</i> front-coded values. Linear search is then
     *            performed within the bucket of front-coded values in which the
     *            key would be found if it is present. Therefore the ratio is
     *            also the maximum of steps in the linear scan.
     *            <p>
     *            Let <code>m := n / ratio</code>, where <i>n</i> is the #of
     *            entries in the <code>byte[][]</code> (the size of the total
     *            search problem), <i>m</i> is the size of the binary search
     *            problem and ratio is the size of the linear search problem.
     *            Solving for ratio, we have: <code>ratio := n / m</code>. Some
     *            examples:
     * 
     *            <pre>
     * m = n(64)/ratio(16) = 4
     * 
     * m = n(64)/ratio(8) = 8
     * 
     * m = n(64)/ratio(6) &tilde; 11
     * 
     * m = n(64)/ratio(4) = 16
     * </pre>
     */
    public FrontCodedDataCoder(final int ratio) {

        this.ratio = ratio;

    }

    public boolean isNullAllowed() {

        return false;

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        ratio = in.readInt();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeInt(ratio);

    }

    public IRabaDecoder encode(final IRandomAccessByteArray raba) {

        // final int size = raba.size();

        if (log.isInfoEnabled())
            log.info("n=" + raba.size() + ", capacity=" + raba.capacity()
                    + ", ratio=" + ratio);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {

            final DataOutputStream dos = new DataOutputStream(baos);

            // // #of entries (zero length indicates NO data)
            // dos.writeInt(size);

            // // the ratio used to front code the data.
            // dos.writeInt(ratio);

            // more than one key.
            final CustomByteArrayFrontCodedList c = new CustomByteArrayFrontCodedList(
                    raba.iterator(), ratio);

            c.getBackingBuffer().writeOn(dos);

            dos.flush();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        final byte[] a = baos.toByteArray();

        // final int headerSize = Bytes.SIZEOF_INT * 1;

        // final ByteBuffer view = ByteBuffer.wrap(a, headerSize, a.length
        // - headerSize).slice();

        return decode(raba.size(), ByteBuffer.wrap(a));

    }

    public IRabaDecoder decode(final int size, final ByteBuffer data) {

        // // The first int32 value in the buffer is the ratio.
        // final int ratio = data.getInt(0);

        // /*
        // * Setup the view using a copy to avoid side effects on the limit
        // * and position of the callers buffer.
        // */
        // final ByteBuffer tmp = data.asReadOnlyBuffer();
        // tmp.limit(data.capacity());
        // tmp.position(Bytes.SIZEOF_INT); // skip ratio.
        //            
        // // Create the view.
        // final ByteBuffer view = tmp.slice();

        // Decode.
        return new FrontCodedDecoder(size, ratio, data);

    }

    /**
     * Decoder for an ordered logical byte[][] without <code>null</code>s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class FrontCodedDecoder implements IRabaDecoder {

        final ByteBuffer data;

        private final CustomByteArrayFrontCodedList decoder;

        /**
         * 
         * @param size
         *            The #of entries in the coded logical byte[][].
         * @param ratio
         *            The ratio used to front-code the data.
         * @param data
         *            The record containing the coded data.
         */
        public FrontCodedDecoder(final int size, final int ratio,
                final ByteBuffer data) {

            this.decoder = new CustomByteArrayFrontCodedList(size, ratio, data);

            this.data = data;

        }

        // /**
        // * The ratio specified when the data were front-coded.
        // */
        // public int ratio() {
        //                
        // return decoder.ratio();
        //                
        // }

        public ByteBuffer data() {

            return data;

        }

        /**
         * Implementation is read-only.
         */
        final public boolean isReadOnly() {

            return true;

        }

        /**
         * Search is supported.
         */
        public boolean isSearchable() {

            return true;

        }

        /**
         * <code>null</code>s are not allowed.
         */
        final public boolean isNullAllowed() {

            return false;

        }

        final public int size() {

            return decoder.size();

        }

        final public int capacity() {

            return decoder.size();

        }

        final public boolean isEmpty() {

            return size() == 0;

        }

        /**
         * Always returns <code>true</code> since the front-coded representation
         * is dense.
         */
        final public boolean isFull() {

            return true;

        }

        /**
         * Always returns <code>false</code> (<code>null</code>s are not
         * allowed).
         */
        final public boolean isNull(final int index) {

            return false;

        }

        final public byte[] get(final int index) {

            return decoder.get(index);

        }

        final public int length(final int index) {

            return decoder.arrayLength(index);

        }

        public int copy(final int index, final OutputStream os) {

            try {

                // FIXME add writeOn(int, DO) to BackingBuffer for this (faster)
                
                final byte[] a = get(index);
                
                os.write(a);

                return a.length;

            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

        }

        public Iterator<byte[]> iterator() {

            return decoder.iterator();

        }

        public int search(final byte[] searchKey) {

            return decoder.search(searchKey);

        }

        public int add(byte[] a) {
            throw new UnsupportedOperationException();
        }

        public int add(byte[] value, int off, int len) {
            throw new UnsupportedOperationException();
        }

        public int add(DataInput in, int len) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void set(int index, byte[] a) {
            throw new UnsupportedOperationException();
        }

    }

}
