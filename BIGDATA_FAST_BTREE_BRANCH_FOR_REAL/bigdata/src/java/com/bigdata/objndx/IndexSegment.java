package com.bigdata.objndx;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.Bytes;

/**
 * An index segment is read-only btree corresponding to some key range of a
 * segmented and potentially distributed index. The file format of the index
 * segment includes a metadata record, the leaves of the segment in key order,
 * and the nodes of the segment in an arbitrary order. It is possible to map or
 * buffer the part of the file containing the index nodes or the entire file
 * depending on application requirements.
 * 
 * Note: iterators returned by this class do not support removal (the nodes and
 * leaves will all refuse mutation operations).
 * 
 * @todo Support efficient leaf scans in forward order (could also do reverse
 *       order by serializing the priorId into the leaf).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegment extends AbstractBTree implements IBTree {

    /**
     * Type safe reference to the backing store.
     */
    final protected FileStore fileStore;
    
    /**
     * The root of the btree. Since this is a read-only index the root can never
     * be replaced.
     */
    final protected AbstractNode root;

    /**
     * An optional bloom filter that will be used to filter point tests. Since
     * bloom filters do not support removal of keys the option to use a filter
     * is restricted to {@link IndexSegment}s since they are read-only data
     * structures.
     */
    final it.unimi.dsi.mg4j.util.BloomFilter bloomFilter;
    
    /**
     * Text of a message used in exceptions for mutation operations on the
     * index segment.
     */
    final protected static String MSG_READ_ONLY = "Read-only index";

    public int getBranchingFactor() {
        
        return fileStore.metadata.branchingFactor;
        
    }

    public ArrayType getKeyType() {
        
        return fileStore.metadata.keyType;
        
    }
    
    public int getHeight() {
        
        return fileStore.metadata.height;
        
    }

    public int getLeafCount() {
        
        return fileStore.metadata.nleaves;
        
    }

    public int getNodeCount() {
        
        return fileStore.metadata.nnodes;
        
    }

    public AbstractNode getRoot() {
        
        return root;
        
    }
    
    public int getEntryCount() {

        return fileStore.metadata.nentries;
        
    }

    /**
     * Open a read-only index segment.
     * 
     * @param fileStore
     *            The store containing the {@link IndexSegment}.
     * @param hardReferenceQueue
     *            The index segment is read only so we do not need to do IO on
     *            eviction. All the listener needs to do is count queue
     *            evictions to collect statistics on the index performance. The
     *            capacity should be relatively low and the #of entries to scan
     *            should be relatively high since each entry is relatively
     *            large, e.g., try with 100 and 20 respectively.
     * @param NEGINF
     * @param comparator
     * @param keySer
     * @param valSer
     * @throws IOException
     * 
     * @todo explore good defaults for the hard reference queue. consider
     *       splitting into a leafQueue and a nodeQueue.
     */
    public IndexSegment(FileStore fileStore,
            HardReferenceQueue<PO> hardReferenceQueue, Object NEGINF,
            Comparator comparator, IKeySerializer keySer,
            IValueSerializer valSer) throws IOException {

        super(fileStore, fileStore.metadata.keyType,
                fileStore.metadata.branchingFactor, hardReferenceQueue, NEGINF,
                comparator, new CustomAddressSerializer(
                        fileStore.metadata.offsetNodes), keySer, valSer,
                ImmutableNodeFactory.INSTANCE);

        // Type-safe reference to the backing store.
        this.fileStore = (FileStore) fileStore;
        
        /*
         * This buffer should be perfectly sized. It is used by the methods on
         * the base class to read a node or leaf from the store.
         * 
         * @todo if there are concurrent reads on the index segment then this
         * buffer should not be shared and would have to be allocated on each
         * read against the file - no great loss - however, the base class
         * assumes a shared instance buffer.
         * 
         * @todo if the index is just a root leaf then we do not need to retain
         * this buffer.
         */
        buf = ByteBuffer.allocateDirect(fileStore.metadata.maxNodeOrLeafLength);
        
        // Read the root node.
        this.root = readNodeOrLeaf(fileStore.metadata.addrRoot);

        // read in the optional bloom filter from its addr.
        if( fileStore.metadata.addrBloom == 0L ) {
        
            this.bloomFilter = null;
            
        } else {

            this.bloomFilter = readBloomFilter(fileStore.metadata.addrBloom);
            
        }
        
    }

    /**
     * Reads the bloom filter from the file.
     * 
     * Note: this goes around the {@link FileStore} API since the bloom filter
     * is not (currently) written as a compressed record and since the size of
     * the largest compressed record does not pay attention to the serialized
     * size of the optional bloom filter.
     */
    protected BloomFilter readBloomFilter(long addr) throws IOException {

        assert addr != 0L;
        
        System.err.println("reading bloom filter: "+Addr.toString(addr));
        
        final int off = Addr.getOffset(addr);
        
        final int len = Addr.getByteCount(addr);
        
        ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            final int nread = fileStore.raf.getChannel().read(buf, off);
            
            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        ByteBufferInputStream bais = new ByteBufferInputStream(buf);
        
//        ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
        
        ObjectInputStream ois = new ObjectInputStream(bais);
        
        try {

            BloomFilter bloomFilter = (BloomFilter) ois.readObject();
            
            log.info("Read bloom filter: minKeys=" + bloomFilter.size()
                    + ", entryCount=" + getEntryCount() + ", bytesOnDisk="
                    + len + ", errorRate=" + fileStore.metadata.errorRate);
            
            return bloomFilter;
            
        }
        
        catch(Exception ex) {
            
            IOException ex2 = new IOException("Could not read bloom filter: "+ex);
            
            ex2.initCause(ex);
            
            throw ex2;
            
        }

    }
    
    /**
     * @todo move to parent class and have various methods test to validate that
     *       the index is open (lookup, insert, remove, scan).
     */
    public void close() {
        
        fileStore.close();
        
    }
    
//    /**
//     * The internal addresses for child nodes found in a node of the index
//     * segment are relative to the start of the index nodes block in the file.
//     * To differentiate them from addresses for leaves, which are correct, the
//     * sign is flipped so that a node address is always a negative integer. This
//     * method looks for the negative address, flips the sign, and adds in the
//     * offset of the node block in the file so that the resulting address
//     * correctly addresses an absolute offset in the file.
//     * 
//     * @param addr
//     *            An {@link Addr}. When negative, the address is for a node and
//     *            must be decoded per the commentary above.
//     * 
//     * @return The node or leaf at that address in the file.
//     * 
//     * @see IndexSegmentBuilder.SimpleNodeData
//     */
//    protected AbstractNode readNodeOrLeaf(long addr) {
//
//        if (addr < 0) {
//    
//            /*
//             * Always a reference to a node as represented in childAddr[] of
//             * some node.
//             */
//            
//            // flip the sign
//            addr = -(addr);
//            
//            // compute the absolute offset into the file.
//            int offset = (int) fileStore.metadata.offsetNodes
//                    + Addr.getOffset(addr);
//            
//            // the size of the record in bytes.
//            int nbytes = Addr.getByteCount(addr);
//            
//            // form an absolute Addr.
//            addr = Addr.toLong(nbytes, offset);
//            
//            // read the node from the file.
//            return (Node) super.readNodeOrLeaf(addr);
//
//        } else {
//            
//            /*
//             * Either a leaf -or- the root node (which does not use an encoded
//             * address!)
//             */
//            
//            // read the node or leaf from the file.
//            return super.readNodeOrLeaf(addr);
//
//        }
//    
//    }

    /**
     * Overrides the base class to use the optional bloom filter when present.
     */
    public Object lookup(Object key) {

        if (key == null) {

            throw new IllegalArgumentException();
            
        }

        if( bloomFilter != null && ! containsKey(key)) {

            /*
             * If the bloom filter reports that the key does not exist then we
             * always believe it.
             */

            counters.nbloomRejects++;
            
            return null;
        
        }
        
        /*
         * Either there is no bloom filter or the bloom filter believes that the
         * key exists. Either way we now lookup the entry in the btree.
         */
        counters.nfinds++;
        
        return getRoot().lookup(key);

    }

    /**
     * Returns true if the optional bloom filter reports that the key exists.
     * 
     * @param key
     *            The key.
     * 
     * @return True if the bloom filter believes that the key is present in the
     *         index. When true, you must still test the key to verify that it
     *         is, in fact, present in the index. When false, you do NOT need to
     *         test the index.
     * 
     * @todo This reuses a type-specific private instance array to test the
     *       bloom filter and is therefore not safe for concurrent callers as
     *       they will overwrite one anothers data. This could be fixed simply
     *       by adding an appropriate synchronized keyword on the method, but
     *       that is not necessary in a single threaded environment.
     * 
     * @todo handle all key types.
     * 
     * @todo examine the #of weights in use by the filter.
     */
    final protected boolean containsKey(Object key) {
        assert bloomFilter != null;
        switch(keyType) {
//        case BYTE: bloomFilter.add(((byte[])keys)[index]); break;
//        case SHORT: bloomFilter.add(((short[])keys)[index]); break;
//        case CHAR: bloomFilter.add(((char[])keys)[index]); break;
        case INT: {
            _bloomKeys_int[0] = ((Integer)key).intValue(); 
            return bloomFilter.contains(_bloomKeys_int);
        }
//        case LONG: bloomFilter.add(((long[])keys)[index]); break;
//        case FLOAT: bloomFilter.add(((float[])keys)[index]); break;
//        case DOUBLE: bloomFilter.add(((double[])keys)[index]); break;
//        case OBJECT: bloomFilter.add(((Object[])keys)[index]); break;
        default: throw new UnsupportedOperationException();
        }
    }
    final private int stride = 1;
    final private int[] _bloomKeys_int = new int[stride];

    /**
     * Operation is disallowed.
     */
    public Object insert(Object key, Object entry) {

        throw new UnsupportedOperationException(MSG_READ_ONLY);

    }

    /**
     * Operation is disallowed.
     */
    public Object remove(Object key) {

        throw new UnsupportedOperationException(MSG_READ_ONLY);

    }
    
    /**
     * Factory for immutable nodes and leaves used by the {@link NodeSerializer}.
     */
    protected static class ImmutableNodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new ImmutableNodeFactory();
        
        private ImmutableNodeFactory() {}
        
        public ILeafData allocLeaf(IBTree btree, long id, int branchingFactor,
                ArrayType keyType, int nkeys, Object keys, Object[] values) {

            return new ImmutableLeaf((AbstractBTree) btree, id,
                    branchingFactor, nkeys, keys, values);

        }

        public INodeData allocNode(IBTree btree, long id, int branchingFactor,
                ArrayType keyType,
//                int nnodes, int nleaves,
                int nentries,
                int nkeys, Object keys, long[] childAddr, int[] childEntryCount) {

            return new ImmutableNode((AbstractBTree) btree, id,
                    branchingFactor,
//                    nnodes, nleaves, 
                    nentries, nkeys, keys,
                    childAddr,
                    childEntryCount
                    );
            
        }

        /**
         * Immutable node throws {@link UnsupportedOperationException} for the
         * public mutator API but does not try to override all low-level
         * mutation behaviors.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class ImmutableNode extends Node {

            /**
             * @param btree
             * @param id
             * @param branchingFactor
             * @param nentries
             * @param nkeys
             * @param keys
             * @param childKeys
             */
            protected ImmutableNode(AbstractBTree btree, long id,
                    int branchingFactor,
//                    int nnodes, int nleaves,
                    int nentries,
                    int nkeys, Object keys, long[] childKeys, int[] childEntryCount) {

                super(btree, id, branchingFactor,
//                        nnodes, nleaves,
                        nentries,
                        nkeys, keys, childKeys, childEntryCount);
                
            }

            public void delete() {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }

            public Object insert(Object key,Object val) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }
            
            public Object remove(Object key) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }
            
        }
        
        /**
         * Immutable leaf throws {@link UnsupportedOperationException} for the
         * public mutator API but does not try to override all low-level
         * mutation behaviors.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class ImmutableLeaf extends Leaf {

            /**
             * @param btree
             * @param id
             * @param branchingFactor
             * @param nkeys
             * @param keys
             * @param values
             */
            protected ImmutableLeaf(AbstractBTree btree, long id, int branchingFactor, int nkeys, Object keys, Object[] values) {
                super(btree, id, branchingFactor, nkeys, keys, values);
            }
            
            public void delete() {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }

            public Object insert(Object key,Object val) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }
            
            public Object remove(Object key) {

                throw new UnsupportedOperationException(MSG_READ_ONLY);
                
            }

        }
        
    }

    /**
     * A read-only store backed by a file. The section of the file containing
     * the index nodes may be fully buffered.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class FileStore implements IRawStore2 {
        
        /**
         * A buffer containing the disk image of the nodes in the index segment.
         * While some nodes will be held in memory by the hard reference queue
         * the use of this buffer means that reading a node that has fallen off
         * of the queue does not require any IOs.
         */
        protected final ByteBuffer buf_nodes;

        /**
         * The file containing the index segment.
         */
        protected final File file;

        /**
         * The random access file used to read the index segment.
         */
        protected final RandomAccessFile raf;

        /**
         * A read-only view of the metadata record for the index segment.
         */
        protected final IndexSegmentMetadata metadata;
        
        /**
         * Used to decompress nodes and leaves as they are read.
         * 
         * @todo we do not need to retain this if the index consists of just a
         *       root leaf.
         */
        protected final RecordCompressor compressor = new RecordCompressor();

        /**
         * Open the read-only store.
         * 
         * @param file
         * 
         * @throws IOException
         * 
         * @todo make it optional to fully buffer the index nodes?
         * @todo make it optional to fully buffer the entire file.
         * @todo hide IOException?
         */
        public FileStore(File file) throws IOException {
            
            if (file == null)
                throw new IllegalArgumentException();
            
            this.file = file;
            
            if (!file.exists()) {

                throw new IOException("File does not exist: "
                        + file.getAbsoluteFile());

            }

            // open the file.
            this.raf = new RandomAccessFile(file, "r");

            // read the metadata record from the file.
            this.metadata = new IndexSegmentMetadata(raf);

            log.info(metadata.toString());
            
            /*
             * Read the index nodes from the file into a buffer. If there are no
             * index nodes then we skip this step. Note that we always read in
             * the root, so if the index is just a root leaf then the root will
             * be a deserialized object and the file will not be buffered in
             * memory.
             */
            this.buf_nodes = (metadata.nnodes > 0 ? bufferIndexNodes(raf) : null);

            this.open = true;
            
        }

        /**
         * Close the read-only store.
         */
        public void close() {
            
            if( !open ) throw new IllegalStateException();

            try {

                raf.close();
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            open = false;
            
        }
        
        private boolean open = false;
        
        public void delete(long addr) {

            throw new UnsupportedOperationException();
            
        }

        public long write(ByteBuffer data) {
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Read from the index segment. If the request is in the node region and
         * the nodes have been buffered then this uses a slice on the node
         * buffer. Otherwise this reads through to the backing file. In either
         * case the data are decompressed before they are returned to the
         * caller.
         * 
         * @param addr
         *            The address (encoding both the offset and the length).
         * 
         * @return A read-only buffer containing the data at that address.
         * 
         * @todo javadoc: this method uses [dst], which should be big enough for
         *       any record serialized on the store based on the index segment
         *       metadata, but always returns a shared instance buffer internal
         *       to the {@link RecordCompressor}.
         */
        public ByteBuffer read(long addr, ByteBuffer dst) {

            if(!open) throw new IllegalStateException();
            
            /*
             * The caller generally passes in [buf], but this is in keeping with
             * our API contract. (One exception is when we read the bloom filter
             * from the store).
             */
            if (dst == null) {
                
                dst = ByteBuffer.allocate(Addr.getByteCount(addr));

            }
         
            final int offset = Addr.getOffset(addr);

            final int length = Addr.getByteCount(addr);

            if (offset >= metadata.offsetNodes && buf_nodes != null) {

                /*
                 * the data are buffered. create a slice onto the read-only
                 * buffer that reveals only those bytes that contain the desired
                 * node. the position() of the slice will be zero(0) and the
                 * limit() will be the #of bytes in the compressed record.
                 */

                // correct the offset so that it is relative to the buffer.
                int off = offset - (int)metadata.offsetNodes;
                
                // set the limit on the buffer to the end of the record.
                System.err.println("offset="+offset+", length="+length);
                buf_nodes.limit(off + length);

                // set the position on the buffer to the start of the record.
                buf_nodes.position(off);
                
                // create a slice of that view.
                dst = buf_nodes.slice();
                
            } else {

                /*
                 * the data need to be read from the file.
                 */

                dst.limit(length);

                dst.position(0);

                try {

                    // read into [dst] - does not modify the channel's position().
                    raf.getChannel().read(dst, offset);

                    dst.flip(); // Flip buffer for reading.
                    
                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }
            
            /*
             * Decompress the data, returning a view into a shared instance
             * buffer.
             * 
             * Note: [dst] contains the compressed data. position() is the start
             * of the compressed record, and may be a view onto a buffered
             * region of the file. limit() is set to the first byte beyond the
             * end of the compressed record.
             * 
             * Note: The returned buffer will be a view onto a shared instance
             * buffer held internally by the RecordCompressor.
             */

            return compressor.decompress(dst); // Decompress.

        }
        
        /**
         * Reads the index nodes into a buffer.
         * 
         * @return A read-only view of a buffer containing the index nodes.
         */
        protected ByteBuffer bufferIndexNodes(RandomAccessFile raf)
                throws IOException {

            long start = metadata.offsetNodes;

            long length = metadata.length - start;

            if (length > Integer.MAX_VALUE)
                throw new RuntimeException();

            final int capacity = (int) length;

            ByteBuffer buf = ByteBuffer.allocateDirect(capacity);

            raf.getChannel().read(buf, start);

            return buf.asReadOnlyBuffer();

        }

    }

    /**
     * <p>
     * A custom serializer class provides a workaround for node offsets (which
     * are relative to the start of the nodes in the file) in contract to leaf
     * offsets (which are relative to the start of the file). This condition
     * arises as a side effect of serializing nodes onto a temporary channel at
     * the same time that the {@link IndexSegmentBuilder} is serializing leaves
     * onto the primary channel.
     * </p>
     * <p>
     * Addresses are required to be left-shifted by one bit on the
     * {@link INodeData} interface during serialization and the low bit must be
     * a one (1) iff the address is of a child node and a zero (0) iff the
     * address is of a child leaf. During de-serialization, the low bit is
     * examined so that the address may be appropriately decoded and the addr is
     * then right shifted one bit. A leaf address does not require further
     * decodiing. Decoding for a node address requires that we add in the offset
     * of the start of the nodes in the file, which is recorded in
     * {@link IndexSegmentMetadata#offsetNodes} and is specified as a parameter
     * to the {@link CustomAddressSerializer} constructor.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CustomAddressSerializer implements IAddressSerializer {

        /**
         * The offset within the file of the first node. All nodes are written
         * densely on the file beginning at this offset. The child addresses for
         * a node are relative to this offset and must be corrected during
         * decoding (this is handled automatically by this class).
         */
        protected final int offsetNodes;

        /**
         * Constructor variant used when the offset of the nodes is not known.
         * This is used by the {@link IndexSegmentBuilder}. When using this
         * constructor de-serialization of addresses is disabled.
         */
        public CustomAddressSerializer() {
            
            this.offsetNodes = 0;
            
        }

        /**
         * 
         * @param nodesOffset
         *            The offset within the file of the first node. All nodes
         *            are written densely on the file beginning at this offset.
         *            The child addresses for a node are relative to this offset
         *            and must be corrected during decoding (this is handled
         *            automatically by this class). When zero(0) node
         *            deserialization is not permitted (the nodesOffset will be
         *            zero in the metadata record iff no nodes were generated by
         *            the index segment builder).
         * 
         * @see IndexSegmentMetadata#offsetNodes
         */
        public CustomAddressSerializer(long offsetNodes) {
            
            /*
             * Note: trim to int (we restrict the maximum size of the segment).
             */
            this.offsetNodes = (int) offsetNodes;
            
            System.err.println("offsetNodes="+offsetNodes);
            
        }
        
        /**
         * This over-estimates the space requirements. 
         */
        public int getSize(int n) {
            
            return Bytes.SIZEOF_LONG * n;
            
        }

        /**
         * Packs the addresses, which MUST already have been encoded according
         * to the conventions of this class.
         */
        public void putChildAddresses(DataOutputStream os, long[] childAddr, int nchildren) throws IOException {
            
            for (int i = 0; i < nchildren; i++) {

                long addr = childAddr[i];

                /*
                 * Children MUST have assigned persistent identity.
                 */
                if (addr == 0L) {

                    throw new RuntimeException("Child is not persistent: index="
                            + i);

                }

                // test the low bit.  when set this is a node; otherwise a leaf.
                final boolean isLeaf = (addr & 1) == 0;
                
                // strip off the low bit.
                addr >>= 1;
                
                final int offset = Addr.getOffset(addr);
                
                final int nbytes = Addr.getByteCount(addr);
                
                final int adjustedOffset = (isLeaf ? (offset << 1)
                        : ((offset << 1) | 1));
                
                // write the adjusted offset (requires decoding).
                LongPacker.packLong(os, adjustedOffset);
                
                // write the #of bytes (does not require decoding).
                LongPacker.packLong(os, nbytes);

            }

        }

        /**
         * Unpacks and decodes the addresses.
         */
        public void getChildAddresses(DataInputStream is, long[] childAddr,
                int nchildren) throws IOException {

            // check that we know the offset for deserialization.
            assert offsetNodes > 0;
            
            for (int i = 0; i < nchildren; i++) {

                /*
                 * Note: the Address is packed as two long integers. The first
                 * is the offset. The way the packed values are written, the
                 * offset is left-shifted by one and its low bit indicates
                 * whether the referent is a node (1) or a leaf (0).
                 */
                
                /*
                 * offset (this field must be decoded).
                 */
                long v = LongPacker.unpackLong(is);
                
                assert v <= Integer.MAX_VALUE;
                
                // test the low bit.  when set this is a node; otherwise a leaf.
                final boolean isLeaf = (v & 1) == 0;

                // right shift by one to remove the low bit.
                v >>= 1;

                // compute the real offset into the file.
                final int offset = isLeaf? (int)v : (int)v + offsetNodes;
                
                /*
                 * nbytes (this field does not need any further interpretation).
                 */
                
                v = LongPacker.unpackLong(is);
                
                assert v <= Integer.MAX_VALUE;
                
                final int nbytes = (int) v;

                /*
                 * combine into the correct address.
                 */
                final long addr = Addr.toLong(nbytes, offset);
                
                if (addr == 0L) {

                    throw new RuntimeException(
                            "Child does not have persistent address: index=" + i);

                }

                childAddr[i] = addr;

            }

        }

        /**
         * Encode an address. The address is left shifted by one bit. If the
         * address is of a node then the low bit is set to one (1) otherwise it
         * will be zero(0).
         * 
         * @param nbytes
         *            The #of bytes in the allocation.
         * @param offset
         *            The offset of the allocation.
         * @param isLeaf
         *            true iff this is the address of a leaf and false iff this
         *            is the address of a node.
         * 
         * @return The encoded address.
         */
        static public long encode(int nbytes,int offset,boolean isLeaf) {
            
            long addr = Addr.toLong(nbytes, (int) offset);
            
            addr <<= 1; // (addr << 1)
            
            if (!isLeaf) {
                
                addr |= 1; // addr++;
            
            }
            
            return addr;
            
        }

    }
    
}
