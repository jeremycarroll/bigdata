package com.bigdata.btree;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * A read-only store backed by a file. The section of the file containing the
 * index nodes may be fully buffered.
 * <p>
 * Note: An LRU disk cache is a poor choice for the leaves. Since the btree
 * already maintains a cache of the recently touched leaf objects, a recent read
 * against the disk is the best indication that we have that we will not want to
 * read that region again soon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentFileStore implements IRawStore {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentFileStore.class);

    /**
     * A buffer containing the disk image of the nodes in the index segment.
     * While some nodes will be held in memory by the hard reference queue
     * the use of this buffer means that reading a node that has fallen off
     * of the queue does not require any IOs.
     */
    private ByteBuffer buf_nodes;

    /**
     * The file containing the index segment.
     */
    protected final File file;

    /**
     * The random access file used to read the index segment.
     */
    private RandomAccessFile raf;

    /**
     * A read-only view of the metadata record for the index segment.
     */
    protected IndexSegmentMetadata metadata;

    /**
     * A read-only view of the extension metadata record for the index segment.
     */
    protected IndexSegmentExtensionMetadata extensionMetadata;
    
    /**
     * True iff the store is open.
     */
    private boolean open = false;

    /**
     * Open the read-only store.
     * 
     * @param file
     * 
     * @throws IOException
     * 
     * @todo make it optional to fully buffer the index nodes?
     * @todo make it optional to fully buffer the leaves as well as the nodes?
     * 
     * @see #load()
     */
    public IndexSegmentFileStore(File file) {

        if (file == null)
            throw new IllegalArgumentException();

        this.file = file;

        reopen();

    }

    /**
     * Re-open a closed store. This operation should succeed if the backing file
     * is still accessible.
     * 
     * @exception IllegalStateException
     *                if the store is not closed.
     * 
     * @see #close()
     */
    public void reopen() {

        if (open)
            throw new IllegalStateException("Already open.");
        
        if (!file.exists()) {

            throw new RuntimeException("File does not exist: "
                    + file.getAbsoluteFile());

        }

        try {

            // open the file.
            this.raf = new RandomAccessFile(file, "r");

            // read the metadata record from the file.
            this.metadata = new IndexSegmentMetadata(raf);

            IndexSegment.log.info(metadata.toString());

            /*
             * Read in the extension metadata record.
             */
            this.extensionMetadata = readExtensionMetadata();

            /*
             * Read the index nodes from the file into a buffer. If there are no
             * index nodes then we skip this step. Note that we always read in
             * the root, so if the index is just a root leaf then the root will
             * be a deserialized object and the file will not be buffered in
             * memory.
             */
            this.buf_nodes = (metadata.nnodes > 0 ? bufferIndexNodes(raf)
                    : null);

            /*
             * Mark as open so that we can use read(long addr) to read other
             * data (the root node/leaf).
             */
            this.open = true;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Load the {@link IndexSegment} or derived class from the store. The
     * {@link IndexSegment} or derived class MUST provide a public constructor
     * with the following signature: <code>
     * 
     * <i>className</i>(IndexSegmentFileStore store)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * 
     * @return The {@link IndexSegment} or derived class loaded from that store.
     * 
     * @see IndexSegmentExtensionMetadata, which provides a metadata extension
     *      protocol for the {@link IndexSegment}.
     */
    public IndexSegment load() {
        
        try {
            
            Class cl = Class.forName(extensionMetadata.getClassName());
            
            Constructor ctor = cl
                    .getConstructor(new Class[] { IndexSegmentFileStore.class });

            IndexSegment seg = (IndexSegment) ctor
                    .newInstance(new Object[] { this });
            
            return seg;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    public boolean isOpen() {
        
        return open;
        
    }
   
    public boolean isStable() {
        
        return true;
        
    }
    
    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    public File getFile() {
        
        return file;
        
    }
    
    /**
     * Closes the file and releases the internal buffers and metadata records.
     * This operation may be reversed by {@link #reopen()} as long as the
     * backing file remains available.
     */
    public void close() {

        if (!open)
            throw new IllegalStateException();

        try {

            raf.close();
            
            raf = null;
            
            buf_nodes = null;

            metadata = null;
            
            extensionMetadata = null;
            
            open = false;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public void closeAndDelete() {
        
        close();
        
        if(!file.delete()) {
            
            System.err.println("WARN: Could not delete: "+file.getAbsolutePath());
            
        }
        
    }

    public long write(ByteBuffer data) {

        throw new UnsupportedOperationException();

    }

    public void force(boolean metadata) {
        
        throw new UnsupportedOperationException();
        
    }
    
    public long size() {
        
        return metadata.length;
        
    }

    /**
     * Read from the index segment. If the request is in the node region and
     * the nodes have been buffered then this uses a slice on the node
     * buffer. Otherwise this reads through to the backing file.
     */
    public ByteBuffer read(long addr) {

        if (!open)
            throw new IllegalStateException();

        final int offset = Addr.getOffset(addr);

        final int length = Addr.getByteCount(addr);
        
        final int offsetNodes = Addr.getOffset(metadata.addrNodes);

        ByteBuffer dst;

        if (offset >= offsetNodes && buf_nodes != null) {

            /*
             * the data are buffered. create a slice onto the read-only
             * buffer that reveals only those bytes that contain the desired
             * node. the position() of the slice will be zero(0) and the
             * limit() will be the #of bytes in the compressed record.
             */

            // correct the offset so that it is relative to the buffer.
            int off = offset - offsetNodes;

            // System.err.println("offset="+offset+", length="+length);

            // set the limit on the buffer to the end of the record.
            buf_nodes.limit(off + length);

            // set the position on the buffer to the start of the record.
            buf_nodes.position(off);

            // create a slice of that view.
            dst = buf_nodes.slice();

        } else {

            // Allocate buffer.
            dst = ByteBuffer.allocate(length);

            /*
             * the data need to be read from the file.
             */

            dst.limit(length);

            dst.position(0);

            try {

                // read into [dst] - does not modify the channel's position().
                raf.getChannel().read(dst, offset);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            dst.flip(); // Flip buffer for reading.

        }

        return dst;

    }

    /**
     * Reads the index nodes into a buffer.
     * 
     * @return A read-only view of a buffer containing the index nodes.
     */
    protected ByteBuffer bufferIndexNodes(RandomAccessFile raf)
            throws IOException {

        if(metadata.addrNodes == 0L) {
            
            throw new IllegalStateException("No nodes.");
            
        }
        
        final int offset = Addr.getOffset(metadata.addrNodes);

        final int nbytes = Addr.getByteCount(metadata.addrLeaves);

        /*
         * Note: The direct buffer imposes a higher burden on the JVM and all
         * operations after we read the data from the disk should be faster with
         * a heap buffer, so my expectation is that a heap buffer is the correct
         * choice here.
         */
//        ByteBuffer buf = ByteBuffer.allocateDirect(nbytes);
        ByteBuffer buf = ByteBuffer.allocate(nbytes);

        raf.getChannel().read(buf, offset);

        return buf.asReadOnlyBuffer();

    }

    /**
     * Reads the bloom filter directly from the file.
     * 
     * @return The bloom filter -or- <code>null</code> if the bloom filter was
     *         not constructed when the {@link IndexSegment} was built.
     */
    protected BloomFilter readBloomFilter() throws IOException {

        final long addr = metadata.addrBloom;
        
        if(addr == 0L) {
            
            return null;
            
        }
        
        log.info("reading bloom filter: "+Addr.toString(addr));
        
        final int off = Addr.getOffset(addr);
        
        final int len = Addr.getByteCount(addr);
        
        ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            final int nread = raf.getChannel().read(buf, off);
            
            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

//        ByteBufferInputStream bais = new ByteBufferInputStream(buf);
//        
////        ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
//        
//        ObjectInputStream ois = new ObjectInputStream(bais);
//        
//        try {
//
//            BloomFilter bloomFilter = (BloomFilter) ois.readObject();
//            
//            log.info("Read bloom filter: minKeys=" + bloomFilter.size()
//                    + ", entryCount=" + metadata.nentries + ", bytesOnDisk="
//                    + len + ", errorRate=" + metadata.errorRate);
//            
//            return bloomFilter;
//            
//        }
//        
//        catch(Exception ex) {
//            
//            IOException ex2 = new IOException("Could not read bloom filter: "+ex);
//            
//            ex2.initCause(ex);
//            
//            throw ex2;
//            
//        }

      BloomFilter bloomFilter = (BloomFilter) SerializerUtil.deserialize(buf);
      
      log.info("Read bloom filter: minKeys=" + bloomFilter.size()
              + ", entryCount=" + metadata.nentries + ", bytesOnDisk="
              + len + ", errorRate=" + metadata.errorRate);
      
      return bloomFilter;

    }
    
    /**
     * Reads the {@link IndexSegmentExtensionMetadata} record directly from the
     * file.
     */
    protected IndexSegmentExtensionMetadata readExtensionMetadata() throws IOException {

        final long addr = metadata.addrExtensionMetadata;
        
        assert addr != 0L;
        
        log.info("reading extension metadata record: "+Addr.toString(addr));
        
        final int off = Addr.getOffset(addr);
        
        final int len = Addr.getByteCount(addr);
        
        ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            final int nread = raf.getChannel().read(buf, off);
            
            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        IndexSegmentExtensionMetadata extensionMetadata = (IndexSegmentExtensionMetadata) SerializerUtil
                .deserialize(buf);

        log.info("Read extension metadata: " + extensionMetadata);

        return extensionMetadata;

    }
    
}
