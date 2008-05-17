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
package com.bigdata.btree;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.RootBlockException;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.MetadataService;

/**
 * A read-only store backed by a file containing a single {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentStore extends AbstractRawStore implements IRawStore {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentStore.class);

    /**
     * A clone of the properties specified to the ctor.
     */
    private final Properties properties;
    
    /**
     * An object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {
        
        return new Properties( properties );
        
    }

    /**
     * The file containing the index segment.
     */
    protected final File file;

    /**
     * Used to correct decode region-based addresses. The
     * {@link IndexSegmentBuilder} encodes region-based addresses using
     * {@link IndexSegmentRegion}. Those addresses are then transparently
     * decoded by this class. The {@link IndexSegment} itself knows nothing
     * about this entire slight of hand.
     */
    private IndexSegmentAddressManager addressManager;
    
    /**
     * See {@link Options#MAX_BYTES_TO_FULLY_BUFFER_NODES}.
     */
    private final long maxBytesToFullyBufferNodes;
    
    /**
     * A buffer containing the disk image of the nodes in the
     * {@link IndexSegment}. While some nodes will be held in memory by the
     * hard reference queue the use of this buffer means that reading a node
     * that has fallen off of the queue does not require any IOs.
     */
    private ByteBuffer buf_nodes;
    
    /**
     * The random access file used to read the index segment.
     */
    private RandomAccessFile raf;

    /**
     * A read-only view of the checkpoint record for the index segment.
     */
    private IndexSegmentCheckpoint checkpoint;

    /**
     * The metadata record for the index segment.
     */
    private IndexMetadata metadata;

    /**
     * The optional bloom filter.
     */
    private BloomFilter bloomFilter;
    
    protected void assertOpen() {

        if (!open) {
            
            throw new IllegalStateException();
            
        }

    }
    
    /**
     * Used to correct decode region-based addresses. The
     * {@link IndexSegmentBuilder} encodes region-based addresses using
     * {@link IndexSegmentRegion}. Those addresses are then transparently
     * decoded by this class. The {@link IndexSegment} itself knows nothing
     * about this entire slight of hand.
     */
    protected final IndexSegmentAddressManager getAddressManager() {
        
        assertOpen();
        
        return addressManager;
        
    }
    
    /**
     * A read-only view of the checkpoint record for the index segment.
     */
    public final IndexSegmentCheckpoint getCheckpoint() {
        
        assertOpen();

        return checkpoint;
        
    }

    /**
     * The {@link IndexMetadata} record for the {@link IndexSegment}.
     * <p>
     * Note: The {@link IndexMetadata#getBranchingFactor()} for the
     * {@link IndexSegment} is overriden by the value of the
     * {@link IndexMetadata#getIndexSegmentBranchingFactor()}.
     * <p>
     * Note: The {@link IndexMetadata#getPartitionMetadata()} always reports
     * that {@link LocalPartitionMetadata#getResources()} is <code>null</code>.
     * This is because the {@link BTree} on the {@link AbstractJournal} defines
     * the index partition view and each {@link IndexSegment} generally
     * participates in MANY views - one per commit point on each
     * {@link AbstractJournal} where the {@link IndexSegment} is part of an
     * index partition view.
     */
    public final IndexMetadata getIndexMetadata() {
    
        assertOpen();
        
        return metadata;
        
    }

    /**
     * Return the optional bloom filter.
     * 
     * @return The bloom filter -or- <code>null</code> iff the bloom filter
     *         was not requested when the {@link IndexSegment} was built.
     */
    public final BloomFilter getBloomFilter() {
        
        assertOpen();
        
        return bloomFilter;
        
    }
    
    /**
     * True iff the store is open.
     */
    private boolean open = false;

    /**
     * Options understood by the {@link IndexSegmentStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
        /**
         * <code>segmentFile</code> - The name of the file to be opened.
         */
        String SEGMENT_FILE = "segmentFile";
        
        /**
         * <code> maxBytesToFullyBufferedNodes</code> - the maximum #of bytes
         * in the node region for which the {@link IndexSegmentStore} will fully
         * buffer (default <code>10485760</code> bytes (10M)). This may be set
         * to a small value, e.g., <code>1L</code>, to disable buffering.
         * When ZERO (<code>0L</code>) the nodes will be fully buffered
         * regardless of the memory requirements. Nodes are buffered when the
         * {@link IndexSegmentStore} is (re-)opened.
         * <p>
         * The nodes in the {@link IndexSegment} are serialized in a contiguous
         * region by the {@link IndexSegmentBuilder}. That region may be fully
         * buffered, in which case queries against the {@link IndexSegment} will
         * incur NO disk hits for the nodes and only one disk hit per visited
         * leaf.
         * 
         * @see #DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES
         */
        String MAX_BYTES_TO_FULLY_BUFFER_NODES = "maxBytesToFullyBufferNodes";
        
        /**
         * @see #MAX_BYTES_TO_FULLY_BUFFER_NODES
         * 
         * FIXME The JVM will allocate a temporary direct buffer when reading in
         * a large chunk from the store and then have problems releasing it.
         * I've disabled buffering of nodes by default for now to avoid this
         * memory leak. This could be addressed using the static pool currently
         * allocated within {@link TemporaryRawStore}.
         */
        //String DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES = ""+Bytes.megabyte*10;
        String DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES = "1";
        
    }
    
    /**
     * Used solely to align the old ctor with the new.
     */
    private static Properties getDefaultProperties(File file) {

        if (file == null)
            throw new IllegalArgumentException();

        Properties p = new Properties();
        
        p.setProperty(Options.SEGMENT_FILE, ""+file);
        
        return p;
        
    }
    
    /**
     * Open a read-only store containing an {@link IndexSegment}.
     * 
     * @param file
     *            The name of the store file.
     * 
     * @deprecated by {@link IndexSegmentStore#IndexSegmentStore(Properties)}
     */
    public IndexSegmentStore(File file) {

        this( getDefaultProperties(file));

    }
    
    /**
     * Open a read-only store containing an {@link IndexSegment}.
     * <p>
     * Note: If an exception is thrown then the backing file will be closed.
     * 
     * @param properties
     *            The properties. See {@link Options}.
     * 
     * @see Options
     * @see #loadIndexSegment()
     * 
     * @throws RuntimeException
     *             if there is a problem.
     * @throws RootBlockException
     *             if the root block is invalid.
     */
    public IndexSegmentStore(Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();

        // clone to avoid side effects from modifications by the caller.
        this.properties = (Properties)properties.clone();
        
        // segmentFile
        {

            String val = properties.getProperty(Options.SEGMENT_FILE);

            if (val == null) {

                throw new RuntimeException("Required property not found: "
                        + Options.SEGMENT_FILE);

            }

            file = new File(val);
            
        }
        
        // maxBytesToFullyBufferNodes
        
        {
            
            maxBytesToFullyBufferNodes = Long.parseLong(properties.getProperty(
                    Options.MAX_BYTES_TO_FULLY_BUFFER_NODES,
                    Options.DEFAULT_MAX_BYTES_TO_FULLY_BUFFER_NODES));

            log.info(Options.MAX_BYTES_TO_FULLY_BUFFER_NODES + "="
                    + maxBytesToFullyBufferNodes);
            
            if (maxBytesToFullyBufferNodes < 0L) {

                throw new RuntimeException(
                        Options.MAX_BYTES_TO_FULLY_BUFFER_NODES
                                + " must be non-negative");
                
            }
            
        }
        
        reopen();

    }
    
    /**
     * Closes out the {@link IndexSegmentStore} iff it is still open.
     * <p>
     * Note: The {@link IndexSegment} has hard reference to the
     * {@link IndexSegmentStore} but not the other way around. Therefore an
     * {@link IndexSegment} will be swept before its store is finalized.
     */
    protected void finalize() throws Exception {
        
        if(open) {

            final String name = MetadataService.getIndexPartitionName(metadata
                    .getName(), metadata.getPartitionMetadata()
                    .getPartitionId());

            if (log.isInfoEnabled())
                log.info("Closing index segment store: " + name + ", file="
                        + getFile());
            
            _close();
            
        }
        
    }

    public String toString() {
        
        // @todo add filename if filename dropped from resourcemetadata.
        return getResourceMetadata().toString();
        
    }

    public IResourceMetadata getResourceMetadata() {
        
        if(!open) reopen();

        return new SegmentMetadata(file, checkpoint.length,
                checkpoint.segmentUUID, checkpoint.commitTime);
        
    }
    
    /**
     * Re-open a closed store. This operation should succeed if the backing file
     * is still accessible.
     * <p>
     * Note: If an exception is thrown then the backing file will be closed.
     * 
     * @throws IllegalStateException
     *             if the store is already open.
     * @throws RootBlockException
     *             if the root block is invalid.
     * @throws RuntimeException
     *             if there is a problem.
     * 
     * @see #close()
     */
    synchronized public void reopen() {

        if (open)
            throw new IllegalStateException("Already open.");
        
        if (!file.exists()) {

            throw new RuntimeException("File does not exist: "
                    + file.getAbsoluteFile());

        }
        
        /*
         * This should already be null (see close()), but make sure reference is
         * cleared first.
         */
        raf = null;

        try {

            // open the file.
            this.raf = new RandomAccessFile(file, "r");

            // read the checkpoint record from the file.
            this.checkpoint = new IndexSegmentCheckpoint(raf);

            if (log.isInfoEnabled())
                log.info(checkpoint.toString());

            // handles transparent decoding of offsets within regions.
            this.addressManager = new IndexSegmentAddressManager(checkpoint);
            
            // Read the metadata record.
            this.metadata = readMetadata();

            /*
             * Read the index nodes from the file into a buffer. If there are no
             * index nodes (that is if everything fits in the root leaf of the
             * index) then we skip this step.
             * 
             * Note: We always read in the root in IndexSegment#_open() and hold
             * a hard reference to the root while the IndexSegment is open.
             */
            if(checkpoint.nnodes == 0) {
                
                this.buf_nodes = null;
                
            } else {

                final boolean bufferNodes = (maxBytesToFullyBufferNodes == 0L)
                        || (checkpoint.extentNodes < maxBytesToFullyBufferNodes);
                
                this.buf_nodes = (bufferNodes ? bufferIndexNodes(raf) : null);

            }

            if (checkpoint.addrBloom == 0L) {

                /*
                 * No bloom filter.
                 */

                this.bloomFilter = null;

            } else {

                /*
                 * Read in the optional bloom filter from its addr.
                 */

                try {

                    this.bloomFilter = readBloomFilter();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

            /*
             * Mark as open so that we can use read(long addr) to read other
             * data (the root node/leaf).
             */
            this.open = true;

        } catch (IOException ex) {

            // clean up.
            _close();

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Load the {@link IndexSegment}. The {@link IndexSegment} (or derived
     * class) MUST provide a public constructor with the following signature:
     * <code>
     * 
     * <i>className</i>(IndexSegmentFileStore store)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * 
     * @return The {@link IndexSegment} or derived class loaded from that store.
     */
    synchronized public IndexSegment loadIndexSegment() {
        
        try {
            
            Class cl = Class.forName(metadata.getClassName());
            
            Constructor ctor = cl
                    .getConstructor(new Class[] { IndexSegmentStore.class });

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
   
    public boolean isReadOnly() {

        assertOpen();

        return true;
        
    }
    
    public boolean isStable() {
        
        return true;
        
    }

    /**
     * Return <code>false</code> since the leaves are not fully buffered even
     * if the nodes are fully buffered.
     */
    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * Return <code>true</code> if the nodes of the {@link IndexSegment} are
     * fully buffered in memory.
     */
    public boolean isNodesFullyBuffered() {
        
        return isOpen() && buf_nodes != null;
        
    }
    
    final public File getFile() {
        
        return file;
        
    }
    
    /**
     * Closes the file and releases the internal buffers and metadata records.
     * This operation may be reversed by {@link #reopen()} as long as the
     * backing file remains available.
     */
    synchronized public void close() {

        log.info("");
        
        assertOpen();
     
        _close();
        
    }
        
    /**
     * Method is safe to invoke whether or not the store is "open" and will
     * always close {@link #raf} (if open), release various buffers, and set
     * {@link #open} to <code>false</code>. All exceptions are trapped, a log
     * message is written, and the exception is NOT re-thrown.
     */
    synchronized private void _close() {
        
        if (raf != null) {

            try {

                raf.close();
                
            } catch (IOException ex) {
                
                log.error("Problem closing file: " + file, ex);
                
                // ignore exception.
                
            }

            raf = null;

        }

        buf_nodes = null;

        checkpoint = null;

        metadata = null;

        bloomFilter = null;
        
        open = false;

    }
    
    synchronized public void deleteResources() {
        
        if (open)
            throw new IllegalStateException();
        
        if(!file.delete()) {
            
            throw new RuntimeException("Could not delete: "
                    + file.getAbsolutePath());
            
        }

    }
    
    synchronized public void closeAndDelete() {
        
        close();

        deleteResources();
        
    }

    final public long write(ByteBuffer data) {

        throw new UnsupportedOperationException();

    }

    final public void force(boolean metadata) {
        
        throw new UnsupportedOperationException();
        
    }
    
    final public long size() {

        assertOpen();
        
        return checkpoint.length;
        
    }

    /**
     * @todo report some interesting stats, but most are on the
     *       {@link IndexSegment} itself.
     * 
     * @todo report the #of reads and whether or not the nodes are buffered.
     * 
     * FIXME report {@link Counters} for the {@link IndexSegment} here (#of
     * nodes read, leaves read, de-serialization times, etc). Some additional
     * counters probably need to be collected (Bloom filter tests, etc).
     */
    synchronized public CounterSet getCounters() {

        if(counterSet==null) {
        
            counterSet = new CounterSet();
            
            counterSet.addCounter("file", new OneShotInstrument<String>(file
                    .toString()));

            // checkpoint
            {
                final CounterSet tmp = counterSet.makePath("checkpoint");
                
                tmp.addCounter("segment UUID", new Instrument<String>() {
                    protected void sample() {
                        final IndexSegmentCheckpoint checkpoint = IndexSegmentStore.this.checkpoint;
                        if (checkpoint != null) {
                            setValue(checkpoint.segmentUUID.toString());
                        }
                    }
                });

                // length in bytes of the file.
                tmp.addCounter("length", new Instrument<Long>() {
                    protected void sample() {
                        final IndexSegmentCheckpoint checkpoint = IndexSegmentStore.this.checkpoint;
                        if (checkpoint != null) {
                            setValue(checkpoint.length);
                        }
                    }
                });

                tmp.addCounter("entries", new Instrument<Integer>() {
                    protected void sample() {
                        final IndexSegmentCheckpoint checkpoint = IndexSegmentStore.this.checkpoint;
                        if (checkpoint != null) {
                            setValue(checkpoint.nentries);
                        }
                    }
                });

                tmp.addCounter("height", new Instrument<Integer>() {
                    protected void sample() {
                        final IndexSegmentCheckpoint checkpoint = IndexSegmentStore.this.checkpoint;
                        if (checkpoint != null) {
                            setValue(checkpoint.height);
                        }
                    }
                });

            }
            
            // metadata
            {
                
                final CounterSet tmp = counterSet.makePath("metadata");
                
                tmp.addCounter("name", new Instrument<String>() {
                    protected void sample() {
                        final IndexMetadata md = metadata;
                        if (md != null) {
                            setValue(md.getName());
                        }
                    }
                });

                tmp.addCounter("index UUID", new Instrument<String>() {
                    protected void sample() {
                        final IndexMetadata md = metadata;
                        if (md != null) {
                            setValue(md.getIndexUUID().toString());
                        }
                    }
                });
                
            }
            
        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;

    /**
     * Read a record from the {@link IndexSegmentStore}. If the request is in
     * the node region and the nodes have been buffered then this uses a slice
     * on the node buffer. Otherwise this reads through to the backing file.
     * <p>
     * Note: An LRU disk cache is a poor choice for the leaves. Since the btree
     * already maintains a cache of the recently touched leaf objects, a recent
     * read against the disk is the best indication that we have that we will
     * NOT want to read that region again soon.
     */
    public ByteBuffer read(long addr) {

        assertOpen();
        
        /*
         * True IFF the starting address lies entirely within the region
         * dedicated to the B+Tree nodes.
         */
        final boolean isNodeAddr = addressManager.isNodeAddr(addr);
        
        if (log.isDebugEnabled()) {

            log.debug("addr=" + addr + "(" + addressManager.toString(addr)
                    + "), isNodeAddr="+isNodeAddr);
            
        }
        
        // abs. offset of the record in the file.
        final long offset = addressManager.getOffset(addr);

        // length of the record.
        final int length = addressManager.getByteCount(addr);

        final ByteBuffer dst;

        if (isNodeAddr && buf_nodes != null) {

            /*
             * The [addr] addresses a node and the data are buffered. Create a
             * view so that concurrent reads do not modify the buffer state.
             * 
             * Note: This is synchronized on [this] for paranoia. As long as the
             * state of [buf_nodes] (its position and limit) can not be changed
             * concurrently this operation should not need to be synchronized.
             */
            final ByteBuffer tmp;
            synchronized(this) {
                
                tmp = buf_nodes.asReadOnlyBuffer();
                
            }

            // correct the offset so that it is relative to the buffer.
            final long off = offset - checkpoint.offsetNodes;

            // set the limit on the buffer to the end of the record.
            tmp.limit((int)(off + length));

            // set the position on the buffer to the start of the record.
            tmp.position((int)off);

            /*
             * Create a slice of that view showing only the desired record. The
             * position() of the slice will be zero(0) and the limit() will be
             * the #of bytes in the record.
             */
            dst = tmp.slice();

        } else {

            /*
             * The data need to be read from the file.
             */

            // Allocate buffer: limit = capacity; pos = 0.
            dst = ByteBuffer.allocate(length);

            try {

                // read into [dst] - does not modify the channel's position().
                FileChannelUtility.readAll(raf.getChannel(), dst, offset);
//                raf.getChannel().read(dst, offset);

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
    private ByteBuffer bufferIndexNodes(RandomAccessFile raf)
            throws IOException {

        if(checkpoint.nnodes == 0) {
        
            throw new IllegalStateException("No nodes.");
            
        }

        if(checkpoint.offsetNodes == 0L) {
            
            throw new IllegalStateException("No nodes.");
            
        }

        if(checkpoint.extentNodes > Integer.MAX_VALUE) {
            
            throw new IllegalStateException(
                    "Nodes region exceeds int32: extent="
                            + checkpoint.extentNodes);
            
        }

        final long offset = checkpoint.offsetNodes;

        final int nbytes = (int) checkpoint.extentNodes;

        if (log.isInfoEnabled())
            log.info("Buffering nodes: #nodes=" + checkpoint.nnodes
                    + ", #bytes=" + nbytes + ", file=" + file);
        
        /*
         * Note: The direct buffer imposes a higher burden on the JVM and all
         * operations after we read the data from the disk should be faster with
         * a heap buffer, so my expectation is that a heap buffer is the correct
         * choice here.
         * 
         * FIXME This would be true except that the JVM will allocate a
         * temporary direct buffer and then have problems releasing it. I've
         * disabled buffering of nodes by default for now to avoid this memory
         * leak. This could be fixed using a static direct buffer pool and then
         * either reading the data into a buffer from that pool and releasing
         * the buffer when the index store is closed -or- using the direct
         * buffer just to copy the data from the store and then releasing it
         * immediately back to the pool.
         */
//        ByteBuffer buf = ByteBuffer.allocateDirect(nbytes);
        final ByteBuffer buf = ByteBuffer.allocate(nbytes);

        FileChannelUtility.readAll(raf.getChannel(), buf, offset);
//        raf.getChannel().read(buf, offset);

        return buf.asReadOnlyBuffer();

    }

    /**
     * Reads the bloom filter directly from the file.
     * 
     * @return The bloom filter -or- <code>null</code> if the bloom filter was
     *         not constructed when the {@link IndexSegment} was built.
     */
    private BloomFilter readBloomFilter() throws IOException {

        final long addr = checkpoint.addrBloom;
        
        if(addr == 0L) {
            
            return null;
            
        }
        
        if (log.isInfoEnabled())
            log.info("reading bloom filter: "+addressManager.toString(addr));
        
        final long off = addressManager.getOffset(addr);
        
        final int len = addressManager.getByteCount(addr);
        
        final ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            FileChannelUtility.readAll(raf.getChannel(), buf, off);
//            final int nread = raf.getChannel().read(buf, off);
//            
//            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        final BloomFilter bloomFilter = (BloomFilter) SerializerUtil.deserialize(buf);

        if (log.isInfoEnabled())
            log.info("Read bloom filter: minKeys=" + bloomFilter.size()
                    + ", entryCount=" + checkpoint.nentries + ", bytesOnDisk="
                    + len + ", errorRate=" + metadata.getErrorRate());

        return bloomFilter;

    }

    /**
     * Reads the {@link IndexMetadata} record directly from the file.
     */
    private IndexMetadata readMetadata() throws IOException {

        final long addr = checkpoint.addrMetadata;
        
        assert addr != 0L;
        
        if (log.isInfoEnabled())
            log.info("reading metadata: "+addressManager.toString(addr));
        
        final long off = addressManager.getOffset(addr);
        
        final int len = addressManager.getByteCount(addr);
        
        final ByteBuffer buf = ByteBuffer.allocate(len);

        buf.limit(len);

        buf.position(0);

        try {

            // read into [dst] - does not modify the channel's position().
            FileChannelUtility.readAll(raf.getChannel(), buf, off);
            
//            final int nread = raf.getChannel().read(buf, off);
//            
//            assert nread == len;
            
            buf.flip(); // Flip buffer for reading.
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        assert buf.position() == 0;
        assert buf.limit() == len;

        final IndexMetadata md = (IndexMetadata) SerializerUtil
                .deserialize(buf);

        if (log.isInfoEnabled())
            log.info("Read metadata: " + md);

        return md;

    }

    /*
     * IAddressManager
     */
    
    final public int getByteCount(long addr) {
        return addressManager.getByteCount(addr);
    }

    final public long getOffset(long addr) {
        return addressManager.getOffset(addr);
    }

    final public void packAddr(DataOutput out, long addr) throws IOException {
        addressManager.packAddr(out, addr);
    }

    final public long toAddr(int nbytes, long offset) {
        return addressManager.toAddr(nbytes, offset);
    }

    final public String toString(long addr) {
        return addressManager.toString(addr);
    }

    final public long unpackAddr(DataInput in) throws IOException {
        return addressManager.unpackAddr(in);
    }
    
}
