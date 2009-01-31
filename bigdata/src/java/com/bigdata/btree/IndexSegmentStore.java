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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.IValueAge;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.RootBlockException;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StoreManager;

/**
 * A read-only store backed by a file containing a single {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentStore extends AbstractRawStore implements IRawStore,
        IValueAge {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentStore.class);

    protected static final boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

//    /**
//     * A clone of the properties specified to the ctor.
//     */
//    private final Properties properties;
//    
//    /**
//     * An object wrapping the properties specified to the ctor.
//     */
//    public Properties getProperties() {
//        
//        return new Properties( properties );
//        
//    }

    /**
     * The file containing the index segment.
     */
    protected final File file;

    /**
     * The mode that will be used to open the {@link #file} .
     */
    protected static final String mode = "r"; 
    
    /**
     * Used to correct decode region-based addresses. The
     * {@link IndexSegmentBuilder} encodes region-based addresses using
     * {@link IndexSegmentRegion}. Those addresses are then transparently
     * decoded by this class. The {@link IndexSegment} itself knows nothing
     * about this entire slight of hand.
     */
    private final IndexSegmentAddressManager addressManager;
    
//    /**
//     * See {@link IndexMetadata.Options#INDEX_SEGMENT_BUFFER_NODES}.
//     */
//    private boolean bufferNodes;
//    
//    /**
//     * See {@link IndexMetadata.Options#INDEX_SEGMENT_LEAF_CACHE_SIZE}
//     */
//    protected int leafCacheSize;
    
    /**
     * An optional <strong>direct</strong> {@link ByteBuffer} containing a disk
     * image of the nodes in the {@link IndexSegment}.
     * <p>
     * Note: This buffer is acquired from the {@link DirectBufferPool} and MUST
     * be released back to that pool.
     * <p>
     * Note: While some nodes will be held in memory by the hard reference queue
     * the use of this buffer means that reading a node that has fallen off of
     * the queue does not require any IO.
     */
    private ByteBuffer buf_nodes;
    
    /**
     * The random access file used to read the index segment.
     */
    private RandomAccessFile raf;

    /**
     * A read-only view of the checkpoint record for the index segment.
     */
    private final IndexSegmentCheckpoint checkpoint;

    /**
     * The metadata record for the index segment.
     */
    private final IndexMetadata indexMetadata;

    private static class IndexSegmentStoreCounters {

        /**
         * The #of times the store was (re-)opened.
         */
        long openCount;

        /**
         * The #of times the store was closed.
         */
        long closeCount;

        /**
         * A read on a node (whether or not it is buffered).
         */
        long nodesRead;

        /**
         * A read on a node when the nodes are not buffered.
         */
        long nodesReadFromDisk;

        /**
         * A read on a leaf (always reads through to the disk).
         */
        long leavesReadFromDisk;

    }
    
    private final IndexSegmentStoreCounters counters = new IndexSegmentStoreCounters();
    
//    /**
//     * The optional bloom filter.
//     */
//    private BloomFilter bloomFilter;
    
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

        if (checkpoint == null)
            throw new IllegalStateException();
        
//        assertOpen();

        return checkpoint;
        
    }

    /**
     * The {@link IndexMetadata} record for the {@link IndexSegment}.
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
        
        return indexMetadata;
        
    }

//    /**
//     * Return the optional bloom filter.
//     * 
//     * @return The bloom filter -or- <code>null</code> iff the bloom filter
//     *         was not requested when the {@link IndexSegment} was built.
//     */
//    public final BloomFilter getBloomFilter() {
//        
//        assertOpen();
//        
//        return bloomFilter;
//        
//    }
    
    /**
     * True iff the store is open.
     */
    private boolean open = false;

//    /**
//     * Options understood by the {@link IndexSegmentStore}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public interface Options {
//        
//    }
    
    /**
     * Open a read-only store containing an {@link IndexSegment}, but does not
     * load the {@link IndexSegment} from the store.
     * <p>
     * Note: If an exception is thrown then the backing file will be closed.
     * <p>
     * Note: Normally access to {@link IndexSegmentStore}s is mediated by the
     * {@link StoreManager} which imposes a canonicalizing weak value cache to
     * ensure that we do not double-open an {@link IndexSegmentStore}.
     * 
     * @param file
     *            The file
     * 
     * @see #loadIndexSegment()
     * 
     * @throws RuntimeException
     *             if there is a problem.
     * @throws RootBlockException
     *             if the root block is invalid.
     */
    public IndexSegmentStore(final File file) {

        if (file == null)
            throw new IllegalArgumentException();

        // segmentFile
        {

            this.file = file;
            
            try {
                
                // open the file.
                this.raf = new RandomAccessFile(file, mode);

                // read the checkpoint record from the file.
                this.checkpoint = new IndexSegmentCheckpoint(raf);
                
                // handles transparent decoding of offsets within regions.
                this.addressManager = new IndexSegmentAddressManager(checkpoint);
                
                // Read the metadata record.
                this.indexMetadata = readMetadata();
                
            } catch (IOException ex) {

                if (raf != null) {

                    try {

                        // close the backing file on error during open.
                        raf.close();
                    
                    } catch (Throwable t) {
                    
                        log.warn("Ignored: " + t);

                        // fall through.
                        
                    }

                }

                throw new RuntimeException(ex);
                
            }

            if (INFO)
                log.info(checkpoint.toString());

        }
        
        /*
         * Mark as open so that we can use read(long addr) to read other
         * data (the root node/leaf).
         */
        this.open = true;

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

            _close();
            
        }
        
    }

    public String toString() {

        /*
         * Note: Only depends on final fields.
         */
        // @todo add filename if filename dropped from resourcemetadata.
        return getResourceMetadata().toString();
        
    }

    public IResourceMetadata getResourceMetadata() {

        /*
         * Note: Only depends on final fields.
         */
//        if(!open) reopen();

        return new SegmentMetadata(file, //checkpoint.length,
                checkpoint.segmentUUID, checkpoint.commitTime);
        
    }
    
    /**
     * Re-open a (possibly closed) store. This operation should succeed if the
     * backing file is still accessible.
     * <p>
     * Note: If an exception is thrown then the backing file will be closed.
     * 
     * @throws RootBlockException
     *             if the root block is invalid.
     * @throws RuntimeException
     *             if there is a problem, including a
     *             {@link FileNotFoundException}.
     * 
     * @see #close()
     */
    synchronized public void reopen() {

        if (open)
            throw new IllegalStateException("Already open.");
        
//        if (!file.exists()) {
//
//            throw new RuntimeException("File does not exist: "
//                    + file.getAbsoluteFile());
//
//        }
        
//        /*
//         * This should already be null (see close()), but make sure reference is
//         * cleared first.
//         */
//        raf = null;

        try {

            // open the file.
            if (this.raf == null) {

                // Note: Throws FileNotFoundException
                this.raf = new RandomAccessFile(file, mode);

            }

//            final boolean bufferNodes = metadata.getIndexSegmentBufferNodes();
//
//            /*
//             * Read the index nodes from the file into a buffer. If there are no
//             * index nodes (that is if everything fits in the root leaf of the
//             * index) then we skip this step.
//             * 
//             * Note: We always read in the root in IndexSegment#_open() and hold
//             * a hard reference to the root while the IndexSegment is open.
//             */
//            if (checkpoint.nnodes > 0 && bufferNodes) {
//
//                bufferIndexNodes();
//
//            }
//
//            if (checkpoint.addrBloom != 0L) {
//
//                // Read in the optional bloom filter from its addr.
//                this.bloomFilter = readBloomFilter();
//
//            }

            /*
             * Mark as open so that we can use read(long addr) to read other
             * data (the root node/leaf).
             */
            this.open = true;
            
            counters.openCount++;

        } catch (Throwable t) {

            // clean up.
            _close();

            // re-throw the exception.
            throw new RuntimeException("Could not (re-) open: file=" + file, t);

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
     * <p>
     * Note: Normally access to {@link IndexSegment}s is mediated by the
     * {@link StoreManager} which imposes a canonicalizing weak value cache to
     * ensure that we do not double-open an {@link IndexSegment}.
     * 
     * @param store
     *            The store.
     * 
     * @return The {@link IndexSegment} or derived class loaded from that store.
     */
    synchronized public IndexSegment loadIndexSegment() {

        IndexSegment seg = ref == null ? null : ref.get();

        if (seg != null) {

            // ensure "open".
            seg.reopen();
            
        } else {

            try {

                final Class cl = Class.forName(indexMetadata
                        .getBTreeClassName());

                final Constructor ctor = cl
                        .getConstructor(new Class[] { IndexSegmentStore.class });

                seg = (IndexSegment) ctor.newInstance(new Object[] { this });

                ref = new WeakReference<IndexSegment>(seg);

                /*
                 * Attach the counters maintained by AbstractBTree to those
                 * reported for the IndexSegmentStore.
                 * 
                 * Note: These counters are only allocated when the IndexSegment
                 * object is created and this is where we enforce a 1:1
                 * correspondence between an IndexSegmentStore and the
                 * IndexSegment loaded from that store.
                 */

                getCounters().attach(seg.counters.getCounters());

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }

        return seg;
        
    }
    private volatile WeakReference<IndexSegment> ref = null;
    
    final public boolean isOpen() {
        
        return open;
        
    }
   
    final public boolean isReadOnly() {

        assertOpen();

        return true;
        
    }
    
    final public boolean isStable() {
        
        return true;
        
    }

    /**
     * Return <code>false</code> since the leaves are not fully buffered even
     * if the nodes are fully buffered.
     */
    final public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * Return <code>true</code> if the nodes of the {@link IndexSegment} are
     * fully buffered in memory. The result is consistent as of the time that
     * this method examines the state of the {@link IndexSegmentStore}.
     */
    public boolean isNodesFullyBuffered() {
        
        synchronized(this) {

            return isOpen() && buf_nodes != null;
            
        }
        
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

        if(INFO)
            log.info(file.toString());
        
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

        if (buf_nodes != null) {

            try {

                // release the buffer back to the pool.
                DirectBufferPool.INSTANCE.release(buf_nodes);

            } catch(Throwable t) {
                
                // log error but continue anyway.
                log.error(this, t);
                
            } finally {

                // clear reference since buffer was released.
                buf_nodes = null;
                
            }
            
        }

        /*
         * Note: Don't deallocate. It is small and holds useful metadata such as
         * the #of index entries that we would always like to have on hand.
         */
//        checkpoint = null;

        /*
         * Note: Don't deallocate. Relatively small and it holds some important
         * metadata. By reading this during the ctor we do not have to force the
         * entire index segment to be loaded just to access the index metadata.
         */
//        metadata = null;

        // Note: field was moved to IndexSegment.
//        bloomFilter = null;
        
        open = false;
        
        counters.closeCount++;
        
        if (INFO)
            log.info("Closed: file=" + getFile());
        
    }
    
    synchronized public void deleteResources() {
        
        if (open)
            throw new IllegalStateException();
        
        if(!file.delete()) {
            
            throw new RuntimeException("Could not delete: "
                    + file.getAbsolutePath());
            
        }

    }
    
    synchronized public void destroy() {
        
        if(isOpen()) {

            close();
            
        }

        deleteResources();
        
    }

    final public long write(ByteBuffer data) {

        throw new UnsupportedOperationException();

    }

    final public void force(boolean metadata) {
        
        throw new UnsupportedOperationException();
        
    }
    
    final public long size() {

//        assertOpen();
        
        return checkpoint.length;
        
    }

    synchronized public CounterSet getCounters() {

        if (counterSet == null) {
        
            counterSet = new CounterSet();
            
            counterSet.addCounter("file", new OneShotInstrument<String>(file
                    .toString()));

            // checkpoint (counters are all oneshot).
            {
                
                final CounterSet tmp = counterSet.makePath("checkpoint");
                
                tmp.addCounter("segment UUID", new OneShotInstrument<String>(
                        checkpoint.segmentUUID.toString()));

                // length in bytes of the file.
                tmp.addCounter("length", new OneShotInstrument<String>(
                        Long.toString(checkpoint.length)));

                tmp.addCounter("#nodes", new OneShotInstrument<String>(
                        Long.toString(checkpoint.nnodes)));

                tmp.addCounter("#leaves", new OneShotInstrument<String>(
                        Long.toString(checkpoint.nleaves)));

                tmp.addCounter("#entries", new OneShotInstrument<String>(
                        Long.toString(checkpoint.nentries)));

                tmp.addCounter("height", new OneShotInstrument<String>(
                        Long.toString(checkpoint.height)));

            }
            
            // metadata (all oneshot).
            {
                
                final CounterSet tmp = counterSet.makePath("metadata");
                
                tmp.addCounter("name", new OneShotInstrument<String>(
                        indexMetadata.getName()));

                tmp.addCounter("index UUID", new OneShotInstrument<String>(
                        indexMetadata.getIndexUUID().toString()));
                
            }
            
            // dynamic counters.
            {

                final CounterSet tmp = counterSet.makePath("store");
                
                tmp.addCounter("nodesBuffered", new Instrument<Boolean>() {
                    protected void sample() {
                        setValue(buf_nodes != null);
                    }
                });
                
                tmp.addCounter("openCount", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.openCount));
                    }
                });

                tmp.addCounter("closeCount", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.closeCount));
                    }
                });

                tmp.addCounter("nodesRead", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.nodesRead));
                    }
                });

                tmp.addCounter("nodeReadFromDisk", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.nodesReadFromDisk));
                    }
                });

                tmp.addCounter("leavesReadFromDisk", new Instrument<String>() {
                    protected void sample() {
                        setValue(Long.toString(counters.leavesReadFromDisk));
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
    public ByteBuffer read(final long addr) {

        assertOpen();
        
        /*
         * True IFF the starting address lies entirely within the region
         * dedicated to the B+Tree nodes.
         */
        final boolean isNodeAddr = addressManager.isNodeAddr(addr);
        
        if (DEBUG) {

            log.debug("addr=" + addr + "(" + addressManager.toString(addr)
                    + "), isNodeAddr="+isNodeAddr);
            
        }
        
        // abs. offset of the record in the file.
        final long offset = addressManager.getOffset(addr);

        // length of the record.
        final int length = addressManager.getByteCount(addr);

        if (isNodeAddr) {

            // a node.
            
            counters.nodesRead++;
            
            synchronized (this) {

                if (buf_nodes != null) {

                    counters.nodesReadFromDisk++;
                    
                    return readFromBuffer(offset, length);

                }

            }

            // The data need to be read from the file.
            return readFromFile(offset, length);

        } else {

            // A leaf.
            
            counters.leavesReadFromDisk++;

            // The data need to be read from the file.
            return readFromFile(offset, length);

        }

    }

    /**
     * The [addr] addresses a node and the data are buffered. Create and return
     * a read-only view so that concurrent reads do not modify the buffer state.
     * <p>
     * Note: The caller MUST be synchronized on [this] and ensure that the
     * {@link #buf_nodes} is non-<code>null</code> before calling this
     * method. This ensures that the buffer is valid across the call.
     * 
     * @param offset
     * @param length
     * @return
     */
    private ByteBuffer readFromBuffer(final long offset, final int length) {
        
        /*
         * Note: As long as the state of [buf_nodes] (its position and limit)
         * can not be changed concurrently, we should not need to serialize
         * creations of the view (this is a bit paranoid, but the operation is
         * very lightweight).
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
         * 
         * Note: slice restricts the view available to the caller to the
         * view that was setup on the buffer at the moment that the slice
         * was obtained.
         */
        return tmp.slice();
        
    }
    
    /**
     * Read the record from the file.
     */
    private ByteBuffer readFromFile(final long offset, final int length) {

        // Allocate buffer: limit = capacity; pos = 0.
        final ByteBuffer dst = ByteBuffer.allocate(length);

        /*
         * Read the record from the file.
         * 
         * Note: Java will close the backing FileChannel if Thread is
         * interrupted during an NIO operation. Since the index segment is a
         * read-only data structure, all of the in-memory state remains
         * valid and we only need to re-open the FileChannel to the backing
         * store and retry.
         */

        while(true) {

            try {

                // read into [dst] - does not modify the channel's
                // position().
                FileChannelUtility.readAll(raf.getChannel(), dst, offset);

                // successful read - exit the loop.
                break;

            } catch (ClosedByInterruptException ex) {

                /*
                 * This indicates that this thread was interrupted. We
                 * always abort in this case.
                 */

                throw new RuntimeException(ex);

            } catch (AsynchronousCloseException ex) {

                /*
                 * The channel was closed asynchronously while blocking
                 * during the read. If the buffer strategy still thinks that
                 * it is open then we re-open the channel and re-read.
                 */

                if (reopenChannel())
                    continue;

                throw new RuntimeException(ex);

            } catch (ClosedChannelException ex) {

                /*
                 * The channel is closed. If the buffer strategy still
                 * thinks that it is open then we re-open the channel and
                 * re-read.
                 */

                if (reopenChannel())
                    continue;

                throw new RuntimeException(ex);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        } // while(true)
        
        // Flip buffer for reading.
        dst.flip();

        return dst;
        
    }
    
    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Note: This method is synchronized so that concurrent readers do not try
     * to all open the store at the same time.
     * <p>
     * Note: Java will close the backing {@link FileChannel} if {@link Thread}
     * is interrupted during an NIO operation. However, since the
     * {@link IndexSegment} is a read-only data structure, all of the in-memory
     * state remains valid and we only need to re-open the {@link FileChannel}
     * to the backing store and retry. In particular, we do not need to re-read
     * the root node, {@link IndexMetadata}, {@link BloomFilter}, etc. All we
     * have to do is re-open the {@link FileChannel}.
     * 
     * @return true iff the channel was re-opened.
     * 
     * @throws RuntimeException
     *             if the backing file can not be opened (can not be found or
     *             can not acquire a lock).
     */
    synchronized private boolean reopenChannel() {
        
        if (raf != null && raf.getChannel().isOpen()) {
            
            /*
             * The channel is still open. If you are allowing concurrent reads
             * on the channel, then this could indicate that two readers each
             * found the channel closed and that one was able to re-open the
             * channel before the other such that the channel was open again by
             * the time the 2nd reader got here.
             */
            
            return true;
            
        }
        
        if(!isOpen()) {

            // the IndexSegmentStore has been closed.
            
            return false;
            
        }
        
        try {

            // open the file.
            this.raf = new RandomAccessFile(file, "r");

            if (WARN)
                log.warn("Re-opened file: "+file);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        return true;
        
    }

    /**
     * Attempts to read the index nodes into {@link #buf_nodes}.
     * <p>
     * Note: If the nodes could not be buffered then reads against the nodes
     * will read through to the backing file.
     */
    protected void bufferIndexNodes() throws IOException {

        if (buf_nodes != null) {

            throw new IllegalStateException();
            
        }
        
        if(checkpoint.nnodes == 0) {
        
            throw new IllegalStateException();
            
        }

        if(checkpoint.offsetNodes == 0L) {
            
            throw new IllegalStateException();
            
        }

        if(checkpoint.extentNodes > DirectBufferPool.INSTANCE.getBufferCapacity()) {
            
            /*
             * The buffer would be too small to contain the nodes.
             */
            
            log.warn("Node extent exceeds buffer capacity: extent="
                    + checkpoint.extentNodes + ", bufferCapacity="
                    + DirectBufferPool.INSTANCE.getBufferCapacity());
            
            return;
            
        }

        /*
         * This code is designed to be robust. If anything goes wrong then we
         * make certain that the direct buffer is released back to the pool, log
         * any errors, and return to the caller. While the nodes will not be
         * buffered if there is an error throw in this section, if the backing
         * file is Ok then they can still be read directly from the backing
         * file.
         */
        try {

            /*
             * Attempt to allocate a buffer to hold the disk image of the nodes.
             */
            
            buf_nodes = DirectBufferPool.INSTANCE.acquire(100/* ms */,
                    TimeUnit.MILLISECONDS);
            
            if (INFO)
                log.info("Buffering nodes: #nodes=" + checkpoint.nnodes
                        + ", #bytes=" + checkpoint.extentNodes + ", file=" + file);

            // #of bytes to read.
            buf_nodes.limit((int)checkpoint.extentNodes);
            
            // attempt to read the nodes into the buffer.
            FileChannelUtility.readAll(raf.getChannel(), buf_nodes,
                    checkpoint.offsetNodes);
            
            buf_nodes.flip();
            
        } catch (Throwable t1) {

            /*
             * If we could not obtain a buffer without blocking, or if there was
             * ANY problem reading the data into the buffer, then release the
             * buffer and return. The nodes will not be buffered, but if the
             * file is Ok then the index will simply read through to the disk
             * for the nodes.
             */

            if (buf_nodes != null) {

                try {
                
                    // release buffer back to the pool.
                    DirectBufferPool.INSTANCE.release(buf_nodes);
                    
                } catch (Throwable t) {
                    
                    // log error and continue.
                    log.error(this, t);
                    
                } finally {
                    
                    // make sure the reference is cleared.
                    buf_nodes = null;
                    
                }
                
            }

            // log error and continue.
            log.error(this, t1);

        }

    }

    /**
     * Reads the bloom filter directly from the file.
     * 
     * @return The bloom filter -or- <code>null</code> if the bloom filter was
     *         not constructed when the {@link IndexSegment} was built.
     */
    protected BloomFilter readBloomFilter() throws IOException {

        final long addr = checkpoint.addrBloom;
        
        if(addr == 0L) {
            
            return null;
            
        }
        
        if (INFO)
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

        if (INFO)
            log.info("Read bloom filter: bytesOnDisk=" + len );

        return bloomFilter;

    }

    /**
     * Reads the {@link IndexMetadata} record directly from the file.
     */
    private IndexMetadata readMetadata() throws IOException {

        final long addr = checkpoint.addrMetadata;
        
        assert addr != 0L;
        
        if (INFO)
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

        if (INFO)
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
    
    /*
     * API used to report how long it has been since the store was last
     * used. This is used to clear stores are not in active use from the
     * value cache, which helps us to better manage RAM.
     */
    
    final public void touch() {
    
        timestamp = System.nanoTime();
        
    }
    
    final public long timestamp() {
        
        return timestamp;
        
    }
    
    private long timestamp = System.nanoTime();

}
