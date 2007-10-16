/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 13, 2007
 */

package com.bigdata.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.isolation.ReadOnlyIsolatedIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ReadCommittedTx.ReadCommittedIndex;
import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.scaleup.MasterJournal;
import com.bigdata.scaleup.SlaveJournal;
import com.bigdata.util.ChecksumUtility;

/**
 * <p>
 * An append-only persistence capable data structure supporting atomic commit,
 * named indices, and transactions. Writes are logically appended to the journal
 * to minimize disk head movement. This is an abstract implementation of the
 * {@link IJournal} interface that does not implement services that are
 * independent in a scale-out solution (transaction management, partitioned
 * indices, and index metadata management).
 * <p>
 * The {@link IIndexStore} implementation on this class is NOT thread-safe. The
 * basic limitation is that the mutable {@link BTree} is NOT thread-safe. The
 * {@link #getIndex(String)} method exposes this mutable {@link BTree}. If you
 * use this method to access the mutable {@link BTree} then YOU are responsible
 * for avoiding concurrent writes on the returned object.
 * <p>
 * See {@link ConcurrentJournal#submit(AbstractTask)} for a thread-safe API that
 * provides suitable concurrency control for both isolated and unisolated
 * operations on named indices. Note that the use of the thread-safe API does
 * NOT protect against applications that directly access the mutable
 * {@link BTree} using {@link #getIndex(String)}.
 * <p>
 * The {@link IRawStore} interface on this class is thread-safe. However, this
 * is a low-level API that is not used by directly by most applications. The
 * {@link BTree} class uses this low-level API to read and write its nodes and
 * leaves on the store. Applications generally use named indices rather than the
 * {@link IRawStore} interface.
 * </p>
 * <p>
 * Commit processing. The journal maintains two root blocks. Commit updates the
 * root blocks using the Challis algorithm. (The root blocks are updated using
 * an alternating pattern and "timestamps" are recorded at the head and tail of
 * each root block to detect partial writes. See {@link IRootBlockView} and
 * {@link RootBlockView}.) When the journal is backed by a disk file, the data
 * are {@link Options#FORCE_ON_COMMIT optionally flushed to disk on commit}. If
 * desired, the writes may be flushed before the root blocks are updated to
 * ensure that the writes are not reordered - see {@link Options#DOUBLE_SYNC}.
 * </p>
 * <p>
 * Note: transaction processing MAY occur be concurrent since the write set of a
 * each transaction is written on a distinct {@link TemporaryStore}. However,
 * without additional concurrency controls, each transaction is NOT thread-safe
 * and MUST NOT be executed by more than one concurrent thread. Again, see
 * {@link ConcurrentJournal#submit(AbstractTask)} for a high-concurrency API for
 * both isolated operations (transactions) and unisolated operations. Note that
 * the {@link TemporaryStore} backing a transaction will spill automatically
 * from memory onto disk if the write set of the transaction grows too large.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Priority items are:
 * <ol>
 * <li> Resource management (closing down unused unisolated btrees, index
 * segments and journals based on LRU policy and timeout; in a distributed
 * solution resources would be migrated to other hosts to reduce the total
 * sustained resource load).</li>
 * <li> Concurrent load for RDFS w/o rollback. There should also be an
 * unisolated read mode that does "read-behind", i.e., reading from the last
 * committed state on the store. The purpose of this is to permit fully
 * concurrent readers with the writer in an unisolated mode. If concurrent
 * readers actually read from the same btree instance as the writer then
 * exceptions would arise from concurrent modification. This problem is
 * trivially avoided by maintaining a distinction between a concurrent read-only
 * btree emerging from the last committed state and a single non-concurrent
 * access btree for the writer. In this manner readers may read from the
 * unisolated state of the database with concurrent modification -- to another
 * instance of the btree. Once the writer commits, any new readers will read
 * from its committed state and the older btree objects will be flushed from
 * cache soon after their readers terminate -- thereby providing a consistent
 * view to a reader (or the reader could always switch to read from the then
 * current btree after a commit - just like the distinction between an isolated
 * reader and a read-committed reader).</li>
 * <li> Minimize (de-)serialization costs for B+Trees since we are not IO bound.</li>
 * <li> Reduce heap churn through the use allocation pools for ByteBuffers and
 * byte[]s. There are several places where we can do this, including:
 * {@link BTree}, the {@link Tx} backing store, etc. Bin the objects into
 * buckets within a pool. Each bucket can be a {@link HardReferenceQueue}.
 * Limit the #of objects retained in a given bucket so that the pool does not
 * become a memory sink. Profile using the BEA tool to look for sources of
 * memory allocation and leakage. </li>
 * <li> AIO for the Direct and Disk modes (low priority since not IO bound).</li>
 * <li> GOM integration, including: support for primary key (clustered) indices;
 * using queues from GOM to journal/database segment server supporting both
 * embedded and remote scenarios; and using state-based conflict resolution to
 * obtain high concurrency for generic objects, link set metadata, indices, and
 * distributed split cache and hot cache support.</li>
 * <li> Scale-out database, including:
 * <ul>
 * <li> Data server (mixture of journal server and read-optimized database
 * server). Implemented, but needs optimized network IO.</li>
 * <li> Media replication for data service failover </li>
 * <li> Automatic management of partitioned indices. Note that the split point
 * must be choosen with some awareness of the application keys in order to
 * provide an atomic row update guarentee when using keys formed as {
 * primaryKey, columnName, timestamp }.</li>
 * <li> Transaction service (low-latency with failover instances that keep track
 * of the current transaction metadata).</li>
 * <li> Metadata index services (one per named index with failover).
 * Implemented, but uses static partitioning.</li>
 * <li> Resource reclaimation. </li>
 * <li> Job scheduler to map functional programs across the data (possible
 * Hadoop integration point).</li>
 * </ul>
 * </ol>
 * 
 * @todo Define distributed transaction protocol. Pay attention to 2-phase or
 *       3-phase commits when necessary, but take advantage of locality when
 *       possible (all writes on a single journal). There is a dependency in a
 *       distributed database architecture on transaction begin time. A very
 *       long running transaction could force the journal to hold onto
 *       historical states. If a decision is made to discard those states and
 *       the transaction begins to read from the journal then the transaction
 *       must be rolled back. This should be worked out with the resource
 *       deallocation for old journals and segments.
 * 
 * @todo Define distributed protocol for robust startup, operation, and
 *       failover.
 * 
 * @todo I need to revisit the assumptions for very large objects. I expect that
 *       using an index with a key formed as [URI][chuck#] would work just fine.
 *       Chunks would then be limited to 32k or so. Writes on such indices
 *       should probably be directed to a journal using a disk-only mode.
 * 
 * @todo Checksums and/or record compression are currently handled on a per-{@link BTree}
 *       or other persistence capable data structure basis. It is nice to be
 *       able to choose for which indices and when ( {@link Journal} vs
 *       {@link IndexSegment}) to apply these algorithms. However, it might be
 *       nice to factor their application out a bit into a layered api - as long
 *       as the right layering is correctly re-established on load of the
 *       persistence data structure.
 * 
 * @todo The UUID of the store file should make it into all error messages and
 *       log messages so that they can be aggregated in a distributed
 *       enviornment.
 */
public abstract class AbstractJournal implements IJournal, ITxCommitProtocol {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(IJournal.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The index of the root address containing the address of the persistent
     * {@link Name2Addr} mapping names to {@link BTree}s registered for the
     * store.
     */
    public static transient final int ROOT_NAME2ADDR = 0;

    /**
     * A clone of the properties used to initialize the {@link Journal}.
     */
    final protected Properties properties;

    /**
     * The directory that should be used for temporary files.
     */
    final public File tmpDir;
    
    /**
     * The metadata for a pre-existing journal -or- <code>null</code> if the journal was
     * created for the first time.
     */
    final FileMetadata fileMetadata;
    
    /**
     * The implementation logic for the current {@link BufferMode}.
     */
    final protected IBufferStrategy _bufferStrategy;

    /**
     * The object used by the journal to compute the checksums of its root
     * blocks (this object is NOT thread-safe so there is one instance per
     * journal).
     */
    private final ChecksumUtility checker = new ChecksumUtility();
    
    /**
     * The current root block. This is updated each time a new root block is
     * written.
     */
    private IRootBlockView _rootBlock;

    /**
     * The registered committers for each slot in the root block.
     */
    private ICommitter[] _committers = new ICommitter[ICommitRecord.MAX_ROOT_ADDRS];

    /**
     * Used to cache the most recent {@link ICommitRecord} -- discarded on
     * {@link #abort()}.
     */
    private ICommitRecord _commitRecord;

    /**
     * The "live" BTree mapping index names to the last metadata record
     * committed for the named index. The keys are index names (unicode
     * strings). The values are the names and the last known address of the
     * named btree.
     * <p>
     * The "live" name2addr index is required for unisolated writers regardless
     * whether they are adding an index, dropping an index, or just recovering
     * the "live" version of an existing named index.
     * <p>
     * Operations that read on historical {@link Name2Addr} objects can of
     * course be concurrent. Those objects are loaded from an
     * {@link ICommitRecord}. See {@link #getIndex(String, ICommitRecord)}.
     * <p>
     * Note: access to the "live" {@link Name2Addr} index MUST be bracketed with
     * <code>synchronized({@link #name2Addr})</code>.
     */
    protected Name2Addr name2Addr;

    /**
     * BTree mapping commit timestamps to the address of the corresponding
     * {@link ICommitRecord}. The keys are timestamps (long integers). The
     * values are the address of the {@link ICommitRecord} with that commit
     * timestamp.
     * 
     * @todo this should be private, but {@link DumpJournal} is using it to
     *       report on the historical states of named btrees.
     */
    /*private*/ CommitRecordIndex _commitRecordIndex;

    /**
     * Option controls whether the journal forces application data to disk
     * before updating the root blocks.
     */
    protected final boolean doubleSync;

    /**
     * Option controls how the journal behaves during a commit.
     */
    protected final ForceEnum forceOnCommit;

    /**
     * Option set by the test suites causes the file backing the journal to be
     * deleted when the journal is closed.
     */
    protected final boolean deleteOnClose;

    /**
     * The maximum extent before a {@link #commit()} will {@link #overflow()}.
     * In practice, overflow tries to trigger before this point in order to
     * avoid extending the journal.
     * 
     * @see Options#MAXIMUM_EXTENT
     */
    protected final long maximumExtent;

    /**
     * Create or open a journal.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     * 
     * @todo Write tests that verify (a) that read-only mode does not permit
     *       writes; (b) that read-only mode is not supported for a transient
     *       buffer (since the buffer does not pre-exist by definition); (c)
     *       that read-only mode reports an error if the file does not
     *       pre-exist; and (d) that you can not write on a read-only journal.
     */
    public AbstractJournal(Properties properties) {

        long initialExtent = Options.DEFAULT_INITIAL_EXTENT;
        long maximumExtent = Options.DEFAULT_MAXIMUM_EXTENT;
        int offsetBits = Options.DEFAULT_OFFSET_BITS;
        boolean validateChecksum = true;
        boolean useDirectBuffers = Options.DEFAULT_USE_DIRECT_BUFFERS;
        boolean create = Options.DEFAULT_CREATE;
        boolean createTempFile = Options.DEFAULT_CREATE_TEMP_FILE;
        boolean isEmptyFile = false;
        boolean deleteOnClose = Options.DEFAULT_DELETE_ON_CLOSE;
        boolean deleteOnExit = Options.DEFAULT_DELETE_ON_EXIT;
        boolean readOnly = Options.DEFAULT_READ_ONLY;
        ForceEnum forceWrites = Options.DEFAULT_FORCE_WRITES;
        ForceEnum forceOnCommit = Options.DEFAULT_FORCE_ON_COMMIT;
        boolean doubleSync = Options.DEFAULT_DOUBLE_SYNC;

        String val;

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties = (Properties) properties.clone();
//        this.properties = properties;

        /*
         * "bufferMode" mode. Note that very large journals MUST use the
         * disk-based mode.
         */

        val = properties.getProperty(Options.BUFFER_MODE);

        final BufferMode bufferMode;
        
        if (val != null) {

            bufferMode = BufferMode.parse(val);
            
            log.info(Options.BUFFER_MODE+"="+bufferMode);
            
        } else bufferMode = Options.DEFAULT_BUFFER_MODE;

        System.err.println(Options.BUFFER_MODE+"="+bufferMode);
        
        /*
         * "useDirectBuffers"
         */

        val = properties.getProperty(Options.USE_DIRECT_BUFFERS);

        if (val != null) {

            useDirectBuffers = Boolean.parseBoolean(val);
            
            log.info(Options.USE_DIRECT_BUFFERS+"="+useDirectBuffers);

        }

        /*
         * "initialExtent"
         */

        val = properties.getProperty(Options.INITIAL_EXTENT);

        if (val != null) {

            initialExtent = Long.parseLong(val);

            if (initialExtent < Bytes.megabyte) {

                throw new RuntimeException("The '" + Options.INITIAL_EXTENT
                        + "' must be at least one megabyte(" + Bytes.megabyte
                        + ")");

            }

            log.info(Options.INITIAL_EXTENT+"="+initialExtent);           

        }

        /*
         * "maximumExtent"
         */

        val = properties.getProperty(Options.MAXIMUM_EXTENT);

        if (val != null) {

            maximumExtent = Long.parseLong(val);

            if (maximumExtent < initialExtent) {

                throw new RuntimeException("The '" + Options.MAXIMUM_EXTENT
                        + "' (" + maximumExtent
                        + ") is less than the initial extent ("+initialExtent+").");

            }

            log.info(Options.MAXIMUM_EXTENT+"="+maximumExtent); 
            
        }

        this.maximumExtent = maximumExtent;

        /*
         * "offsetBits"
         */
        
        val = properties.getProperty(Options.OFFSET_BITS);
        
        if(val != null) {
            
            offsetBits = Integer.parseInt(val);
            
            WormAddressManager.assertOffsetBits(offsetBits);
            
            log.info(Options.OFFSET_BITS+"="+offsetBits);

            if(offsetBits!=WormAddressManager.DEFAULT_OFFSET_BITS) {
                
                /*
                 * FIXME The problem is that the CommitRecordIndex and the
                 * Name2Addr class both define IValueSerializer objects that
                 * need to know how to pack and unpack addresses. Those objects
                 * are serialized into the BTreeMetadata records using Java
                 * default serialization. When when they are de-serialized they
                 * no longer have a reference to the store and therefore do not
                 * know the #of offset bits to use when encoding and decoding
                 * addresses. This has been patched by requiring the use of the
                 * default #of offset bits. A proper solution would take an
                 * approach similar to the extSer package which provides a
                 * marker interface used to identify objects who need to have
                 * the store reference set during de-serialization.
                 */
                
                throw new RuntimeException("Only "
                        + WormAddressManager.DEFAULT_OFFSET_BITS
                        + " is supported at this time");
                
            }
            
        }
        
        /*
         * "createTempFile"
         */

        val = properties.getProperty(Options.CREATE_TEMP_FILE);

        if (val != null) {

            createTempFile = Boolean.parseBoolean(val);

            log.info(Options.CREATE_TEMP_FILE+"="+createTempFile);
            
            if(createTempFile) {
                
                create = false;
            
                isEmptyFile = true;
                
            }
         
        }

        // "tmp.dir"
        {

            val = properties.getProperty(Options.TMP_DIR);

            tmpDir = val == null ? new File(System
                    .getProperty("java.io.tmpdir")) : new File(val);

            if (!tmpDir.exists()) {

                if (!tmpDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + tmpDir.getAbsolutePath());

                }

            }

            if (!tmpDir.isDirectory()) {

                throw new RuntimeException("Not a directory: "
                        + tmpDir.getAbsolutePath());

            }

            if(val!=null) {
                
                log.info(Options.TMP_DIR+"="+tmpDir); 
                
            }
            
        }
            
        /*
         * "validateChecksum"
         */

        val = properties.getProperty(Options.VALIDATE_CHECKSUM);

        if (val != null) {

            validateChecksum = Boolean.parseBoolean(val);

            log.info(Options.VALIDATE_CHECKSUM+"="+validateChecksum);

        }

        /*
         * "readOnly"
         */

        val = properties.getProperty(Options.READ_ONLY);

        if (val != null) {

            readOnly = Boolean.parseBoolean(val);

            create = false;
            
            log.info(Options.READ_ONLY+"="+readOnly);

        }

        /*
         * "forceWrites"
         */

        val = properties.getProperty(Options.FORCE_WRITES);

        if (val != null) {

            forceWrites = ForceEnum.parse(val);

            log.info(Options.FORCE_WRITES+"="+forceWrites);

        }

        /*
         * "forceOnCommit"
         */

        val = properties.getProperty(Options.FORCE_ON_COMMIT);

        if (val != null) {

            forceOnCommit = ForceEnum.parse(val);

            log.info(Options.FORCE_ON_COMMIT+"="+forceOnCommit);
            
        }
        
        this.forceOnCommit = forceOnCommit;

        /*
         * "doubleSync"
         */

        val = properties.getProperty(Options.DOUBLE_SYNC);

        if (val != null) {

            doubleSync = Boolean.parseBoolean(val);

            log.info(Options.DOUBLE_SYNC+"="+doubleSync);
            
        }

        this.doubleSync = doubleSync;
        
        /*
         * "deleteOnClose"
         */

        val = properties.getProperty(Options.DELETE_ON_CLOSE);

        if (val != null) {

            deleteOnClose = Boolean.parseBoolean(val);

            log.info(Options.DELETE_ON_CLOSE+"="+deleteOnClose);

        }

        this.deleteOnClose = deleteOnClose;

        /*
         * "deleteOnExit"
         */

        val = properties.getProperty(Options.DELETE_ON_EXIT);

        if (val != null) {

            deleteOnExit = Boolean.parseBoolean(val);

            log.info(Options.DELETE_ON_EXIT+"="+deleteOnExit);

        }
        
        /*
         * "file"
         */

        File file;
        
        if(bufferMode==BufferMode.Transient) {
            
            file = null;
            
        } else {
            
            val = properties.getProperty(Options.FILE);

            if(createTempFile && val != null) {
                
                throw new RuntimeException("Can not use option '"
                        + Options.CREATE_TEMP_FILE + "' with option '"
                        + Options.FILE + "'");
                
            }

            if( createTempFile ) {
                
                try {

                    val = File.createTempFile("bigdata-" + bufferMode + "-",
                            ".jnl", tmpDir).toString();
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }
            
            if (val == null) {

                throw new RuntimeException("Required property: '"
                        + Options.FILE + "'");

            }

            file = new File(val);

            log.info(Options.FILE+"="+val);

        }

        /*
         * Create the appropriate IBufferStrategy object.
         */

        switch (bufferMode) {

        case Transient: {

            /*
             * Setup the buffer strategy.
             */

            if (readOnly) {

                throw new RuntimeException(
                        "readOnly not supported for transient journals.");

            }

            fileMetadata = null;
            
            _bufferStrategy = new TransientBufferStrategy(offsetBits,
                    initialExtent, 0L/* soft limit for maximumExtent */,
                    useDirectBuffers);

            /*
             * setup the root blocks.
             */
            final int nextOffset = 0;
            final long firstCommitTime = 0L;
            final long lastCommitTime = 0L;
            final long commitCounter = 0L;
            final long commitRecordAddr = 0L;
            final long commitRecordIndexAddr = 0L;
            final UUID uuid = UUID.randomUUID();
            IRootBlockView rootBlock0 = new RootBlockView(true, offsetBits,
                    nextOffset, firstCommitTime, lastCommitTime, commitCounter,
                    commitRecordAddr, commitRecordIndexAddr, uuid, checker);
            IRootBlockView rootBlock1 = new RootBlockView(false, offsetBits,
                    nextOffset, firstCommitTime, lastCommitTime, commitCounter,
                    commitRecordAddr, commitRecordIndexAddr, uuid, checker);
            _bufferStrategy.writeRootBlock(rootBlock0, ForceEnum.No);
            _bufferStrategy.writeRootBlock(rootBlock1, ForceEnum.No);

            this._rootBlock = rootBlock1;

            break;

        }

        case Direct: {

            /*
             * Setup the buffer strategy.
             */

            fileMetadata = new FileMetadata(file,
                    BufferMode.Direct, useDirectBuffers, initialExtent,
                    maximumExtent, create, isEmptyFile, deleteOnExit,
                    readOnly, forceWrites, offsetBits, validateChecksum,
                    checker);

            _bufferStrategy = new DirectBufferStrategy(
                    0L/* soft limit for maximumExtent */, fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        case Mapped: {

            /*
             * Setup the buffer strategy.
             */

            fileMetadata = new FileMetadata(file,
                    BufferMode.Mapped, useDirectBuffers, initialExtent,
                    maximumExtent, create, isEmptyFile, deleteOnExit,
                    readOnly, forceWrites, offsetBits, validateChecksum,
                    checker);

            /*
             * Note: the maximumExtent is a hard limit in this case only since
             * resize is not supported for mapped files.
             */
            _bufferStrategy = new MappedBufferStrategy(
                    maximumExtent /* hard limit for maximum extent */,
                    fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        case Disk: {

            /*
             * Setup the buffer strategy.
             */

            fileMetadata = new FileMetadata(file,
                    BufferMode.Disk, useDirectBuffers, initialExtent,
                    maximumExtent, create, isEmptyFile, deleteOnExit,
                    readOnly, forceWrites, offsetBits, validateChecksum,
                    checker);

            _bufferStrategy = new DiskOnlyStrategy(
                    0L/* soft limit for maximumExtent */, fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        default:

            throw new AssertionError();

        }

        /*
         * Create or re-load the index of commit records.
         */
        this._commitRecordIndex = getCommitRecordIndex(this._rootBlock
                .getCommitRecordIndexAddr());

        /*
         * Give the store a chance to set any committers that it defines.
         */
        setupCommitters();

        // report event.
        ResourceManager.openJournal(getFile() == null ? null : getFile()
                .toString(), size(), getBufferStrategy().getBufferMode());

    }

    /**
     * @todo consider wrapping up the properties rather than cloning them.
     *  
     * @todo consider making the properties restart safe so that they can be
     *       read from the journal. This will let some properties be specified
     *       on initialization while letting others default or be overriden on
     *       restart. This is trivially accomplished by dedicating a root slot
     *       to a Properties object, or a flattened Properties object serialized
     *       as key-value pairs, in which case the data could just be loaded
     *       into a btree and the btree api could be used to change the
     *       persistent properties as necessary.
     */
    final public Properties getProperties() {

        return (Properties) properties.clone();

    }

    /**
     * The delegate that implements the {@link BufferMode}.
     * <p>
     * Note: this method MUST NOT check to see whether the journal is open since
     * we need to use it if we want to invoke {@link IBufferStrategy#delete()}
     * and we can only invoke that method once the journal is closed.
     */
    final public IBufferStrategy getBufferStrategy() {

        return _bufferStrategy;

    }

    /**
     * Shutdown the journal (running tasks will run to completion, but no new
     * tasks will start).
     * <p>
     * Note: You SHOULD use this method rather than {@link #close()} for normal
     * shutdown of the journal.
     * 
     * @see #shutdownNow()
     */
    public void shutdown() {

        assertOpen();

        log.info("");
        
//        // shutdown the write service. 
//        writeService.shutdown();

        // close immediately.
        _close();
        
        log.info("Shutdown complete.");

    }

    /**
     * Immediate shutdown (running tasks are cancelled rather than being
     * permitted to complete).
     * 
     * @see #shutdown()
     */
    public void shutdownNow() {
        
        assertOpen();

        log.info("");
        
//        writeService.shutdownNow();

        // close immediately.
        _close();

        log.info("Shutdown complete.");

    }

    public File getFile() {
        
        return _bufferStrategy.getFile();
        
    }

    /**
     * Core implementation of immediate shutdown handles event reporting.
     */
    protected void _close() {
        
        assertOpen();

        log.info("");
        
//        // force the commit thread to quit immediately.
//        writeService.shutdownNow();
        
        _bufferStrategy.close();

        // report event.
        ResourceManager.closeJournal(getFile() == null ? null : getFile()
                .toString());
        
        if (deleteOnClose) {

            /*
             * This option is used by the test suite and MUST NOT be used with
             * live data.
             */

            delete();
            
        }

    }

    /**
     * Deletes the backing file(s) (if any).
     * <p>
     * Note: This is the core implementation of delete and handles event
     * reporting.
     * 
     * @exception IllegalStateException
     *                if the journal is open.
     */
    public void delete() {

        log.info("");
        
        _bufferStrategy.delete();

        ResourceManager.deleteJournal(getFile() == null ? null : getFile()
                .toString());

    }
    
    /**
     * Invokes {@link #shutdownNow()}.
     */
    public void close() {

        log.info("");
        
        shutdownNow();

    }

    public void closeAndDelete() {

        log.info("");
        
        shutdownNow();
        
        if (!deleteOnClose) {

            /*
             * Note: if deleteOnClose was specified then the resource
             * was already deleted by _close().
             */
            
            delete();
            
        }
        
    }
    
    /**
     * Assert that the store is open.
     * <p>
     * Note: You can see an {@link IllegalStateException} thrown out of here if
     * there are tasks running during {@link #shutdown()} and one of the various
     * task services times out while awaiting termination. Such exceptions are
     * normal since the store was closed asynchronously while task(s) were still
     * running.
     * 
     * @exception IllegalStateException
     *                if the store is closed.
     */
    protected void assertOpen() {

        if (!_bufferStrategy.isOpen()) {

            throw new IllegalStateException();

        }

    }

    public boolean isOpen() {

        return _bufferStrategy.isOpen();

    }

    public boolean isStable() {

        return _bufferStrategy.isStable();

    }

    public boolean isFullyBuffered() {
        
        return _bufferStrategy.isFullyBuffered();
        
    }
    
    /**
     * Return a read-only view of the current root block.
     * 
     * @return The current root block.
     */
    final public IRootBlockView getRootBlockView() {

        return _rootBlock;

    }

    /**
     * Set a persistence capable data structure for callback during the commit
     * protocol.
     * <p>
     * Note: the committers must be reset after restart or whenever the
     * committers are discarded (the committers are themselves transient
     * objects).
     * 
     * @param rootSlot
     *            The slot in the root block where the address of the
     *            {@link ICommitter} will be recorded.
     * 
     * @param committer
     *            The commiter.
     */
    final public void setCommitter(int rootSlot, ICommitter committer) {

        assertOpen();

        _committers[rootSlot] = committer;

    }

    /**
     * Notify all registered committers and collect their reported root
     * addresses in an array.
     * 
     * @return The array of collected root addresses for the registered
     *         committers.
     */
    final private long[] notifyCommitters() {

        int ncommitters = 0;

        long[] rootAddrs = new long[_committers.length];

        for (int i = 0; i < _committers.length; i++) {

            if (_committers[i] == null)
                continue;

            rootAddrs[i] = _committers[i].handleCommit();

            ncommitters++;

        }

        return rootAddrs;

    }

    /**
     * Discards any unisolated writes since the last {@link #commitNow(long)()}.
     * <p>
     * This is invoked if a transaction fails after it has begun writing data
     * onto the global state from its isolated state. Once the transaction has
     * begun this process it has modified the global (unisolated) state and the
     * next commit will make those changes restart-safe. While this processing
     * is not begun unless the commit SHOULD succeed, errors can nevertheless
     * occur. Therefore, if the transaction fails its writes on the unisolated
     * state must be discarded. Since the isolatable data structures (btrees)
     * use a copy-on-write policy, writing new data never overwrites old data so
     * nothing has been lost.
     * <p>
     * We can not simply reload the last root block since concurrent
     * transactions may write non-restart safe data onto the store (transactions
     * may use btrees to isolate changes, and those btrees will write on the
     * store). Reloading the root block would discarding all writes, including
     * those occurring in isolation in concurrent transactions.
     * <p>
     * Instead, what we do is discard the unisolated objects, reloading them
     * from the current root addresses on demand. This correctly discards any
     * writes on those unisolated objects while NOT resetting the nextOffset at
     * which writes will occur on the store and NOT causing persistence capable
     * objects (btrees) used for isolated by concurrent transactions to lose
     * their write sets.
     */
    public void abort() {

        // clear the root addresses - they will be reloaded.
        _commitRecord = null;

        // clear the array of committers.
        _committers = new ICommitter[_committers.length];

        /*
         * Re-load the commit record index from the address in the current root
         * block.
         * 
         * Note: This may not be strictly necessary since the only time we write
         * on this index is a single record during each commit. So, it should be
         * valid to simply catch an error during a commit and discard this index
         * forcing its reload. However, doing this here is definately safer.
         */
        _commitRecordIndex = getCommitRecordIndex(_rootBlock
                .getCommitRecordIndexAddr());

        // discard any hard references that might be cached.
        discardCommitters();

        // setup new committers, e.g., by reloading from their last root addr.
        setupCommitters();

    }
    
    /**
     * Note: This method can not be implemented by the {@link AbstractJournal}
     * since it lacks a commit timestamp factory, which is properly part of the
     * {@link ITransactionManager}.
     * 
     * @see Journal#commit()
     */
    abstract public long commit();

    /**
     * An atomic commit is performed by directing each registered
     * {@link ICommitter} to flush its state onto the store using
     * {@link ICommitter#handleCommit()}. The address returned by that method
     * is the address from which the {@link ICommitter} may be reloaded (and its
     * previous address if its state has not changed). That address is saved in
     * the {@link ICommitRecord} under the index for which that committer was
     * {@link #registerCommitter(int, ICommitter) registered}. We then force
     * the data to stable store, update the root block, and force the root block
     * and the file metadata to stable store.
     * 
     * @param commitTime
     *            The commit time either of a transaction or of an unisolated
     *            commit. Note that when mixing isolated and unisolated commits
     *            you MUST use the same {@link ITimestampService} for both
     *            purposes.
     * 
     * @return The timestamp assigned to the commit record -or- 0L if there were
     *         no data to commit.
     */
    protected long commitNow(long commitTime) {

        assertOpen();

        log.info("");
        
        /*
         * First, run each of the committers accumulating the updated root
         * addresses in an array. In general, these are btrees and they may have
         * dirty nodes or leaves that needs to be evicted onto the store. The
         * first time through, any newly created btrees will have dirty empty
         * roots (the btree code does not optimize away an empty root at this
         * time). However, subsequent commits without intervening data written
         * on the store should not cause any committers to update their root
         * address.
         */
        final long[] rootAddrs = notifyCommitters();

        /*
         * See if anything has been written on the store since the last commit.
         */
        if (_bufferStrategy.getNextOffset() == _rootBlock.getNextOffset()) {

            /*
             * No data was written onto the store so the commit can not achieve
             * any useful purpose.
             */
            
            log.info("Nothing to commit");

            return 0L;

        }
        
        /*
         * Write the commit record onto the store.
         */

        final IRootBlockView old = _rootBlock;

        final long newCommitCounter = old.getCommitCounter() + 1;

        final ICommitRecord commitRecord = new CommitRecord(commitTime,
                newCommitCounter, rootAddrs);

        final long commitRecordAddr = write(ByteBuffer
                .wrap(CommitRecordSerializer.INSTANCE.serialize(commitRecord)));

        /*
         * Add the comment record to an index so that we can recover historical
         * states efficiently.
         */
        _commitRecordIndex.add(commitRecordAddr, commitRecord);

        /*
         * Flush the commit record index to the store and stash the address of
         * its metadata record in the root block.
         * 
         * Note: The address of the root of the CommitRecordIndex itself needs
         * to go right into the root block. We are unable to place it into the
         * commit record since we need to serialize the commit record, get its
         * address, and add the entry to the CommitRecordIndex before we can
         * flush the CommitRecordIndex to the store.
         */
        final long commitRecordIndexAddr = _commitRecordIndex.write();

        /*
         * Force application data to stable storage _before_ we update the root
         * blocks. This option guarentees that the application data is stable on
         * the disk before the atomic commit. Some operating systems and/or file
         * systems may otherwise choose an ordered write with the consequence
         * that the root blocks are laid down on the disk before the application
         * data and a hard failure could result in the loss of application data
         * addressed by the new root blocks (data loss on restart).
         * 
         * Note: We do not force the file metadata to disk
         */
        if (doubleSync) {

            _bufferStrategy.force(false);

        }

        // next offset at which user data would be written.
        final long nextOffset = _bufferStrategy.getNextOffset();

        /*
         * update the root block.
         */
        {

            /*
             * Update the firstCommitTime the first time a transaction commits
             * and the lastCommitTime each time a transaction commits (these are
             * commit timestamps of isolated or unisolated transactions).
             */

            final long firstCommitTime = (old.getFirstCommitTime() == 0L ? commitTime
                    : old.getFirstCommitTime());

            final long priorCommitTime = old.getLastCommitTime();
            
            if(priorCommitTime != 0L) {
                
                /*
                 * This is a local sanity check to make sure that the commit
                 * timestamps are strictly increasing. An error will be reported
                 * if the commit time for the current (un)isolated transaction
                 * is not strictly greater than the last commit time on the
                 * store as read back from the current root block.
                 */

                if(commitTime<= priorCommitTime) {
                    
                    throw new RuntimeException(
                            "Time goes backwards: commitTime=" + commitTime
                                    + ", but lastCommitTime=" + priorCommitTime
                                    + " on the current root block");
                    
                }
                
            }
            
            final long lastCommitTime = commitTime;

            // Create the new root block.
            IRootBlockView newRootBlock = new RootBlockView(
                    !old.isRootBlock0(), old.getOffsetBits(), nextOffset,
                    firstCommitTime, lastCommitTime, newCommitCounter,
                    commitRecordAddr, commitRecordIndexAddr, old.getUUID(),
                    checker);

            _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

            _rootBlock = newRootBlock;

            _commitRecord = commitRecord;

        }

        /*
         * Look for overflow condition.
         */
        {

            /*
             * Choose maximum of the target maximum extent and the current user
             * data extent so that we do not re-trigger overflow immediately if
             * the buffer has been extended beyond the target maximum extent.
             * Among other things this lets you run the buffer up to a
             * relatively large extent (if you use a disk-only mode since you
             * will run out of memory if you use a fully buffered mode).
             */
            final long limit = Math.max(maximumExtent, _bufferStrategy
                    .getUserExtent());
            
            if (nextOffset > .9 * limit) {

                if( overflow() ) {
                    
                    /*
                     * Someone handled the overflow event by opening a new
                     * journal to absorb further writes.
                     */
                    ResourceManager.overflowJournal(getFile() == null ? null
                            : getFile().toString(), size());
                    
                }

            }

        }

        log.info("Done: nextOffset="+nextOffset);
        
        return commitTime;

    }

    /**
     * Note: This implementation does not handle overflow of the journal. The
     * journal capacity will simply be extended by {@link #write(ByteBuffer)}
     * until the available resources are exhausted.
     * 
     * @return This implementation returns <code>false</code> since it does
     *         NOT open a new journal.
     */
    public boolean overflow() {

        return false;
        
    }

    public void force(boolean metadata) {

        assertOpen();

        _bufferStrategy.force(metadata);

    }

    public long size() {
        
        return _bufferStrategy.size();
        
    }
    
    public long write(ByteBuffer data) {

        assertOpen();

        return _bufferStrategy.write(data);

    }

    public ByteBuffer read(long addr) {

        assertOpen();

        return _bufferStrategy.read(addr);

    }

    final public long getRootAddr(int index) {

        assertOpen();

        if (_commitRecord == null) {

            return getCommitRecord().getRootAddr(index);

        } else {

            return _commitRecord.getRootAddr(index);

        }

    }

    /**
     * Returns a read-only view of the root addresses. The caller may modify the
     * returned array without changing the effective state of those addresses as
     * used by the store.
     * 
     * @return The root addresses.
     * 
     * @todo Synchronization was added to this method since the
     *       {@link StatusThread} and {@link AbstractTask}s may all invoke
     *       this concurrently. The synchronization could be removed if we made
     *       sure that this was never null outside of initialization or commit.
     */
    synchronized public ICommitRecord getCommitRecord() {

        assertOpen();
        
        if (_commitRecord == null) {

            long commitRecordAddr = _rootBlock.getCommitRecordAddr();

            if (commitRecordAddr == 0L) {

                _commitRecord = new CommitRecord();

            } else {

                _commitRecord = CommitRecordSerializer.INSTANCE
                        .deserialize(_bufferStrategy.read(commitRecordAddr));

            }

        }

        return _commitRecord;

    }

    /**
     * The default implementation discards the btree mapping names to named
     * btrees.
     * <p>
     * Subclasses MAY extend this method to discard their own committers but
     * MUST NOT override it completely.
     */
    public void discardCommitters() {

        // discard.
        name2Addr = null;

    }

    /**
     * The basic implementation sets up the btree that is responsible for
     * resolving named btrees.
     * <p>
     * Subclasses may extend this method to setup their own committers but MUST
     * NOT override it completely.
     */
    public void setupCommitters() {

        setupName2AddrBTree(getRootAddr(ROOT_NAME2ADDR));

    }

    /*
     * named indices.
     */

    /**
     * Setup the btree that resolved named btrees.
     * 
     * @param addr
     *            The root address of the btree -or- 0L iff the btree has not
     *            been defined yet.
     */
    private void setupName2AddrBTree(long addr) {

        assert name2Addr == null;

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             */

            // create btree mapping names to addresses.
            name2Addr = new Name2Addr(this);

        } else {

            /*
             * Reload the btree from its root address.
             */

            name2Addr = (Name2Addr) BTree.load(this, addr);

        }

        // register for commit notices.
        setCommitter(ROOT_NAME2ADDR, name2Addr);

    }

    /**
     * Create or re-load the index that resolves timestamps to
     * {@link ICommitRecord}s.
     * <p>
     * The current commit record index is {@link #_commitRecordIndex}.
     * 
     * @param addr
     *            The root address of the index -or- 0L if the index has not
     *            been created yet.
     * 
     * @return The {@link CommitRecordIndex} for that address or a new index if
     *         0L was specified as the address.
     * 
     * @see #_commitRecordIndex
     */
    private CommitRecordIndex getCommitRecordIndex(long addr) {

        CommitRecordIndex ndx;

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             */

            // create btree mapping names to addresses.
            ndx = new CommitRecordIndex(this);

        } else {

            /*
             * Reload the btree from its root address.
             */

            ndx = (CommitRecordIndex) BTree.load(this, addr);

        }

        return ndx;

    }

    /**
     * @todo the {@link CommitRecordIndex} is a possible source of thread
     *       contention since transactions need to use this code path in order
     *       to locate named indices but the {@link #writeService} can also
     *       write on this index. I have tried some different approaches to
     *       handling this.
     */
    public ICommitRecord getCommitRecord(long commitTime) {

        assertOpen();

        return _commitRecordIndex.find(commitTime);

    }

    /**
     * Returns a read-only named index loaded from the given root block. This
     * method imposes a canonicalizing mapping and contracts that there will be
     * at most one instance of the historical index at a time. This contract is
     * used to facilitate buffer management. Writes on the index will NOT be
     * made persistent and the index will NOT participate in commits.
     * <p>
     * Note: since this is always a request for historical read-only data, this
     * method MUST NOT register a committer and the returned btree MUST NOT
     * participate in the commit protocol.
     * <p>
     * Note: The caller MUST take care not to permit writes since they could be
     * visible to other users of the same read-only index. This is typically
     * accomplished by wrapping the returned object in class that will throw an
     * exception for writes such as {@link ReadOnlyIndex},
     * {@link ReadOnlyIsolatedIndex}, or {@link ReadCommittedIndex}.
     * 
     * @return The named index -or- <code>null</code> iff the named index did
     *         not exist as of that commit record.
     */
    protected IIndex getIndex(String name, ICommitRecord commitRecord) {

        assertOpen();

        if (name == null)
            throw new IllegalArgumentException();

        if (commitRecord == null)
            throw new IllegalArgumentException();

        /*
         * The address of an historical Name2Addr mapping used to resolve named
         * indices for the historical state associated with this commit record.
         */
        final long metaAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);

        if (metaAddr == 0L) {

            log.warn("No name2addr entry in this commit record: "
                    + commitRecord);

            return null;
            
        }

        /*
         * Resolve the address of the historical Name2Addr object using the
         * canonicalizing object cache. This prevents multiple historical
         * Name2Addr objects springing into existance for the same commit
         * record.
         */
        Name2Addr name2Addr = (Name2Addr)getIndex(metaAddr);
        
        /*
         * The address at which the named index was written for that historical
         * state.
         */
        final long indexAddr = name2Addr.getAddr(name);
        
        // No such index by name for that historical state.
        if(indexAddr==0L) return null;
        
        /*
         * Resolve the named index using the object cache to impose a
         * canonicalizing mapping on the historical named indices based on the
         * address on which it was written in the store.
         */
        return getIndex(indexAddr);

    }
    
    /**
     * A cache that is used by the {@link Journal} to provide a canonicalizing
     * mapping from an address to the instance of a read-only historical object
     * loaded from that address.
     * <p>
     * Note: the "live" version of an object MUST NOT be placed into this cache
     * since its state will continue to evolve with additional writes while the
     * cache is intended to provide a canonicalizing mapping to only the
     * historical states of the object. This means that objects such as indices
     * and the {@link Name2Addr} index MUST NOT be inserted into the cache if
     * the are being read from the store for "live" use. For this reason
     * {@link Name2Addr} uses its own caching mechanisms.
     * 
     * @todo discard cache on abort? that should not be necessary. even through
     *       it can contain objects whose addresses were not made restart safe
     *       those addresses should not be accessible to the application and
     *       hence the objects should never be looked up and will be evicted in
     *       due time from the cache. (this does rely on the fact that the store
     *       never reuses an address.)
     * 
     * FIXME This is the place to solve the resource (RAM) burden for indices is
     * Name2Addr. Currently, indices are never closed once opened which is a
     * resource leak. We need to close them out eventually based on LRU plus
     * timeout plus NOT IN USE. The way to approach this is a weak reference
     * cache combined with an LRU or hard reference queue that tracks reference
     * counters (just like the BTree hard reference cache for leaves). Eviction
     * events lead to closing an index iff the reference counter is zero.
     * Touches keep recently used indices from closing even though they may have
     * a zero reference count.
     * 
     * @todo the {@link MasterJournal} needs to do similar things with
     *       {@link IndexSegment}.
     * 
     * @todo review the metadata index lookup in the {@link SlaveJournal}. This
     *       is a somewhat different case since we only need to work with the
     *       current metadata index as along as we make sure not to reclaim
     *       resources (journals and index segments) until there are no more
     *       transactions which can read from them.
     * 
     * @todo support metering of index resources and timeout based shutdown of
     *       indices. note that the "live" {@link Name2Addr} has its own cache
     *       for the unisolated indices and that metering needs to pay attention
     *       to the indices in that cache as well. Also, those indices can be
     *       shutdown as long as they are not dirty (pending a commit).
     */
    final private WeakValueCache<Long, ICommitter> objectCache = new WeakValueCache<Long, ICommitter>(
            new LRUCache<Long, ICommitter>(20));
    
    /**
     * A canonicalizing mapping for index objects.
     * 
     * @param addr
     *            The address of the index object.
     *            
     * @return The index object.
     */
    final protected IIndex getIndex(long addr) {
        
        synchronized (objectCache) {

            IIndex obj = (IIndex) objectCache.get(addr);

            if (obj == null) {
                
                obj = BTree.load(this,addr);
                
            }
            
            objectCache.put(addr, (ICommitter)obj, false/*dirty*/);
    
            return obj;

        }
        
    }

//    /**
//     * Insert or touch an object in the object cache.
//     * 
//     * @param addr
//     *            The {@link Addr address} of the object in the store.
//     * @param obj
//     *            The object.
//     * 
//     * @see #getIndex(long), which provides a canonicalizing mapping for index
//     *      objects using the object cache.
//     */
//    final protected void touch(long addr,Object obj) {
//        
//        synchronized(objectCache) {
//            
//            objectCache.put(addr, (ICommitter)obj, false/*dirty*/);
//            
//        }
//        
//    }

    /**
     * Registers an {@link UnisolatedBTree} that will support transactional
     * isolation.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name) {
        
        return registerIndex(name, new UnisolatedBTree(this, UUID.randomUUID()));
        
    }
    
    /**
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name, IIndex ndx) {

        assertOpen();

        /*
         * This is a performance tweak. It flushes the index to the backing
         * store before we synchronize on [name2addr] in order to afford greater
         * concurrency. Note that this is wasted effort only in the case where
         * the index is pre-existing as we would NOT flush it to disk in that
         * case. In the index to be registered is empty or if indices are not
         * normally pre-existing then this should be a performance win where a
         * large #of indices are created concurrently.
         */

        ((ICommitter)ndx).handleCommit();
        
        synchronized (name2Addr) {
                
            // add to the persistent name map.
            name2Addr.registerIndex(name, ndx);

            // report event (the application has access to the named index).
            ResourceManager.openUnisolatedBTree(name);

        }

        return ndx;
        
    }

    public void dropIndex(String name) {
        
        assertOpen();
        
        synchronized(name2Addr) {
                
            // drop from the persistent name map.
            name2Addr.dropIndex(name);

            // report event.
            ResourceManager.dropUnisolatedBTree(name);

        }

    }
    
    /**
     * Return the mutable view of the named index (aka the "live" index). This
     * object is NOT thread-safe. You MUST NOT write on this index unless you
     * KNOW that you are the only writer. Other consumers SHOULD wrap this as a
     * {@link ReadOnlyIndex} to avoid the possibility of mishap.
     * 
     * @return The mutable view of the index.
     * 
     * @todo add hard reference queue for {@link AbstractBTree} to the journal
     *       and track the #of instances of each {@link AbstractBTree} on the
     *       queue using #referenceCount and "touch()", perhaps in Name2Addr;
     *       write tests. consider one queue for mutable btrees and another for
     *       index segments, partitioned indices, metadata indices, etc.
     *       consider the meaning of "queue length" here and how to force close
     *       based on timeout. improve reporting of index segments by name and
     *       partition.<br>
     *       Mutable indices are low-cost to close/open. Closing them once they
     *       are no longer receiving writes can release some large buffers and
     *       reduce the latency of commits since dirty nodes will already have
     *       been flushed to disk. The largest cost on re-open is de-serializing
     *       nodes and leaves for subsequent operations. Those nodes will be
     *       read from a fully buffered store, so the latency will be small even
     *       though deserialization is CPU intensive. <br>
     *       Close operations on unisolated indices need to be queued in the
     *       {@link #writeService} so that they are executed in the same thread
     *       as other operations on the unisolated index.<br>
     *       Make sure that we close out old {@link Journal}s that are no
     *       longer required by any open index. This will require a distinct
     *       referenceCount on the {@link Journal}.
     */
    public IIndex getIndex(String name) {

        assertOpen();

        if (name == null)
            throw new IllegalArgumentException();

        synchronized (name2Addr) {

            return name2Addr.get(name);

        }

    }

    /*
     * transaction support.
     */
    
    /**
     * Note: This is declared here since the transaction commit protocol
     * requires access to a timestamp service.
     */
    abstract public long nextTimestamp();

    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has either successfully
     * prepared or aborted.
     */
    final Map<Long, ITx> activeTx = new ConcurrentHashMap<Long, ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     * 
     * @todo A transaction will be in this map only while it is actively
     *       committing, so this is always a "map" of one and could be replaced
     *       by a scalar reference (except that we may allow concurrent prepare
     *       and commit of read-only transactions).
     */
    final Map<Long, ITx> preparedTx = new ConcurrentHashMap<Long, ITx>();

//    /**
//     * A thread used to {@link #serialize(Callable)} operations on the
//     * unisolated store, including serializing atomic transaction prepare-commit
//     * operations.
//     * <p>
//     * A writable transaction that attempts to {@link #commit()} is added as a
//     * {@link TxCommitTask} and queued for execution by this thread. When its
//     * turn comes, it will validate its write set and commit iff validation
//     * succeeds.
//     */
//    final private ExecutorService writeService = Executors
//            .newSingleThreadExecutor(DaemonThreadFactory
//                    .defaultThreadFactory());
//    
//    /**
//     * Submit a task for eventual serialized execution by the
//     * {@link #writeService}. This method may be used to ensure serial
//     * execution of tasks that write on unisolated indices.
//     * 
//     * @param task
//     *            The task.
//     * 
//     * @return The future for the task.
//     */
//    public Future serialize(Callable task) {
//        
//        return writeService.submit(task);
//        
//    }
    
//    /**
//     * Abort a transaction (synchronous, low latency for read-only transactions
//     * but aborts for read-write transactions are serialized since there may be
//     * latency in communications with the transaction server or deletion of the
//     * temporary backing store for the transaction).
//     * 
//     * @param ts
//     *            The transaction identifier (aka start time).
//     */
//    public void abort(long ts) {
//
//        ITx tx = getTx(ts);
//        
//        if (tx == null)
//            throw new IllegalArgumentException("No such tx: " + ts);
//        
//        if(tx.isReadOnly()) {
//         
//            // abort is synchronous.
//            tx.abort();
//            
//        } else {
//
//            // queue the abort request.
//            writeService.submit(new TxAbortTask(tx));
//            
//        }
//        
//    }

//    /**
//     * Task aborts a transaction when it is run by the
//     * {@link Journal#writeService}. This is used to serialize some abort
//     * processing which would otherwise be concurrent with commit processing.
//     * {@link Journal#abort()} makes some assumptions that it is a
//     * single-threaded environment and those assumptions would be violated
//     * without serialization aborts and commits on the same queue.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    private static class TxAbortTask implements Callable<Long> {
//        
//        private final ITx tx;
//        
//        public TxAbortTask(ITx tx) {
//            
//            assert tx != null;
//            
//            this.tx = tx;
//            
//        }
//
//        public Long call() throws Exception {
//            
//            tx.abort();
//            
//            return 0L;
//           
//        }
//        
//    }

    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * @todo test for transactions that have already been completed? that would
     *       represent a protocol error in the transaction manager service.
     */
    protected void activateTx(ITx tx) throws IllegalStateException {

        Long timestamp = tx.getStartTimestamp();

        if (activeTx.containsKey(timestamp))
            throw new IllegalStateException("Already active: tx=" + tx);

        if (preparedTx.containsKey(timestamp))
            throw new IllegalStateException("Already prepared: tx=" + tx);

        activeTx.put(timestamp, tx);

    }

    /**
     * Notify the journal that a transaction has prepared (and hence is no
     * longer active).
     * 
     * @param tx
     *            The transaction
     * 
     * @throws IllegalStateException
     */
    protected void prepared(ITx tx) throws IllegalStateException {

        Long id = tx.getStartTimestamp();

        ITx tx2 = activeTx.remove(id);

        if (tx2 == null)
            throw new IllegalStateException("Not active: tx=" + tx);

        assert tx == tx2;

        if (preparedTx.containsKey(id))
            throw new IllegalStateException("Already preparing: tx=" + tx);

        preparedTx.put(id, tx);

    }

    /**
     * Notify the journal that a transaction is completed (either aborted or
     * committed).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    protected void completedTx(ITx tx) throws IllegalStateException {

        assert tx != null;
        assert tx.isComplete();

        Long id = tx.getStartTimestamp();

        ITx txActive = activeTx.remove(id);

        ITx txPrepared = preparedTx.remove(id);

        if (txActive == null && txPrepared == null) {

            throw new IllegalStateException(
                    "Neither active nor being prepared: tx=" + tx);

        }

    }

    /**
     * Lookup an active or prepared transaction (exact match).
     * 
     * @param startTime
     *            The start timestamp for the transaction.
     * 
     * @return The transaction with that start time or <code>null</code> if
     *         the start time is not mapped to either an active or prepared
     *         transaction.
     */
    public ITx getTx(long startTime) {

        ITx tx = activeTx.get(startTime);

        if (tx == null) {

            tx = preparedTx.get(startTime);

        }

        return tx;

    }

    public IIndex getIndex(String name, long ts) {
        
        if (name == null) {

            throw new IllegalArgumentException();
            
        }
        
        ITx tx = activeTx.get(ts);
        
        if (tx == null) {

            throw new IllegalStateException();
            
        }
        
        return tx.getIndex(name);
        
    }

    /*
     * IStoreSerializer
     */
    
    final public Object deserialize(byte[] b, int off, int len) {
        return _bufferStrategy.deserialize(b, off, len);
    }

    final public Object deserialize(byte[] b) {
        return _bufferStrategy.deserialize(b);
    }

    final public Object deserialize(ByteBuffer buf) {
        return _bufferStrategy.deserialize(buf);
    }

    final public byte[] serialize(Object obj) {
        return _bufferStrategy.serialize(obj);
    }

    /*
     * IAddressManager
     */

    final public long getOffset(long addr) {
        return _bufferStrategy.getOffset(addr);
    }

    final public int getByteCount(long addr) {
        return _bufferStrategy.getByteCount(addr);
    }

    final public void packAddr(DataOutput out, long addr) throws IOException {
        _bufferStrategy.packAddr(out, addr);
    }

    final public long toAddr(int nbytes, long offset) {
        return _bufferStrategy.toAddr(nbytes, offset);
    }

    final public String toString(long addr) {
        return _bufferStrategy.toString(addr);
    }

    final public long unpackAddr(DataInput in) throws IOException {
        return _bufferStrategy.unpackAddr(in);
    }

    final public int getOffsetBits() {
        
        return ((AbstractRawWormStore)_bufferStrategy).getOffsetBits();
        
    }
    
    /**
     * The maximum length of a record that may be written on the store.
     */
    final public int getMaxRecordSize() {

        return ((AbstractRawWormStore) _bufferStrategy).getAddressManger()
                .getMaxByteCount();

    }

}
