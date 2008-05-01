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
 * Created on Mar 13, 2007
 */

package com.bigdata.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.LazyEvaluationCounterSet;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.AbstractRawWormStore;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceClient;
import com.bigdata.util.ChecksumUtility;

/**
 * <p>
 * The journal is an append-only persistence capable data structure supporting
 * atomic commit, named indices, and transactions. Writes are logically appended
 * to the journal to minimize disk head movement.
 * </p><p>
 * This class is an abstract implementation of the {@link IJournal} interface
 * that does not implement the {@link IConcurrencyManager},
 * {@link IResourceManager}, or {@link ITransactionManager} interfaces. There
 * are several classes which DO support all of these features, relying on the
 * {@link AbstractJournal} for their underlying persistence store. These
 * include:
 * <dl>
 * <dt>{@link Journal}</dt>
 * <dd>A concrete implementation that may be used for a standalone immortal
 * database complete with concurrency control and transaction management.</dd>
 * <dt>{@link DataService}</dt>
 * <dd>A class supporting remote clients, key-range partitioned indices,
 * concurrency, and scale-out.</dd>
 * <dt>{@link IBigdataClient}</dt>
 * <dd>Clients connect to an {@link IBigdataFederation}, which is the basis
 * for the scale-out architecture. There are several variants of a federation
 * available, including:
 * <dl>
 * <dt>{@link LocalDataServiceClient}</dt>
 * <dd>Purely local operations against a {@link DataService} with full
 * concurrency controls and transaction management</dd>
 * <dt>{@link EmbeddedClient}</dt>
 * <dd>Operations against a collection of services running in the same JVM with
 * full concurrency controls, transaction management, and key-range partitioned
 * indices.</dd>
 * <dt>{@link JiniClient}</dt>
 * <dd>Operations against a collection of services running on a distributed
 * services framework such as Jini with full concurrency controls, transaction
 * management, and key-range partitioned indices. This is the scale-out
 * solution.</dd>
 * </dl>
 * </dd>
 * </dl>
 * </p>
 * <h2>Limitations</h2>
 * <p>
 * The {@link IIndexStore} implementation on this class is NOT thread-safe. The
 * basic limitation is that the mutable {@link BTree} is NOT thread-safe. The
 * {@link #getIndex(String)} method exposes this mutable {@link BTree}. If you
 * use this method to access the mutable {@link BTree} then YOU are responsible
 * for avoiding concurrent writes on the returned object.
 * </p>
 * <p>
 * See {@link IConcurrencyManager#submit(AbstractTask)} for a thread-safe API
 * that provides suitable concurrency control for both isolated and unisolated
 * operations on named indices. Note that the use of the thread-safe API does
 * NOT protect against applications that directly access the mutable
 * {@link BTree} using {@link #getIndex(String)}.
 * </p>
 * <p>
 * The {@link IRawStore} interface on this class is thread-safe. However, this
 * is a low-level API that is not used by directly by most applications. The
 * {@link BTree} class uses this low-level API to read and write its nodes and
 * leaves on the store. Applications generally use named indices rather than the
 * {@link IRawStore} interface.
 * </p>
 * <p>
 * Note: transaction processing MAY occur be concurrent since the write set of a
 * each transaction is written on a distinct {@link TemporaryStore}. However,
 * without additional concurrency controls, each transaction is NOT thread-safe
 * and MUST NOT be executed by more than one concurrent thread. Again, see
 * {@link IConcurrencyManager#submit(AbstractTask)} for a high-concurrency API
 * for both isolated operations (transactions) and unisolated operations. Note
 * that the {@link TemporaryStore} backing a transaction will spill
 * automatically from memory onto disk if the write set of the transaction grows
 * too large.
 * </p>
 * <h2>Commit processing</h2>
 * <p>
 * The journal maintains two root blocks. Commit updates the root blocks using
 * the Challis algorithm. (The root blocks are updated using an alternating
 * pattern and "timestamps" are recorded at the head and tail of each root block
 * to detect partial writes. See {@link IRootBlockView} and
 * {@link RootBlockView}.) When the journal is backed by a disk file, the data
 * are {@link Options#FORCE_ON_COMMIT optionally flushed to disk on commit}. If
 * desired, the writes may be flushed before the root blocks are updated to
 * ensure that the writes are not reordered - see {@link Options#DOUBLE_SYNC}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Priority items are:
 * <ol>
 * <li> Minimize (de-)serialization costs for B+Trees since we are not IO bound.</li>
 * <li> AIO for the Direct and Disk modes (low priority since not IO bound).</li>
 * <li> GOM integration features, including: support for primary key (clustered)
 * indices; supporting both embedded and remote scenarios; and using state-based
 * conflict resolution to obtain high concurrency for generic objects, link set
 * metadata, indices, and distributed split cache and hot cache support.</li>
 * <li> Scale-out database, including:
 * <ul>
 * <li> Media replication for data service failover </li>
 * <li> Transaction service (low-latency with failover instances that keep track
 * of the current transaction metadata).</li>
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
 * @todo Checksums and/or record compression are currently handled on a per-{@link BTree}
 *       or other persistence capable data structure basis. It is nice to be
 *       able to choose for which indices and when ( {@link Journal} vs
 *       {@link IndexSegment}) to apply these algorithms. However, it might be
 *       nice to factor their application out a bit into a layered api - as long
 *       as the right layering is correctly re-established on load of the
 *       persistence data structure. In that view the {@link IRawStore} either
 *       computes checksums or it does not and the checksums is stored in the
 *       record, perhaps in the last 4 bytes. The checksum itself would not be
 *       visible at the {@link IRawStore} API layer.
 * 
 * @todo There are lots of annoying ways in which asynchronously closing the
 *       journal, e.g., using {@link #close()} or {@link #shutdown()} can cause
 *       exceptions to be thrown out of concurrent threads. It would be nice if
 *       we could throw a single exception that indicated that the journal had
 *       been asynchronously closed.
 */
public abstract class AbstractJournal implements IJournal, ITimestampService {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(IJournal.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
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
    final private IBufferStrategy _bufferStrategy;

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
     * The configured capacity for the LRU backing the index cache maintained by
     * the "live" {@link Name2Addr} object.
     * 
     * @see Options#LIVE_INDEX_CACHE_CAPACITY
     */
    private final int liveIndexCacheCapacity;
    
    /**
     * The configured capacity for the LRU baching the {@link #historicalIndexCache}.
     * 
     * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
     */
    private final int historicalIndexCacheCapacity;
    
    /**
     * A cache that is used by the {@link AbstractJournal} to provide a
     * canonicalizing mapping from an address to the instance of a read-only
     * historical object loaded from that address and which indirectly controls
     * how long the journal will "keep open" historical index objects by prevent
     * them from being swept by the garbage collector.
     * <p>
     * Note: the "live" version of an object MUST NOT be placed into this cache
     * since its state will continue to evolve with additional writes while the
     * cache is intended to provide a canonicalizing mapping to the historical
     * committed states of the object. This means that objects such as indices
     * and the {@link Name2Addr} index MUST NOT be inserted into the cache if
     * the are being read from the store for "live" use. For this reason
     * {@link Name2Addr} uses its own caching mechanisms.
     * <p>
     * Note: {@link #abort()} discards the contents of this cache in order to
     * ensure that partial writes are discarded.
     * 
     * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
     */
    final private WeakValueCache<Long, ICommitter> historicalIndexCache;
    
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
     * 
     * @see #getName2Addr()
     */
    /*private*/Name2Addr name2Addr; // Note: used by some unit tests.

    /**
     * A read-only view of the {@link Name2Addr} object mapping index names to
     * the most recent committed {@link Entry} for the named index. The keys are
     * index names (unicode strings). The values are {@link Entry}s containing
     * the names, commitTime, and last known {@link Checkpoint} address of the
     * named {@link BTree} on the {@link Journal}.
     */
    public IIndex getName2Addr() {

        final long checkpointAddr = name2Addr.getCheckpoint().getCheckpointAddr();
        
        /*
         * Note: This uses the canonicalizing mapping to get an instance that is
         * distinct from the live #name2Addr object while not allowing more than
         * a single such distinct instance to exist for the current name2Addr
         * object.
         */ 
        final BTree btree = getIndex(checkpointAddr);
        
        /*
         * Wrap up in a read-only view since writes MUST NOT be allowed.
         */
        return new ReadOnlyIndex(btree);
        
    }
    
    /**
     * Return a read-only view of the {@link Name2Addr} object as of the
     * specified commit time.
     * 
     * @param commitTime
     *            A commit time.
     * 
     * @return The read-only view -or- <code>null</code> if there is no commit
     *         record for that commitTime.
     * 
     * @see #getName2Addr()
     */
    public IIndex getName2Addr(long commitTime) {
        
        final ICommitRecord commitRecord = getCommitRecord(commitTime);
        
        if (commitRecord == null) {

            return null;
            
        }
     
        final long checkpointAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);
        
        return new ReadOnlyIndex( getIndex(checkpointAddr) );
        
    }

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
     * True iff the journal was opened in a read-only mode
     */
    private final boolean readOnly;
    
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
    private final long maximumExtent;
    
    /**
     * The maximum extent before a {@link #commit()} will {@link #overflow()}.
     * In practice, overflow tries to trigger before this point in order to
     * avoid extending the journal.
     * 
     * @see Options#MAXIMUM_EXTENT
     */
    final public long getMaximumExtent() {
       
        return maximumExtent;
        
    }

    /**
     * The default branching factor for indices created using {@link #registerIndex(String)}.
     * 
     * @see Options#BRANCHING_FACTOR
     * 
     * @deprecated by the use of {@link IndexMetadata} 
     */
    private final int defaultBranchingFactor;
    
    /**
     * A {@link ThreadLocal} variable providing access to thread-specific
     * instances of a configured {@link IKeyBuilder}.
     * <p>
     * Note: this {@link ThreadLocal} is not static since we need configuration
     * properties from the constructor - those properties can be different for
     * different {@link Journal}s on the same machine.
     */
    private ThreadLocal<IKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<IKeyBuilder>() {

        protected synchronized IKeyBuilder initialValue() {

            return KeyBuilder.newUnicodeInstance(properties);

        }

    };

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured
     * using the properties specified to the journal constructor.
     */
    public IKeyBuilder getKeyBuilder() {
        
        return threadLocalKeyBuilder.get();
        
    }
    
    /**
     * The default branching factor for indices created using
     * {@link #registerIndex(String)}.
     * 
     * @see Options#BRANCHING_FACTOR
     * 
     * @deprecated See {@link Options#BRANCHING_FACTOR}
     */
    public final int getDefaultBranchingFactor() {
        
        return defaultBranchingFactor;
        
    }
    
    /**
     * Used to allocate the {@link ByteBuffer} for the {@link BufferMode#Disk}
     * write cache.
     * 
     * @param properties
     *            The properties that will be used to configure the journal.
     * 
     * @return The write cache buffer -or- <code>null</code> iff no buffer is
     *         required (either because the selected {@link BufferMode} does not
     *         support a write cache, because the journal would be opened in a
     *         read-only mode, or because the configured write cache capacity
     *         was ZERO(0)).
     * 
     * @see Options#BUFFER_MODE
     * @see Options#READ_ONLY
     * @see Options#WRITE_CACHE_CAPACITY
     */
    public static ByteBuffer getWriteCache(Properties properties) {

        final BufferMode bufferMode = BufferMode.parse(properties.getProperty(
                Options.BUFFER_MODE, "" + Options.DEFAULT_BUFFER_MODE));

        final boolean readOnly = Boolean.parseBoolean(properties.getProperty(
                Options.READ_ONLY, "" + Options.DEFAULT_READ_ONLY));

        if (bufferMode.equals(BufferMode.Disk) && !readOnly) {
            
            final int capacity = Integer.parseInt(properties.getProperty(
                Options.WRITE_CACHE_CAPACITY, ""
                        + Options.DEFAULT_WRITE_CACHE_CAPACITY));

            final int minWriteCacheCapacity = (int) Math.min(
                    Options.minimumInitialExtent,
                    Options.minimumWriteCacheCapacity);

            log.info(Options.WRITE_CACHE_CAPACITY + "=" + capacity);
            
            if (capacity > 0 && capacity < minWriteCacheCapacity) {

                throw new RuntimeException(Options.WRITE_CACHE_CAPACITY
                        + " must be ZERO (0) or at least "
                        + minWriteCacheCapacity + " bytes");

            }

           return ByteBuffer.allocateDirect(capacity);
            
        }
        
        return null;

    }
    
    /**
     * Create or re-open a journal.
     * <p>
     * Note: Creating a new journal registers some internal indices but does NOT
     * perform a commit. Those indices will become restart safe with the first
     * commit.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     */
    protected AbstractJournal(Properties properties) {
        
        this(properties, getWriteCache(properties));
        
    }
    
    /**
     * Create or re-open a journal.
     * <p>
     * Note: Creating a new journal registers some internal indices but does NOT
     * perform a commit. Those indices will become restart safe with the first
     * commit.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * @param writeCache
     *            When non-<code>null</code> and when {@link BufferMode#Disk}
     *            is selected, this {@link ByteBuffer} will be used as the write
     *            cache. This allows the same write cache to be used by the next
     *            journal when the current journal overflows. This is
     *            necessitated by JVM bug <a
     *            href="http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8fab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541">
     *            6210541</a>
     *            which describes a failure by
     *            <code>releaseTemporaryDirectBuffer()</code> to release
     *            temporary direct {@link ByteBuffer}s that are allocated for
     *            channel IO.
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     */
    protected AbstractJournal(Properties properties, final ByteBuffer writeCache) {

        boolean create = Options.DEFAULT_CREATE;
        boolean isEmptyFile = false;

        String val;

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties = (Properties) properties.clone();

        /*
         * "bufferMode" mode.
         * 
         * Note: very large journals MUST use the disk-based mode.
         */

        final BufferMode bufferMode = BufferMode.parse(properties.getProperty(
                Options.BUFFER_MODE, "" + Options.DEFAULT_BUFFER_MODE));

        log.info(Options.BUFFER_MODE + "=" + bufferMode);

        /*
         * historicalIndexCacheCapacity
         */
        {
            historicalIndexCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.HISTORICAL_INDEX_CACHE_CAPACITY,
                    Options.DEFAULT_HISTORICAL_INDEX_CACHE_CAPACITY));

            log.info(Options.HISTORICAL_INDEX_CACHE_CAPACITY+"="+historicalIndexCacheCapacity);

            if (historicalIndexCacheCapacity <= 0)
                throw new RuntimeException(Options.HISTORICAL_INDEX_CACHE_CAPACITY
                        + " must be non-negative");

            historicalIndexCache = new WeakValueCache<Long, ICommitter>(
                    new LRUCache<Long, ICommitter>(historicalIndexCacheCapacity));

        }
        
        /*
         * liveIndexCacheCapacity
         */
        {
        
            liveIndexCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.LIVE_INDEX_CACHE_CAPACITY,
                    Options.DEFAULT_LIVE_INDEX_CACHE_CAPACITY));

            log.info(Options.LIVE_INDEX_CACHE_CAPACITY+"="+liveIndexCacheCapacity);

            if (liveIndexCacheCapacity <= 0)
                throw new RuntimeException(Options.LIVE_INDEX_CACHE_CAPACITY
                        + " must be non-negative");

        }
        
        /*
         * "useDirectBuffers"
         */

        final boolean useDirectBuffers = Boolean.parseBoolean(properties
                .getProperty(Options.USE_DIRECT_BUFFERS, ""
                        + Options.DEFAULT_USE_DIRECT_BUFFERS));
            
        log.info(Options.USE_DIRECT_BUFFERS+"="+useDirectBuffers);

        /*
         * "initialExtent"
         */

        final long initialExtent = Long.parseLong(properties.getProperty(
                Options.INITIAL_EXTENT, "" + Options.DEFAULT_INITIAL_EXTENT));

        if (initialExtent < Options.minimumInitialExtent) {

            throw new RuntimeException("The '" + Options.INITIAL_EXTENT
                    + "' must be at least " + Options.minimumInitialExtent
                    + " bytes");

        }

        log.info(Options.INITIAL_EXTENT + "=" + initialExtent);           

        /*
         * "maximumExtent"
         */

        maximumExtent = Long.parseLong(properties.getProperty(
                Options.MAXIMUM_EXTENT, "" + Options.DEFAULT_MAXIMUM_EXTENT));

        if (maximumExtent < initialExtent) {

            throw new RuntimeException("The '" + Options.MAXIMUM_EXTENT + "' ("
                    + maximumExtent + ") is less than the initial extent ("
                    + initialExtent + ").");

        }

        log.info(Options.MAXIMUM_EXTENT + "=" + maximumExtent); 

        /*
         * "offsetBits"
         */
        
        final int offsetBits = Integer.parseInt(properties.getProperty(
                Options.OFFSET_BITS, Options.DEFAULT_OFFSET_BITS));

        WormAddressManager.assertOffsetBits(offsetBits);

        log.info(Options.OFFSET_BITS + "=" + offsetBits);

        /*
         * readCacheCapacity
         */
        final int readCacheCapacity = Integer.parseInt(properties.getProperty(
                Options.READ_CACHE_CAPACITY,
                Options.DEFAULT_READ_CACHE_CAPACITY));

        log.info(Options.READ_CACHE_CAPACITY + "=" + readCacheCapacity);

        if (readCacheCapacity < 0)
            throw new RuntimeException(Options.READ_CACHE_CAPACITY
                    + " must be non-negative");
        
        /*
         * readCacheMaxRecordSize
         */
        final int readCacheMaxRecordSize = Integer.parseInt(properties.getProperty(
                Options.READ_CACHE_MAX_RECORD_SIZE,
                Options.DEFAULT_READ_CACHE_MAX_RECORD_SIZE));

        log.info(Options.READ_CACHE_MAX_RECORD_SIZE + "=" + readCacheMaxRecordSize);

        if (readCacheMaxRecordSize <= 0)
            throw new RuntimeException(Options.READ_CACHE_MAX_RECORD_SIZE
                    + " must be positive");
        
        /*
         * "createTempFile"
         */

        final boolean createTempFile = Boolean.parseBoolean(properties
                .getProperty(Options.CREATE_TEMP_FILE, ""
                        + Options.DEFAULT_CREATE_TEMP_FILE));

        log.info(Options.CREATE_TEMP_FILE + "=" + createTempFile);

        if (createTempFile) {

            create = false;

            isEmptyFile = true;

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

        final boolean validateChecksum = Boolean.parseBoolean(properties
                .getProperty(Options.VALIDATE_CHECKSUM, ""
                        + Options.DEFAULT_VALIDATE_CHECKSUM));

        log.info(Options.VALIDATE_CHECKSUM+"="+validateChecksum);

        /*
         * "readOnly"
         */

        readOnly = Boolean.parseBoolean(properties.getProperty(
                Options.READ_ONLY, "" + Options.DEFAULT_READ_ONLY));

        if (readOnly) {

            create = false;

        }
        
        log.info(Options.READ_ONLY+"="+readOnly);

        /*
         * "forceWrites"
         */

        final ForceEnum forceWrites = ForceEnum.parse(properties.getProperty(
                Options.FORCE_WRITES, "" + Options.DEFAULT_FORCE_WRITES));

        log.info(Options.FORCE_WRITES + "=" + forceWrites);

        /*
         * "forceOnCommit"
         */

        forceOnCommit = ForceEnum.parse(properties.getProperty(
                Options.FORCE_ON_COMMIT, "" + Options.DEFAULT_FORCE_ON_COMMIT));

        log.info(Options.FORCE_ON_COMMIT+"="+forceOnCommit);

        /*
         * "doubleSync"
         */

        doubleSync = Boolean.parseBoolean(properties.getProperty(
                Options.DOUBLE_SYNC, "" + Options.DEFAULT_DOUBLE_SYNC));

        log.info(Options.DOUBLE_SYNC + "=" + doubleSync);

        /*
         * "deleteOnClose"
         */

        deleteOnClose = Boolean.parseBoolean(properties.getProperty(
                Options.DELETE_ON_CLOSE, "" + Options.DEFAULT_DELETE_ON_CLOSE));

        log.info(Options.DELETE_ON_CLOSE + "=" + deleteOnClose);

        /*
         * "deleteOnExit"
         */

        final boolean deleteOnExit = Boolean.parseBoolean(properties
                .getProperty(Options.DELETE_ON_EXIT, ""
                        + Options.DEFAULT_DELETE_ON_EXIT));

        log.info(Options.DELETE_ON_EXIT + "=" + deleteOnExit);
        
        /*
         * branchingFactor.
         */
        
        defaultBranchingFactor = Integer.parseInt(properties.getProperty(
                Options.BRANCHING_FACTOR, Options.DEFAULT_BRANCHING_FACTOR));

        if (defaultBranchingFactor < BTree.MIN_BRANCHING_FACTOR) {

            throw new IllegalArgumentException(Options.BRANCHING_FACTOR
                    + " must be at least " + BTree.MIN_BRANCHING_FACTOR);
            
        }
        
        log.info(Options.BRANCHING_FACTOR+"="+defaultBranchingFactor);
        
        /*
         * "file"
         */

        File file;

        if (bufferMode == BufferMode.Transient) {
            
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
         * Note: The caller SHOULD specify an explicit [createTime] when its
         * value is critical. The default assigned here does NOT attempt to use
         * a clock that is consistent with the commit protocol or even a clock
         * that assigns unique timestamps.
         */
        final long createTime = Long.parseLong(properties.getProperty(
                Options.CREATE_TIME, "" + System.currentTimeMillis()));
        
        assert createTime != 0L;
        
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
            final UUID uuid = UUID.randomUUID(); // Journal's UUID.
            final long closedTime = 0L;
            IRootBlockView rootBlock0 = new RootBlockView(true, offsetBits,
                    nextOffset, firstCommitTime, lastCommitTime, commitCounter,
                    commitRecordAddr, commitRecordIndexAddr, uuid, createTime,
                    closedTime, checker);
            IRootBlockView rootBlock1 = new RootBlockView(false, offsetBits,
                    nextOffset, firstCommitTime, lastCommitTime, commitCounter,
                    commitRecordAddr, commitRecordIndexAddr, uuid, createTime,
                    closedTime, checker);
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
                    BufferMode.Direct,
                    useDirectBuffers, initialExtent, maximumExtent, create,
                    isEmptyFile, deleteOnExit, readOnly, forceWrites,
                    offsetBits, 0/* readCacheCapacity */,
                    0/* readCacheMaxRecordSize */, null/* writeCache */,
                    validateChecksum, createTime, checker);

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
                    maximumExtent, create,
                    isEmptyFile, deleteOnExit, readOnly, forceWrites,
                    offsetBits, 0/*readCacheCapacity*/,
                    0/* readCacheMaxRecordSize */, null/* writeCache */,
                    validateChecksum, createTime, checker);

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

            fileMetadata = new FileMetadata(file, BufferMode.Disk,
                    useDirectBuffers, initialExtent, maximumExtent, create,
                    isEmptyFile, deleteOnExit, readOnly, forceWrites,
                    offsetBits, readCacheCapacity, readCacheMaxRecordSize,
                    readOnly ? null : writeCache, validateChecksum,
                    createTime, checker);

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
        this._commitRecordIndex = getCommitRecordIndex();

        /*
         * Give the store a chance to set any committers that it defines.
         */
        setupCommitters();

        // report event.
        ResourceManager.openJournal(getFile() == null ? null : getFile()
                .toString(), size(), getBufferStrategy().getBufferMode());

    }

    /**
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

        return new Properties( properties );

    }

    /**
     * The delegate that implements the {@link BufferMode}.
     * <p>
     * Note: this method MUST NOT check to see whether the journal is open since
     * we need to use it if we want to invoke {@link IBufferStrategy#deleteResources()}
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

        // Note: per contract for shutdown.
        if(!isOpen()) return;

        log.info("");

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
    synchronized public void shutdownNow() {
        
        // Note: per contract for shutdownNow()
        if(!isOpen()) return;

        log.info("");

        // close immediately.
        _close();

        log.info("Shutdown complete.");

    }

    /**
     * Closes out the journal iff it is still open.
     */
    protected void finalize() throws Exception {
        
        if(_bufferStrategy.isOpen()) {
            
            log.warn("Closing journal: "+getFile());
            
            shutdownNow();
            
        }
        
    }
    
//    /**
//     * Statistics describing the journal including IO, registered indices, etc.
//     * 
//     * @todo Since this reports on the registered indices as of the current
//     *       commit record it presents data which is not available via
//     *       {@link #getCounters()}. Is there any meaningful way to add that
//     *       data to {@link #getCounters()} since it is structurally dynamic?
//     * 
//     * @todo make this 100% XML by changing {@link BTree} to report counters as
//     *       XML and placing the index counters under a path corresponding to
//     *       the index name. indices are added and dropped, but the counter
//     *       reporting should probably be "at the instant" rather than modifying
//     *       the defined counter hierarchy as those indices are created and
//     *       destroyed just to keep down the memory profile if there happens to
//     *       be a large #of indices. the internal indices (name2addr and the
//     *       commit record index) could also be reported on in this manner.
//     */
//    public String getStatistics() {
//
//        StringBuilder sb = new StringBuilder();
//
//        sb.append(_bufferStrategy.getCounters().toString());
//        
//        /*
//         * Report on the registered indices.
//         */
//        synchronized(name2Addr) {
//            
//            ITupleIterator itr = name2Addr.entryIterator();
//
//            while (itr.hasNext()) {
//
//                ITuple tuple = itr.next();
//                
//                Entry entry = EntrySerializer.INSTANCE.deserialize(tuple.getValueStream());
//                
//                IIndex ndx = name2Addr.get(entry.name);
//                
//                sb.append("\nindex: name="+entry.name+" : "+ndx.getStatistics());
//                
//            }
//            
//        }
//
//        return sb.toString();
//        
//    }
    
    /**
     * Return counters reporting on various aspects of the journal.
     */
    synchronized public CounterSet getCounters() {
        
        if (counters == null) {

            counters = new CounterSet();

            counters.addCounter("file", new Instrument<String>() {
                public void sample() {
                    File file = _bufferStrategy.getFile();
                    setValue(file == null ? "N/A" : file.getAbsolutePath());
                }
            });

            counters.attach(_bufferStrategy.getCounters());

            /*
             * Report on the registered indices (read-committed view).
             * 
             * Note: this always re-generates the counter set for the indices
             * since they can be added and dropped at any time.
             * 
             * FIXME Lazy evaluation is not working yet. attach() assumes that
             * it is attaching a CounterSet and tries to copy the children.
             */
            if(false)
            counters.makePath("indices").attach(new LazyEvaluationCounterSet(1000/*ms*/){
                
                public CounterSet refreshCounterSet() {

                    log.info("Refreshing index counter set.");
                    
                    final CounterSet tmp = new CounterSet();

                    if(!AbstractJournal.this.isOpen()) return tmp;
                    
                    final IIndex name2Addr = getName2Addr();

                    final ITupleIterator itr = name2Addr.rangeIterator(null,
                            null);

                    while (itr.hasNext()) {

                        final ITuple tuple = itr.next();

                        final Entry entry = EntrySerializer.INSTANCE
                                .deserialize(tuple.getValueStream());

                        final IIndex ndx = getIndex(entry.checkpointAddr);

                        /*
                         * @todo might have to clone the counters since the
                         * could already be attached.
                         * 
                         * @todo if the path separator shows up in the index
                         * name then it will cause the path to be broken down
                         * into more components than are warranted.
                         */

                        tmp.makePath(entry.name).attach(ndx.getCounters());

                    }
                    
                    return tmp;

                }
                
            });

        }

        return counters;
        
    }
    private CounterSet counters;
    
    public File getFile() {
        
        return _bufferStrategy.getFile();
        
    }

    /**
     * Core implementation of immediate shutdown handles event reporting.
     */
    protected void _close() {
        
        assertOpen();

        log.info("file="+getFile());
        
        _bufferStrategy.close();

        // report event.
        ResourceManager.closeJournal(getFile() == null ? null : getFile()
                .toString());
        
        if (deleteOnClose) {

            /*
             * This option is used by the test suite and MUST NOT be used with
             * live data.
             */

            deleteResources();
            
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
    public void deleteResources() {

        if(isOpen()) throw new IllegalStateException();

        log.info("");
        
        _bufferStrategy.deleteResources();

        ResourceManager.deleteJournal(getFile() == null ? null : getFile()
                .toString());

    }
    
    /**
     * Restart safe conversion of the store into a read-only store with the
     * specified <i>closeTime</i>.
     * <p>
     * This implementation sets the "closeTime" on the root block such that the
     * journal will no longer accept writes, flushes all buffered writes, and
     * releases any write cache buffers since they will no longer be used. This
     * method is normally used when one journal is being closed out for writes
     * during synchronous overflow processing and new writes will be buffered on
     * a new journal. This has advantages over closing the journal directly
     * including that it does not disturb concurrent readers.
     * <p>
     * Note: The caller MUST have exclusive write access to the journal.
     * <p>
     * Note: This does NOT perform a commit - any uncommitted writes will be
     * discarded.
     * 
     * @throws IllegalStateException
     *             If there are no commits on the journal.
     * 
     * FIXME There should also be an option to convert a journal from
     * {@link BufferMode#Direct} to {@link BufferMode#Disk}. We would want to
     * do that not when the journal is sealed but as soon as asynchronous
     * overflow processing is done. Ideally this will not require us to close
     * and reopen the journal since that will disturb concurrent readers.
     */
    public void closeForWrites(long closeTime) {
        
        log.warn("Closing journal for further writes: closeTime=" + closeTime
                + ", lastCommitTime=" + _rootBlock.getLastCommitTime());
        
        final IRootBlockView old = _rootBlock;

        if (old.getCommitCounter() == 0L) {

            throw new IllegalStateException("No commits on journal");

        }
        
        /*
         * Create the final root block.
         * 
         * Note: We MUST bump the commitCounter in order to have the new root
         * block be selected over the old one!
         * 
         * Note: This will throw an error if nothing has ever been committed on
         * the journal. The problem is that the root block does not permit a
         * non-zero commitCounter unless the commitRecordAddr and perhaps some
         * other stuff are non-zero as well.
         */
        IRootBlockView newRootBlock = new RootBlockView(!old.isRootBlock0(),
                old.getOffsetBits(), old.getNextOffset(), old
                        .getFirstCommitTime(), old.getLastCommitTime(), old
                        .getCommitCounter()+1, old.getCommitRecordAddr(), old
                        .getCommitRecordIndexAddr(), old.getUUID(), old
                        .getCreateTime(), closeTime, checker);

        /*
         * Write it on the store.
         * 
         * Note: We request that the write is forced to disk to ensure that all
         * buffered writes are forced to the disk. This is necessary in order to
         * make sure that the updated root block (and anything left in the write
         * cache for the disk buffer) get forced through onto the disk. We do
         * not need to specify ForceMetadata here since the file size is
         * unchanged by this operation.
         */
        _bufferStrategy.writeRootBlock(newRootBlock, ForceEnum.Force);

        // discard write cache and make store read-only.
        _bufferStrategy.closeForWrites();
        
        // replace the root block reference.
        _rootBlock = newRootBlock;

        // discard current commit record - can be re-read from the store.
        _commitRecord = null;
       
//        close();
        
    }
    
    /**
     * Invokes {@link #shutdownNow()}.
     */
    synchronized public void close() {

        // Note: per contract for close().
        if(!isOpen()) throw new IllegalStateException();
        
        log.info("");
        
        shutdownNow();

    }

    synchronized public void closeAndDelete() {

        // Note: per contract for close().
        if(!isOpen()) throw new IllegalStateException();

        log.info("");
        
        shutdownNow();
        
        if (!deleteOnClose) {

            /*
             * Note: if deleteOnClose was specified then the resource
             * was already deleted by _close().
             */
            
            deleteResources();
            
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

    public IResourceMetadata getResourceMetadata() {
        
        return new JournalMetadata(this);
        
    }
    
    public boolean isOpen() {

        return _bufferStrategy.isOpen();

    }

    /**
     * Return <code>true</code> if the journal was opened in a read-only mode
     * or if {@link #closeForWrites(long)} was used to seal the journal against
     * further writes.
     */
    public boolean isReadOnly() {
        
//        return readOnly || getRootBlockView().getCloseTime() != 0L;
        
        assertOpen();
        
        return _bufferStrategy.isReadOnly();
        
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
     * @param commitTime
     *            The timestamp assigned to the commit.
     * 
     * @return The array of collected root addresses for the registered
     *         committers.
     */
    final private long[] notifyCommitters(final long commitTime) {

        assert commitTime > 0L;
        
        int ncommitters = 0;

        long[] rootAddrs = new long[_committers.length];

        for (int i = 0; i < _committers.length; i++) {

            if (_committers[i] == null)
                continue;

            final long addr = _committers[i].handleCommit(commitTime);
            
            rootAddrs[i] = addr;

            ncommitters++;

        }

        return rootAddrs;

    }

    /**
     * Discards any unisolated writes since the last {@link #commitNow(long)()}
     * and also discards the unisolated (aka live) btree objects, reloading them
     * from the current {@link ICommitRecord} on demand.
     * <p>
     * Note: The {@link WriteExecutorService} handles commit group and uses an
     * index {@link Checkpoint} strategy so that it is able to abort individual
     * tasks simply by discarding their changes and without interrupting
     * concurrent writers. An {@link #abort()} is therefore an action of last
     * resort and is generally triggered by things such as running out of disk
     * space or memory within the JVM.
     * <p>
     * If a {@link Thread}s is interrupted in the midst of an IO operation on a
     * {@link Channel} then the channel will be asynchronously closed by the
     * JDK. Since some {@link IBufferStrategy}s use a {@link FileChannel} to
     * access the backing store, this means that we need to re-open the backing
     * store transparently so that we can continue operations after the commit
     * group was aborted. This is done automatially when we re-load the current
     * {@link ICommitRecord} from the root blocks of the store.
     */
    public void abort() {

        log.info("start");
        
        /*
         * Discard hard references to any indices. The Name2Addr reference will
         * also be discarded below. This should be sufficient to ensure that any
         * index requested by the methods on the AbstractJournal will be re-read
         * from disk using the commit record which we re-load below. This is
         * necessary in order to discard any checkpoints that may have been
         * written on indices since the last commit.
         */
    
        historicalIndexCache.clear();
        
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
         * 
         * Note: This reads on the store. If the backing channel for a stable
         * store was closed by an interrupt, e.g., during an abort, then this
         * will cause the backing channel to be transparent re-opened. At that
         * point both readers and writers will be able to access the channel
         * again.
         */
        
        _commitRecordIndex = getCommitRecordIndex();

        // discard any hard references that might be cached.
        discardCommitters();

        // setup new committers, e.g., by reloading from their last root addr.
        setupCommitters();
        
        log.info("done");

    }

    /**
     * Return the object providing the {@link ILocalTransactionManager} for this
     * journal.
     */
    abstract public ILocalTransactionManager getLocalTransactionManager();
    
    public long commit() {
        
        final long commitTime = getLocalTransactionManager().nextTimestampRobust();
        
        return commitNow( commitTime );
        
    }

    /**
     * An atomic commit is performed by directing each registered
     * {@link ICommitter} to flush its state onto the store using
     * {@link ICommitter#handleCommit(long)}. The address returned by that
     * method is the address from which the {@link ICommitter} may be reloaded
     * (and its previous address if its state has not changed). That address is
     * saved in the {@link ICommitRecord} under the index for which that
     * committer was {@link #registerCommitter(int, ICommitter) registered}. We
     * then force the data to stable store, update the root block, and force the
     * root block and the file metadata to stable store.
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
    protected long commitNow(final long commitTime) {

        assertOpen();

        log.info("commitTime="+commitTime);
        
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
        final long[] rootAddrs = notifyCommitters(commitTime);

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
        final long commitRecordIndexAddr = _commitRecordIndex.writeCheckpoint();

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
                    old.getCreateTime(), old.getCloseTime(),
                    checker);

            _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

            _rootBlock = newRootBlock;

            _commitRecord = commitRecord;

        }

        if(INFO)
        log.info("Done: commitTime="+commitTime+", nextOffset="+nextOffset);
        
        return commitTime;

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

        if(isReadOnly()) {
            
            throw new UnsupportedOperationException();
            
        }
        
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
     * Returns a read-only view of the {@link ICommitRecord} containing the root
     * addresses.
     * <p>
     * Note: Synchronization was added to this method since the
     * {@link StatusThread} and {@link AbstractTask}s may all invoke this
     * concurrently. The synchronization could be removed if we made sure that
     * this was never null outside of initialization or commit.
     * 
     * @return The current {@link ICommitRecord} and never <code>null</code>.
     */
    synchronized public ICommitRecord getCommitRecord() {

        assertOpen();

        if (_commitRecord == null) {

            // the address of the current commit record from the root block.
            final long commitRecordAddr = _rootBlock.getCommitRecordAddr();

            if (commitRecordAddr == 0L) {

                // No commit record on the store yet.
                _commitRecord = new CommitRecord();

            } else {

                // Read the commit record from the store.
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
     * 
     * @todo remove from {@link IJournal} (and all impls) and make [protected].
     */
    public void discardCommitters() {

        // discard.
        name2Addr = null;

    }

    /**
     * The basic implementation sets up the btree that is responsible for
     * resolving named btrees.
     * <p>
     * Subclasses may extend this method to setup their own committers.
     * 
     * @todo remove from {@link IJournal} (and all impls) and make [protected].
     */
    public void setupCommitters() {

        setupName2AddrBTree(getRootAddr(ROOT_NAME2ADDR));

    }

    /*
     * named indices.
     */

    /**
     * Setup the btree that resolves named indices.
     * 
     * @param addr
     *            The root address of the btree -or- 0L iff the btree has not
     *            been defined yet.
     * 
     * @see Options#LIVE_INDEX_CACHE_CAPACITY
     */
    Name2Addr setupName2AddrBTree(long addr) {

        assert name2Addr == null;

        if (addr == 0L) {

            /*
             * Create btree mapping names to addresses.
             * 
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             * 
             * Note: if the journal is read-only the we create the commit record
             * index on an in-memory store in order to avoid triggering a write
             * exception on the journal.
             */

            name2Addr = Name2Addr
                    .create((isReadOnly() ? new SimpleMemoryRawStore() : this));

        } else {

            /*
             * Reload the btree from its checkpoint address.
             * 
             * Note: This is the live view of the B+Tree. In this specific case
             * we DO NOT use the canonicalizing mapping since we do not want
             * anyone else to have access to this same instance of the B+Tree.
             */

            name2Addr = (Name2Addr) BTree.load(this, addr);

        }

        name2Addr.setupCache(liveIndexCacheCapacity);
        
        // register for commit notices.
        setCommitter(ROOT_NAME2ADDR, name2Addr);

        return name2Addr;
        
    }

    /**
     * Re-load the last committed state of the index that resolves timestamps to
     * {@link ICommitRecord}s and create it if the index does not exist.
     * 
     * @return The {@link CommitRecordIndex}.
     * 
     * @see #_commitRecordIndex
     */
    protected CommitRecordIndex getCommitRecordIndex() {

        final long addr = _rootBlock.getCommitRecordIndexAddr();

        try {
            
            return getCommitRecordIndex(addr);
    
        } catch (RuntimeException ex) {
            
            /*
             * Log the root block for post-mortem.
             */
            log.fatal("Could not read the commit record index:\n" + _rootBlock,
                    ex);

//            // Log the commit record also, if possible.
//            try {
//                log.fatal("commitRecord:" + getCommitRecord());
//            } catch (Exception ex2) {
//                /* Ignore. */
//            }
            
            throw ex;
            
        }
        
    }
    
    /**
     * Create or re-load the index that resolves timestamps to
     * {@link ICommitRecord}s.
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
    protected CommitRecordIndex getCommitRecordIndex(long addr) {

        log.info("addr=" + toString(addr));
        
        final CommitRecordIndex ndx;

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             * 
             * Note: if the journal is read-only then we create the commit record
             * index on an in-memory store in order to avoid triggering a write
             * exception on the journal.
             */

            // create btree mapping names to addresses.
            ndx = CommitRecordIndex.create((isReadOnly()?new SimpleMemoryRawStore() :this));

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
     *       to locate named indices but the {@link WriteExecutorService} can
     *       also write on this index. I have tried some different approaches to
     *       handling this.
     */
    public ICommitRecord getCommitRecord(long commitTime) {

        assertOpen();

        return _commitRecordIndex.find(commitTime);

    }

    /**
     * Return the first commit record whose timestamp is strictly greater than
     * the given commitTime.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The commit record -or- <code>null</code> if there is no commit
     *         record whose timestamp is strictly greater than <i>commitTime</i>.
     */
    public ICommitRecord getCommitRecordStrictlyGreaterThan(long commitTime) {

        assertOpen();

        return _commitRecordIndex.findNext(commitTime);

    }

    /**
     * @todo verify that {@link Journal#getIndex(String, long)} can in fact
     *       return a view.
     */
    public IIndex getIndex(String name, long commitTime) {

        assertOpen();

        final ICommitRecord commitRecord = getCommitRecord(commitTime);

        if (commitRecord == null) {
            
            log.info("No commit record for timestamp=" + commitTime);

            return null;
            
        }
        
        return getIndex(name, commitRecord );
        
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
     * accomplished using {@link BTree#setReadOnly(boolean)}.
     * 
     * @return The named index -or- <code>null</code> iff the named index did
     *         not exist as of that commit record.
     */
    public BTree getIndex(String name, ICommitRecord commitRecord) {

        assertOpen();

        if (name == null)
            throw new IllegalArgumentException();

        if (commitRecord == null)
            throw new IllegalArgumentException();

        /*
         * The address of an historical Name2Addr mapping used to resolve named
         * indices for the historical state associated with this commit record.
         */
        final long checkpointAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);

        if (checkpointAddr == 0L) {

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
        final Name2Addr name2Addr = (Name2Addr)getIndex(checkpointAddr);
        
        /*
         * The address at which the named index was written for that historical
         * state.
         */
        final Name2Addr.Entry entry = name2Addr.getEntry(name);
        
        if (entry == null) {

            // No such index by name for that historical state.

            return null;
            
        }
        
        /*
         * Resolve the named index using the object cache to impose a
         * canonicalizing mapping on the historical named indices based on the
         * address on which it was written in the store.
         */

        final BTree btree = getIndex(entry.checkpointAddr);
        
        assert entry.commitTime != 0L : "Entry="+entry;
        
        // Set the last commit time on the btree.
        btree.setLastCommitTime(entry.commitTime);
        
        return btree;

    }
    
    /**
     * A canonicalizing mapping for {@link BTree}s.
     * 
     * @param checkpointAddr
     *            The address of the {@link Checkpoint} record for the {@link BTree}.
     *            
     * @return The {@link BTree} loaded from that {@link Checkpoint}.
     * 
     * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
     */
    final public BTree getIndex(long checkpointAddr) {
        
        synchronized (historicalIndexCache) {

            BTree obj = (BTree) historicalIndexCache.get(checkpointAddr);

            if (obj == null) {

                // Note: Does not set lastCommitTime.
                obj = BTree.load(this, checkpointAddr);
                
            }
            
            historicalIndexCache.put(checkpointAddr, (ICommitter)obj, false/*dirty*/);
    
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
     * Registers a named index. Once registered the index will participate in
     * atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @exception IllegalStateException
     *                if there is an index already registered under that name.
     *                
     * @see Options#BRANCHING_FACTOR
     * 
     * @deprecated This is only used by the test suites.
     */
    public BTree registerIndex(String name) {

        IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());

        metadata.setBranchingFactor(defaultBranchingFactor);
        
//        metadata.setIsolatable(true);
        
        return registerIndex(name, metadata);
        
    }
    
    /**
     * Registers a named index. Once registered the index will participate in
     * atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public void registerIndex(IndexMetadata metadata) {

        registerIndex(metadata.getName(),metadata);
        
    }
    
    /**
     * Registers a named index. Once registered the index will participate in
     * atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public BTree registerIndex(String name, IndexMetadata metadata) {

        validateIndexMetadata(name, metadata);
        
        final BTree btree = BTree.create(this, metadata);

        return registerIndex(name, btree);
        
    }
    
    /**
     * Validates some aspects of the {@link IndexMetadata} for an index
     * partition and sets the {@link IResourceMetadata} to this journal if it is
     * <code>null</code> since a remote caller can not have the correct
     * metadata on hand when they formulate the request.
     */
    protected void validateIndexMetadata(String name, IndexMetadata metadata) {

        final LocalPartitionMetadata pmd = metadata.getPartitionMetadata();
        if (pmd != null) {

            if (pmd.getResources() == null) {

                /*
                 * A [null] for the resources field is a specific indication
                 * that we need to specify the resource metadata for the live
                 * journal at the time that the index partition is registered.
                 * This indicator is used when the metadata service registers an
                 * index partition remotely on a data service since it does not
                 * (and can not) have access to the resource metadata for the
                 * live journal as of the time that the index partition actually
                 * gets registered on the data service.
                 * 
                 * The index partition split and join tasks do not have this
                 * problem since they are run locally. However, an index
                 * partition move operation also needs to do this.
                 */

                metadata.setPartitionMetadata(//
                        new LocalPartitionMetadata(//
                                pmd.getPartitionId(),//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey(),//
                                new IResourceMetadata[] {//
                                    // The live journal.
                                    getResourceMetadata() //
                                },
                                /*
                                 * Note: Retains whatever history given by the
                                 * caller.
                                 */
                                pmd.getHistory()+
                                "register(name="+name+",partitionId="+pmd.getPartitionId()+") "
                                ));

            } else {

                if (pmd.getResources().length == 0) {

                    throw new RuntimeException(
                            "Missing resource description: name=" + name
                                    + ", pmd=" + pmd);

                }

                if (!pmd.getResources()[0].isJournal()) {

                    throw new RuntimeException(
                            "Expecting resources[0] to be journal: name="
                                    + name + ", pmd=" + pmd);

                }

                if (!pmd.getResources()[0].getUUID().equals(
                        getRootBlockView().getUUID())) {

                    throw new RuntimeException(
                            "Expecting resources[0] to be this journal but has wrong UUID: name="
                                    + name + ", pmd=" + pmd);

                }

            }
            
        }

    }
    
    /**
     * Registers a named index (core impl). Once registered the index will
     * participate in atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public BTree registerIndex(String name, BTree ndx) {

        assertOpen();

        /*
         * This is a minor performance tweak. It flushes the index to the
         * backing store before we synchronize on [name2addr] in order to afford
         * greater concurrency.
         * 
         * Note: this is wasted effort only in the case where the index is
         * pre-existing as we would NOT flush it to disk in that case. In the
         * index to be registered is empty or if indices are not normally
         * pre-existing then this should be a performance win where a large #of
         * indices are created concurrently.
         * 
         * Note: in the case where the index is pre-existing, this will also
         * force a commit since the caller's index is flushed to the backing
         * store before we know that the index already exists. For this reason I
         * recommend against this (and it will break some unit tests in
         * TestAddDropIndex if you do this).
         */

//        ((ICommitter)ndx).handleCommit();
        
        synchronized (name2Addr) {
                
            // add to the persistent name map.
            name2Addr.registerIndex(name, ndx);

            // report event (the application has access to the named index).
            ResourceManager.openUnisolatedBTree(name);

        }

        return ndx;
        
    }

    /**
     * Drops the named index. The index will no longer participate in atomic
     * commits and will not be visible to new transactions. Resources are NOT
     * reclaimed on the {@link AbstractJournal} (it is an immortal store) and
     * historical states of the index will continue to be accessible.
     */
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
     * KNOW that you are the only writer. See {@link ConcurrencyManager}, which
     * handles exclusive locks for unisolated indices. Other consumers SHOULD
     * use {@link BTree#setReadOnly(boolean)} to avoid the possibility of
     * mishap.
     * 
     * @return The mutable view of the index.
     */
    public BTree getIndex(String name) {

        assertOpen();

        if (name == null)
            throw new IllegalArgumentException();

        if (Thread.interrupted()) {

            throw new RuntimeException(new InterruptedException());
            
        }
        
        // Note: NullPointerException can be thrown here if asynchronously closed.
        synchronized (name2Addr) {

            return name2Addr.get(name);

        }

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
