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

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.LRUNexus;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.btree.keys.ICUVersionRecord;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerRangeValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongRangeValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.ResourceManager;
import com.bigdata.rwstore.IAllocationContext;
import com.bigdata.util.ChecksumUtility;

/**
 * <p>
 * The journal is a persistence capable data structure supporting atomic commit,
 * named indices, and full transactions. The {@link BufferMode#DiskRW} mode
 * provides an persistence scheme based on reusable allocation slots while the
 * {@link BufferMode#DiskWORM} mode provides an append only persistence scheme.
 * Journals may be configured in highly available quorums.
 * </p>
 * <p>
 * This class is an abstract implementation of the {@link IJournal} interface
 * that does not implement the {@link IConcurrencyManager},
 * {@link IResourceManager}, or {@link ITransactionService} interfaces. The
 * {@link Journal} provides a concrete implementation that may be used for a
 * standalone database complete with concurrency control and transaction
 * management.
 * </p> <h2>Limitations</h2>
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
 * @todo There are lots of annoying ways in which asynchronously closing the
 *       journal, e.g., using {@link #close()} or {@link #shutdown()} can cause
 *       exceptions to be thrown out of concurrent threads. It would be nice if
 *       we could throw a single exception that indicated that the journal had
 *       been asynchronously closed.
 */
public abstract class AbstractJournal implements IJournal/* , ITimestampService */{

	/**
	 * Logger.
	 */
	private static final Logger log = Logger.getLogger(AbstractJournal.class);

	/**
	 * The index of the root address containing the address of the persistent
	 * {@link Name2Addr} mapping names to {@link BTree}s registered for the
	 * store.
	 */
	public static transient final int ROOT_NAME2ADDR = 0;

    /**
     * The index of the root address where the root block copy from the previous
     * commit is stored.
     */
	public static transient final int PREV_ROOTBLOCK = 1;

	/**
	 * The index of the root address of the delete blocks associated with
	 * this transaction.
	 */
	public static transient final int DELETEBLOCK = 2;

    /**
     * The index of the root address containing the {@link ICUVersionRecord}.
     * That record specifies the ICU version metadata which was in force when
     * the journal was created.
     */
    public static transient final int ROOT_ICUVERSION = 3;
    
	/**
	 * A clone of the properties used to initialize the {@link Journal}.
	 */
	final protected Properties properties;

    /**
     * The #of open journals (JVM wide). This is package private. It is used to
     * chase down unit tests which are not closing() the Journal.
     */
    final static AtomicInteger nopen = new AtomicInteger();

    /**
     * The #of closed journals (JVM wide). This is package private. It is used
     * to chase down unit tests which are not {@link #close() closing} the
     * Journal.
     */
    final static AtomicInteger nclose = new AtomicInteger();

    /**
     * The #of destroyed journals (JVM wide). This is package private. It is
     * used to chase down unit tests which are not {@link #destroy() destroying}
     * the journal.
     */
    final static AtomicInteger ndestroy = new AtomicInteger();
    
	/**
	 * The directory that should be used for temporary files.
	 */
	final public File tmpDir;

	/**
	 * The object used by the journal to compute the checksums of its root
	 * blocks (this object is NOT thread-safe so there is one instance per
	 * journal).
	 */
	private final ChecksumUtility checker = new ChecksumUtility();

	// /*
	// * These fields were historically marked as [final] and set by the
	// * constructor. With the introduction of high availability these fields
	// can
	// * not be final because the CREATE of the journal must be deferred until a
	// * quorum leader has been elected.
	// *
	// * The pattern for these fields is that they are assigned by create() and
	// * are thereafter immutable. The fields are marked as [volatile] so the
	// * state change when they are set will be visible without explicit
	// * synchronization (many methods use volatile reads on these fields).
	// */

	/**
	 * The metadata for a pre-existing journal -or- <code>null</code> if the
	 * journal was created for the first time.
	 */
	private FileMetadata fileMetadata;

	/** package private method exported to {@link DumpJournal}. */
	FileMetadata getFileMetadata() {
		return fileMetadata;
	}

	/**
	 * The implementation logic for the current {@link BufferMode}.
	 */
	private final IBufferStrategy _bufferStrategy;

	/**
	 * A description of the journal as a resource.
	 */
	private final JournalMetadata journalMetadata;

	/*
     * 
     */

	/**
	 * The current root block. This is updated each time a new root block is
	 * written.
	 */
	private volatile IRootBlockView _rootBlock;

	/**
	 * The registered committers for each slot in the root block.
	 */
	private volatile ICommitter[] _committers = new ICommitter[ICommitRecord.MAX_ROOT_ADDRS];

	/*
	 * 
	 */
	
	/**
	 * This lock is used to prevent clearing or setting of critical fields while
	 * a concurrent thread is seeking to operate on the referenced objects. A
	 * read-write lock was chosen because nearly all operations are "reads" (the
	 * read the field reference). For this purpose the "writer" is a thread
	 * which causes the value of either of these fields to be changed. The
	 * "writers" are {@link #abort()}, {@link #closeForWrites(long)}, etc. These
	 * methods are invoked relatively infrequently in comparison with "read"
	 * access to these fields.
	 */
	private final ReentrantReadWriteLock _fieldReadWriteLock = new ReentrantReadWriteLock(false/* fair */);

	/**
	 * Used to cache the most recent {@link ICommitRecord} -- discarded on
	 * {@link #abort()}; set by {@link #commitNow(long)}.
	 */
	private volatile ICommitRecord _commitRecord;

	/**
	 * BTree mapping commit timestamps to the address of the corresponding
	 * {@link ICommitRecord}. The keys are timestamps (long integers). The
	 * values are the address of the {@link ICommitRecord} with that commit
	 * timestamp.
	 * <p>
	 * Note: The {@link CommitRecordIndex} object is NOT systematically
	 * protected by <code>synchronized</code> within this class. Therefore it is
	 * NOT safe for use by outside classes and CAN NOT be made safe simply by
	 * synchronizing their access on the {@link CommitRecordIndex} object
	 * itself. This is mainly for historical reasons and it may be possible to
	 * systematically protect access to this index within a synchronized block
	 * and then expose it to other classes.
	 */
	private volatile CommitRecordIndex _commitRecordIndex;

	/**
	 * The {@link ICUVersionRecord} iff known.
	 */
	private volatile ICUVersionRecord _icuVersionRecord;
	
	/**
	 * The configured capacity for the {@link HardReferenceQueue} backing the
	 * index cache maintained by the "live" {@link Name2Addr} object.
	 * 
	 * @see Options#LIVE_INDEX_CACHE_CAPACITY
	 */
	private final int liveIndexCacheCapacity;

	/**
	 * The configured timeout in milliseconds for stale entries in the
	 * {@link HardReferenceQueue} backing the index cache maintained by the
	 * "live" {@link Name2Addr} object.
	 * 
	 * @see Options#LIVE_INDEX_CACHE_TIMEOUT
	 */
	private final long liveIndexCacheTimeout;

	/**
	 * The configured capacity for the LRU backing the
	 * {@link #historicalIndexCache}.
	 * 
	 * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
	 */
	private final int historicalIndexCacheCapacity;

	/**
	 * The configured timeout for stale entries in the
	 * {@link HardReferenceQueue} backing the {@link #historicalIndexCache}.
	 * 
	 * @see Options#HISTORICAL_INDEX_CACHE_TIMEOUT
	 */
	private final long historicalIndexCacheTimeout;

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
	 * @see Options#HISTORICAL_INDEX_CACHE_TIMEOUT
	 */
	// final private WeakValueCache<Long, ICommitter> historicalIndexCache;
	final private ConcurrentWeakValueCache<Long, BTree> historicalIndexCache;

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
	 * <code>synchronized({@link #_name2Addr})</code>.
	 * 
	 * @see #getName2Addr()
	 */
	private volatile Name2Addr _name2Addr;

	/**
	 * Return the "live" BTree mapping index names to the last metadata record
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
	 * Note: the "live" {@link Name2Addr} index is a mutable {@link BTree}. All
	 * access to the object MUST be synchronized on that object.
	 */
	protected Name2Addr _getName2Addr() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			final Name2Addr tmp = _name2Addr;

			if (tmp == null)
				throw new AssertionError();

			return tmp;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * A read-only view of the {@link Name2Addr} object mapping index names to
	 * the most recent committed {@link Entry} for the named index. The keys are
	 * index names (unicode strings). The values are {@link Entry}s containing
	 * the names, commitTime, and last known {@link Checkpoint} address of the
	 * named {@link BTree} on the {@link Journal}.
	 */
	public IIndex getName2Addr() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			final long checkpointAddr;

			if (_name2Addr == null) {

				checkpointAddr = getRootAddr(ROOT_NAME2ADDR);

			} else {

				checkpointAddr = _name2Addr.getCheckpoint().getCheckpointAddr();

			}

			/*
			 * Note: This uses the canonicalizing mapping to get an instance
			 * that is distinct from the live #name2Addr object while not
			 * allowing more than a single such distinct instance to exist for
			 * the current name2Addr object.
			 */
			final BTree btree = getIndex(checkpointAddr);

			/*
			 * Wrap up in a read-only view since writes MUST NOT be allowed.
			 */
			return new ReadOnlyIndex(btree);

		} finally {

			lock.unlock();

		}

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
	public IIndex getName2Addr(final long commitTime) {

		final ICommitRecord commitRecord = getCommitRecord(commitTime);

		if (commitRecord == null) {

			return null;

		}

		final long checkpointAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);

		return new ReadOnlyIndex(getIndex(checkpointAddr));

	}

	/**
	 * Return the root block view associated with the commitRecord for the
	 * provided commit time.  This requires accessing the next commit record
	 * since the previous root block is stored with each record.
	 * 
	 * @param commitTime
	 *            A commit time.
	 * 
	 * @return The root block view -or- <code>null</code> if there is no commit
	 *         record for that commitTime.
	 * 
	 */
	public IRootBlockView getRootBlock(final long commitTime) {

		final ICommitRecord commitRecord = getCommitRecordIndex().findNext(commitTime);

		if (commitRecord == null) {
			return null;
		}

		final long rootBlockAddr = commitRecord.getRootAddr(PREV_ROOTBLOCK);
		
		if (rootBlockAddr == 0) {
			return null;
		} else {
			ByteBuffer bb = read(rootBlockAddr);
			
			return new RootBlockView(true /* rb0 - WTH */, bb, checker);
		}

	}
	
	/**
	 * 
	 * @param startTime from which to begin iteration
	 * 
	 * @return an iterator over the committed root blocks
	 */
	public Iterator<IRootBlockView> getRootBlocks(final long startTime) {
		return new Iterator<IRootBlockView>() {
			ICommitRecord commitRecord = getCommitRecordIndex().findNext(startTime);

			public boolean hasNext() {
				return commitRecord != null;
			}
			
			public IRootBlockView next() {
				final long rootBlockAddr = commitRecord.getRootAddr(PREV_ROOTBLOCK);
				
				commitRecord = getCommitRecordIndex().findNext(commitRecord.getTimestamp());
				
				if (rootBlockAddr == 0) {
					return null;
				} else {
					ByteBuffer bb = read(rootBlockAddr);
					
					return new RootBlockView(true /* rb0 - WTH */, bb, checker);
				}
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	/**
	 * True iff the journal was opened in a read-only mode.
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
	private final long initialExtent;
	private final long minimumExtension;

	private RootBlockCommitter m_rootBlockCommitter;

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
	 * Resolves the property value (static variant for ctor initialization).
	 * 
	 * @see Configuration#getProperty(IIndexManager, Properties, String, String,
	 *      String)
	 */
	static protected String getProperty(final Properties properties, final String name, final String defaultValue) {

		return Configuration.getProperty(null/* indexManager */, properties, ""/*
																			 * no
																			 * namespace
																			 */, name, defaultValue);

	}

	/**
	 * Resolves the property value.
	 * 
	 * @see Configuration#getProperty(IIndexManager, Properties, String, String,
	 *      String)
	 */
	protected String getProperty(final String name, final String defaultValue) {

		return Configuration.getProperty(this, properties, ""/* no namespace */, name, defaultValue);

	}

	/**
	 * Resolves, parses, and validates the property value.
	 * 
	 * @param name
	 *            The property name.
	 * @param defaultValue
	 *            The default value.
	 * @return
	 */
	protected <E> E getProperty(final String name, final String defaultValue, IValidator<E> validator) {

		return Configuration.getProperty(this, properties, ""/* no namespace */, name, defaultValue, validator);

	}

    /**
     * Create or re-open a journal.
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
    protected AbstractJournal(final Properties properties) {

        this(properties, null/* quorum */);

    }

    /**
     * Create or re-open a journal as part of a highly available {@link Quorum}.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * @param quorum
     *            The quorum with which the journal will join (HA mode only).
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     */
    protected AbstractJournal(Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties = (Properties) properties.clone();

        // null unless in HA mode.
		this.quorum = quorum;
		
		/*
		 * Set various 'final' properties.
		 */
		{

			historicalIndexCacheCapacity = getProperty(Options.HISTORICAL_INDEX_CACHE_CAPACITY,
					Options.DEFAULT_HISTORICAL_INDEX_CACHE_CAPACITY, IntegerValidator.GT_ZERO);

			historicalIndexCacheTimeout = getProperty(Options.HISTORICAL_INDEX_CACHE_TIMEOUT,
					Options.DEFAULT_HISTORICAL_INDEX_CACHE_TIMEOUT, LongValidator.GTE_ZERO);

			// historicalIndexCache = new WeakValueCache<Long, ICommitter>(
			// new LRUCache<Long, ICommitter>(historicalIndexCacheCapacity));

			historicalIndexCache = new ConcurrentWeakValueCacheWithTimeout<Long, BTree>(historicalIndexCacheCapacity,
					TimeUnit.MILLISECONDS.toNanos(historicalIndexCacheTimeout));

			liveIndexCacheCapacity = getProperty(Options.LIVE_INDEX_CACHE_CAPACITY,
					Options.DEFAULT_LIVE_INDEX_CACHE_CAPACITY, IntegerValidator.GT_ZERO);

			liveIndexCacheTimeout = getProperty(Options.LIVE_INDEX_CACHE_TIMEOUT,
					Options.DEFAULT_LIVE_INDEX_CACHE_TIMEOUT, LongValidator.GTE_ZERO);

		}

		initialExtent = getProperty(Options.INITIAL_EXTENT, Options.DEFAULT_INITIAL_EXTENT, new LongRangeValidator(
				Options.minimumInitialExtent, Long.MAX_VALUE));

		maximumExtent = getProperty(Options.MAXIMUM_EXTENT, Options.DEFAULT_MAXIMUM_EXTENT, new LongRangeValidator(
				initialExtent, Long.MAX_VALUE));

		minimumExtension = getProperty(Options.MINIMUM_EXTENSION, Options.DEFAULT_MINIMUM_EXTENSION,
				new LongRangeValidator(Options.minimumMinimumExtension, Long.MAX_VALUE));

        readOnly = (Boolean.parseBoolean(getProperty(Options.READ_ONLY, Options.DEFAULT_READ_ONLY)));

		forceOnCommit = ForceEnum.parse(getProperty(Options.FORCE_ON_COMMIT, Options.DEFAULT_FORCE_ON_COMMIT));

		doubleSync = Boolean.parseBoolean(getProperty(Options.DOUBLE_SYNC, Options.DEFAULT_DOUBLE_SYNC));

		deleteOnClose = Boolean.parseBoolean(getProperty(Options.DELETE_ON_CLOSE, Options.DEFAULT_DELETE_ON_CLOSE));

		// "tmp.dir"
		{

			tmpDir = new File(getProperty(Options.TMP_DIR, System.getProperty("java.io.tmpdir")));

			if (!tmpDir.exists()) {

				if (!tmpDir.mkdirs()) {

					throw new RuntimeException("Could not create directory: " + tmpDir.getAbsolutePath());

				}

			}

			if (!tmpDir.isDirectory()) {

				throw new RuntimeException("Not a directory: " + tmpDir.getAbsolutePath());

			}

		}

		/*
		 * Create the appropriate IBufferStrategy object.
		 * 
		 * Note: the WriteLock is obtained here because various methods such as
		 * _getCommitRecord() assert that the caller is holding the write lock
		 * in order to provide runtime safety checks.
		 */
		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			/*
			 * "bufferMode" mode.
			 * 
			 * Note: very large journals MUST use the disk-based mode.
			 */
			if (BufferMode.valueOf(getProperty(Options.BUFFER_MODE, Options.DEFAULT_BUFFER_MODE)) == BufferMode.Transient) {

				/*
				 * Setup the buffer strategy.
				 */

				if (readOnly) {

					throw new RuntimeException("readOnly not supported for transient journals.");

				}

				fileMetadata = null;

				final boolean useDirectBuffers = Boolean
						.parseBoolean(getProperty(Options.USE_DIRECT_BUFFERS,
								Options.DEFAULT_USE_DIRECT_BUFFERS));

				final int offsetBits = getProperty(
						Options.OFFSET_BITS,
						Integer
								.toString((this instanceof Journal ? WormAddressManager.SCALE_UP_OFFSET_BITS
										: WormAddressManager.SCALE_OUT_OFFSET_BITS)),
						new IntegerRangeValidator(
								WormAddressManager.MIN_OFFSET_BITS,
								WormAddressManager.MAX_OFFSET_BITS));

				final long createTime = Long.parseLong(getProperty(
						Options.CREATE_TIME, "" + System.currentTimeMillis()));

				_bufferStrategy = new TransientBufferStrategy(offsetBits, initialExtent, 0L/*
																							 * soft
																							 * limit
																							 * for
																							 * maximumExtent
																							 */, useDirectBuffers);

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
				final IRootBlockView rootBlock0 = new RootBlockView(true, offsetBits, nextOffset, firstCommitTime,
						lastCommitTime, commitCounter, commitRecordAddr, commitRecordIndexAddr, uuid, quorumToken, 0L, // metaStartAddr
						0L, // metaStartBits
						StoreTypeEnum.WORM,//
						createTime, closedTime, RootBlockView.currentVersion, checker);
				final IRootBlockView rootBlock1 = new RootBlockView(false, offsetBits, nextOffset, firstCommitTime,
						lastCommitTime, commitCounter, commitRecordAddr, commitRecordIndexAddr, uuid, quorumToken, 0L, // metaStartAddr
						0L, // metaStartBits
						StoreTypeEnum.WORM,//
						createTime, closedTime, RootBlockView.currentVersion, checker);
				_bufferStrategy.writeRootBlock(rootBlock0, ForceEnum.No);
				_bufferStrategy.writeRootBlock(rootBlock1, ForceEnum.No);

				this._rootBlock = rootBlock1;

			} else {

				/*final FileMetadata*/ fileMetadata = FileMetadata.createInstance(
						properties, !(this instanceof Journal), quorumToken);

				final BufferMode bufferMode = fileMetadata.bufferMode;

//				/*
//				 * Note: The caller SHOULD specify an explicit [createTime] when
//				 * its value is critical. The default assigned here does NOT
//				 * attempt to use a clock that is consistent with the commit
//				 * protocol or even a clock that assigns unique timestamps.
//				 */
//				final long createTime = Long
//						.parseLong(getProperty(Options.CREATE_TIME, "" + System.currentTimeMillis()));
//
//				assert createTime != 0L;

				switch (bufferMode) {

				case Direct: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new DirectBufferStrategy(0L/*
																 * soft limit
																 * for
																 * maximumExtent
																 */, fileMetadata);

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

				case Mapped: {

					/*
					 * Setup the buffer strategy.
					 */

					/*
					 * Note: the maximumExtent is a hard limit in this case only
					 * since resize is not supported for mapped files.
					 */
					_bufferStrategy = new MappedBufferStrategy(maximumExtent /*
																			 * hard
																			 * limit
																			 * for
																			 * maximum
																			 * extent
																			 */, fileMetadata);

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

//				case Disk: {
//
//					/*
//					 * Setup the buffer strategy.
//					 */
//
//					_bufferStrategy = new DiskOnlyStrategy(0L/*
//															 * soft limit for
//															 * maximumExtent
//															 */,
//					// minimumExtension,
//							fileMetadata);
//
//					this._rootBlock = fileMetadata.rootBlock;
//
//					break;
//
//				}
				case Disk:
				case DiskWORM: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new WORMStrategy(
					        0L,// soft limit for maximumExtent
					        minimumExtension,//
                            fileMetadata, //
                            quorum//
                            );

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

				case DiskRW: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new RWStrategy(fileMetadata, quorum);

					this._rootBlock = fileMetadata.rootBlock;
					
					break;

				}

				case TemporaryRW: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new RWStrategy(fileMetadata, quorum);

					this._rootBlock = fileMetadata.rootBlock;
					
					break;

				}

				case Temporary: {

					/*
					 * Setup the buffer strategy.
					 * 
					 * FIXME Add test suite for this buffer mode. It should
					 * support MRMW but is not restart-safe.
					 */

					// FIXME Change BufferMode.Temporary to use WORMStrategy
					_bufferStrategy = new DiskOnlyStrategy(0L/*
															 * soft limit for
															 * maximumExtent
															 */,
					// minimumExtension,
							fileMetadata);

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

				default:

					throw new AssertionError();

				}
			}

            /*
             * Note: Creating a new journal registers some internal indices but
             * does NOT perform a commit. Those indices will become restart safe
             * with the first commit.
             */
			
			// Save resource description (sets value returned by getUUID()).
			this.journalMetadata = new JournalMetadata(this);

			// new or reload from the store root block.
			this._commitRecord = _getCommitRecord();

			// new or re-load commit record index from store via root block.
			this._commitRecordIndex = _getCommitRecordIndex();

            // new or re-load from the store.
			this._icuVersionRecord = _getICUVersionRecord();

			// verify the ICU version.
            if (this._icuVersionRecord != null
                    && !ICUVersionRecord.newInstance().equals(
                            this._icuVersionRecord)) {

                final boolean update = Boolean.valueOf(properties.getProperty(
                        Options.UPDATE_ICU_VERSION, "false"));

                if (!update) {

                    throw new RuntimeException("ICUVersionChange: store="
                            + this._icuVersionRecord + ", runtime="
                            + ICUVersionRecord.newInstance());

                }
                
			}
			
			// Give the store a chance to set any committers that it defines.
			setupCommitters();

			// report event.
			ResourceManager.openJournal(getFile() == null ? null : getFile().toString(), size(), getBufferStrategy()
					.getBufferMode());
			
		} finally {

			lock.unlock();

		}

		nopen.incrementAndGet();
		
	}

	/**
	 * @todo consider making the properties restart safe so that they can be
	 *       read from the journal. This will let some properties be specified
	 *       on initialization while letting others default or be overridden on
	 *       restart. This is trivially accomplished by dedicating a root slot
	 *       to a Properties object, or a flattened Properties object serialized
	 *       as key-value pairs, in which case the data could just be loaded
	 *       into a btree and the btree api could be used to change the
	 *       persistent properties as necessary.
	 */
	final public Properties getProperties() {

		return new Properties(properties);

	}

	/**
	 * The delegate that implements the {@link BufferMode}.
	 * <p>
	 * Note: this method MUST NOT check to see whether the journal is open since
	 * we need to use it if we want to invoke
	 * {@link IBufferStrategy#deleteResources()} and we can only invoke that
	 * method once the journal is closed.
	 */
	final public IBufferStrategy getBufferStrategy() {

		return _bufferStrategy;

	}

	/**
	 * Service for running arbitrary tasks in support of
	 * {@link IResourceLocator}. There is no concurrency control associated with
	 * this service, but tasks run here may submit tasks to the
	 * {@link ConcurrencyManager}.
	 */
	abstract public ExecutorService getExecutorService();

	/**
	 * Shutdown the journal (running tasks will run to completion, but no new
	 * tasks will start).
	 * <p>
	 * Note: You SHOULD use this method rather than {@link #close()} for normal
	 * shutdown of the journal.
	 * 
	 * @see #shutdownNow()
	 */
	synchronized public void shutdown() {

		// Note: per contract for shutdown.
		if (!isOpen())
			return;

		if (log.isInfoEnabled())
			log.info("");

		// close immediately.
		_close();

		if (log.isInfoEnabled())
			log.info("Shutdown complete.");

	}

	/**
	 * Immediate shutdown (running tasks are canceled rather than being
	 * permitted to complete).
	 * 
	 * @see #shutdown()
	 */
	synchronized public void shutdownNow() {

		// Note: per contract for shutdownNow()
		if (!isOpen())
			return;

		if (log.isInfoEnabled())
			log.info("");

		// close immediately.
		_close();

		if (log.isInfoEnabled())
			log.info("Shutdown complete.");

	}

	/**
	 * Closes out the journal iff it is still open.
	 */
    protected void finalize() throws Throwable {
        
		if (_bufferStrategy.isOpen()) {

			if (log.isInfoEnabled())
				log.info("Closing journal: " + getFile());

			shutdownNow();

		}

	}

	/**
	 * Return counters reporting on various aspects of the journal.
	 */
	public CounterSet getCounters() {

		return CountersFactory.getCounters(this);

	}

	/**
	 * Note: A combination of a static inner class and a weak reference to the
	 * outer class are used to avoid the returned {@link CounterSet} having a
	 * hard reference to the outer class while retaining the ability to update
	 * the {@link CounterSet} dynamically as long as the referenced object
	 * exists.
	 * <p>
	 * Note: one-shot counter are NOT used so that the LBS can aggregate the
	 * different values which this counter takes on across different live
	 * journal instances for the same data service. For example, the createTime
	 * for each live journal or the name of the file backing the current live
	 * journal.
	 */
	private static class CountersFactory {

		static public CounterSet getCounters(final AbstractJournal jnl) {

			final CounterSet counters = new CounterSet();

			final WeakReference<AbstractJournal> ref = new WeakReference<AbstractJournal>(jnl);

			counters.addCounter("file", new Instrument<String>() {
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
                        final File file = jnl.getFile();
                        if (file != null)
                            setValue(file.toString());
                    }
				}
			});
			// counters.addCounter("file", new OneShotInstrument<String>(""
			// + jnl.getFile()));

			counters.addCounter("createTime", new Instrument<Long>() {
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final IRootBlockView rootBlock = jnl._rootBlock;
						if (rootBlock != null) {
							setValue(rootBlock.getCreateTime());
						}
					}
				}
			});

			counters.addCounter("closeTime", new Instrument<Long>() {
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final IRootBlockView rootBlock = jnl._rootBlock;
						if (rootBlock != null) {
							setValue(rootBlock.getCloseTime());
						}
					}
				}
			});

			counters.addCounter("commitCount", new Instrument<Long>() {
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final IRootBlockView rootBlock = jnl._rootBlock;
						if (rootBlock != null) {
							setValue(rootBlock.getCommitCounter());
						}
					}
				}
			});

			counters.addCounter("historicalIndexCacheSize", new Instrument<Integer>() {
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						setValue(jnl.historicalIndexCache.size());
					}
				}
			});

			counters.addCounter("liveIndexCacheSize", new Instrument<Integer>() {
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final Name2Addr name2Addr = jnl._name2Addr;
						if (name2Addr != null) {
							setValue(name2Addr.getIndexCacheSize());
						}
					}
				}
			});

			counters.attach(jnl._bufferStrategy.getCounters());

			return counters;

		}

	}

	// /**
	// * Returns a {@link CounterSet} that it reflects the {@link Counter}s for
	// * the currently open unisolated named indices as reported by
	// * {@link Name2Addr#getIndexCounters()}.
	// */
	// public CounterSet getNamedIndexCounters() {
	//
	// if (log.isInfoEnabled())
	// log.info("Refreshing index counter set.");
	//
	// /*
	// * @todo probably does not need to be synchronized any more.
	// */
	// // synchronized (name2Addr) {
	//
	// return name2Addr.getIndexCounters();
	//
	// // }
	//            
	// }

	final public File getFile() {

		final IBufferStrategy tmp = getBufferStrategy();

		if (tmp == null)
			return null;

		return tmp.getFile();

	}

	/**
	 * Core implementation of immediate shutdown handles event reporting.
	 */
	protected void _close() {

		assertOpen();

		if (log.isInfoEnabled())
			log.info("file=" + getFile());

		_bufferStrategy.close();

        // Stop watching for quorum related events.
        if (quorum != null)
            quorum.terminate();

		// report event.
		ResourceManager.closeJournal(getFile() == null ? null : getFile().toString());

		if (LRUNexus.INSTANCE != null) {

			try {

				LRUNexus.INSTANCE.deleteCache(getUUID());

			} catch (Throwable t) {

				log.error(t, t);

			}

		}

		if (deleteOnClose) {

			/*
			 * This option is used by the test suite and MUST NOT be used with
			 * live data.
			 */

			deleteResources();

		}
        
        nclose.incrementAndGet();
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

		if (isOpen())
			throw new IllegalStateException();

		if (log.isInfoEnabled())
			log.info("");

		final IBufferStrategy bufferStrategy = getBufferStrategy();

		if (bufferStrategy != null) {

			bufferStrategy.deleteResources();

			if (LRUNexus.INSTANCE != null) {

				try {

					LRUNexus.INSTANCE.deleteCache(getUUID());

				} catch (Throwable t) {

					log.error(t, t);

				}

			}

		}

		ResourceManager.deleteJournal(getFile() == null ? null : getFile().toString());

	}

	/**
	 * Truncate the backing buffer such that there is no remaining free space in
	 * the journal.
	 * <p>
	 * Note: The caller MUST have exclusive write access to the journal. When
	 * the {@link ConcurrencyManager} is used, that means that the caller MUST
	 * have an exclusive lock on the {@link WriteExecutorService}.
	 * <p>
	 * Note: The {@link BufferMode#DiskRW} does NOT support this operation. This
	 * is because it stores meta-allocation information at the end of the file,
	 * which makes it impossible to shrink the file. Therefore this method will
	 * return without causing the file on disk to be shrunk for the RWStore.
	 */
	public void truncate() {

		assertOpen();

		if (isReadOnly())
			throw new IllegalStateException();

		final IBufferStrategy backingBuffer = getBufferStrategy();

		switch (backingBuffer.getBufferMode()) {
		case DiskRW:
			/*
			 * Operation is not supported for the RWStore.
			 */
			return;
		default:
			break;
		}
		
		final long oldExtent = backingBuffer.getExtent();

		final long newExtent = backingBuffer.getHeaderSize() + backingBuffer.getNextOffset();

		backingBuffer.truncate(newExtent);

		if (log.isInfoEnabled())
			log.info("oldExtent=" + oldExtent + ", newExtent=" + newExtent);

	}

	/**
	 * Make sure that the journal has at least the specified number of bytes of
	 * unused capacity remaining in the user extent.
	 * <p>
	 * Note: You need an exclusive write lock on the journal to extend it.
	 * 
	 * @param minFree
	 *            The minimum #of bytes free for the user extent.
	 * 
	 * @return The #of bytes of free space remaining in the user extent.
	 */
	public long ensureMinFree(final long minFree) {

		assertOpen();

		if (minFree < 0L)
			throw new IllegalArgumentException();

		final IBufferStrategy buf = _bufferStrategy;

		final long remaining = buf.getUserExtent() - buf.getNextOffset();

		if (remaining < minFree) {

			buf.truncate(buf.getExtent() + minFree);

		}

		return buf.getUserExtent() - buf.getNextOffset();

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
	 * Note: The caller MUST have exclusive write access to the journal. When
	 * the {@link ConcurrencyManager} is used, that means that the caller MUST
	 * have an exclusive lock on the {@link WriteExecutorService}.
	 * <p>
	 * Note: This does NOT perform a commit - any uncommitted writes will be
	 * discarded.
	 * 
	 * @throws IllegalStateException
	 *             If there are no commits on the journal.
	 * 
	 * @todo There should also be an option to convert a journal from
	 *       {@link BufferMode#Direct} to {@link BufferMode#Disk}. We would want
	 *       to do that not when the journal is sealed but as soon as
	 *       asynchronous overflow processing is done. Ideally this will not
	 *       require us to close and reopen the journal since that will disturb
	 *       concurrent readers.
	 */
	public void closeForWrites(final long closeTime) {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			final long lastCommitTime = _rootBlock.getLastCommitTime();

			if (log.isInfoEnabled())
				log.info("Closing journal for further writes: closeTime=" + closeTime + ", lastCommitTime="
						+ lastCommitTime);

			if (log.isDebugEnabled())
				log.debug("before: " + _rootBlock);

			final IRootBlockView old = _rootBlock;

			if (old.getCommitCounter() == 0L) {

				throw new IllegalStateException("No commits on journal");

			}

			// release any unused space.
			truncate();

			/*
			 * Create the final root block.
			 * 
			 * Note: We MUST bump the commitCounter in order to have the new
			 * root block be selected over the old one!
			 * 
			 * Note: This will throw an error if nothing has ever been committed
			 * on the journal. The problem is that the root block does not
			 * permit a non-zero commitCounter unless the commitRecordAddr and
			 * perhaps some other stuff are non-zero as well.
			 */
			final long metaStartAddr = _bufferStrategy.getMetaStartAddr();
			final long metaBitsAddr = _bufferStrategy.getMetaBitsAddr();
			final IRootBlockView newRootBlock = new RootBlockView(//
					!old.isRootBlock0(), old.getOffsetBits(), old.getNextOffset(), old.getFirstCommitTime(), old
							.getLastCommitTime(), //
					old.getCommitCounter() + 1, //
					old.getCommitRecordAddr(), //
					old.getCommitRecordIndexAddr(), //
					old.getUUID(), //
					quorumToken, //
					metaStartAddr, //
					metaBitsAddr, //
					old.getStoreType(), //
					old.getCreateTime(), closeTime, //
					old.getVersion(), checker);

			/*
			 * Write it on the store.
			 * 
			 * Note: We request that the write is forced to disk to ensure that
			 * all buffered writes are forced to the disk. This is necessary in
			 * order to make sure that the updated root block (and anything left
			 * in the write cache for the disk buffer) get forced through onto
			 * the disk. We do not need to specify ForceMetadata here since the
			 * file size is unchanged by this operation.
			 */
			_bufferStrategy.writeRootBlock(newRootBlock, ForceEnum.Force);

			// discard write cache and make store read-only.
			_bufferStrategy.closeForWrites();

			// replace the root block reference.
			_rootBlock = newRootBlock;

			if (log.isDebugEnabled())
				log.debug("after: " + _rootBlock);

			// discard current commit record and re-read from the store.
			_commitRecord = _getCommitRecord();

			/*
			 * FIXME Verify that we can safely convert the writeRetentionQueue
			 * and readOnly flags on the BTree and then re-enable this code
			 * block. The tricky issue is the safe publication of the change to
			 * the writeRetentionQueue field (and populating it with the old
			 * queue's data) and the readOnly field. If those changes are not
			 * safely published then I also need to consider what the side
			 * effects of inconsistent views of those fields might be. One way
			 * to handle the safe publication is using AtomicBoolean and
			 * AtomicReference for those fields.
			 */
			// /* Convert all of the unisolated BTree objects into
			// read-historical
			// * BTrees as of the lastCommitTime on the journal. This is done in
			// * order to benefit from any data buffered by those BTrees since
			// * buffered data is data that we don't need to read from disk and
			// we
			// * don't need to de-serialize. This is especially important for
			// * asynchronous overflow processing which performs full index
			// scans
			// * of the BTree's shortly after synchronous overflow process (and
			// * which is the main reason why closeForWrites() exists).
			// *
			// * Note: The caller already promises that they hold the exclusive
			// * write lock so we don't really need to synchronize on
			// [name2Addr].
			// *
			// * Note: If we find a dirty mutable BTree then we ignore it rather
			// * than repurposing it. This allows the possibility that there are
			// * uncommitted writes.
			// */
			// synchronized (_name2Addr) {
			//
			// final Iterator<Map.Entry<String, WeakReference<BTree>>> itr =
			// _name2Addr
			// .indexCacheEntryIterator();
			//
			// while (itr.hasNext()) {
			//
			// final java.util.Map.Entry<String, WeakReference<BTree>> entry =
			// itr
			// .next();
			//
			// final String name = entry.getKey();
			//
			// final BTree btree = entry.getValue().get();
			//
			// if (btree == null) {
			//
			// // Note: Weak reference was cleared.
			// continue;
			//
			// }
			//
			// if (btree.needsCheckpoint()) {
			//
			// // Note: Don't convert a dirty BTree.
			// continue;
			//
			// }
			//
			// // Recover the Entry which has the last checkpointAddr.
			// final Name2Addr.Entry _entry = _name2Addr.getEntry(name);
			//
			// if (_entry == null) {
			//
			// /*
			// * There must be an Entry for each index in Name2Addr's
			// * cache.
			// */
			//
			// throw new AssertionError("No entry: name=" + name);
			//
			// }
			//
			// /*
			// * Mark the index as read-only (the whole journal no longer
			// * accepts writes) before placing it in the historical index
			// * cache (we don't want concurrent requests to be able to
			// * obtain a BTree that is not marked as read-only from the
			// * historical index cache).
			// */
			//
			// btree.convertToReadOnly();
			//
			// /*
			// * Put the BTree into the historical index cache under that
			// * checkpointAddr.
			// *
			// * Note: putIfAbsent() avoids the potential problem of
			// * having more than one object for the same checkpointAddr.
			// */
			//
			// historicalIndexCache.putIfAbsent(_entry.checkpointAddr,
			// btree);
			//
			// } // next index.
			//
			// // discard since no writers are allowed.
			// _name2Addr = null;
			//
			// }
			// close();
		} finally {

			lock.unlock();

		}

	}

	/**
	 * Invokes {@link #shutdownNow()}.
	 */
	synchronized public void close() {

		// Note: per contract for close().
		if (!isOpen())
			throw new IllegalStateException();

		if (log.isInfoEnabled())
			log.info("");

		shutdownNow();

	}

	synchronized public void destroy() {

		if (log.isInfoEnabled())
			log.info("");

		if (isOpen())
			shutdownNow();

		if (!deleteOnClose) {

			/*
			 * Note: if deleteOnClose was specified then the resource was
			 * already deleted by _close().
			 */

			deleteResources();

		}

		ndestroy.incrementAndGet();
		
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

		if (_bufferStrategy != null && !_bufferStrategy.isOpen()) {

            throw new IllegalStateException((getFile()==null?"transient":"file=" + getFile()));

		}

	}

	final public UUID getUUID() {

		return journalMetadata.getUUID();

	}

	final public IResourceMetadata getResourceMetadata() {

		return journalMetadata;

	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Note: This will report <code>false</code> for a new highly available
	 * journal until the quorum has met and {@link #init()} has been invoked for
	 * the {@link Quorum}.
	 */
	public boolean isOpen() {

		return _bufferStrategy != null && _bufferStrategy.isOpen();

	}

    /**
     * Return <code>true</code> if the journal was opened in a read-only mode or
     * if {@link #closeForWrites(long)} was used to seal the journal against
     * further writes.
     */
	public boolean isReadOnly() {

        return readOnly || getRootBlockView().getCloseTime() != 0L;

    }

    /**
     * Assert that the journal is readable.
     * 
     * @throws IllegalStateException
     *             if the journal is not writable at this time.
     */
    protected void assertCanRead() {

        if (_bufferStrategy == null) {
            // only possible during the constructor call.
            throw new IllegalStateException();
        }

        if (!_bufferStrategy.isOpen()) {
            throw new IllegalStateException();
        }

    }

    /**
     * Assert that the journal is writable.
     * 
     * @throws IllegalStateException
     *             if the journal is not writable at this time.
     */
    protected void assertCanWrite() {

        if (_bufferStrategy == null) {
            // only possible during the constructor call.
            throw new IllegalStateException();
        }

        if (!_bufferStrategy.isOpen()) {
            throw new IllegalStateException();
        }

        if (_bufferStrategy.isReadOnly()) {
            throw new IllegalStateException();
        }

    }

	public boolean isStable() {

	    return _bufferStrategy.isStable();

	}

	public boolean isFullyBuffered() {

		return _bufferStrategy.isFullyBuffered();

	}

	public boolean isDoubleSync() {

		return doubleSync;

	}

	/**
	 * Return <code>true</code> if the persistence store uses record level
	 * checksums. When <code>true</code>, the store will detect bad reads by
	 * comparing the record as read from the disk against the checksum for that
	 * record.
	 */
	public boolean isChecked() {

		return _bufferStrategy.useChecksums();

	}

	/**
	 * Return <code>true</code> if the journal is configured for high
	 * availability. High availability exists (in principle) when the
	 * {@link QuorumManager#replicationFactor()} <em>k</em> is greater than one.
	 * 
	 * @see #getQuorum()
	 * @see QuorumManager#isHighlyAvailable()
	 */
	public boolean isHighlyAvailable() {

		return getQuorum().isHighlyAvailable();

	}

	final public IRootBlockView getRootBlockView() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			if (_rootBlock == null) {

				/*
				 * This can happen before the journal file has been created.
				 * Once it has been created the root block will always be
				 * non-null when viewed while holding the lock.
				 */

				throw new IllegalStateException();

			}

			return _rootBlock;

		} finally {

			lock.unlock();

		}

	}

    final public long getLastCommitTime() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			return _rootBlock.getLastCommitTime();

		} finally {

			lock.unlock();

		}

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
	final public void setCommitter(final int rootSlot, final ICommitter committer) {

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

		final long[] rootAddrs = new long[_committers.length];

		for (int i = 0; i < _committers.length; i++) {

			if (_committers[i] == null)
				continue;

			final long addr = _committers[i].handleCommit(commitTime);

			rootAddrs[i] = addr;

			ncommitters++;

		}

		return rootAddrs;

	}

	public void abort() {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

            if (quorum != null) {

                // HA mode
                quorum.getClient().abort2Phase(quorumToken);

            } else {

                // Non-HA mode.
                _abort();
                
            }

		} catch (Throwable e) {

			throw new RuntimeException(e);

		} finally {

			lock.unlock();

		}

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
	 * group was aborted. This is done automatically when we re-load the current
	 * {@link ICommitRecord} from the root blocks of the store.
	 */
	private void _abort() {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			if (log.isInfoEnabled())
				log.info("start");

			if (_bufferStrategy == null) {

				// Nothing to do.
				return;

			}

			if (LRUNexus.INSTANCE != null) {

				/*
				 * Discard the LRU for this store. It may contain writes which
				 * have been discarded. The same addresses may be reissued by
				 * the WORM store after an abort, which could lead to incorrect
				 * reads from a dirty cache.
				 * 
				 * FIXME An optimization would essentially isolate the writes on
				 * the cache per BTree or between commits. At the commit point,
				 * the written records would be migrated into the "committed"
				 * cache for the store. The caller would read on the uncommitted
				 * cache, which would read through to the "committed" cache.
				 * This would prevent incorrect reads without requiring us to
				 * throw away valid records in the cache. This could be a
				 * significant performance gain if aborts are common on a
				 * machine with a lot of RAM.
				 */

				LRUNexus.getCache(this).clear();

			}

			/*
			 * The buffer strategy has a hook which is used to discard buffered
			 * writes. This is both an optimization (it ensures that those
			 * writes are not asynchronously laid down on the disk) and a
			 * requirement for the WORM store.
			 * 
			 * Note: The WriteCache for the WORM store has internal state giving
			 * the firstOffset of the records in the internal buffer - if we do
			 * not call reset() on that write cache then the firstOffset will be
			 * incorrect and the records will be laid down on the wrong offsets
			 * on the store. This correctness issue does not arise for the RW
			 * store because the WriteCache has the actual offset at which each
			 * record will be written and ordered writes are used to lay down
			 * those records. However, for the WORM store we use a single large
			 * write and require that the data in the buffer exactly matches the
			 * target state on the backing file.
			 */
			_bufferStrategy.abort();

			/*
			 * Discard hard references to any indices. The Name2Addr reference
			 * will also be discarded below. This should be sufficient to ensure
			 * that any index requested by the methods on the AbstractJournal
			 * will be re-read from disk using the commit record which we
			 * re-load below. This is necessary in order to discard any
			 * checkpoints that may have been written on indices since the last
			 * commit.
			 * 
			 * FIXME Verify this is not required. Historical index references
			 * should not be discarded on abort as they remain valid. Discarding
			 * them admits the possibility of a non-canonicalizing cache for the
			 * historical indices since an existing historical index reference
			 * will continue to be held but a new copy of the index will be
			 * loaded on the next request if we clear the cache here.
			 */
			// historicalIndexCache.clear();
			// discard the commit record and re-read from the store.
			_commitRecord = _getCommitRecord();

			/*
			 * Re-load the commit record index from the address in the current
			 * root block.
			 * 
			 * Note: This may not be strictly necessary since the only time we
			 * write on this index is a single record during each commit. So, it
			 * should be valid to simply catch an error during a commit and
			 * discard this index forcing its reload. However, doing this here
			 * is definitely safer.
			 * 
			 * Note: This reads on the store. If the backing channel for a
			 * stable store was closed by an interrupt, e.g., during an abort,
			 * then this will cause the backing channel to be transparent
			 * re-opened. At that point both readers and writers will be able to
			 * access the channel again.
			 */

			// clear reference and reload from the store.
			_commitRecordIndex = _getCommitRecordIndex();
			
			// clear reference and reload from the store.
			_icuVersionRecord = _getICUVersionRecord();
			
			// clear the array of committers.
			_committers = new ICommitter[_committers.length];

			// discard any hard references that might be cached.
			discardCommitters();

			// setup new committers, e.g., by reloading from their last root
			// addr.
			setupCommitters();

			if (log.isInfoEnabled())
				log.info("done");

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Rollback a journal to its previous commit point.
	 * <p>
	 * Note: You MUST have an exclusive write lock on the journal.
	 * <p>
	 * Note: To restore the last root block we copy the alternative root block
	 * over the current root block. That gives us two identical root blocks and
	 * restores us to the root block that was in effect before the last commit.
	 */
	public void rollback() {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			assertOpen();

			if (isReadOnly())
				throw new IllegalStateException();

			log.warn("");

			/*
			 * Read the alternate root block (NOT the current one!).
			 */
			final ByteBuffer buf = _bufferStrategy.readRootBlock(!_rootBlock.isRootBlock0());

			/*
			 * Create a view from the alternate root block, but using the SAME
			 * [rootBlock0] flag state as the current root block so that this
			 * will overwrite the current root block.
			 */
			final IRootBlockView newRootBlock = new RootBlockView(_rootBlock.isRootBlock0(), buf, checker);

			/*
			 * Overwrite the current root block on the backing store with the
			 * state of the alternate root block.
			 */
			_bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

			// Use the new root block.
			_rootBlock = newRootBlock;

			/*
			 * Discard all in-memory state - it will need be re-loaded from the
			 * restored root block.
			 */
			abort();

		} finally {

			lock.unlock();

		}

	}

//	/**
//	 * Return the object providing the {@link AbstractLocalTransactionManager}
//	 * for this journal.
//	 */
//	abstract public AbstractLocalTransactionManager getLocalTransactionManager();

	public long commit() {

		final ILocalTransactionManager transactionManager = getLocalTransactionManager();

		/*
		 * Get timestamp that will be assigned to this commit (RMI if the
		 * journal is part of a distributed federation).
		 */
		final long commitTime = transactionManager.nextTimestamp();

		// do the commit.
		final long commitTime2 = commitNow(commitTime);

		if (commitTime2 == 0L) {

			// Nothing to commit.

			return 0L;

		}

		// commitNow() should return either 0L or the commitTime we gave it.
		assert commitTime2 == commitTime;

		/*
		 * Now that we have committed the data we notify the federation that it
		 * should advance its global lastCommitTime.
		 * 
		 * @todo we could use IBufferStrategy#rollback() if the notice failed,
		 * e.g., due to a service outage.
		 */

		transactionManager.notifyCommit(commitTime);

		return commitTime;

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
	 * <p>
	 * Note: Each invocation of this method MUST use a distinct
	 * <i>commitTime</i> and the commitTimes MUST be monotonically increasing.
	 * These guarantees support both the database version history mechanisms and
	 * the High Availability mechanisms.
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

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			assertOpen();

			final long beginNanos = System.nanoTime();

			// #of bytes on the journal as of the previous commit point.
			final long byteCountBefore = _rootBlock.getNextOffset();

			if (log.isInfoEnabled())
				log.info("commitTime=" + commitTime);

			assertCommitTimeAdvances(commitTime);

			/*
			 * First, run each of the committers accumulating the updated root
			 * addresses in an array. In general, these are btrees and they may
			 * have dirty nodes or leaves that needs to be evicted onto the
			 * store. The first time through, any newly created btrees will have
			 * dirty empty roots (the btree code does not optimize away an empty
			 * root at this time). However, subsequent commits without
			 * intervening data written on the store should not cause any
			 * committers to update their root address.
			 */
			final long[] rootAddrs = notifyCommitters(commitTime);

			/*
			 * See if anything has been written on the store since the last
			 * commit.
			 */
			if (!_bufferStrategy.requiresCommit(_rootBlock)) {

				/*
				 * No data was written onto the store so the commit can not
				 * achieve any useful purpose.
				 */

				if (log.isInfoEnabled())
					log.info("Nothing to commit");

				return 0L;
			}
			
			/*
			 * Explicitly call the RootBlockCommitter
			 */
			rootAddrs[PREV_ROOTBLOCK] = this.m_rootBlockCommitter.handleCommit(commitTime);
			
			/*
			 * Write the commit record onto the store.
			 * 
			 * @todo Modify to log the current root block and set the address of
			 * that root block in the commitRecord. This will be of use solely
			 * in disaster recovery scenarios where your root blocks are toast,
			 * but good root blocks can be found elsewhere in the file.
			 */

			final IRootBlockView old = _rootBlock;

			final long newCommitCounter = old.getCommitCounter() + 1;

			final ICommitRecord commitRecord = new CommitRecord(commitTime, newCommitCounter, rootAddrs);

			final long commitRecordAddr = write(ByteBuffer
					.wrap(CommitRecordSerializer.INSTANCE.serialize(commitRecord)));

			/*
			 * Add the commit record to an index so that we can recover
			 * historical states efficiently.
			 */
			_commitRecordIndex.add(commitRecordAddr, commitRecord);

			/*
			 * Flush the commit record index to the store and stash the address
			 * of its metadata record in the root block.
			 * 
			 * Note: The address of the root of the CommitRecordIndex itself
			 * needs to go right into the root block. We are unable to place it
			 * into the commit record since we need to serialize the commit
			 * record, get its address, and add the entry to the
			 * CommitRecordIndex before we can flush the CommitRecordIndex to
			 * the store.
			 */
			final long commitRecordIndexAddr = _commitRecordIndex.writeCheckpoint();
			
			/*
			 * DEBUG: The commitRecordIndexAddr should not be deleted, the
			 * call to lockAddress forces a runtime check protecting the address
			 */
			if (_bufferStrategy instanceof RWStrategy) {
				((RWStrategy) _bufferStrategy).lockAddress(commitRecordIndexAddr);
			}

            if (quorum != null) {
                /*
                 * Verify that the last negotiated quorum is still in valid.
                 */
                quorum.assertLeader(quorumToken);
            }

			/*
			 * Call commit on buffer strategy prior to retrieving root block,
			 * required for RWStore since the metaBits allocations are not made
			 * until commit, leading to invalid addresses for recent store
			 * allocations.
			 */
			_bufferStrategy.commit(this);

			/*
			 *  next offset at which user data would be written.
			 *  Calculated, after commit!
			 */
			
			final long nextOffset = _bufferStrategy.getNextOffset();

			/*
			 * Prepare the new root block.
			 */
			final IRootBlockView newRootBlock;
			{

				/*
				 * Update the firstCommitTime the first time a transaction
				 * commits and the lastCommitTime each time a transaction
				 * commits (these are commit timestamps of isolated or
				 * unisolated transactions).
				 */

				final long firstCommitTime = (old.getFirstCommitTime() == 0L ? commitTime : old.getFirstCommitTime());

				final long priorCommitTime = old.getLastCommitTime();

				if (priorCommitTime != 0L) {

					/*
					 * This is a local sanity check to make sure that the commit
					 * timestamps are strictly increasing. An error will be
					 * reported if the commit time for the current (un)isolated
					 * transaction is not strictly greater than the last commit
					 * time on the store as read back from the current root
					 * block.
					 */

                    assertPriorCommitTimeAdvances(commitTime, priorCommitTime);

				}

				final long lastCommitTime = commitTime;
				final long metaStartAddr = _bufferStrategy.getMetaStartAddr();
				final long metaBitsAddr = _bufferStrategy.getMetaBitsAddr();

				// Create the new root block.
                newRootBlock = new RootBlockView(!old.isRootBlock0(), old
                        .getOffsetBits(), nextOffset, firstCommitTime,
                        lastCommitTime, newCommitCounter, commitRecordAddr,
                        commitRecordIndexAddr, old.getUUID(), quorumToken,
                        metaStartAddr, metaBitsAddr, old.getStoreType(),
                        old.getCreateTime(), old.getCloseTime(), 
                        old.getVersion(), checker);

			}

            if (quorum == null) {
                
                /*
                 * Non-HA mode.
                 */

                /*
                 * Force application data to stable storage _before_
                 * we update the root blocks. This option guarantees
                 * that the application data is stable on the disk
                 * before the atomic commit. Some operating systems
                 * and/or file systems may otherwise choose an
                 * ordered write with the consequence that the root
                 * blocks are laid down on the disk before the
                 * application data and a hard failure could result
                 * in the loss of application data addressed by the
                 * new root blocks (data loss on restart).
                 * 
                 * Note: We do not force the file metadata to disk.
                 * If that is done, it will be done by a force()
                 * after we write the root block on the disk.
                 */
                if (doubleSync) {

                    _bufferStrategy.force(false/* metadata */);

                }

                // write the root block on to the backing store.
                _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

                // set the new root block.
                _rootBlock = newRootBlock;

                // reload the commit record from the new root block.
                _commitRecord = _getCommitRecord();

            } else {
                
                /*
                 * HA mode.
                 */
                
                final QuorumService<HAGlue> quorumService = quorum.getClient();

                try {

                    final int minYes = (quorum.replicationFactor() + 1) >> 1;

                    // @todo config prepare timeout (Watch out for long GC
                    // pauses!)
                    final int nyes = quorumService.prepare2Phase(//
                            !old.isRootBlock0(),//
                            newRootBlock,//
                            1000, // timeout
                            TimeUnit.MILLISECONDS//
                            );

                    if (nyes >= minYes) {

                        quorumService.commit2Phase(quorumToken, commitTime);

                    } else {

                        quorumService.abort2Phase(quorumToken);

                    }

                } catch (Throwable e) {
                    /*
                     * FIXME At this point the quorum is probably inconsistent
                     * in terms of their root blocks. Rather than attempting to
                     * send an abort() message to the quorum, we probably should
                     * force the master to yield its role at which point the
                     * quorum will attempt to elect a new master and
                     * resynchronize.
                     */
                    if (quorumService != null) {
                        try {
                            quorumService.abort2Phase(quorumToken);
                        } catch (Throwable t) {
                            log.warn(t, t);
                        }
                    }
                    throw new RuntimeException(e);
                }

            }

			final long elapsedNanos = System.nanoTime() - beginNanos;

			if (BigdataStatics.debug || log.isInfoEnabled()) {
				final String msg = "commit: commitTime=" + commitTime + ", latency="
						+ TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + ", nextOffset=" + nextOffset + ", byteCount="
						+ (nextOffset - byteCountBefore);
				if (BigdataStatics.debug)
					System.err.println(msg);
				else if (log.isInfoEnabled())
					log.info(msg);
				if (BigdataStatics.debug && LRUNexus.INSTANCE != null) {
					System.err.println(LRUNexus.INSTANCE.toString());
				}
			}

			return commitTime;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Method verifies that the commit time strictly advances on the local store
	 * by checking against the current root block.
	 * 
	 * @param commitTime
	 *            The proposed commit time.
	 * 
	 * @throws IllegalArgumentException
	 *             if the <i>commitTime</i> is LTE the value reported by
	 *             {@link IRootBlockView#getLastCommitTime()}.
	 */
	protected void assertCommitTimeAdvances(final long commitTime) {

		if (commitTime <= _rootBlock.getLastCommitTime()) {

			/*
			 * The commit times must strictly advance.
			 */

			throw new IllegalArgumentException();

		}

	}

    /**
     * Method verifies that the commit time strictly advances on the local store
     * by checking against the current root block.
     * 
     * @param commitTime
     *            The proposed commit time.
     * 
     * @throws IllegalArgumentException
     *             if the <i>commitTime</i> is LTE the value reported by
     *             {@link IRootBlockView#getLastCommitTime()}.
     */
    protected void assertPriorCommitTimeAdvances(final long currentCommitTime,
            final long priorCommitTime) {

        if (currentCommitTime <= priorCommitTime) {

            throw new RuntimeException("Time goes backwards: commitTime="
                    + currentCommitTime + ", but lastCommitTime="
                    + priorCommitTime + " on the current root block");

        }

    }

	public void force(final boolean metadata) {

		assertOpen();

		_bufferStrategy.force(metadata);

	}

	public long size() {

		return _bufferStrategy.size();

	}

    public ByteBuffer read(final long addr) {
            assertOpen();

        assertCanRead();
            
        return _bufferStrategy.read(addr);
            
	}
    
    public long write(final ByteBuffer data) {

        assertCanWrite();

        return _bufferStrategy.write(data);
	
    }

    // Note: RW store method.
    public long write(final ByteBuffer data, final long oldAddr) {

        assertCanWrite();

        return _bufferStrategy.write(data, oldAddr);
        
    }

    public long write(final ByteBuffer data, final long oldAddr,
            final IAllocationContext context) {

        assertCanWrite();

        if (_bufferStrategy instanceof RWStrategy) {

            return ((RWStrategy) _bufferStrategy).write(data, oldAddr, context);

        } else {

            return _bufferStrategy.write(data, oldAddr);

        }

    }

    public long write(final ByteBuffer data, final IAllocationContext context) {

        assertCanWrite();

        if (_bufferStrategy instanceof RWStrategy) {

            return ((RWStrategy) _bufferStrategy).write(data, context);
            
        } else {

            return _bufferStrategy.write(data);

        }
        
    }

	// Note: NOP for WORM. Used by RW for eventual recycle protocol.
    public void delete(final long addr) {

        assertCanWrite();

        _bufferStrategy.delete(addr);

    }

    public void delete(final long addr, final IAllocationContext context) {

        assertCanWrite();

        if(_bufferStrategy instanceof RWStrategy) {
        
            ((RWStrategy) _bufferStrategy).delete(addr, context);
            
        } else {
            
            _bufferStrategy.delete(addr);
            
        }

    }
    
    public void detachContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof RWStrategy) {

            ((RWStrategy) _bufferStrategy).detachContext(context);
            
        }
    	
    }

    public void abortContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof RWStrategy) {

            ((RWStrategy) _bufferStrategy).abortContext(context);
            
        }
        
    }

	final public long getRootAddr(final int index) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final ICommitRecord commitRecord = _commitRecord;

			if (commitRecord == null)
				throw new AssertionError();

			return commitRecord.getRootAddr(index);

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Returns a read-only view of the most recently committed
	 * {@link ICommitRecord} containing the root addresses.
	 * 
	 * @return The current {@link ICommitRecord} and never <code>null</code>.
	 */
	public ICommitRecord getCommitRecord() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final ICommitRecord commitRecord = _commitRecord;

			if (commitRecord == null)
				throw new AssertionError();

			return commitRecord;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Return the commit record, either new or read from the root block.
	 */
	private ICommitRecord _getCommitRecord() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		// the address of the current commit record from the root block.
		final long commitRecordAddr = _rootBlock.getCommitRecordAddr();
		
		if (log.isInfoEnabled())
			log.info("Reading commit record from: " + commitRecordAddr);
		
		if (commitRecordAddr == NULL) {

			// No commit record on the store yet.
			return new CommitRecord();

		} else {

			// Read the commit record from the store.
			return CommitRecordSerializer.INSTANCE.deserialize(_bufferStrategy.read(commitRecordAddr));

		}

	}

	/**
	 * This method is invoked by {@link #abort()} when the store must discard
	 * any hard references that it may be holding to objects registered as
	 * {@link ICommitter}s.
	 * <p>
	 * The default implementation discards the btree mapping names to named
	 * btrees.
	 * <p>
	 * Subclasses MAY extend this method to discard their own committers but
	 * MUST NOT override it completely.
	 */
	protected void discardCommitters() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		// discard.
		_name2Addr = null;

	}

	/**
	 * Invoked when a journal is first created, re-opened, or when the
	 * committers have been {@link #discardCommitters() discarded}.
	 * <p>
	 * The basic implementation sets up the btree that is responsible for
	 * resolving named btrees.
	 * <p>
	 * Subclasses may extend this method to setup their own committers.
	 */
	protected void setupCommitters() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		if (!isReadOnly()) { // quorumManager.getQuorum().isLeader()) {

			/*
			 * Only the leader can accept writes so only the leader will
			 * register the Name2Addr object. Followers can access the
			 * historical Name2Addr objects from the CommitRecordIndex for
			 * historical commit points, but not the live Name2Addr object,
			 * which is only on the leader.
			 */

			setupName2AddrBTree(getRootAddr(ROOT_NAME2ADDR));
			
			/**
			 * Do not register committer to write previous root block, but
			 * instead just create it and call explicitly when required.  This
			 * is a workaround to allow "void" transactions.
			 */
			m_rootBlockCommitter = new RootBlockCommitter(this);
			
			/**
			 * If the strategy is a RWStrategy, then register the delete
			 * block committer to store the deferred deletes for each
			 * commit record.
			 */
			if (_bufferStrategy instanceof RWStrategy)
				setCommitter(DELETEBLOCK, new DeleteBlockCommitter((RWStrategy) _bufferStrategy));

            /*
             * Responsible for writing the ICUVersionRecord exactly once onto
             * the backing store, e.g., when the store is created or when it is
             * open with the "update" option specified for ICU.
             */
            setCommitter(ROOT_ICUVERSION, new ICUVersionCommitter());

		}

	}

    /**
     * Return the {@link ICUVersionRecord} from the current
     * {@link ICommitRecord} -or- a new instance for the current runtime
     * environment if the root address for {@link #ROOT_ICUVERSION} is
     * {@link #NULL}.
     */
    private ICUVersionRecord _getICUVersionRecord() {

        assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

        final long addr = getRootAddr(ROOT_ICUVERSION);

        final ICUVersionRecord r;
        if (addr == NULL) {
            // New instance for the current runtime environment.
            r = ICUVersionRecord.newInstance();
        } else {
            // Existing instance from the store.
            r = (ICUVersionRecord) SerializerUtil.deserialize(read(addr));
        }
        return r;
        
    }

    /**
     * Writes the {@link ICUVersionRecord} onto the store iff either (a) it does
     * not exist; or (b) it exists, it differs from the last persistent record,
     * and the update flag was specified.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see Options#UPDATE_ICU_VERSION
     */
    private class ICUVersionCommitter implements ICommitter {

        private boolean update;
        
        private long lastAddr;

        private ICUVersionCommitter() {
            
            // the "update" option.
            update = Boolean.valueOf(properties.getProperty(
                    Options.UPDATE_ICU_VERSION, "false"));
            
            // lookup the address of the ICU version record (may be NULL).
            lastAddr = getRootAddr(ROOT_ICUVERSION);
            
        }

        /**
         * Commits a new {@link ICUVersionRecord} IF none is defined -OR- IF one
         * is defined, it is a different version of ICU, and the update flag is
         * set.
         */
        public long handleCommit(final long commitTime) {

            if(!update && lastAddr != NULL) {
                
                // Nothing changed.
                return lastAddr;
                
            }

            /*
             * Note: The Journal only validates the persistent ICU version
             * record in its constructor. By the time the code reaches this
             * point, it is either in agreement or will be written.
             */

            final ICUVersionRecord r = ICUVersionRecord.newInstance();

            if (lastAddr == NULL || !(r.equals(_icuVersionRecord) && update)) {

                if (_icuVersionRecord != null && update)
                    log.warn("Updating ICUVersion: old=" + _icuVersionRecord
                            + ", new=" + r);

                // do not update next time.
                update = false;
                
                // write ICU version record onto the store.
                lastAddr = write(ByteBuffer.wrap(SerializerUtil.serialize(r)));
            
                // return address of the ICU version record.
                return lastAddr;
                
            }
            
            // Nothing changed.
            return lastAddr;
            
        }
	    
	}

	/*
	 * named indices.
	 */

	/**
	 * Setup the btree that resolves named indices. This is invoke when the
	 * journal is opened and by {@link #abort()} .
	 * 
	 * @param addr
	 *            The root address of the btree -or- 0L iff the btree has not
	 *            been defined yet.
	 * 
	 * @see Options#LIVE_INDEX_CACHE_CAPACITY
	 */
	Name2Addr setupName2AddrBTree(final long addr) {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		assert _name2Addr == null;

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

			if (log.isInfoEnabled())
				log.info("New " + Name2Addr.class.getName());

			_name2Addr = Name2Addr.create((isReadOnly() ? new SimpleMemoryRawStore() : this));

		} else {

			/*
			 * Reload the mutable btree from its checkpoint address.
			 * 
			 * Note: This is the live view of the B+Tree. In this specific case
			 * we DO NOT use the canonicalizing mapping since we do not want
			 * anyone else to have access to this same instance of the B+Tree.
			 */

			if (log.isInfoEnabled())
				log.info("Loading " + Name2Addr.class.getName() + " from " + addr);

			_name2Addr = (Name2Addr) BTree.load(this, addr);

		}

		_name2Addr.setupCache(liveIndexCacheCapacity, liveIndexCacheTimeout);

		// register for commit notices.
		setCommitter(ROOT_NAME2ADDR, _name2Addr);

		return _name2Addr;

	}

    /**
     * Return a read-only view of the last committed state of the
     * {@link CommitRecordIndex}.
     * 
     * @return The read-only view of the {@link CommitRecordIndex}.
     */
	public IIndex getReadOnlyCommitRecordIndex() {

	    final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            assertOpen();

            final CommitRecordIndex commitRecordIndex = getCommitRecordIndex(_rootBlock
                    .getCommitRecordIndexAddr());

            return new ReadOnlyIndex(commitRecordIndex);

        } finally {

            lock.unlock();

        }
 
	}
	
	/**
	 * Return the current state of the index that resolves timestamps to
	 * {@link ICommitRecord}s.
	 * <p>
	 * Note: The returned object is NOT safe for concurrent operations and is
	 * NOT systematically protected by the use of synchronized blocks within
	 * this class.
	 * 
	 * @return The {@link CommitRecordIndex}.
	 * 
	 * @todo If you need access to this object in an outside class consider
	 *       using {@link #getRootBlockView()},
	 *       {@link IRootBlockView#getCommitRecordIndexAddr()}, and
	 *       {@link #getCommitRecord(long)} to obtain a distinct instance
	 *       suitable for read-only access.
	 */
	protected CommitRecordIndex getCommitRecordIndex() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

			if (commitRecordIndex == null)
				throw new AssertionError();

			return commitRecordIndex;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Read and return the {@link CommitRecordIndex} from the current root
	 * block.
	 * 
	 * @return The {@link CommitRecordIndex} and never <code>null</code>.
	 */
	private CommitRecordIndex _getCommitRecordIndex() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		assert _rootBlock != null;

		final long addr = _rootBlock.getCommitRecordIndexAddr();

		try {

			return getCommitRecordIndex(addr);

		} catch (RuntimeException ex) {

			/*
			 * Log the root block for post-mortem.
			 */
			log.fatal("Could not read the commit record index:\n" + _rootBlock, ex);

			throw ex;

		}

	}

	/**
	 * Create or re-load the index that resolves timestamps to
	 * {@link ICommitRecord}s.
	 * <p>
	 * Note: The returned object is NOT cached. When addr is non-{@link #NULL},
	 * each invocation will return a distinct {@link CommitRecordIndex} object.
	 * This behavior is partly for historical reasons but it does serve to
	 * protect the live {@link CommitRecordIndex} from outside access. This is
	 * important since access to the live {@link CommitRecordIndex} is NOT
	 * systematically protected by <code>synchronized</code> within this class.
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
	protected CommitRecordIndex getCommitRecordIndex(final long addr) {

		if (log.isInfoEnabled())
			log.info("addr=" + toString(addr));

		final CommitRecordIndex ndx;

		if (addr == NULL) {

			/*
			 * The btree has either never been created or if it had been created
			 * then the store was never committed and the btree had since been
			 * discarded. In any case we create a new btree now.
			 * 
			 * Note: if the journal is read-only then we create the commit
			 * record index on an in-memory store in order to avoid triggering a
			 * write exception on the journal.
			 * 
			 * Note: if the journal is not the quorum leader then it is
			 * effectively read-only.
			 */
			if (isReadOnly()) {

				ndx = CommitRecordIndex.createTransient();

			} else {

				ndx = CommitRecordIndex.create(this);

			}

		} else {

			/*
			 * Reload the mutable btree from its root address.
			 */

			ndx = (CommitRecordIndex) BTree.load(this, addr);

		}

		assert ndx != null;

		return ndx;

	}

	/**
	 * @todo the {@link CommitRecordIndex} is a possible source of thread
	 *       contention since transactions need to use this code path in order
	 *       to locate named indices but the {@link WriteExecutorService} can
	 *       also write on this index. I have tried some different approaches to
	 *       handling this.
	 */
	public ICommitRecord getCommitRecord(final long commitTime) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

			if (commitRecordIndex == null)
				throw new AssertionError();

			return commitRecordIndex.find(commitTime);

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Return the first commit record whose timestamp is strictly greater than
	 * the given commitTime.
	 * 
	 * @param commitTime
	 *            The commit time.
	 * 
	 * @return The commit record -or- <code>null</code> if there is no commit
	 *         record whose timestamp is strictly greater than
	 *         <i>commitTime</i>.
	 */
	public ICommitRecord getCommitRecordStrictlyGreaterThan(final long commitTime) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

			if (commitRecordIndex == null)
				throw new AssertionError();

			return commitRecordIndex.findNext(commitTime);

		} finally {

			lock.unlock();

		}

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws UnsupportedOperationException
	 *             If you pass in {@link ITx#UNISOLATED},
	 *             {@link ITx#READ_COMMITTED}, or a timestamp that corresponds
	 *             to a read-write transaction since those are not "commit
	 *             times".
	 */
	public IIndex getIndex(final String name, final long commitTime) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			if (commitTime == ITx.UNISOLATED || commitTime == ITx.READ_COMMITTED
					|| TimestampUtility.isReadWriteTx(commitTime)) {

				throw new UnsupportedOperationException();

			}

			final ICommitRecord commitRecord = getCommitRecord(commitTime);

			if (commitRecord == null) {

				if (log.isInfoEnabled())
					log.info("No commit record for timestamp=" + commitTime);

				return null;

			}

			return getIndex(name, commitRecord);

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Returns a read-only named index loaded from a {@link ICommitRecord}. The
	 * {@link BTree} will be marked as read-only, it will NOT permit writes, and
	 * {@link BTree#getLastCommitTime(long)} will report the value associated
	 * with {@link Entry#commitTime} for the historical {@link Name2Addr}
	 * instance for that {@link ICommitRecord}.
	 * 
	 * @return The named index -or- <code>null</code> iff the named index did
	 *         not exist as of that commit record.
	 */
	public BTree getIndex(final String name, final ICommitRecord commitRecord) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			if (name == null)
				throw new IllegalArgumentException();

			if (commitRecord == null)
				throw new IllegalArgumentException();

			/*
			 * The address of an historical Name2Addr mapping used to resolve
			 * named indices for the historical state associated with this
			 * commit record.
			 */
			final long checkpointAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);

			if (checkpointAddr == 0L) {

				log.warn("No name2addr entry in this commit record: " + commitRecord);

				return null;

			}

			/*
			 * Resolve the address of the historical Name2Addr object using the
			 * canonicalizing object cache. This prevents multiple historical
			 * Name2Addr objects springing into existence for the same commit
			 * record.
			 */
			final Name2Addr name2Addr = (Name2Addr) getIndex(checkpointAddr);

			/*
			 * The address at which the named index was written for that
			 * historical state.
			 */
			final Name2Addr.Entry entry = name2Addr.getEntry(name);

			if (entry == null) {

				// No such index by name for that historical state.

				return null;

			}

			/*
			 * Resolve the named index using the object cache to impose a
			 * canonicalizing mapping on the historical named indices based on
			 * the address on which it was written in the store.
			 */

			final BTree btree = getIndex(entry.checkpointAddr);

			assert entry.commitTime != 0L : "Entry=" + entry;

			// Set the last commit time on the btree.
			btree.setLastCommitTime(entry.commitTime);

			return btree;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * A canonicalizing mapping for <em>historical</em> {@link BTree}s.
	 * <p>
	 * Note: This method imposes a canonicalizing mapping and ensures that there
	 * will be at most one instance of the historical index at a time. This
	 * guarentee is used to facilitate buffer management. Writes on indices
	 * returned by this method are NOT allowed.
	 * <p>
	 * Note: This method marks the {@link BTree} as read-only but does not set
	 * {@link BTree#setLastCommitTime(long)} since it does not have access to
	 * the {@link Entry#commitTime}, only the {@link BTree}s checkpointAddr and
	 * {@link Checkpoint} record. See {@link #getIndex(String, ICommitRecord)}
	 * which does set {@link BTree#setLastCommitTime(long)}.
	 * <p>
	 * Note: The canonicalizing mapping for unisolated {@link BTree}s is
	 * maintained by the {@link ITx#UNISOLATED} {@link Name2Addr} instance.
	 * 
	 * @param checkpointAddr
	 *            The address of the {@link Checkpoint} record for the
	 *            {@link BTree}.
	 * 
	 * @return The {@link BTree} loaded from that {@link Checkpoint}.
	 * 
	 * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
	 */
	final public BTree getIndex(final long checkpointAddr) {

		/*
		 * Note: There are potentially three IO operations here. Reading the
		 * Checkpoint record, reading the IndexMetadata record, then reading the
		 * root node/leaf of the BTree.
		 * 
		 * Note: We use putIfAbsent() here rather than the [synchronized]
		 * keyword for higher concurrency with atomic semantics.
		 */
		// synchronized (historicalIndexCache) {
		// BTree btree = (BTree) historicalIndexCache.get(checkpointAddr);
		BTree btree = historicalIndexCache.get(checkpointAddr);

		if (btree == null) {

			/*
			 * Load BTree from the store.
			 * 
			 * Note: Does not set lastCommitTime.
			 */

			btree = BTree.load(this, checkpointAddr, true/* readOnly */);

		}

		// Note: putIfAbsent is used to make concurrent requests atomic.
		BTree oldval = historicalIndexCache.putIfAbsent(checkpointAddr, btree);

		if (oldval != null) {

			/*
			 * If someone beat us to it then use the BTree instance that they
			 * loaded.
			 */
			btree = oldval;

		}

		// historicalIndexCache.put(checkpointAddr, (ICommitter)btree,
		// false/*dirty*/);

		return btree;

		// }

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
	 * 
	 * @param name
	 *            The name that can be used to recover the index.
	 * 
	 * @return The object that would be returned by {@link #getIndex(String)}.
	 * 
	 * @exception IllegalStateException
	 *                if there is an index already registered under that name.
	 * 
	 * @see Options#BTREE_BRANCHING_FACTOR
	 * 
	 * @deprecated This is only used by the test suites.
	 */
	public BTree registerIndex(final String name) {

		final IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());

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
	public void registerIndex(final IndexMetadata metadata) {

		if (metadata == null)
			throw new IllegalArgumentException();

		final String name = metadata.getName();

		if (name == null)
			throw new IllegalArgumentException();

		registerIndex(name, metadata);

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
	public BTree registerIndex(final String name, final IndexMetadata metadata) {

		validateIndexMetadata(name, metadata);

		final BTree btree = BTree.create(this, metadata);

		return registerIndex(name, btree);

	}

	/**
	 * Provides an opportunity to validate some aspects of the
	 * {@link IndexMetadata} for an index partition.
	 */
	protected void validateIndexMetadata(final String name, final IndexMetadata metadata) {

		// NOP, but extended by the ManagedJournal.

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
	public BTree registerIndex(final String name, final BTree ndx) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			synchronized (_name2Addr) {

				// add to the persistent name map.
				_name2Addr.registerIndex(name, ndx);

			}

			return ndx;

		} finally {

			lock.unlock();

		}

	}

    /**
     * Drops the named index. The index will no longer participate in atomic
     * commits and will not be visible to new transactions.  Storage will be
     * reclaimed IFF the backing store support that functionality.
     */
	public void dropIndex(final String name) {

	    final BTree ndx = getIndex(name);
	    
	    if(ndx == null)
	        throw new NoSuchIndexException(name);
	    
	    if(getBufferStrategy() instanceof RWStrategy) {
            /*
             * Reclaim storage associated with the index.
             */
	        ndx.removeAll();
	    }
	    
		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			synchronized (_name2Addr) {

				// drop from the persistent name map.
				_name2Addr.dropIndex(name);

			}

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Return the mutable view of the named index (aka the "live" or
	 * {@link ITx#UNISOLATED} index). This object is NOT thread-safe. You MUST
	 * NOT write on this index unless you KNOW that you are the only writer. See
	 * {@link ConcurrencyManager}, which handles exclusive locks for
	 * {@link ITx#UNISOLATED} indices.
	 * 
	 * @return The mutable view of the index.
	 * 
	 * @see #getIndex(String, long)
	 */
	public BTree getIndex(final String name) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			if (name == null)
				throw new IllegalArgumentException();

			if (Thread.interrupted()) {

				throw new RuntimeException(new InterruptedException());

			}

			// Note: NullPointerException can be thrown here if asynchronously
			// closed (should be fixed by the ReadLock).
			synchronized (_name2Addr) {

				return _name2Addr.getIndex(name);

			}

		} finally {

			lock.unlock();

		}

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

	final public long toAddr(int nbytes, long offset) {
		return _bufferStrategy.toAddr(nbytes, offset);
	}

	final public String toString(long addr) {
		return _bufferStrategy.toString(addr);
	}

	final public int getOffsetBits() {
		return _bufferStrategy.getOffsetBits();
	}

	/**
	 * The maximum length of a record that may be written on the store.
	 */
	final public int getMaxRecordSize() {
		return _bufferStrategy.getMaxRecordSize();
	}

	/*
	 * High Availability
	 */

    /**
     * The current quorum token or {@value Quorum#NO_QUORUM} if the node is not
     * part of a {@link Quorum}.
     * <p>
     * The state of the field changes when a new quorum is negotiated or when an
     * existing quorum is broken. However, state changes in this field MUST be
     * coordinated with the journal in order to cover transitions into the
     * quorum (blocked read/write requests must be released) and transitions
     * when the quorum breaks (the current write set must be discarded by the
     * master). In addition, when there is no quorum, the resynchronization
     * protocol may affect both the persistent state of the journal and any
     * state which the journal keeps buffered in memory (record cache, address
     * translation cache, etc).
     * <p>
     * Access to this field is protected by the {@link #_fieldReadWriteLock} but
     * MUST also be coordinated as described above.
     * 
     * FIXME Many methods need to be modified to (a) await a quorum if there is
     * none; and (b) handle a change in the quorum token. When a quorum breaks,
     * these operations can block until a new quorum meets. As long as the
     * master has not changed, the master's state remains valid and the other
     * nodes can be brought into synchronization (they are synchronized if they
     * are at the same message count for the write pipeline). If the master
     * fails, then the new master will not have the same buffered write set and
     * the outstanding operations (both unisolated and read/write tx) must about
     * so we can resume from a known good commit point (this is true even if the
     * secondaries remain active since (a) we do not have a strong guarantee
     * that the new master is at the same message count on the write pipeline as
     * the old master; and (b) transaction write sets are buffered within the
     * JVM and/or disk on the master and are not available to the secondaries on
     * failover.
     * 
     * @see #read(long)
     * @see #write(ByteBuffer)
     * @see #write(ByteBuffer, long)
     * @see #delete(long)
     * @see #closeForWrites(long) However, some methods probably should be
     *      allowed to proceed without a quorum (or a node must be permitted to
     *      disconnect permanently from a quorum). E.g.:
     * 
     * @see #close()
     * @see #destroy() Another interesting question is whether we have to be in
     *      a quorum to create a new journal. I would say, "yes" for a data
     *      service. The atomic cutover to a new journal should only be taken by
     *      a quorum.
     *      <p>
     *      Probably you need to be in a quorum to create an HA journal (outside
     *      of a data service) as well. That would appear to be necessary in
     *      order to create the same initial root blocks on each node. In that
     *      case we have a bootstrapping problem for new HA journals. Either
     *      they must have a quorum before hand and go through a coordinated
     *      "commit" protocol for the journal create or they should be created
     *      first, then negotiate the quorum membership and who is the master
     *      and then resynchronize before the journal comes on line.
     * 
     * @todo maintain readOnly flag based on whether or not the QuorumService
     *       was the leader the last time the quorum met and whether the token
     *       is still valid. use a lightweight test for a valid token to avoid
     *       lock contention.
     * 
     * @todo readers should not block and await a quorum unless we are willing
     *       to handle the case where this QuorumService is not joined with the
     *       met quorum either by reading on the quorum or by throwing an
     *       exception back to the application.
     * 
     * @todo In HA mode, the followers are read-only. However, that is a
     *       high-level constraint on application writes. For synchronization,
     *       we need to be able to write on the journal even when it is not
     *       joined with the quorum. The WriteCacheService will refuse to relay
     *       writes if the service is not the leader of a met quorum.
     *       <p>
     *       For example, it would be appropriate to impose the constraint on
     *       which nodes can be read or written at a load balancer at the SAIL
     *       layer.
     * 
     *       if (quorum.isHighlyAvailable() && quorum.isQuorumMet() &&
     *       quorum.getClient().isFollower(quorumToken)) { return true; }
     */
	private volatile long quorumToken;

    /**
     * The current {@link Quorum} (if any).
     */
	private final Quorum<HAGlue,QuorumService<HAGlue>> quorum;

    /**
     * The {@link Quorum} for this service -or- <code>null</code> if the service
     * is not running with a quorum.
     */
	public Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {

		return quorum;

	}
	
    /**
     * Factory for the {@link HADelegate} object for this
     * {@link AbstractJournal}. This may be overridden to publish additional
     * methods for the low-level HA API. The object returned by this factor is
     * available using {@link QuorumMember#getService()}.
     */
    protected HAGlue newHAGlue(final UUID serviceId) {

        // FIXME This is defaulting to a random port on the loopback address.
        final InetSocketAddress writePipelineAddr;
        try {
            writePipelineAddr =  new InetSocketAddress(getPort(0));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new BasicHA(serviceId, writePipelineAddr);

	}

    /**
     * Implementation hooks into the various low-level operations required to
     * support HA for the journal.
     * 
     * @todo Default to the loopback interface and a random open port?
     * 
     * @todo refactor into one implementation class per HA interface and then
     *       use delegation to route the operations to the implementation
     *       classes.
     */
	protected class BasicHA implements HAGlue  {

	    private final UUID serviceId;
	    private final InetSocketAddress writePipelineAddr;

        protected BasicHA(final UUID serviceId,
                final InetSocketAddress writePipelineAddr) {

            if (serviceId == null)
                throw new IllegalArgumentException();
            if (writePipelineAddr == null)
                throw new IllegalArgumentException();

            this.serviceId = serviceId;
            this.writePipelineAddr = writePipelineAddr;

        }

        /**
         * The most recent prepare request.
         */
        private final AtomicReference<IRootBlockView> prepareRequest = new AtomicReference<IRootBlockView>();

        public UUID getServiceId() {

            return serviceId;
            
		}

        public InetSocketAddress getWritePipelineAddr() {
            
            return writePipelineAddr;
            
        }

        /*
         * @todo if the leader is synchronized with the followers then they
         * should all agree on whether they should be writing rootBlock0 or
         * rootBlock1.
         */
        public Future<Boolean> prepare2Phase(final boolean isRootBlock0,
                final byte[] tmp, final long timeout, final TimeUnit unit) {

            if (tmp == null)
                throw new IllegalStateException();

            // construct view from the root block.
            final IRootBlockView rootBlock = new RootBlockView(//
                    isRootBlock0,//
                    ByteBuffer.wrap(tmp), //
                    new ChecksumUtility()//
            );

			if (rootBlock.getLastCommitTime() <= getLastCommitTime())
				throw new IllegalStateException();

			getQuorum().assertQuorum(rootBlock.getQuorumToken());

			prepareRequest.set(rootBlock);

			// the quorum token from the leader is in the root block.
			final long prepareToken = rootBlock.getQuorumToken();
			
			// true the token is valid and this service is the quorum leader
			final boolean isLeader = quorum.getMember().isLeader(prepareToken);
			
			final FutureTask<Boolean> ft = new FutureTaskMon<Boolean>(new Runnable() {

				public void run() {

					final IRootBlockView rootBlock = prepareRequest.get();

					if (rootBlock == null)
						throw new IllegalStateException();

					getQuorum().assertQuorum(prepareToken);

					/*
					 * Call to ensure strategy does everything required for
					 * itself before final root block commit. At a minimum it
					 * must flush its write cache to the backing file (issue the
					 * writes).
					 */
					// _bufferStrategy.commit(); // lifted to before we retrieve
					// RootBlock in commitNow
					/*
					 * Force application data to stable storage _before_ we
					 * update the root blocks. This option guarantees that the
					 * application data is stable on the disk before the atomic
					 * commit. Some operating systems and/or file systems may
					 * otherwise choose an ordered write with the consequence
					 * that the root blocks are laid down on the disk before the
					 * application data and a hard failure could result in the
					 * loss of application data addressed by the new root blocks
					 * (data loss on restart).
					 * 
					 * Note: We do not force the file metadata to disk. If that
					 * is done, it will be done by a force() after we write the
					 * root block on the disk.
					 */
					if (doubleSync) {

						_bufferStrategy.force(false/* metadata */);

					}

				}
			}, true/* vote=yes */);

			if(isLeader) {

	            /*
	             * Run in the caller's thread.
	             * 
	             * Note: In order to avoid deadlock, when the leader calls back to
	             * itself it MUST do so in the same thread in which it is already
	             * holding the writeLock. [Actually, we do not obtain the writeLock
	             * in prepare2Phase, but all the other quorum commit methods do.]
	             */

			    ft.run();
			    
			} else {

                /*
                 * We can't really handle the timeout in the leader's thread
                 * (and it would be very odd if the leader wound up waiting on
                 * itself!) but the followers can obey the timeout semantics for
                 * prepare() by execute()ing the FutureTask and then returning
                 * it immediately. The leader can wait on the Future up to the
                 * timeout and then cancel the Future.
                 */
                
			    // submit.
			    getExecutorService().execute(ft);
			    
			}
			
			return ft;
			
		}

		public Future<Void> commit2Phase(final long commitTime) {

			final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
			    
				public void run() {

					final IRootBlockView rootBlock = prepareRequest.get();

					if (rootBlock == null)
						throw new IllegalStateException();

					_fieldReadWriteLock.writeLock().lock();

					try {

						if (rootBlock.getLastCommitTime() != commitTime)
							throw new IllegalStateException();

						// verify that the qourum has not changed.
						getQuorum().assertQuorum(rootBlock.getQuorumToken());

						// write the root block on to the backing store.
						_bufferStrategy.writeRootBlock(rootBlock, forceOnCommit);

						// set the new root block.
						_rootBlock = rootBlock;

						// reload the commit record from the new root block.
						_commitRecord = _getCommitRecord();

						prepareRequest.set(null/* discard */);

					} finally {

						_fieldReadWriteLock.writeLock().unlock();

					}

				}
			}, null/* Void */);

            /*
             * Run in the caller's thread.
             * 
             * Note: In order to avoid deadlock, when the leader calls back to
             * itself it MUST do so in the same thread in which it is already
             * holding the writeLock.
             */
            ft.run();
            
            return ft;

		}

        public Future<Void> abort2Phase(final long token) {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
                public void run() {

                    getQuorum().assertQuorum(token);

                    prepareRequest.set(null/* discard */);

                    _abort();

                }
            }, null/* Void */);

            /*
             * Run in the caller's thread.
             * 
             * Note: In order to avoid deadlock, when the leader calls back to
             * itself it MUST do so in the same thread in which it is already
             * holding the writeLock.
             */
            ft.run();

            return ft;

		}

        /**
         * {@inheritDoc}
         * 
         * @todo We should test the LRUNexus for failover reads and install
         *       records into the cache if there is a cache miss. Unfortunately
         *       the cache holds objects, some of which declare how to access
         *       the underlying {@link IDataRecord} using the
         *       {@link IDataRecordAccess} interface.
         * 
         * @todo Since these are rare events it may not be worthwhile to setup a
         *       separate low-level socket service to send/receive the data.
         */
        public Future<byte[]> readFromDisk(final long token,
                final UUID storeId, final long addr) {

			final FutureTask<byte[]> ft = new FutureTask<byte[]>(new Callable<byte[]>() {
				
			    public byte[] call() throws Exception {

				    quorum.assertQuorum(token);
				    
					// final ILRUCache<Long, Object> cache = (LRUNexus.INSTANCE
					// == null) ? null
					// : LRUNexus.getCache(jnl);
					//
					// Object obj = cache.get(addr);
					//                    
					// if(obj != null && obj instanceof IDataRecordAccess) {
					//                        
					// return ((IDataRecordAccess)obj).data();
					//                        
					// }

                    // read from the local store.
                    final ByteBuffer b = ((IHABufferStrategy) getBufferStrategy())
                            .readFromLocalStore(addr);

                    final byte[] a = BytesUtil.toArray(b);
                    
					// cache.putIfAbsent(addr, b);

					return a;

				}
			});
			
			ft.run();
			
			return ft;

		}

        public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
                throws IOException {

            /*
             * Adjust the size on the disk of the local store to that given in
             * the message. For the RW store, this can cause the allocation
             * blocks to be moved if the store is extended.
             * 
             * @todo Trap truncation vs extend?
             */
            try {
                ((IHABufferStrategy) AbstractJournal.this._bufferStrategy)
                        .setExtentForLocalStore(msg.getFileExtent());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return getQuorum().getClient().receiveAndReplicate(msg);

		}

        public byte[] getRootBlock(final UUID storeId) {

            if (storeId == null)
                throw new IllegalArgumentException();

            if (getUUID().equals(storeId)) {
                // A request for a different journal's root block.
                throw new UnsupportedOperationException();
            }

            return BytesUtil.toArray(AbstractJournal.this.getRootBlockView()
                    .asReadOnlyBuffer());

        }

		/** NOP. */
        public Future<Void> bounceZookeeperConnection() {
            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
                public void run() {
                }
            }, null);
            ft.run();
            return ft;
        }

        /**
         * Does pipeline remove/add.
         */
        public Future<Void> moveToEndOfPipeline() {
            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
                public void run() {
                    final QuorumActor<?, ?> actor = quorum.getActor();
                    actor.pipelineRemove();
                    actor.pipelineAdd();
                }
            }, null/* result */);
            getExecutorService().execute(ft);
            return ft;
        }

	};

    /**
     * Return an unused port.
     * 
     * @param suggestedPort
     *            The suggested port.
     *            
     * @return The suggested port, unless it is zero or already in use, in which
     *         case an unused port is returned.
     * 
     * @throws IOException
     */
    static protected int getPort(int suggestedPort) throws IOException {
        ServerSocket openSocket;
        try {
            openSocket = new ServerSocket(suggestedPort);
        } catch (BindException ex) {
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        }
        final int port = openSocket.getLocalPort();
        openSocket.close();
        return port;
    }

}
