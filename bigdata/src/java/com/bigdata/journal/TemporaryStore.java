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
 * Created on Feb 21, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A temporary store that supports named indices but no concurrency controls.
 * <p>
 * If you want a temporary store that supports named indices and concurrency
 * controls then choose a {@link Journal} with {@link BufferMode#Temporary}.
 * This has the advantage of full concurrency support, group commit, and
 * low-latency startup (since the file is not created until the store attempts
 * to write through to the disk). However, {@link TemporaryStore} is lighter
 * weight precisely because it does not provide concurrency control.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
//* {@link #checkpoint()} may be used to checkpoint the indices and
//* {@link #restoreLastCheckpoint()} may be used to revert to the last
//* checkpoint. If you note the checkpoint addresses from {@link #checkpoint()}
//* then you can restore any checkpoint with {@link #restoreCheckpoint(long)}
public class TemporaryStore extends TemporaryRawStore implements IBTreeManager {

    private static final Logger log = Logger.getLogger(TemporaryStore.class);

    /**
     * The size of the live index cache for the {@link Name2Addr} instance.
     * 
     * @todo this should be a configuration property once the temporary store
     *       accepts a {@link Properties} object in its ctor.
     * 
     * @see Options#DEFAULT_LIVE_INDEX_CACHE_CAPACITY
     */
    private final int liveIndexCacheCapacity = 20;

    /**
     * The timeout in milliseconds for stale entries in the live index cache for
     * the {@link Name2Addr} instance.
     * 
     * @todo this should be a configuration property once the temporary store
     *       accepts a {@link Properties} object in its ctor.
     * 
     * @see Options#DEFAULT_LIVE_INDEX_CACHE_TIMEOUT
     */
    private final long liveIndexCacheTimeout = Long
            .parseLong(Options.DEFAULT_LIVE_INDEX_CACHE_TIMEOUT);
    
    /**
     * A {@link TemporaryStore} that can scale-up. The backing file will be
     * created using the Java temporary file mechanism.
     * 
     * @see WormAddressManager#SCALE_UP_OFFSET_BITS
     * @see #getTempFile()
     */
    public TemporaryStore() {

        this(WormAddressManager.SCALE_UP_OFFSET_BITS);
        
    }

    /**
     * A {@link TemporaryStore} provisioned with the specified <i>offsetBits</i>.
     * The backing file will be created using the Java temporary file mechanism.
     * 
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record. The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     */
    public TemporaryStore(final int offsetBits) {

        this(offsetBits, getTempFile());
        
    }

    /**
     * A {@link TemporaryStore} provisioned with the specified <i>offsetBits</i>
     * and backed by the specified file.
     * 
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record. The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     * @param file
     *            The backing file (may exist, but must be empty if it exists).
     */
    public TemporaryStore(final int offsetBits, final File file) {

        super(0L/* maximumExtent */, offsetBits, file);

        setupName2AddrBTree();

        executorService = Executors.newCachedThreadPool(new DaemonThreadFactory
                (getClass().getName()+".executorService"));
        
        resourceLocator = new DefaultResourceLocator(//
                this,//
                null // delegate
        );
        
    }
    
    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known address of the named btree.
     * <p>
     * Note: This is a mutable {@link BTree} so it is NOT thread-safe. We always
     * synchronize on this object before accessing it.
     */
    private Name2Addr name2Addr;

    /**
     * Setup the btree that resolved named btrees.
     */
    private void setupName2AddrBTree() {

        assert name2Addr == null;
        
        name2Addr = Name2Addr.create(this);

        name2Addr.setupCache(liveIndexCacheCapacity, liveIndexCacheTimeout);
        
    }
    
//    /**
//     * The address of the last checkpoint written. When ZERO(0L) no checkpoint
//     * has been written and {@link #name2Addr} is simple discarded on
//     * {@link #abort()}.
//     */
//    private long lastCheckpointAddr = 0L;
//
//    /**
//     * Reverts to the last checkpoint, if any. If there is no last checkpoint,
//     * then the post-condition is as if the store had never been written on
//     * (except that the storage on the backing file is not reclaimed).
//     */
//    public void restoreLastCheckpoint() {
//        
//        restoreCheckpoint(lastCheckpointAddr);
//        
//    }
//    
//    /**
//     * Reverts to the checkpoint associated with the given <i>checkpointAddr</i>.
//     * When ZERO(0L), the post-condition is as if the store had never been
//     * written on (except that the storage on the backing file is not
//     * reclaimed). The <i>checkpointAddr</i> is noted as the current
//     * {@link #restoreLastCheckpoint()} point.
//     */
//    public void restoreCheckpoint(final long checkpointAddr) {
//
//        assertOpen();
//
//        name2Addr = null;
//        
//        if (checkpointAddr != 0L) {
//
//            name2Addr = (Name2Addr) Name2Addr
//                    .load(this, checkpointAddr, false/* readOnly */);
//
//        } else {
//            
//            setupName2AddrBTree();
//            
//        }
//        
//        // note the restore point.
//        lastCheckpointAddr = checkpointAddr;
//
//    }
//
//    /**
//     * Checkpoints the dirty indices and notes the new
//     * {@link #restoreLastCheckpoint()} point. You can revert to the last
//     * written checkpoint using {@link #restoreLastCheckpoint()} or to an
//     * arbitrary checkpoint using {@link #restoreCheckpoint(long)}.
//     * <p>
//     * Note: {@link ITx#READ_COMMITTED} views of indices become available after
//     * a {@link #checkpoint()}. If the store has not been checkpointed, then
//     * the read committed views are unavailable for an index. After a checkpoint
//     * in which a given index was dirty, a new read-committed view is available
//     * for that index and checkpoint.
//     * <p>
//     * Note: This is NOT an atomic commit protocol, but the restore point will
//     * be updated iff the checkpoint succeeds.
//     * 
//     * @return The checkpoint address.
//     */
//    public long checkpoint() {
//
//        assertOpen();
//
//        // checkpoint the indices and note the restore point.
//        return lastCheckpointAddr = name2Addr.handleCommit(System
//                .currentTimeMillis());
//        
//    }

    public void registerIndex(final IndexMetadata metadata) {
        
        registerIndex(metadata.getName(), metadata);
        
    }
    
    public BTree registerIndex(final String name, final IndexMetadata metadata) {
    
        final BTree btree = BTree.create(this, metadata);

        return registerIndex(name, btree);
        
    }
    
    public BTree registerIndex(final String name, final BTree btree) {

        synchronized (name2Addr) {

            assertOpen();

            // add to the persistent name map.
            name2Addr.registerIndex(name, btree);

            return btree;

        }
        
    }
    
    public void dropIndex(final String name) {
        
        synchronized(name2Addr) {

            assertOpen();

            // drop from the persistent name map.
            name2Addr.dropIndex(name);
            
        }
        
    }

    /**
     * Return an {@link ITx#UNISOLATED} view of the named index -or-
     * <code>null</code> if there is no registered index by that name.
     */
    public BTree getIndex(final String name) {

        synchronized(name2Addr) {

            assertOpen();

            return name2Addr.getIndex(name);
            
        }

    }

    /**
     * Historical reads and transactions are not supported.
     * 
     * @param name
     * @param timestamp
     * 
     * @throws UnsupportedOperationException
     *             unless the timestamp is {@link ITx#UNISOLATED}.
     */
//    * <p>
//    * Note: If {@link ITx#READ_COMMITTED} is requested, then the returned
//    * {@link BTree} will reflect the state of the named index as of the last
//    * {@link #checkpoint()}. This view will be read-only and is NOT updated by
//    * {@link #checkpoint()}. You must actually {@link #checkpoint()} before an
//    * {@link ITx#READ_COMMITTED} view will be available.
    public BTree getIndex(final String name, final long timestamp) {

        assertOpen();

        if(timestamp == ITx.READ_COMMITTED) {
            
            final long checkpointAddr;
            
            synchronized(name2Addr) {
                
                final Entry entry = name2Addr.getEntry(name);
                
                if (entry == null) {

                    log.warn("No such index: name=" + name + ", timestamp="
                            + TimestampUtility.toString(timestamp));
                    
                    return null;
                    
                }

                checkpointAddr = entry.checkpointAddr;
                
            }
            
            final BTree btree = BTree
                    .load(this, checkpointAddr, true/*readOnly*/);
            
//            btree.setReadOnly(true);
            
            return btree;
            
        }
        
        if(timestamp == ITx.UNISOLATED) {
            
            return getIndex(name);
            
        }

        /*
         * FIXME The RWStore uses a read-only transaction to protect against
         * recycling of the B+Tree revisions associated with the commit point
         * on which it is reading.  The temporary store only supports unisolated
         * reads, so this is just ignoring the tx specified by the mutation rule
         * for reading on the temporary store and going with the unisolated index
         * anyway.  See https://sourceforge.net/apps/trac/bigdata/ticket/215.
         */
        
//        throw new UnsupportedOperationException(
//                "Not supported: timestamp="
//                + TimestampUtility.toString(timestamp));

		return getIndex(name);

    }

    public SparseRowStore getGlobalRowStore() {

        assertOpen();
        
        return globalRowStoreHelper.getGlobalRowStore();
        
    }
    final private GlobalRowStoreHelper globalRowStoreHelper = new GlobalRowStoreHelper(this); 

    public BigdataFileSystem getGlobalFileSystem() {

        assertOpen();
        
        return globalFileSystemHelper.getGlobalFileSystem();
        
    }
    final private GlobalFileSystemHelper globalFileSystemHelper = new GlobalFileSystemHelper(this); 

    public DefaultResourceLocator getResourceLocator() {

        assertOpen();
        
        return resourceLocator;
        
    }
    private final DefaultResourceLocator resourceLocator;
    
    public ExecutorService getExecutorService() {
    
        assertOpen();
        
        return executorService;
        
    }
    private final ExecutorService executorService;
    
    final public IResourceLockService getResourceLockService() {

        return resourceLockManager;
        
    }
    final private ResourceLockService resourceLockManager = new ResourceLockService();

    public void close() {

        // immediate shutdown.
        executorService.shutdownNow();
        
        super.close();
        
    }

	/**
	 * Always returns ZERO (0L) since you can not perform a commit on a
	 * {@link TemporaryRawStore} (it supports checkpoints but not commits).
	 */
	public long getLastCommitTime() {

		return 0L;
		
	}

    /**
     * Always returns <i>this</i> {@link TemporaryStore}.
     */
    public TemporaryStore getTempStore() {
        
        return this;
        
    }

    /**
     * Not supported, returns <code>null</code>.
     */
	public ScheduledFuture<?> addScheduledTask(Runnable task,
			long initialDelay, long delay, TimeUnit unit) {
		return null;
	}

    /**
     * Not supported, returns <code>false</code>.
     */
	public boolean getCollectPlatformStatistics() {
		return false;
	}

    /**
     * Not supported, returns <code>false</code>.
     */
	public boolean getCollectQueueStatistics() {
		return false;
	}

    /**
     * Not supported, returns <code>false</code>.
     */
	public int getHttpdPort() {
		return -1;
	}
    
}
