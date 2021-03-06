/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.journal;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.sparse.SparseRowStore;

/**
 * {@link IJournal} delegation pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JournalDelegate implements IJournal {
	
	private final AbstractJournal delegate;
	
	public JournalDelegate(final AbstractJournal source) {
		this.delegate = source;
	}

	public Properties getProperties() {
		return delegate.getProperties();
	}

	public void shutdown() {
		delegate.shutdown();	
	}

	public void shutdownNow() {
		delegate.shutdownNow();		
	}

	public void abort() {
		delegate.abort();
	}

	public long commit() {
		return delegate.commit();
	}

	public ICommitRecord getCommitRecord(long timestamp) {
		return delegate.getCommitRecord(timestamp);
	}

	public long getRootAddr(int index) {
		return delegate.getRootAddr(index);
	}

	public IRootBlockView getRootBlockView() {
		return delegate.getRootBlockView();
	}

//    public IRootBlockView getRootBlock(long commitTime) {
//        return delegate.getRootBlock(commitTime);
//    }
//
//	public Iterator<IRootBlockView> getRootBlocks(long startTime) {
//		return delegate.getRootBlocks(startTime);
//	}

	public void setCommitter(int index, ICommitter committer) {
		delegate.setCommitter(index, committer);
	}

	public void close() {
		delegate.close();
	}

	public void delete(long addr) {
		delegate.delete(addr);
	}

	public void deleteResources() {
		delegate.deleteResources();
	}

	public void destroy() {
		delegate.destroy();
	}

	public void force(boolean metadata) {
		delegate.force(metadata);
	}

	public CounterSet getCounters() {
		return delegate.getCounters();
	}

	public File getFile() {
		return delegate.getFile();
	}

	public IResourceMetadata getResourceMetadata() {
		return delegate.getResourceMetadata();
	}

	public UUID getUUID() {
		return delegate.getUUID();
	}

	public boolean isFullyBuffered() {
		return delegate.isFullyBuffered();
	}

	public boolean isOpen() {
		return delegate.isOpen();
	}

	public boolean isReadOnly() {
		return delegate.isOpen();
	}

	public boolean isStable() {
		return delegate.isStable();
	}

	public ByteBuffer read(long addr) {
		return delegate.read(addr);
	}

	public long size() {
		return delegate.size();
	}

	public long write(ByteBuffer data) {
		return delegate.write(data);
	}

//	@Override
//	public long write(ByteBuffer data, long oldAddr) {
//		return delegate.write(data, oldAddr);
//	}

	public int getByteCount(long addr) {
		return delegate.getByteCount(addr);
	}

	public long getOffset(long addr) {
		return delegate.getOffset(addr);
	}

    public long getPhysicalAddress(final long addr) {
        return delegate.getPhysicalAddress(addr);
    }

	public long toAddr(int nbytes, long offset) {
		return delegate.toAddr(nbytes, offset);
	}

	public String toString(long addr) {
		return delegate.toString(addr);
	}

	public IIndex getIndex(String name) {
		return delegate.getIndex(name);
	}

	public IIndex registerIndex(String name, BTree btree) {
		return delegate.registerIndex(name, btree);
	}

	public IIndex registerIndex(String name, IndexMetadata indexMetadata) {
		return delegate.registerIndex(name, indexMetadata);
	}

	public void dropIndex(String name) {
		delegate.dropIndex(name);
	}

	public void registerIndex(IndexMetadata indexMetadata) {
		delegate.registerIndex(indexMetadata);
	}

	public ExecutorService getExecutorService() {
		return delegate.getExecutorService();
	}

	public BigdataFileSystem getGlobalFileSystem() {
		return delegate.getGlobalFileSystem();
	}

    public SparseRowStore getGlobalRowStore() {
        return delegate.getGlobalRowStore();
    }

    public SparseRowStore getGlobalRowStore(final long timestamp) {
        return delegate.getGlobalRowStore(timestamp);
    }

	public IIndex getIndex(String name, long timestamp) {
		return delegate.getIndex(name, timestamp);
	}

	public long getLastCommitTime() {
		return delegate.getLastCommitTime();
	}

	public IResourceLocator getResourceLocator() {
		return delegate.getResourceLocator();
	}

    public ILocalTransactionManager getLocalTransactionManager() {
        return delegate.getLocalTransactionManager();
    }

    public IResourceLockService getResourceLockService() {
		return delegate.getResourceLockService();
	}

	public TemporaryStore getTempStore() {
		return delegate.getTempStore();
	}

	public ScheduledFuture<?> addScheduledTask(Runnable task,
			long initialDelay, long delay, TimeUnit unit) {
		return delegate.addScheduledTask(task, initialDelay, delay, unit);
	}

	public boolean getCollectPlatformStatistics() {
		return delegate.getCollectPlatformStatistics();
	}

	public boolean getCollectQueueStatistics() {
		return delegate.getCollectQueueStatistics();
	}

	public int getHttpdPort() {
		return delegate.getHttpdPort();
	}

    @Override
    public Iterator<String> indexNameScan(String prefix, long timestamp) {
        return delegate.indexNameScan(prefix, timestamp);
    }

	@Override
	public IPSOutputStream getOutputStream() {
		return delegate.getOutputStream();
	}

	@Override
	public InputStream getInputStream(long addr) {
		return delegate.getInputStream(addr);
	}

	@Override
	public boolean isDirty() {
		return delegate.isDirty();
	}

    @Override
    public ICheckpointProtocol register(String name, IndexMetadata metadata) {
        return delegate.register(name, metadata);
    }

    @Override
    public ICheckpointProtocol getIndexLocal(String name, long commitTime) {
        return delegate.getIndexLocal(name, commitTime);
    }

    @Override
    public ICheckpointProtocol getUnisolatedIndex(String name) {
        return delegate.getUnisolatedIndex(name);
    }

}
