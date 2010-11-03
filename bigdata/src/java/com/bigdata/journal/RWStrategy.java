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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rwstore.IAllocationContext;
import com.bigdata.rwstore.RWStore;

/**
 * The hook that accesses the RWStore to provide read/write services as opposed
 * to the WORM characteristics of the DiskOnlyStrategy AddressManager.
 * 
 * The intent behind this approach is to try to manage the protocol differences
 * between the IStore implementation, assumed as a backing service to the CTC
 * ObjectManager, and an IBufferStrategy service for a BigData Journal.
 * 
 * The most fundamental difference is with the root data, with RootBlock
 * including both low-level store data - such as metabits info - and higher
 * level, journal maintained data.
 * 
 * @author Martyn Cutcher
 * 
 *       FIXME Review life cycle state changes and refusal of methods when the
 *       backing store is closed. m_open should probably be moved into RWStore
 *       which could then expose an isOpen() method to be used by this class.
 */
public class RWStrategy extends AbstractRawStore implements IBufferStrategy, IHABufferStrategy {

    private static final transient Logger log = Logger.getLogger(RWStrategy.class);

    private final IAddressManager m_am = new RWAddressManager();

    /**
     * The backing store implementation.
     */
	private final RWStore m_store;
	
	/**
	 * The {@link UUID} for the store.
	 */
	private final UUID m_uuid;

	/**
	 * <code>true</code> iff the backing store is open.
	 */
	private volatile boolean m_open = false;

	/**
	 * The size of the backing file when it was opened by the constructor.
	 */
    final private long m_initialExtent;

	/**
	 * 
	 * @param fileMetadata
	 * @param quorum
	 */
    RWStrategy(final FileMetadata fileMetadata, final Quorum<?, ?> quorum) {

	    m_uuid = fileMetadata.rootBlock.getUUID();
	    
		m_store = new RWStore(fileMetadata, quorum); 
		
		m_open = true;
				
		m_initialExtent = fileMetadata.file.length();
	
	}

    public ByteBuffer readRootBlock(final boolean rootBlock0) {

        if (!isOpen())
            throw new IllegalStateException();

        return m_store.readRootBlock(rootBlock0);

	}

	public ByteBuffer read(final long addr) {
		
        if (!isOpen())
            throw new IllegalStateException();
		
		final int rwaddr = decodeAddr(addr);		
		final int sze = decodeSize(addr);

		if (rwaddr == 0L || sze == 0) {
			throw new IllegalArgumentException();
		}
		
		/**
		 * Allocate buffer to include checksum to allow single read
		 * but then return ByteBuffer excluding those bytes
		 */
		final byte buf[] = new byte[sze+4]; // 4 bytes for checksum
		
		m_store.getData(rwaddr, buf);

		return ByteBuffer.wrap(buf, 0, sze);

	}

	public long write(final ByteBuffer data) {
		
	    return write(data, null);
	    
	}

	public long write(final ByteBuffer data, final IAllocationContext context) {

        if (!isOpen())
            throw new IllegalStateException();
		
		if (data == null)
			throw new IllegalArgumentException();

        if (data.hasArray() && data.arrayOffset() != 0) {
            /*
             * @todo [data] is not always backed by an array, the array may not
             * be visible (read-only), the array offset may not be zero, etc.
             * Try to drive the ByteBuffer into the RWStore.alloc() method
             * instead.
             * 
             * See https://sourceforge.net/apps/trac/bigdata/ticket/151
             */
            throw new AssertionError();
        }

        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException();

        final long rwaddr = m_store.alloc(data.array(), nbytes, context);

        data.position(nbytes); // update position to end of buffer

        final long retaddr = encodeAddr(rwaddr, nbytes);

        return retaddr;

    }

	private long encodeAddr(long alloc, final int nbytes) {
		alloc <<= 32;
		alloc += nbytes;

		return alloc;
	}

	private int decodeAddr(long addr) {
		addr >>= 32;

		return (int) addr;
	}

	private int decodeSize(final long addr) {

	    return (int) (addr & 0xFFFFFFFF);
	    
	}

	public void delete(final long addr) {
    
	    delete(addr, null/* IAllocationContext */);
	    
	}

	/**
	 * Must check whether there are existing transactions which may access
	 * this data, and if not free immediately, otherwise defer.
	 */
	public void delete(final long addr, final IAllocationContext context) {
		
		final int rwaddr = decodeAddr(addr);
		final int sze = decodeSize(addr);
		
		m_store.free(rwaddr, sze, context);
		
	}
	
	public void detachContext(final IAllocationContext context) {
		
	    m_store.detachContext(context);
	    
	}

    /**
     * Operation is not supported.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
	public void closeForWrites() {
		// @todo could be implemented at some point.
	    throw new UnsupportedOperationException();
	}

	public BufferMode getBufferMode() {

	    return BufferMode.DiskRW;
	    
	}

    /**
     * @todo Define and implement support for counters. The pattern for this
     *       method is to always return a new object so it may be attached to
     *       various points in hierarchies belonging to the caller.  See the
     *       {@link WORMStrategy} for examples.
     */
	public CounterSet getCounters() {

	    return new CounterSet();
	    
	}

	public long getExtent() {

	    return m_store.getStoreFile().length();
	    
	}

	public int getHeaderSize() {
	 
	    return FileMetadata.headerSize0;
	    
	}

	public long getInitialExtent() {
		
	    return m_initialExtent;
	    
	}

	public long getMaximumExtent() {
		
	    return 0L;
	    
	}

	public boolean useChecksums() {
	    
	    return true;
	    
	}
	
	public long getNextOffset() {
		
	    return m_store.getNextOffset();
	    
	}

	public long getUserExtent() {
	
	    return m_store.getFileStorage();
	    
	}

    /**
     * Operation is not supported.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
	public long transferTo(RandomAccessFile out) throws IOException {
	    
	    // @todo could perhaps be implemented at some point.
	    throw new UnsupportedOperationException();
	    
	}

    public void truncate(final long extent) {
        
        m_store.establishExtent(extent);
        
	}

    public void writeRootBlock(final IRootBlockView rootBlock,
            final ForceEnum forceOnCommit) {

        m_store.writeRootBlock(rootBlock, forceOnCommit);

	}

	/**
	 * Set close time in rootBlock, and close file
	 */
	public void close() {
		if (!m_open) {
			throw new IllegalStateException();
		}
		m_store.close();
        m_open = false;
	}

    public void deleteResources() {

        if (m_open)
            throw new IllegalArgumentException();

        final File file = m_store.getStoreFile();

        if (file.exists()) {

            if (!file.delete()) {

                log.warn("Unable to delete file: " + file);

            }

        }

	}

	public void destroy() {

	    m_store.close();
        m_open = false;

		deleteResources();
		
	}

    public void commit(final IJournal journal) {

        m_store.commitChanges((Journal) journal); // includes a force(false)
        
	}
	
	/**
	 * Calls through to store and then to WriteCacheService.reset
	 */
	public void abort() {

	    m_store.reset();
	    
	}
	
    public void force(final boolean metadata) {
        
        try {
        
            m_store.flushWrites(metadata);
        
        } catch (IOException e) {

            throw new RuntimeException(e);
            
        }
        
	}
    
	public File getFile() {

	    return m_store.getStoreFile();
	    
	}
    
	/**
     * Not supported - this is available on the {@link AbstractJournal}.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public IResourceMetadata getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }
    
	public UUID getUUID() {
		
	    return m_uuid;
	    
	}

	public boolean isFullyBuffered() {
		
	    return false;
	    
	}

	public boolean isOpen() {

	    return m_open;
	    
	}

	public boolean isReadOnly() {
		
	    return false;
	    
	}

	public boolean isStable() {
		
	    return true;
	    
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * This implementation returns the amount of utilized storage.
	 */
	public long size() {
		return m_store.getFileStorage();
	}

    /*
     * IAddressManager
     */

    public IAddressManager getAddressManager() {
        return m_am;
    }

    public int getByteCount(final long addr) {
        return m_am.getByteCount(addr);
    }

    public long getOffset(final long addr) {
        return m_am.getOffset(addr);
    }

    public long toAddr(final int nbytes, final long offset) {
        return m_am.toAddr(nbytes, offset);
    }

    public String toString(final long addr) {
        return m_am.toString(addr);
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The state of the provided block is not relevant since it does not hold
     * information on recent allocations (the meta allocations will only effect
     * the root block after a commit). This is passed through to the
     * {@link RWStore} which examines its internal state.
     */
    public boolean requiresCommit(final IRootBlockView block) {

        return m_store.requiresCommit();
        
    }

    public long getMetaBitsAddr() {
        
        return m_store.getMetaBitsAddr();
        
    }

    public long getMetaStartAddr() {

        return m_store.getMetaStartAddr();
        
    }

	public int getMaxRecordSize() {

	    return m_store.getMaxAllocSize() - 4/* checksum */;
	    
	}

    /**
     * Although the RW Store uses a latched addressing strategy it is not
     * meaningful to make this available in this interface.
     */
    public int getOffsetBits() {

        return 0;
        
    }
	
	/**
	 * Used for unit tests, could also be used to access raw statistics.
	 * 
	 * @return the associated RWStore
	 */
	public RWStore getRWStore() {
		
	    return m_store;
	    
	}

    public long getPhysicalAddress(final long addr) {

        final int rwaddr = decodeAddr(addr);        
        
        return m_store.physicalAddress(rwaddr);
    }

	/*
	 * IHABufferStrategy
	 */
	
    /**
     * Operation is not supported.
     */
    public void writeRawBuffer(HAWriteMessage msg, ByteBuffer b)
            throws IOException, InterruptedException {

        throw new UnsupportedOperationException();

    }

    /**
     * Operation is not supported.
     */
    public ByteBuffer readFromLocalStore(final long addr)
            throws InterruptedException {

        throw new UnsupportedOperationException();

    }

    /**
     * Called from HAGlue.receiveAndReplicate to ensure the correct file extent
     * prior to any writes. For RW this is essential as the allocation blocks
     * for current committed data could otherwise be overwritten and the store
     * invalidated.
     * 
     * @see com.bigdata.journal.IHABufferStrategy#setExtentForLocalStore(long)
     */
    public void setExtentForLocalStore(final long extent) throws IOException,
            InterruptedException {

        m_store.establishExtent(extent);

    }

}
