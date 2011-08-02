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

package com.bigdata.rwstore;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;

import org.apache.log4j.Logger;

import com.bigdata.rwstore.RWStore.AllocationStats;
import com.bigdata.rwstore.StorageStats.Bucket;
import com.bigdata.util.ChecksumUtility;

/**
 * FixedAllocator
 * 
 * Maintains List of AllocBlock(s)
 */
public class FixedAllocator implements Allocator {
    
    private static final Logger log = Logger.getLogger(FixedAllocator.class);
    
    private final int cModAllocation = 1 << RWStore.ALLOCATION_SCALEUP;
    private final int cMinAllocation = cModAllocation * 1; // must be multiple of cModAllocation

	volatile private int m_freeBits;
	volatile private int m_freeTransients;

    /**
     * Address of the {@link FixedAllocator} within the meta allocation space on
     * the disk.
     */
	volatile private int m_diskAddr;
	volatile private int m_index;
	
	Bucket m_statsBucket = null;

	public void setIndex(final int index) {
		final AllocBlock fb = (AllocBlock) m_allocBlocks.get(0);
		
        if (log.isDebugEnabled())
            log.debug("Restored index " + index + " with " + getStartAddr()
                    + "[" + fb.m_live[0] + "] from " + m_diskAddr);

		m_index = index;
	}

	public long getStartAddr() {
		return RWStore.convertAddr(m_startAddr);
	}

    /*
     * Note: Object#equals() is fine with this compareTo() implementation. It is
     * only used to sort the allocators.
     */
	public int compareTo(final Object o) {
		final Allocator other = (Allocator) o;
		if (other.getStartAddr() == 0) {
			return -1;
		} else {
			final long val = getStartAddr() - other.getStartAddr();

			if (val == 0) {
				throw new Error("Two allocators at same address");
			}

			return val < 0 ? -1 : 1;
		}
	}

	public int getDiskAddr() {
		return m_diskAddr;
	}

	public void setDiskAddr(final int addr) {
		m_diskAddr = addr;
	}

    /**
     * The tweak of 3 to the offset is to ensure 1, that no address is zero and
     * 2 to enable the values 1 & 2 to be special cased (this aspect is now
     * historical).
     */
	public long getPhysicalAddress(int offset) {
	  	offset -= 3;

		final int allocBlockRange = 32 * m_bitSize;

		final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
		
		final int bit = offset % allocBlockRange;
		
//		if (RWStore.tstBit(block.m_live, bit) 
//				|| (m_sessionActive && RWStore.tstBit(block.m_transients, bit))) { 
		/*
		 * Just check transients since there are case (eg CommitRecordIndex)
		 * where committed data is accessed even if has been marked as ready to
		 * be recycled after the next commit
		 */
		final long paddr = RWStore.convertAddr(block.m_addr) + ((long) m_size * bit);
		if (RWStore.tstBit(block.m_transients, bit)) {		
			return paddr;
		} else {
			if (RWStore.tstBit(block.m_commit, bit)) {
				throw new IllegalStateException("Address committed but not set in transients");
			}
			m_store.showWriteCacheDebug(paddr);			
			
			return 0L;
		}
	}

	public int getPhysicalSize(final int offset) {
		return m_size;
	}

	public int getBlockSize() {
		return m_size;
	}

	/**
	 * The free list for the allocation slot size serviced by this allocator.
	 * This is a reference back into the corresponding free list as managed by
	 * the RWStore.
	 * 
	 * @see #setFreeList(ArrayList)
	 */
	private ArrayList m_freeList;

	public void setFreeList(ArrayList list) {
		m_freeList = list;

		if (!m_pendingContextCommit && hasFree()) {
			m_freeList.add(this);
			m_freeWaiting = false;
		}
	}

	volatile IAllocationContext m_context;
	volatile Thread m_contextThread;

	/**
	 * Indicates whether session protection has been used to protect
	 * store from re-allocating allocations reachable from read-only
	 * requests and concurrent transactions.
	 */
	boolean m_sessionActive;

	private boolean m_pendingContextCommit = false;
	
	public void setAllocationContext(final IAllocationContext context) {
		if (m_pendingContextCommit) {
			throw new IllegalStateException("Already pending commit");
		}
		
		assert checkBits();

		if (context == null && m_context != null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.deshadow();
			}
			
			// return to dirty list
			m_store.addToCommit(this);
			m_pendingContextCommit  = true;
			
		} else if (context != null && m_context == null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.shadow();
			}
			
			// remove from dirty list if present!
			// NO! m_store.removeFromCommit(this);
		}
		m_context = context;
		
		if (m_context != null) {
			m_contextThread = Thread.currentThread();
		} else {
			m_contextThread = null;
		}
		
		assert checkBits();
		
	}

	/**
	 * Unwinds the allocations made within the context and clears the write
	 * cache of any associated data
	 * @param writeCacheService 
	 */
	public void abortAllocationContext(final IAllocationContext context, RWWriteCacheService writeCacheService) {
		if (m_pendingContextCommit) {
			throw new IllegalStateException("Already pending commit");
		}

		if (m_context != null) {
			// restore commit bits in AllocBlocks
			for (AllocBlock allocBlock : m_allocBlocks) {
				allocBlock.abortshadow(writeCacheService);
			}
			
			// Reset freebits
			m_freeBits = calcFreeBits();
			
			m_context = context;
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * write called on commit, so this is the point when "transient frees" - the
	 * freeing of previously committed memory can be made available since we
	 * are creating a new commit point - the condition being that m_freeBits
	 * was zero and m_freeTransients not.
	 */
	public byte[] write() {
		try {
			
			final AllocBlock fb = m_allocBlocks.get(0);
			if (log.isTraceEnabled())
				log.trace("writing allocator " + m_index + " for " + getStartAddr() + " with " + fb.m_live[0]);
			final byte[] buf = new byte[1024];
			final DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
			final boolean protectTransients = m_sessionActive || m_store.isSessionProtected();
			try {
                str.writeInt(m_size);
                
                assert m_sessionActive || m_freeTransients == transientbits();
    			
                final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
                while (iter.hasNext()) {
                    final AllocBlock block = iter.next();

                    str.writeInt(block.m_addr);
                    for (int i = 0; i < m_bitSize; i++) {
                    	str.writeInt(block.m_live[i]);
                    }

                    if (!protectTransients) {
                    	/**
                    	 * This assert will trip if any address was freed under
                    	 * session protection and therefore remained accessible
                    	 * until released.
                    	 * The value returned by releaseSession should be zero
                    	 * since all "frees" should already have removed any 
                    	 * writes to the writeCacheService
                    	 */
                    	assert m_sessionFrees.intValue() == 0;
                    	// assert block.releaseSession(m_store.m_writeCache) == 0;
                    	
                        block.m_transients = block.m_live.clone();
                    }

                    /**
                     * If this allocator is shadowed then copy the new committed
                     * state to m_saveCommit
                     */
                    if (m_context != null) {
                        throw new IllegalStateException("Must not commit shadowed FixedAllocator!");
//                    } else if (m_store.isSessionPreserved()) {
//                        block.m_commit = block.m_transients.clone();
                    } else {
                        block.m_commit = block.m_live.clone();
                        // if m_saveCommit is set then it must be m_pendingContextCommit
                        if (block.m_saveCommit != null) {
                        	if (!m_pendingContextCommit)
                        		throw new IllegalStateException("Unexpected m_saveCommit when no pending commit");
                        	block.m_saveCommit = null;
                        }
                    }
                }
                // add checksum
                final int chk = ChecksumUtility.getCHK().checksum(buf,
                        str.size());
                str.writeInt(chk);
			} finally {
			    str.close();
			}

			if (m_pendingContextCommit) {
				m_pendingContextCommit = false;
				if (hasFree()) {
					m_freeList.add(this);
				}
			}
			
			if (!protectTransients /*!this.m_sessionActive*/) {
				m_freeBits += m_freeTransients;
	
				// Handle re-addition to free list once transient frees are
				// added back
				if (m_freeWaiting && m_freeBits >= m_store.cDefaultFreeBitsThreshold) {
					m_freeList.add(this);
					m_freeWaiting = false;
				}
	
				m_freeTransients = 0;
				
			}
			
			assert checkBits();
			
			return buf;
		} catch (IOException e) {
			throw new StorageTerminalError("Error on write", e);
		}
	}

	private int calcFreeBits() {
		int freeBits = 0;
		
		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
		final int blockSize = m_bitSize * 32 * m_size;
		while (iter.hasNext()) {
			final AllocBlock block = iter.next();
			for (int i = 0; i < m_bitSize; i++) {
				freeBits += 32 - Integer.bitCount(block.m_transients[i]);
			}
		}
		
		return freeBits;
	}

	private int calcLiveFreeBits() {
		int freeBits = 0;
		
		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
		final int blockSize = m_bitSize * 32 * m_size;
		while (iter.hasNext()) {
			final AllocBlock block = iter.next();
			for (int i = 0; i < m_bitSize; i++) {
				freeBits += 32 - Integer.bitCount(block.m_live[i]);
			}
		}
		
		return freeBits;
	}
	
	private boolean checkBits() {
		final int calcFree = calcFreeBits();
		final int calcLiveFree = calcLiveFreeBits();
		
		return m_freeBits == calcFree
			&& (m_freeBits + m_freeTransients) == calcLiveFree;
		
	}

	// read does not read in m_size since this is read to determine the class of
	// allocator
	public void read(final DataInputStream str) {
		try {
			m_freeBits = 0;

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			final int blockSize = m_bitSize * 32 * m_size;
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();

				block.m_addr = str.readInt();
				for (int i = 0; i < m_bitSize; i++) {
					block.m_live[i] = str.readInt();

					/**
					 * Need to calc how many free blocks are available, minor
					 * optimization by checking against either empty or full to
					 * avoid scanning every bit unnecessarily
					 **/
					if (block.m_live[i] == 0) { // empty
						m_freeBits += 32;
					} else if (block.m_live[i] != 0xFFFFFFFF) { // not full
						final int anInt = block.m_live[i];
//						for (int bit = 0; bit < 32; bit++) {
//							if ((anInt & (1 << bit)) == 0) {
//								m_freeBits++;
//							}
//						}
						
						m_freeBits += 32 - Integer.bitCount(anInt);
					} // else full so no freebits
				}

				block.m_transients = (int[]) block.m_live.clone();
				block.m_commit = (int[]) block.m_live.clone();

				if (m_startAddr == 0) {
					m_startAddr = block.m_addr;
				}
				// int endAddr = block.m_addr + blockSize;
				if (block.m_addr > 0) {
					m_endAddr = block.m_addr + blockSize;
				}
			}
		} catch (IOException e) {
			throw new StorageTerminalError("Error on read", e);
		}

	}

    /** The size of the allocation slots in bytes. */
    final int m_size;

	private int m_startAddr = 0;
	private int m_endAddr = 0;

	/**
	 * The #of ints in the {@link AllocBlock}'s internal arrays.
	 */
	private final int m_bitSize;

	private final ArrayList<AllocBlock> m_allocBlocks;

	final private RWStore m_store;

	/**
	 * Calculating the number of ints (m_bitSize) cannot rely on a power of 2.  Previously this
	 * assumption was sufficient to guarantee a rounding on to an 64k boundary.  However, now
	 * nints * 32 * 64 = 64K, so need multiple of 32 ints.
	 * <p>
	 * So, whatever multiple of 64, if we allocate a multiple of 32 ints we are guaranteed to be 
	 * on an 64K boundary.
	 * <p>
	 * This does mean that for the largest blocks of ~256K, we are allocating 256Mb of space
	 * 
	 * @param size The size of the allocation slots in bytes.
	 * @param preserveSessionData
	 * @param cache
	 */
	FixedAllocator(final RWStore store, final int size) {//, final RWWriteCacheService cache) {
		m_diskAddr = 0;
		m_store = store;

		m_size = size;

		// By default, disk-based allocators should optimise for density
		m_bitSize = calcBitSize(true /* optDensity */, size, cMinAllocation, cModAllocation);

		// number of blocks in this allocator, bitSize plus 1 for start address
		// The 1K allocator is 256 ints, one is used to record the slot size and
		// another for the checksum; leaving 254 to be used to store the 
		// AllocBlocks.
		final int numBlocks = 254 / (m_bitSize + 1);

		/*
		 * Create AllocBlocks for this FixedAllocator, but do not allocate
		 * either the AllocBlocks or their managed allocation slots on the
		 * persistent heap yet.
		 */
		m_allocBlocks = new ArrayList<AllocBlock>(numBlocks);
		for (int i = 0; i < numBlocks; i++) {
			m_allocBlocks.add(new AllocBlock(0, m_bitSize, this));//, cache));
		}

		m_freeTransients = 0;
		m_freeBits = 32 * m_bitSize * numBlocks;
	}
	
	/**
	 * This determines the size of the reservation required in terms of
	 * the number of ints each holding bits for 32 slots.
	 * 
	 * The minimum return value will be 1, for a single int holiding 32 bits.
	 * 
	 * The maximum value will be the number of ints required to fill the minimum
	 * reservation.
	 * 
	 * The minimum reservation will be some multiple of the
	 * address multiplier that allows alloction blocks to address large addresses
	 * with an INT32.  For example, by setting a minimum reservation at 128K, the
	 * allocation blocks INT32 start address may be multiplied by 128K to provide
	 * a physical address.
	 * 
	 * The minReserve must be a power of 2, eg 1K, 2k or 4K.. etc
	 * 
	 * A standard minReserve of 16K is plenty big enough, enabling 32TB of
	 * addressable store.  The logical maximum used store is calculated as the
	 * maximum fixed allocation size * MAX_INT.  So a store with a maximum
	 * fixed slot size of 4K could only allocated 8TB.
	 * 
	 * Since the allocation size must be MOD 0 the minReserve, the lower the 
	 * minReserve the smaller the allocation may be required for larger
	 * slot sizes.
	 * 
	 * Another consideration is file locality.  In this case the emphasis is
	 * on larger contiguous areas to improve the likely locality of allocations
	 * made by a FixedAllocator.  Here the addressability implied by the reserve
	 * is not an issue, and larger reserves are chosen to improve locality.  The
	 * downside is a potential for more wasted space, but this
	 * reduces as the store size grows and in large stores (> 10GB) becomes
	 * insignificant.
	 * 
	 * Therefore, if a FixedAllocator is to be used in a large store and
	 * locality needs to be optimised for SATA disk access then the minReserve
	 * should be high = say 128K, while if the allocator is tuned to ByteBuffer
	 * allocation, a minallocation of 8 to 16K is more suitable.
	 * 
	 * A final consideration is allocator reference efficiency in the sense
	 * to maximise the amount of allocations that can be made.  By this I mean
	 * just how close we can get to MAX_INT allocations.  For example, if we
	 * allow for upto 8192 allocations from a single allocator, but in
	 * practice average closer to 4096 then the maximum number of allocations
	 * comes down from MAX_INT to MAX_INT/2.  This is also a consideration when
	 * considering max fixed allocator size, since if we require a large number
	 * of Blobs this reduces the amount of "virtual" allocations by at least
	 * a factro of three for each blob (at least 2 fixed allocations for
	 * content and 1 more for the header).  A variation on the current Blob
	 * implementation could include the header in the first allocation, thus
	 * reducing the minimum Blob allocations from 3 to 2, but the point still
	 * holds that too small a max fixed allocation could dramatically reduce the
	 * number of allocations that could be made.
	 * 
	 * @param alloc the slot size to be managed
	 * @param minReserve the minimum reservation in bytes
	 * @return the size of the int array
	 */
	public static int calcBitSize(final boolean optDensity, final int alloc, final int minReserve, final int modAllocation) {
		final int intAllocation = 32 * alloc; // min 32 bits
		
		// we need to find smallest number of ints * the intAllocation
		//	such that totalAllocation % minReserve is 0
		// example 6K intAllocation would need 8 ints for 48K for 16K min
		// likewise a 24K intAllocation would require 2 ints
		 // if optimising for density set min ints to 8
		int nints = optDensity ? 8 : 1;
		while ((nints * intAllocation) < minReserve) nints++;
		
		while ((nints * intAllocation) % modAllocation != 0) nints++;
		
		return nints;
	}

	public String getStats(final AtomicLong counter) {

        final StringBuilder sb = new StringBuilder(getSummaryStats());

		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
		while (iter.hasNext()) {
			final AllocBlock block = iter.next();
			if (block.m_addr == 0) {
				break;
			}
            sb.append(block.getStats(null) + "\r\n");
            if (counter != null)
                counter.addAndGet(block.getAllocBits() * (long) m_size);
		}

		return sb.toString();
	}

	public String getSummaryStats() {

        return"Block size : " + m_size
                + " start : " + getStartAddr() + " free : " + m_freeBits
                + "\r\n";
	}

	public boolean verify(int addr) {
		if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.verify(addr, m_size)) {
					return true;
				}
			}
		}

		return false;
	}

	public boolean addressInRange(int addr) {
		if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.addressInRange(addr, m_size)) {
					return true;
				}
			}
		}

		return false;
	}

	private boolean m_freeWaiting = true;

	// track number of frees to be cleared on session releases
	private AtomicInteger m_sessionFrees = new AtomicInteger(0);
	
	public boolean free(final int addr, final int size) {
		return free(addr, size, false);
	}
	
	public boolean free(final int addr, final int size, final boolean overideSession) {
		if (addr < 0) {
			final int offset = ((-addr) & RWStore.OFFSET_BITS_MASK) - 3; // bit adjust

			final int nbits = 32 * m_bitSize;

			final int block = offset/nbits;
			
			/**
			 * When a session is released any m_sessionActive FixedAllocators
			 * should be atomically released.
			 * However, if any state allowed a call to free once the store
			 * is not session protected, this must NOT overwrite m_sessionActive
			 * if it is already set since a commit would reset the transient bits
			 * without first clearing addresses them from the writeCacheService
			 */
			final boolean tmp = m_sessionActive;
			m_sessionActive = tmp || m_store.isSessionProtected();
			if (tmp && !m_sessionActive) throw new AssertionError();
			
			try {
				assert checkBits();
				if (((AllocBlock) m_allocBlocks.get(block))
						.freeBit(offset % nbits, m_sessionActive && !overideSession)) { // bit adjust
					
					if (m_contextThread != null && m_contextThread != Thread.currentThread())
						throw new IllegalStateException("Check thread context");
					
					m_freeBits++;

					checkFreeList();

				} else {
					if (m_sessionActive) {
						m_freeTransients++;
						boolean assertsEnabled = false;
						assert assertsEnabled = true;
						if (assertsEnabled){
							final int sessionFrees = m_sessionFrees.incrementAndGet();
							int sessionBits = 0;
							for (AllocBlock ab : m_allocBlocks) {
								sessionBits += ab.sessionBits();
							}
							assert sessionFrees <= sessionBits : "sessionFrees: " + sessionFrees + " > sessionBits: " + sessionBits;	
						}
					} else {
						m_freeTransients++;
					}						
				}				
				
				if (m_statsBucket != null) {
					m_statsBucket.delete(size);
				}
			} catch (IllegalArgumentException iae) {
				// catch and rethrow with more information
				throw new IllegalArgumentException("IAE with address: " + addr + ", size: " + size + ", context: " + (m_context == null ? -1 : m_context.hashCode()), iae);
			}

			assert checkBits();

			return true;
		} else if (addr >= m_startAddr && addr < m_endAddr) {

			final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
			while (iter.hasNext()) {
				final AllocBlock block = iter.next();
				if (block.free(addr, m_size)) {
					m_freeTransients++;

					assert checkBits();

					return true;
				}
			}
		}
		
		assert checkBits();

		return false;
	}

	private void checkFreeList() {
		if (m_freeWaiting && !m_pendingContextCommit) {
			if (m_freeBits > 0 && this instanceof DirectFixedAllocator) {
				m_freeWaiting = false;
				m_freeList.add(0, this);
			} else if (m_freeBits >= m_store.cDefaultFreeBitsThreshold) {
				m_freeWaiting = false;
				
				if (log.isDebugEnabled())
					log.debug("Returning Allocator to FreeList - " + m_size);
				
				m_freeList.add(this);
			}
		}
	}

	/**
	 * The introduction of IAllocationContexts has added some complexity to
	 * the older concept of a free list.  With AllocationContexts it is
	 * possibly for allocator to have free space available but this being
	 * restricted to a specific AllocaitonContext.  The RWStore alloc method
	 * must therefore handle the 
	 */
	public int alloc(final RWStore store, final int size, final IAllocationContext context) {
		try {
        if (size <= 0)
            throw new IllegalArgumentException(
                    "Allocate requires positive size, got: " + size);

        if (size > m_size)
            throw new IllegalArgumentException(
                    "FixedAllocator with slots of " + m_size 
                    + " bytes requested allocation for "+ size + " bytes");

	    int addr = -1;

		final Iterator<AllocBlock> iter = m_allocBlocks.iterator();
		int count = -1;
		while (addr == -1 && iter.hasNext()) {
			count++;

			final AllocBlock block = iter.next();
			if (block.m_addr == 0) {
				int blockSize = 32 * m_bitSize;
				if (m_statsBucket != null) {
					m_statsBucket.addSlots(blockSize);
				}
				blockSize *= m_size;
				blockSize >>= RWStore.ALLOCATION_SCALEUP;

				block.m_addr = grabAllocation(store, blockSize);
				if (log.isDebugEnabled())
					log.debug("Allocation block at " + block.m_addr + " of " + (blockSize << 16) + " bytes");

				if (m_startAddr == 0) {
					m_startAddr = block.m_addr;
				}
				m_endAddr = block.m_addr - blockSize;
			}
			addr = block.alloc(m_size);
		}

		if (addr != -1) {

			addr += 3; // Tweak to ensure non-zero address for offset 0
			
	    	if (--m_freeBits == 0) {
	    		if (log.isTraceEnabled())
	    			log.trace("Remove from free list");
				m_freeList.remove(this);
				m_freeWaiting = true;

				// Should have been first on list, now check for first
				if (m_freeList.size() > 0) {
                    if (log.isDebugEnabled()) {
    					final FixedAllocator nxt = (FixedAllocator) m_freeList.get(0);
                        log.debug("Freelist head: " + nxt.getSummaryStats());
                    }
				}
			}

			addr += (count * 32 * m_bitSize);

			final int value = -((m_index << RWStore.OFFSET_BITS) + addr);
			
			if (m_statsBucket != null) {
				m_statsBucket.allocate(size);
			}
			
			return value;
		} else {
 			StringBuilder sb = new StringBuilder();
			sb.append("FixedAllocator returning null address, with freeBits: " + m_freeBits + "\n");
		
    		for (AllocBlock ab: m_allocBlocks) {
    			sb.append(ab.show() + "\n");
    		}
    		
    		log.error(sb);

			return 0;
		}
		} finally {
			assert checkBits();
		}
	}

	protected int grabAllocation(RWStore store, int blockSize) {
		return store.allocBlock(blockSize);
	}

	public boolean hasFree() {
		return m_freeBits > 0;
	}

	public void addAddresses(ArrayList addrs) {
		
		final Iterator blocks = m_allocBlocks.iterator();

		// FIXME int baseAddr = -((m_index << 16) + 4); // bit adjust
		int baseAddr = -(m_index << 16); // bit adjust??

		while (blocks.hasNext()) {
		    final AllocBlock block = (AllocBlock) blocks.next();

			block.addAddresses(addrs, baseAddr);

			baseAddr -= 32 * m_bitSize;
		}
	}

	/**
	 * returns the raw start address
	 */
	public int getRawStartAddr() {
		return m_startAddr;
	}

	public int getIndex() {
		return m_index;
	}

    public void appendShortStats(final StringBuilder str,
            final AllocationStats[] stats) {

		int si = -1;

		if (stats == null) {
			str.append("Index: " + m_index + ", " + m_size);
		} else {		
			for (int i = 0; i < stats.length; i++) {
				if (m_size == stats[i].m_blockSize) {
					si = i;
					break;
				}
			}
		}
		
		final Iterator<AllocBlock> blocks = m_allocBlocks.iterator();
		while (blocks.hasNext()) {
			final AllocBlock block = blocks.next();
			if (block.m_addr != 0) {
				str.append(block.getStats(si == -1 ? null : stats[si]));
			} else {
				break;
			}
		}
		str.append("\n");
	}
	
	public int getAllocatedBlocks() {
		int allocated = 0;
		final Iterator<AllocBlock> blocks = m_allocBlocks.iterator();
		while (blocks.hasNext()) {
			if (blocks.next().m_addr != 0) {
				allocated++;
			} else {
				break;
			}
		}

		return allocated;
	}
	
	/**
	 * @return  the amount of heap storage assigned to this allocator over
	 * all reserved allocation blocks.
	 */
	public long getFileStorage() {
		
	    final long blockSize = 32L * m_bitSize * m_size;
		
		long allocated = getAllocatedBlocks();

		allocated *= blockSize;

		return allocated;
	}
	
	/**
	 * Computes the amount of storage allocated using the freeBits count.
	 * 
	 * @return the amount of storage to alloted slots in the allocation blocks
	 */
	public long getAllocatedSlots() {
		final int allocBlocks = getAllocatedBlocks();
		int xtraFree = m_allocBlocks.size() - allocBlocks;
		xtraFree *= 32 * m_bitSize;
		
		final int freeBits = m_freeBits - xtraFree;
		
		final long alloted = (allocBlocks * 32 * m_bitSize) - freeBits;
		
		return alloted * m_size;		
	}

	public boolean isAllocated(int offset) {
	  	offset -= 3;

	  	final int allocBlockRange = 32 * m_bitSize;

		final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
		
		final int bit = offset % allocBlockRange;
		
		return RWStore.tstBit(block.m_live, bit);
	}

	public boolean isCommitted(int offset) {
	  	offset -= 3;

	  	final int allocBlockRange = 32 * m_bitSize;

		final AllocBlock block = (AllocBlock) m_allocBlocks.get(offset / allocBlockRange);
		
		final int bit = offset % allocBlockRange;
		
		return RWStore.tstBit(block.m_commit, bit);
	}

	/**
	 * If the context is this allocators context AND it is not in the commit bits
	 * then we can immediately free.
	 */
    public boolean canImmediatelyFree(final int addr, final int size,
            final IAllocationContext context) {
		if (context == m_context && !m_pendingContextCommit) {
			final int offset = ((-addr) & RWStore.OFFSET_BITS_MASK); // bit adjust

			final boolean ret = !isCommitted(offset);

			return ret;
		} else {
			return false;
		}
	}
    
	public void setBucketStats(Bucket b) {
		m_statsBucket = b;
	}

	void releaseSession(RWWriteCacheService cache) {
		if (m_context != null) {
			throw new IllegalStateException("Calling releaseSession on shadowed allocator");
		}
		
		if (this.m_sessionActive) {
			final int start = m_sessionFrees.intValue();
			// try {
				if (log.isTraceEnabled())
					log.trace("Allocator: #" + m_index + " releasing session protection");
				

				int releasedAllocations = 0;
				for (AllocBlock ab : m_allocBlocks) {
					releasedAllocations += ab.releaseSession(cache);
				}
				
				assert !m_store.isSessionProtected() : "releaseSession called with isSessionProtected: true";
				
				m_sessionActive = false; // should only need indicate that it contains no cached writes
	
				
				m_freeBits = freebits();
				final int freebits = freebits();
				if (m_freeBits > freebits)
					log.error("m_freeBits too high: " + m_freeBits + " > (calc): " + freebits);
				
				m_freeTransients = transientbits();
				
				checkFreeList();
				
				// assert m_sessionFrees == releasedAllocations : "Allocator: " + hashCode() + " m_sessionFrees: " + m_sessionFrees + " != released: " + releasedAllocations;
				if (start > releasedAllocations) {
					log.error("BAD! Allocator: " + hashCode() + ", size: " + m_size + " m_sessionFrees: " + m_sessionFrees.intValue() + " > released: " + releasedAllocations);
				} else {
					// log.error("GOOD! Allocator: " + hashCode() + ", size: " + m_size + " m_sessionFrees: " + m_sessionFrees.intValue() + " <= released: " + releasedAllocations);
				}
			// } finally {
				final int end = m_sessionFrees.getAndSet(0);
				assert start == end : "SessionFrees concurrent modification: " + start + " != " + end;
			// }
		} else {
			assert m_sessionFrees.intValue() == 0 : "Session Inactive with sessionFrees: " + m_sessionFrees.intValue();
		}
	}

	private int freebits() {
		int freeBits = 0;
		for (AllocBlock ab : m_allocBlocks) {
			freeBits += ab.freeBits();
		}

		return freeBits;
	}

	private int transientbits() {
		int freeBits = 0;
		for (AllocBlock ab : m_allocBlocks) {
			freeBits += ab.transientBits();
		}

		return freeBits;
	}
}
