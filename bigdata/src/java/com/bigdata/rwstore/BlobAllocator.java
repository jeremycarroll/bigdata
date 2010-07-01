package com.bigdata.rwstore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.bigdata.util.ChecksumUtility;

/**
 * BlobAllocator
 * 
 * Manages Blob allocations using a list of FixedAllocators.
 * 
 * The main advantage of this is for re-allocation, since the FixedAllocators can be
 * efficiently re-cycled where a fixed Blob creates issues of best fit and fragmentation.
 * 
 * Some simple patterns would cause un-reallocatable storage, consider a Blob that always
 * re-allocated to a larger size, or a pattern where several blobs got larger together, in these
 * scenarios, smaller allocations would never be re-used, whilst the mechanism of component
 * based allocation is easily re-used.
 * 
 * @author mgc
 *
 */
public class BlobAllocator implements Allocator {
	int[] m_hdrs = new int[254];
	RWStore m_store;
	private long m_diskAddr;
	private int m_index;
	private int m_sortAddr;
	
	public BlobAllocator(RWStore store, int sortAddr) {
		m_store = store;
		m_sortAddr = sortAddr;
	}
	
	public void addAddresses(ArrayList addrs) {
		// not relevant for BlobAllocators
	}

	public boolean addressInRange(int addr) {
		// not relevant for BlobAllocators
		return false;
	}

	public int alloc(RWStore store, int size) {
		assert size > RWStore.MAX_FIXED_ALLOC;
		
		return 0;
	}

	public boolean free(int addr, int sze) {
		int blcks = 1 + (sze / RWStore.MAX_FIXED_ALLOC);
		
		
		int hdr_idx = addr & RWStore.OFFSET_BITS_MASK;
		int hdr_addr = m_hdrs[hdr_idx];
		
		if (hdr_addr == 0) {
			return false;
		}
		
		// read in header block, then free each reference
		byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		m_store.getData(hdr_addr, hdr);
		
		try {
			DataInputStream instr = new DataInputStream(
					new ByteArrayInputStream(hdr, 0, hdr.length-4) );
			int allocs = instr.readInt();
			for (int i = 0; i < allocs; i++) {
				int nxt = instr.readInt();
				m_store.free(nxt, RWStore.MAX_FIXED_ALLOC);
			}
			m_store.free(hdr_addr, hdr.length);
			m_hdrs[hdr_idx] = 0;
			if (m_freeSpots++ == 0) {
				m_freeList.add(this);
			}
			
		} catch (IOException ioe) {
			
		}
		
		return false;
	}

	public int getBlockSize() {
		// Not relevant for Blobs
		return 0;
	}

	public long getDiskAddr() {
		return m_diskAddr;
	}

	/**
	 * returns physical address of blob header if any.
	 */
	public long getPhysicalAddress(int offset) {
		return m_store.physicalAddress(m_hdrs[offset]);
	}

	/**
	 * Since the Blob Allocator simply manages access to FixedAllocation blocks it does not manage any
	 * allocations directly.
	 */
	public int getPhysicalSize(int offset) {
		return 0;
	}

	/**
	 * The startAddr
	 */
	public long getStartAddr() {
		// not relevant for blob
		return RWStore.convertAddr(m_sortAddr);
	}

	public String getStats() {
		return null;
	}

	/**
	 * hasFree if there are any non-zero entries in the m_hdr array;
	 */
	public boolean hasFree() {
		return m_freeSpots > 0;
	}

	public void preserveSessionData() {
		// all data held by fixed allocators
	}

	int m_freeSpots = 254;
	
	/**
	 * FIXME: There is a symmetry problem with read/write where one takes a Stream and the other
	 * return a byte[].  This is problematical with using the checksums.
	 */
	public void read(DataInputStream str) {
		m_freeSpots = 0;
		try {
			for (int i = 0; i < 254; i++) {
				m_hdrs[i] = str.readInt();
				if (m_hdrs[i] == 0) m_freeSpots++;
			}
			int chk = str.readInt();
			// checksum int chk = ChecksumUtility.getCHK().checksum(buf, str.size());

		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
		
		if (m_freeSpots > 0) {
			m_freeList.add(this);
		}
	}

	public void setDiskAddr(long addr) {
		m_diskAddr = addr;
	}

	private ArrayList m_freeList;
	private long m_startAddr;

	public void setFreeList(ArrayList list) {
		m_freeList = list;

		if (hasFree()) {
			m_freeList.add(this);
		}
	}

	/**
	 * setIndex is called in two places, firstly to set the original index and secondly on restore
	 * from storage to re-establish the order.
	 * 
	 * When called initially, the m_startAddr will be zero and so must be set by retrieving the
	 * m_startAddr of the previous block (if any).  Now, since a Blob must use fixed allocations we
	 * are guaranteed that a BlobAllocator will not be the first allocator.  To derive a startAddr that
	 * can safely be used to sort a BlobAllocator against the previous (and subsequent) allocators we
	 * access the previous allocators address.
	 */
	public void setIndex(int index) {
		m_index = index;
	}

	public boolean verify(int addr) {
		// TODO Auto-generated method stub
		return false;
	}

	public byte[] write() {
		try {
			byte[] buf = new byte[1024];
			DataOutputStream str = new DataOutputStream(new FixedOutputStream(buf));
	
			str.writeInt(m_sortAddr);

			for (int i = 0; i < 254; i++) {
				str.writeInt(m_hdrs[i]);
			}
	
			// add checksum
			int chk = ChecksumUtility.getCHK().checksum(buf, str.size());
			str.writeInt(chk);
	
			return buf;
		} catch (IOException ioe) {
			throw new IllegalStateException(ioe);
		}
	}

	public int compareTo(Object o) {
		Allocator alloc = (Allocator) o;
		
		assert getStartAddr() != alloc.getStartAddr();
		
		return (getStartAddr() < alloc.getStartAddr()) ? -1 : 1;
	}

	public int register(int addr) {
		assert m_freeSpots > 0;
		
		m_store.addToCommit(this);
		
		for (int i = 0; i < 254; i++) {
			if (m_hdrs[i] ==  0) {
				m_hdrs[i] = addr;
				
				if (--m_freeSpots == 0) {
					m_freeList.remove(this);
				}
				
				return -((m_index << RWStore.OFFSET_BITS) + i);
			}
		}

		return 0;
	}

	public int getRawStartAddr() {
		return m_sortAddr;
	}

	public int getIndex() {
		return m_index;
	}

	public long getBlobHdrAddress(int hdrIndex) {
		return m_hdrs[hdrIndex];
	}

	public void appendShortStats(StringBuffer str) {
		str.append("Index: " + m_index + ", address: " + getStartAddr() + ", BLOB\n");
	}

}
