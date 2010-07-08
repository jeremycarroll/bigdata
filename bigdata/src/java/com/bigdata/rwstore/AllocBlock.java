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

import java.util.ArrayList;

public class AllocBlock {
  int m_addr;
  int m_ints;
  int m_commit[];
  int m_bits[];
  int m_transients[];
  RWWriteCacheService m_writeCache;

  AllocBlock(int addr, int bitSize, RWWriteCacheService cache) {
  	m_writeCache = cache;
    m_ints = bitSize;
    m_commit = new int[bitSize];
    m_bits = new int[bitSize];
    m_transients = new int[bitSize];
  }

  public boolean verify(int addr, int size) {
    if (addr < m_addr || addr >= (m_addr + (size * 32 * m_ints))) {
      return false;
    }

		// Now check to see if it allocated
    int bit = (addr - m_addr) / size;

    return RWStore.tstBit(m_bits, bit);
  }

  public boolean addressInRange(int addr, int size) {
    return (addr >= m_addr && addr <= (m_addr + (size * 32 * m_ints)));
  }
  	
  public boolean free(int addr, int size) {
    if (addr < m_addr || addr >= (m_addr + (size * 32 * m_ints))) {
      return false;
    }

    freeBit((addr - m_addr) / size, addr);

    return true;
  }

  public boolean freeBit(int bit, long addr) {
    // Allocation optimisation - if bit NOT set in committed memory then clear
    //  the transient bit to permit reallocation within this transaction.
    //
    // Note that with buffered IO there is also an opportunity to avoid output to
    //  the file by removing any pending write to the now freed address.  On large
    //  transaction scopes this may be significant.
    RWStore.clrBit(m_bits, bit);
    
    if (!RWStore.tstBit(m_commit, bit)) {
      m_writeCache.clearWrite(addr);

      RWStore.clrBit(m_transients, bit);
      
      return true;
    } else {
    	return false;
    }
  }

  public int alloc(int size) {
    if (size < 0) {
      throw new Error("Storage allocation error : negative size passed");
    }

    int bit = RWStore.fndBit(m_transients, m_ints);

    if (bit != -1) {
      RWStore.setBit(m_bits, bit);
      RWStore.setBit(m_transients, bit);

      return bit;
    } else {
      return -1;
    }
  }

  public boolean hasFree() {
    for (int i = 0; i < m_ints; i++) {
      if (m_bits[i] != 0xFFFFFFFF) {
        return true;
      }
    }

    return false;
  }

	public int getAllocBits() {
    int total = m_ints * 32;
    int allocBits = 0;
    for (int i = 0; i < total; i++) {
      if (RWStore.tstBit(m_bits, i)) {
        allocBits++;
      }
    }
    
    return allocBits;
	}

  public String getStats() {
    int total = m_ints * 32;
    int allocBits = getAllocBits();

    return "Addr : " + m_addr + " [" + allocBits + "::" + total + "]";
  }

  public void addAddresses(ArrayList addrs, int rootAddr) {
    int total = m_ints * 32;
    
    for (int i = 0; i < total; i++) {
      if (RWStore.tstBit(m_bits, i)) {
        addrs.add(new Integer(rootAddr - i));
      }
    }
  }
}
