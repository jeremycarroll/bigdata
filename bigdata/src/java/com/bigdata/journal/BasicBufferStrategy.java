package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.bigdata.journal.Journal.SlotHeader;

/**
 * Implements logic to read from and write on a buffer. This is sufficient
 * for a {@link BufferMode#Transient} implementation or a
 * {@link BufferMode#Mapped} implementation, but the
 * {@link BufferMode#Direct} implementation needs to also implement write
 * through to the disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BasicBufferStrategy extends AbstractBufferStrategy {

    /**
     * A direct buffer containing a write through image of the backing file.
     */
    final ByteBuffer directBuffer;
    
    /**
     * The current length of the backing file in bytes.
     */
    final long extent;

    public long getExtent() {
        
        return extent;
        
    }

    /**
     * The index of the first slot that MUST NOT be addressed (e.g., nslots).
     */
    final int slotLimit;

    public int getSlotLimit() {return slotLimit;}

    /**
     * Asserts that the slot index is in the legal range for the journal
     * <code>[0:slotLimit)</code>
     * 
     * @param slot The slot index.
     */
    
    void assertSlot( int slot ) {
        
        if( slot>=0 && slot<slotLimit ) return;
        
        throw new AssertionError("slot=" + slot + " is not in [0:"
                + slotLimit + ")");
        
    }

  BasicBufferStrategy(BufferMode bufferMode,SlotMath slotMath,ByteBuffer buffer) {
      
      super( bufferMode, slotMath );
      
      this.directBuffer = buffer;
      
      this.extent = buffer.capacity();

      /*
       * The first slot index that MUST NOT be addressed.
       * 
       * Note: The same computation occurs in DiskOnlyStrategy.
       */

      this.slotLimit = (int) (extent - SIZE_JOURNAL_HEADER) / slotSize;

      System.err.println("slotLimit=" + slotLimit);

  }

  public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data) {
      
      // Position the buffer on the current slot.
      final int pos = SIZE_JOURNAL_HEADER + slotSize * thisSlot;
      directBuffer.limit( pos + slotSize );
      directBuffer.position( pos );
      
      // Write the slot header.
      directBuffer.putInt(nextSlot); // nextSlot or -1 iff last
      directBuffer.putInt(priorSlot); // priorSlot or -size iff first

      // Write the slot data, advances data.position().
      directBuffer.put(data);

  }

  public ByteBuffer readFirstSlot(long id, int firstSlot, boolean readData,
          SlotHeader slotHeader) {

      assert slotHeader != null;
      
      final int pos = SIZE_JOURNAL_HEADER + slotSize * firstSlot;
      directBuffer.limit( pos + slotHeaderSize );
      directBuffer.position( pos );
      
      int nextSlot = directBuffer.getInt();
      final int size = -directBuffer.getInt();
      if( size <= 0 ) {
          
          dumpSlot( firstSlot, true );
          throw new RuntimeException("Journal is corrupt: id=" + id
                  + ", firstSlot=" + firstSlot + " reports size=" + size);
          
      }

      // Copy out the header fields.
      slotHeader.nextSlot = nextSlot;
      slotHeader.priorSlot = size;

      if( ! readData ) return null;
      
      // Allocate destination buffer to size.
      ByteBuffer dst = ByteBuffer.allocate(size);
      
      /*
       * We copy no more than the remaining bytes and no more than the data
       * available in the slot.
       */
      
      final int dataSize = slotDataSize;
      
      int thisCopy = (size > dataSize ? dataSize : size);
      
      // Set limit on source for copy.
      directBuffer.limit(directBuffer.position() + thisCopy);
      
      // Copy data from slot.
      dst.put(directBuffer);
      
      return dst;

  }

  public int readNextSlot(long id,int thisSlot,int priorSlot,int slotsRead,ByteBuffer dst ) {

      // Position the buffer on the current slot and set limit for copy.
      final int pos = SIZE_JOURNAL_HEADER + slotSize * thisSlot;
      directBuffer.limit( pos + slotHeaderSize );
      directBuffer.position( pos );
                  
      // read the header.
      final int nextSlot = directBuffer.getInt();
      final int priorSlot2 = directBuffer.getInt();
      if( priorSlot != priorSlot2 ) {
          
          dumpSlot( thisSlot, true );
          throw new RuntimeException("Journal is corrupt:  id=" + id
                  + ", slotsRead=" + slotsRead + ", slot=" + thisSlot
                  + ", expected priorSlot=" + priorSlot
                  + ", actual priorSlot=" + priorSlot2);

      }

      // Copy data from slot.
      if( dst != null ) {
          
          final int size = dst.capacity();
          
          final int remaining = size - dst.position();
          
          // #of bytes to read from this slot (header + data).
          final int thisCopy = (remaining > slotDataSize ? slotDataSize : remaining);

          directBuffer.limit( pos + slotHeaderSize + thisCopy );

          dst.put(directBuffer);
          
      }

      return nextSlot;
      
  }

  /**
   * Utility shows the contents of the slot on stderr.
   * 
   * @param slot
   *            The slot.
   * @param showData
   *            When true, the data in the slot will also be dumped.
   * 
   * @todo Abstract, remove dumpSlot, or make it impl specific (more code
   * duplication).  There are only two implementations required.  One for
   * the buffer modes and one for the disk only mode.
   */
  void dumpSlot(int slot, boolean showData) {
      
      System.err.println("slot="+slot);
      assertSlot( slot );
      
      ByteBuffer view = directBuffer.asReadOnlyBuffer();
      
      int pos = SIZE_JOURNAL_HEADER + slotSize * slot;

      view.limit( pos + slotSize );

      view.position( pos );

      int nextSlot = view.getInt();
      int priorSlot = view.getInt();

      System.err.println("nextSlot="
              + nextSlot
              + (nextSlot == -1 ? " (last slot)"
                      : (nextSlot < 0 ? "(error: negative slotId)"
                              : "(more slots)")));
      System.err.println(priorSlot<0?"size="+(-priorSlot):"priorSlot="+priorSlot);
      
      if( showData ) {
      
          byte[] data = new byte[slotDataSize];

          view.get(data);
          
          System.err.println(Arrays.toString(data));
          
      }
      
  }

}