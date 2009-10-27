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
 * Created on Jan 31, 2007
 */

package com.bigdata.rawstore;

import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.TestCase2;

/**
 * Base class for writing tests of the {@link IRawStore} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRawStoreTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractRawStoreTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRawStoreTestCase(String name) {
        super(name);
    }

    /**
     * Return a new store that will serve as the fixture for the test. A stable
     * store must remove the pre-existing store. A transient store should open a
     * new store every time.
     * 
     * @return The fixture for the test.
     */
    abstract protected IRawStore getStore();

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     * 
     * @todo optimize test helper when ByteBuffer is backed by an array, but
     *       also compensate for the arrayOffset.
     */
    static public void assertEquals(byte[] expected, ByteBuffer actual ) {

        if( expected == null ) throw new IllegalArgumentException();
        
        if( actual == null ) fail("actual is null");

        if(actual.hasArray() && actual.arrayOffset()==0) {
        
            assertEquals(expected,actual.array());
            
            return;
            
        }
        
        /* Create a read-only view on the buffer so that we do not mess with
         * its position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();
        
        final int len = actual.remaining();
        
        final byte[] actual2 = new byte[len];
        
        actual.get(actual2);

        assertEquals(expected,actual2);
        
    }

    /**
     * Test verifies correct rejection of a write operation when the caller
     * supplies an empty buffer (no bytes remaining).
     */
    public void test_write_correctRejection_emptyRecord() {
        
        IRawStore store = getStore();
        
        try {

            store.write( ByteBuffer.wrap(new byte[]{}));
            
            fail("Expecting: "+IllegalArgumentException.class);
                
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }   
        
        try {

            ByteBuffer buf = ByteBuffer.wrap(new byte[2]);
            
            // advance the position to the limit so that no bytes remain.
            buf.position(buf.limit());
            
            store.write( buf );
            
            fail("Expecting: "+IllegalArgumentException.class);
                
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }   

        store.destroy();
        
    }

    /**
     * Test verifies correct rejection of a write operation when the caller
     * supplies a [null] buffer.
     */
    public void test_write_correctRejection_null() {
        
        IRawStore store = getStore();
        
        try {

            store.write( null );
            
            fail("Expecting: "+IllegalArgumentException.class);
                
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }   
        
        store.destroy();

    }
    
    /**
     * A read with a 0L address is always an error.
     */
    public void test_read_correctRejection_0L() {
        
        IRawStore store = getStore();

        try {

            store.read( 0L );
            
            fail("Expecting: "+IllegalArgumentException.class);
                
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }   

        store.destroy();
            
    }
    
    /**
     * A delete with an address encoding a zero length component is an error
     * (the address is ill-formed since we do not allow writes of zero length
     * records).
     */
    public void test_read_correctRejection_zeroLength() {
        
        IRawStore store = getStore();

        try {

            final int nbytes = 0;
            
            final int offset = 10;
            
            store.read( store.toAddr(nbytes, offset) );
            
            fail("Expecting: "+IllegalArgumentException.class);
                
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }   
        
        store.destroy();

    }

    /**
     * A read with a well-formed address that was never written is an error.
     * 
     * @todo Support for detecting this is not present in the WORM store. We
     *       could detect an address beyond the end of the store, but that is
     *       about it. In contrast, a RW store using an indirection table to
     *       translate logical to physical addresses is able to "know" if an
     *       address is valid.
     */
    public void test_read_correctRejection_neverWritten() {
   
        // @todo this test disabled until a RW store is implemented.
        
//        IRawStore store = getStore();
//
//        try {
//
//            final int nbytes = 100;
//            
//            final int offset = 0;
//            
//            store.read( store.toAddr(nbytes, offset) );
//            
//            fail("Expecting: "+IllegalArgumentException.class);
//                
//        } catch(IllegalArgumentException ex) {
//            
//            System.err.println("Ignoring expected exception: "+ex);
//            
//        }   
//
//        store.destroy();

    }
    
//    
//    /**
//     * A delete with a 0L address is always an error.
//     */
//    public void test_delete_correctRejection_0L() {
//        
//        IRawStore store = getStore();
//
//        try {
//
//            store.delete( 0L );
//            
//            fail("Expecting: "+IllegalArgumentException.class);
//                
//        } catch(IllegalArgumentException ex) {
//            
//            System.err.println("Ignoring expected exception: "+ex);
//            
//        }   
//                
//    }
//    
//    /**
//     * A delete with an address encoding a zero length component is an error
//     * (the address is ill-formed since we do not allow writes of zero length
//     * records).
//     */
//    public void test_delete_correctRejection_zeroLength() {
//        
//        IRawStore store = getStore();
//
//        try {
//
//            final int nbytes = 0;
//            
//            final int offset = 10;
//            
//            store.delete( Addr.toLong(nbytes, offset));
//            
//            fail("Expecting: "+IllegalArgumentException.class);
//                
//        } catch(IllegalArgumentException ex) {
//            
//            System.err.println("Ignoring expected exception: "+ex);
//            
//        }   
//                
//    }
//    
//    /**
//     * A delete with a well-formed address that was never written is an error.
//     */
//    public void test_delete_correctRejection_neverWritten() {
//        
//        IRawStore store = getStore();
//
//        try {
//
//            final int nbytes = 100;
//            
//            final int offset = 0;
//            
//            store.delete( Addr.toLong(nbytes, offset) );
//            
//            fail("Expecting: "+IllegalArgumentException.class);
//                
//        } catch(IllegalArgumentException ex) {
//            
//            System.err.println("Ignoring expected exception: "+ex);
//            
//        }   
//                
//    }
    
    /**
     * Test verifies that we can write and then read back a record.
     */
    public void test_writeRead() {
        
        IRawStore store = getStore();
        
        Random r = new Random();
        
        final int len = 100;
        
        byte[] expected = new byte[len];
        
        r.nextBytes(expected);
        
        ByteBuffer tmp = ByteBuffer.wrap(expected);
        
        long addr1 = store.write(tmp);

        // verify that the position is advanced to the limit.
        assertEquals(len,tmp.position());
        assertEquals(tmp.position(),tmp.limit());

        // read the data back.
        ByteBuffer actual = store.read(addr1);
        
        assertEquals(expected,actual);
        
        /*
         * verify the position and limit after the read.
         */
        assertEquals(0,actual.position());
        assertEquals(expected.length,actual.limit());
        assertEquals(actual.limit(),actual.capacity());
        
        store.destroy();

    }

    /**
     * Test verifies that we can write and then read back a record twice.
     */
    public void test_writeReadRead() {
        
        IRawStore store = getStore();
        
        Random r = new Random();
        
        final int len = 100;
        
        byte[] expected = new byte[len];
        
        r.nextBytes(expected);
        
        ByteBuffer tmp = ByteBuffer.wrap(expected);
        
        long addr1 = store.write(tmp);

        // verify that the position is advanced to the limit.
        assertEquals(len, tmp.position());
        assertEquals(tmp.position(), tmp.limit());

        /*
         * 1st read.
         */
        {
            // read the data back.
            ByteBuffer actual = store.read(addr1);

            assertEquals(expected, actual);

            /*
             * verify the position and limit after the read.
             */
            assertEquals(0, actual.position());
            assertEquals(expected.length, actual.limit());
        }

        /*
         * 2nd read.
         */
        {
            // read the data back.
            ByteBuffer actual2 = store.read(addr1);

            assertEquals(expected, actual2);

            /*
             * verify the position and limit after the read.
             */
            assertEquals(0, actual2.position());
            assertEquals(expected.length, actual2.limit());
        }
    
        store.destroy();

    }

//    /**
//     * Test verifies read behavior when the offered buffer has exactly the
//     * required #of bytes of remaining.
//     */
//    public void test_writeReadWith2ndBuffer_exactCapacity() {
//        
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len);
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // verify the data are record correct.
//        assertEquals(expected1,actual);
//
//        /*
//         * the caller's buffer MUST be used since it has sufficient bytes
//         * remaining
//         */
//        assertTrue("Caller's buffer was not used.", actual==buf);
//
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len,actual.limit());
//
//    }
//    
//    public void test_writeReadWith2ndBuffer_excessCapacity_zeroPosition() {
//        
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len+1);
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // verify the data are record correct.
//        assertEquals(expected1,actual);
//
//        /*
//         * the caller's buffer MUST be used since it has sufficient bytes
//         * remaining
//         */
//        assertTrue("Caller's buffer was not used.", actual==buf);
//
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len,actual.limit());
//
//    }
//    
//    public void test_writeReadWith2ndBuffer_excessCapacity_nonZeroPosition() {
//        
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len+2);
//        buf.position(1); // advance the position by one byte.
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // copy the expected data leaving the first byte zero.
//        byte[] expected2 = new byte[len+1];
//        System.arraycopy(expected1, 0, expected2, 1, expected1.length);
//        
//        // verify the data are record correct.
//        assertEquals(expected2,actual);
//
//        /*
//         * the caller's buffer MUST be used since it has sufficient bytes
//         * remaining
//         */
//        assertTrue("Caller's buffer was not used.", actual==buf);
//
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len+1,actual.limit());
//
//    }
//    
//    /**
//     * Test verifies read behavior when the offered buffer does not have
//     * sufficient remaining capacity.
//     */
//    public void test_writeReadWith2ndBuffer_wouldUnderflow_nonZeroPosition() {
//    
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer that is large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len);
//        buf.position(1); // but advance the position so that there is not enough room.
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // verify the data are record correct.
//        assertEquals(expected1,actual);
//
//        /*
//         * the caller's buffer MUST NOT be used since it does not have
//         * sufficient bytes remaining.
//         */
//        assertFalse("Caller's buffer was used.", actual==buf);
//        
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len,actual.limit());
//        
//    }
//
//    /**
//     * Test verifies read behavior when the offered buffer does not have
//     * sufficient remaining capacity.
//     */
//    public void test_writeReadWith2ndBuffer_wouldUnderflow_zeroPosition() {
//    
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer that is not large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len-1);
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // verify the data are record correct.
//        assertEquals(expected1,actual);
//
//        /*
//         * the caller's buffer MUST NOT be used since it does not have
//         * sufficient bytes remaining.
//         */
//        assertFalse("Caller's buffer was used.", actual==buf);
//        
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len,actual.limit());
//        
//    }
//
//    /**
//     * Test verifies that an oversized buffer provided to
//     * {@link IRawStore#read(long, ByteBuffer)} will not cause more bytes to be
//     * read than are indicated by the {@link Addr address}.
//     */
//    public void test_writeReadWith2ndBuffer_wouldOverflow_zeroPosition() {
//    
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer that is more than large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len+1);
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // verify the data are record correct - only [len] bytes should be copied.
//        assertEquals(expected1,actual);
//
//        /*
//         * the caller's buffer MUST be used since it has sufficient bytes
//         * remaining.
//         */
//        assertTrue("Caller's buffer was used.", actual==buf);
//        
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len,actual.limit());
//        
//    }
//
//    /**
//     * Test verifies that an oversized buffer provided to
//     * {@link IRawStore#read(long, ByteBuffer)} will not cause more bytes to be
//     * read than are indicated by the {@link Addr address}.
//     */
//    public void test_writeReadWith2ndBuffer_wouldOverflow_nonZeroPosition() {
//    
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        // a buffer that is more than large enough to hold the record.
//        ByteBuffer buf = ByteBuffer.allocate(len+2);
//        
//        // non-zero position.
//        buf.position(1);
//
//        // read the data, offering our buffer.
//        ByteBuffer actual = store.read(addr1, buf);
//        
//        // copy the expected data leaving the first byte zero.
//        byte[] expected2 = new byte[len+1];
//        System.arraycopy(expected1, 0, expected2, 1, expected1.length);
//
//        // verify the data are record correct - only [len] bytes should be copied.
//        assertEquals(expected2,actual);
//
//        /*
//         * the caller's buffer MUST be used since it has sufficient bytes
//         * remaining.
//         */
//        assertTrue("Caller's buffer was used.", actual==buf);
//        
//        /*
//         * verify the position and limit after the read.
//         */
//        assertEquals(0,actual.position());
//        assertEquals(len+1,actual.limit());
//        
//    }
//
    /**
     * Test verifies that write does not permit changes to the store state by
     * modifying the supplied buffer after the write operation (i.e., a copy
     * is made of the data in the buffer).
     */
    public void test_writeImmutable() {

        IRawStore store = getStore();
        
        Random r = new Random();
        
        final int len = 100;
        
        byte[] expected1 = new byte[len];
        
        r.nextBytes(expected1);

        // write
        ByteBuffer tmp = ByteBuffer.wrap(expected1);
        
        long addr1 = store.write(tmp);

        // verify that the position is advanced to the limit.
        assertEquals(len,tmp.position());
        assertEquals(tmp.position(),tmp.limit());

        // verify read.
        assertEquals(expected1,store.read(addr1));

        // clone the data.
        byte[] expected2 = expected1.clone();
        
        // modify the original data.
        r.nextBytes(expected1);

        /*
         * verify read - this will fail if the original data was not copied by
         * the store.
         */
        assertEquals(expected2,store.read(addr1));

        store.destroy();

    }

    /**
     * Test verifies that read does not permit changes to the store state by
     * modifying the returned buffer.
     */
    public void test_readImmutable() {
       
        IRawStore store = getStore();
        
        Random r = new Random();
        
        final int len = 100;
        
        byte[] expected1 = new byte[len];
        
        r.nextBytes(expected1);
        
        ByteBuffer tmp = ByteBuffer.wrap(expected1);
        
        long addr1 = store.write(tmp);

        // verify that the position is advanced to the limit.
        assertEquals(len,tmp.position());
        assertEquals(tmp.position(),tmp.limit());

        ByteBuffer actual = store.read(addr1);
        
        assertEquals(expected1,actual);

        /*
         * If [actual] is not read-only then we modify [actual] and verify that
         * the state of the store is not changed.
         */
        if( ! actual.isReadOnly() ) {
            
            // overwrite [actual] with some random data.
            
            byte[] tmp2 = new byte[100];
            
            r.nextBytes(tmp2);
            
            actual.clear();
            actual.put(tmp2);
            actual.flip();

            // verify no change in store state.
            
            assertEquals(expected1,store.read(addr1));

        }
        
        store.destroy();

    }
    
    /**
     * Test writes a bunch of records and verifies that each can be read after
     * it is written.  The test then performs a random order read and verifies
     * that each of the records can be read correctly.
     */
    public void test_multipleWrites() {

        IRawStore store = getStore();

        Random r = new Random();

        /*
         * write a bunch of random records.
         */
        final int limit = 100;
        
        final long[] addrs = new long[limit];
        
        final byte[][] records = new byte[limit][];
        
        for(int i=0; i<limit; i++) {

            byte[] expected = new byte[r.nextInt(100) + 1];
        
            r.nextBytes(expected);
        
            ByteBuffer tmp = ByteBuffer.wrap(expected);
            
            long addr = store.write(tmp);

            // verify that the position is advanced to the limit.
            assertEquals(expected.length,tmp.position());
            assertEquals(tmp.position(),tmp.limit());

            assertEquals(expected,store.read(addr));
        
            addrs[i] = addr;
            
            records[i] = expected;
            
        }

        /*
         * now verify data with random reads.
         */

        int[] order = getRandomOrder(limit);
        
        for(int i=0; i<limit; i++) {
            
            long addr = addrs[order[i]];
            
            byte[] expected = records[order[i]];

            assertEquals(expected,store.read(addr));
            
        }
    
        store.destroy();

    }
    
//    /**
//     * Test verifies delete of a record and the behavior of read once the
//     * record has been deleted.
//     */
//    public void test_writeReadDeleteRead() {
//        
//        IRawStore store = getStore();
//        
//        Random r = new Random();
//        
//        final int len = 100;
//        
//        byte[] expected1 = new byte[len];
//        
//        r.nextBytes(expected1);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(expected1);
//        
//        long addr1 = store.write(tmp);
//
//        // verify that the position is advanced to the limit.
//        assertEquals(len,tmp.position());
//        assertEquals(tmp.position(),tmp.limit());
//
//        assertEquals(expected1,store.read(addr1, null));
//        
//        store.delete(addr1);
//
//        if (deleteInvalidatesAddress()) {
//
//            try {
//
//                store.read(addr1, null);
//
//                fail("Expecting: " + IllegalArgumentException.class);
//
//            } catch (IllegalArgumentException ex) {
//
//                System.err.println("Ignoring expected exception: " + ex);
//
//            }
//
//        } else {
//
//            store.read(addr1, null);
//
//        }
//        
//    }

    /**
     * Note: This will leave a test file around each time since we can
     * not really call closeAndDelete() when we are testing close().
     */
    public void test_close() {
        
        IRawStore store = getStore();
        
        assertTrue(store.isOpen());
        
        store.close();

        assertFalse(store.isOpen());
        
        try {

            store.close();
            
            fail("Expecting: "+IllegalStateException.class);
            
        } catch(IllegalStateException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
        }

    }

    /**
     * A random number generated - the seed is NOT fixed.
     */
    protected Random r = new Random();

    /**
     * Returns random data that will fit in N bytes. N is choosen randomly in
     * 1:1024.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
     *         random length and having random contents.
     */
    public ByteBuffer getRandomData() {
        
        final int nbytes = r.nextInt(1024) + 1;
        
        byte[] bytes = new byte[nbytes];
        
        r.nextBytes(bytes);
        
        return ByteBuffer.wrap(bytes);
        
    }
    
}
