/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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

package com.bigdata.ha.pipeline;

import java.util.Random;

import junit.framework.TestCase;

/**
 * This is a test suite for the buffer framing idiom.
 * <p>
 * BufferFraming is required to ensure that pipeline buffers are correctly
 * identified by RMI messages.
 * <p>
 * There is currently a problem where receive tasks can be interrupted leaving
 * data in the pipeline and subsequent data reads are unable to process the
 * correct data.
 * <p>
 * A proposed solution is to prefix the buffer with an 8 byte identifier
 * suitably unique to stochastically avoid problems of random matching errors.
 * <p>
 * This test class tests finding the offset of the long value in otherwise
 * random data. Since it must read each byte this is complicated by the
 * requirement to window shift. This complexity can be somewhat simplified by
 * ensuring that each byte in the long key is unique.
 * <p>
 * Note: only the first byte in the key needs to be unique, guaranteeing that
 * if a match attempt fails it is only necessary to check the failing character
 * to see if that could be the start of a new match attempt.
 * 
 * @author Martyn Cutcher
 */
public class TestBufferFraming extends TestCase {

//	private static final Logger log = Logger
//			.getLogger(junit.framework.Test.class);

	boolean unique(final byte[] bytes) {
		for (int i = 0; i < bytes.length; i++) {
			final byte b = bytes[i];
			for (int t = i + 1; t < bytes.length; t++) {
				if (bytes[t] == b)
					return false;
			}
		}

		return true;
	}
	
	boolean unique1(final byte[] bytes) {
		final byte b = bytes[0];
		for (int t = 1; t < bytes.length; t++) {
			if (bytes[t] == b)
				return false;
		}

		return true;
	}

	/**
	 * Returns n bytes of unique values.
	 * 
	 * The unique values are important to simplify testing
	 * against data streams.
	 * 
	 * In fact the only important aspect is that the initial byte
	 * is unique!  This is sufficient to identify the start point
	 * of the key in a data stream.
	 */
	byte[] genKey(Random r, final int size) {
		final byte[] ret = new byte[size];

		while (!unique1(ret)) {
			r.nextBytes(ret);
		}

		return ret;
	}

	/**
	 * Functional test on performance of key generation
	 */
	public void testGenKey() {
		final Random r = new Random();

		final int keys = 100000000; // 100M

		final long start = System.currentTimeMillis();
		for (int i = 0; i < keys; i++) {
			genKey(r, 8);
		}
		final long end = System.currentTimeMillis();

		final long throughputms = (keys / (end - start));
		
		assertTrue(throughputms > 10000L); // should be able to produce more than 10M keys per second
	}
	
	/**
	 * Let's write a string into the middle of a load
	 * of random data and identify it with our generated key.
	 */
	public void testEmbeddedMessage() {
		doEmbeddedMessage();
	}
	
	public void testStressEmbeddedMessage() {
		for (int t = 0; t < 1000; t++) {
			doEmbeddedMessage();
		}
	}
	
	public void doEmbeddedMessage() {
		final Random r = new Random();
		final byte[] buffer = new byte[10000000]; // 10M bytes
		r.nextBytes(buffer);
		
		final String tst = "Hello World";
		final byte[] tstbytes = tst.getBytes();
		
		final byte[] key = genKey(r, 8);
		
		int offset = r.nextInt(9000000); // somewhere in first 9M bytes
		
		// copy string into buffer
		copy(key, 0, buffer, offset);
		copy(tstbytes, 0, buffer, offset+key.length);
		
		final int position = find(key, buffer);
		
		assertTrue(position == offset);
		
		final byte[] copy = new byte[tstbytes.length];
		copy(buffer, position+key.length, copy, 0);
		
		final String tstRead = new String(copy);
		
		assertTrue(tstRead.equals(tst));
	}
	
	void copy(byte[] src, int srcOffset, byte[] dst, int dstOffset) {
		int len = Math.min(src.length, dst.length);
		
		for (int i = 0; i < len; i++) {
			dst[dstOffset+i] = src[srcOffset+i];
		}
	}
	
	int find(final byte[] key, final byte[] buffer) {
		final int endPos = buffer.length - key.length;
		for (int i = 0; i < endPos; i++) {
			if (buffer[i] == key[0]) {
				boolean match = true;
				for (int t = 1; match && t < key.length; t++) {
					match = buffer[i+t] == key[t];
					
					if (!match) {
						i += t-1;
					}
				}
				if (match) {
					return i;
				}
			}
		}
		
		return -1;
	}
}
