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

package com.bigdata.journal.ha;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.io.TestCase3;

/**
 * Test the raw socket protocol implemented by HASendService and HAReceiveService.
 * 
 * @author martyn Cutcher
 *
 */
public class TestHASendAndReceive extends TestCase3 {

	/**
	 * A random number generated - the seed is NOT fixed.
	 */
	protected final Random r = new Random();

	/**
	 * Returns random data that will fit in N bytes. N is chosen randomly in
	 * 1:256.
	 * 
	 * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
	 *         random length and having random contents.
	 */
	public ByteBuffer getRandomData() {

		final int nbytes = r.nextInt(256) + 1;

		return getRandomData(nbytes);

	}

	/**
	 * Returns random data that will fit in <i>nbytes</i>.
	 * 
	 * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
	 *         having random contents.
	 */
	public ByteBuffer getRandomData(final int nbytes) {

		final byte[] bytes = new byte[nbytes];

		r.nextBytes(bytes);

		return ByteBuffer.wrap(bytes);

	}
	public TestHASendAndReceive() {

	}
	
	HASendService sendService;
	HAReceiveService receiveService;
	
	protected void setUp() throws Exception {
		InetSocketAddress addr = new InetSocketAddress(3000);
		
		receiveService = new HAReceiveService(addr, null);
		receiveService.start();
		
		// Thread.sleep(2000);
		
		sendService = new HASendService(addr);
		
	}
	
	public void testSimpleExchange() {
		ByteBuffer tst = getRandomData(50);
		sendService.send(tst);
		
		HAWriteMessage msg = new HAWriteMessage(50, 0);
		ByteBuffer rcv = ByteBuffer.allocate(2000);
		
		try {
			Future<Integer> fut = receiveService.receiveData(msg, rcv);
			
			int ret = fut.get();
			
			assertTrue("Unexpected value returned: " + ret, ret == 50);
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
