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
 * FIXME Write a stress test for this.
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
	
	private HASendService sendService;
	private HAReceiveService<HAWriteMessage> receiveService;
	
	protected void setUp() throws Exception {

	    final InetSocketAddress addr = new InetSocketAddress(3000);
		
		receiveService = new HAReceiveService<HAWriteMessage>(addr, null);
		receiveService.start();

        sendService = new HASendService(addr);

    }

    protected void tearDown() throws Exception {

        if (receiveService != null)
            receiveService.terminate();

        if (sendService != null)
            sendService.terminate();
	    
	}
	
    /**
     * Should we expect concurrency of the Socket send and RMI? It seems that we shold be able to handle it
     * whatever the logical argument.  The only constraint should be on the processing of each pair of
     * socket/RMI interactions.  OTOH, if we are intending to process the OP_ACCEPT and OP_READ within the 
     * ReadTask that can only be processed AFTER the RMI is received, then we should not sen the socket until we
     * have a returned FutureTask.
     */
	public void testSimpleExchange() {
		ByteBuffer tst1 = getRandomData(50);
		// sendService.send(tst1);
		
		HAWriteMessage msg1 = new HAWriteMessage(50, 0);
		HAWriteMessage msg2 = new HAWriteMessage(100, 0);
		ByteBuffer rcv = ByteBuffer.allocate(2000);
		ByteBuffer rcv2 = ByteBuffer.allocate(2000);
		
		try {
			rcv.limit(50);
			Future<Void> fut = receiveService.receiveData(msg1, rcv);			
			sendService.send(tst1);
			fut.get();			
			assertEquals(tst1, rcv);

			ByteBuffer tst2 = getRandomData(100);
			// sendService.send(tst2);

			rcv2.limit(100);
			fut = receiveService.receiveData(msg2, rcv2);			
			sendService.send(tst2);
			fut.get();			
			assertEquals(tst2, rcv2);

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Sends a large number of random buffers, confirming successful transmission.
	 */
	public void testStress() {
		
		try {
			for (int i = 0; i < 100; i++) {
				int sze = 10000 + r.nextInt(300000);
				HAWriteMessage msg = new HAWriteMessage(sze, 0);				
				ByteBuffer tst = getRandomData(sze);
				ByteBuffer rcv = ByteBuffer.allocate(sze);
				Future<Void> fut = receiveService.receiveData(msg, rcv); // FutureTask return ensures remote ready for Socket data	
				sendService.send(tst);
				fut.get(); // wait for task complete	
				assertEquals(tst, rcv); // make sure buffer has been transmitted
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
