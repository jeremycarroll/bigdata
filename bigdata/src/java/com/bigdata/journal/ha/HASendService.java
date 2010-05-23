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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

/**
 * A service for sending raw {@link ByteBuffer}s across a socket. This service
 * supports the HA write pipeline. This service is designed to be paired with an
 * {@link HAReceiveService}, which typically is running on a different host. The
 * {@link HASendService} provides only an efficient raw data transfer. The HA
 * write pipeline coordinates the transfer of data using RMI messages which tell
 * the receiver how much data to expect, the checksum of the data, etc.
 * 
 * <h2>Implementation</h2>
 * 
 * This class has a private single-threaded Executor to which it submits a
 * {@link SendTask}. The {@link SendTask} will open a blocking-mode
 * {@link SocketChannel} to the service at the configured
 * {@link InetSocketAddress} and send the bytes remaining in a
 * {@link ByteBuffer} to that service on that {@link SocketChannel}. The data
 * will be sent on the socket using
 * {@link WritableByteChannel#write(ByteBuffer)}, which is optimized for the NIO
 * transfer of direct {@link ByteBuffer}s. Since this is a blocking-mode
 * connection, the write operation will block until all bytes have been sent or
 * the current thread is interrupted, e.g., by cancelling its Future.
 * <p>
 * The {@link SendTask} verifies that all bytes were sent as a post-condition
 * (position() == limit). If there is any problem, then the
 * {@link SocketChannel} is closed and the original exception is thrown out of
 * {@link SendTask#call()}. If the socket is closed from the other end while we
 * are still writing data, then that exception gets thrown out as well. The
 * socket connection is closed as a post-condition (it does not survive across
 * multiple sends). Closing the socket connection after each success or
 * unsuccessful send gives us a strong indication of success or failure for the
 * data transfer which is independent of the RMI message and makes it trivial to
 * re-synchronize the {@link HASendService} since it is basically stateless.
 * 
 * @see HAReceiveService
 * 
 * @author Martyn Cutcher
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HASendService {
	
    protected static final Logger log = Logger.getLogger(HASendService.class);

    /**
     * The Internet socket address of the receiving service.
     */
    private final InetSocketAddress addr;

    /**
     * A single threaded executor on which {@link SendTask}s will be executed.
     * 
     * @see #start()
     */
    private final AtomicReference<ExecutorService> executorRef = new AtomicReference<ExecutorService>();

    /**
     * The {@link SocketChannel} for communicating with the downstream
     * {@link HAReceiveService}.
     * 
     * @todo The life cycle management for this socket channel should be
     *       reviewed. It is currently set by {@link #open()}, but that does not
     *       ensure safe publication or protect against "double-open". It is
     *       closed by {@link #terminate()}, which also tears down the
     *       {@link #executorRef} (which is created by the constructor). You can
     *       not close and reopen the connection to the client due to the
     *       asymmetry of these operations.
     */
	private SocketChannel incSocketChannel;

    public String toString() {

        return super.toString() + "{addr=" + addr + "}";
        
    }
	
    /**
     * Starts an {@link HASendService} which will transfer data to a service
     * listening at the specified {@link InetSocketAddress}.
     * 
     * @param addr The Internet socket address of the receiving service.
     */
    public HASendService(final InetSocketAddress addr) {
        
        if (addr == null)
            throw new IllegalArgumentException();
		
        this.addr = addr;
        
	}

    /**
     * Extended to ensure that the private executor service is always
     * terminated.
     */
    @Override
    protected void finalize() throws Throwable {
        
        terminate();
        
        super.finalize();
        
    }

    /**
     * Open the connection to the socket specified in the constructor and
     * start the thread pool on which the payloads will be send.
     */
    synchronized public void start() throws IOException {

        // already running?
        if (executorRef.get() != null)
            throw new IllegalStateException();
        
        if (log.isInfoEnabled())
            log.info(toString());

        executorRef.set(Executors.newSingleThreadExecutor());
        
        if (incSocketChannel == null)           
            incSocketChannel = openChannel(addr);

    }

    /**
     * Immediate shutdown. Any transfer in process will be interrupted.
     */
    synchronized public void terminate() {
        if (log.isInfoEnabled())
            log.info(toString());
        final ExecutorService tmp = executorRef.getAndSet(null);
        if (tmp == null) {
            // Not running.
            return;
        }
        try {
            if (incSocketChannel != null) {
                try {
                    incSocketChannel.close();
                } catch (IOException ex) {
                    log.error("Ignoring exception during close: " + ex, ex);
                } finally {
                    incSocketChannel = null;
                }
            }
        } finally {
            tmp.shutdownNow();
        }
    }

    /**
     * Send the bytes {@link ByteBuffer#remaining()} in the buffer to the
     * configured {@link InetSocketAddress}. This operation DOES NOT have a side
     * effect on the position, limit or mark for the buffer.
     * <p>
     * Note: In order to use efficient NIO operations this MUST be a direct
     * {@link ByteBuffer}.
     * 
     * @param buffer
     *            The buffer.
     * 
     * @return The {@link Future} which can be used to await the outcome of this
     *         operation.
     * 
     * @throws IllegalArgumentException
     *             if the buffer is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the buffer is empty (no bytes remaining).
     * @throws RejectedExecutionException
     *             if this service has been shutdown.
     */
	public Future<Void> send(final ByteBuffer buffer) {

        if (buffer == null)
            throw new IllegalArgumentException();

        if (buffer.remaining() == 0)
            throw new IllegalArgumentException();
	 
        // Note: wrapped as a read-only buffer to prevent side-effects.
        final ExecutorService tmp = executorRef.get();
        
        if (tmp == null)
            throw new IllegalStateException();

        if (log.isTraceEnabled())
            log.trace("Will send " + buffer.remaining() + " bytes");

        return tmp.submit(newIncSendTask(buffer.asReadOnlyBuffer()));

	}

    /**
     * Factory for the {@link SendTask}.
     * 
     * @param buffer
     *            The buffer whose data are to be sent.
     *            
     * @return The task which will send the data to the configured
     *         {@link InetSocketAddress}.
     */
    protected Callable<Void> newIncSendTask(final ByteBuffer buffer) {

         return new IncSendTask(incSocketChannel, buffer);
    }

    /**
     * Open a blocking mode socket channel to the specified socket address.
     * 
     * @param addr
     *            The socket address.
     * 
     * @return The socket channel.
     * 
     * @throws IOException
     */
    static protected SocketChannel openChannel(final InetSocketAddress addr)
            throws IOException {

        final SocketChannel socketChannel = SocketChannel.open();

        try {

            socketChannel.configureBlocking(true);

            if (log.isTraceEnabled())
                log.trace("Connecting to " + addr);

            socketChannel.connect(addr);

            socketChannel.finishConnect();

        } catch (IOException ex) {

            log.error(ex);
            
            throw ex;
            
        }

        return socketChannel;
        
    }
    
    /**
     * This task implements the raw data transfer. Each instance of this task
     * sends the {@link ByteBuffer#remaining()} bytes in a single
     * {@link ByteBuffer} to the receiving service on a specified
     * {@link InetSocketAddress}.
     */
    protected static class IncSendTask implements Callable<Void> {

        private final SocketChannel socketChannel;
        private final ByteBuffer data;

        public IncSendTask(final SocketChannel socketChannel, final ByteBuffer data) {

            if (socketChannel == null)
                throw new IllegalArgumentException();

            if (data == null)
                throw new IllegalArgumentException();

            this.socketChannel = socketChannel;
            
            this.data = data;

        }

        public Void call() throws Exception {

            // The #of bytes to transfer.
            final int remaining = data.remaining();

            try {

                /*
                 * Write the data -- should block until finished or until this
                 * thread is interrupted, e.g., by shutting down the thread pool
                 * on which it is running.
                 */
                socketChannel.write(data);

            } finally {

                // do no close the socket, leave to explicit closeIncSend
                // socketChannel.close();

            }

            if (log.isTraceEnabled())
                log.trace("Sent " + remaining + " bytes");

            // check all data written
            assert data.remaining() == 0 : "remaining=" + data.remaining();

            return null;

        }

    }

}
