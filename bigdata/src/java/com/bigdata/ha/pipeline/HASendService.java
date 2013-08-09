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

package com.bigdata.ha.pipeline;

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
 * <p>
 * Note: This class exposes its synchronization mechanism to
 * {@link HAReceiveService}.
 * 
 * @see HAReceiveService
 * 
 * @author Martyn Cutcher
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HASendService {
	
    private static final Logger log = Logger.getLogger(HASendService.class);

//    static final byte ACK = 1;
//    static final byte NACK = 0;
    
    /**
     * The Internet socket address of the receiving service.
     */
    private final AtomicReference<InetSocketAddress> addrNext = new AtomicReference<InetSocketAddress>();

    /**
     * A single threaded executor on which {@link SendTask}s will be executed.
     */
    private final AtomicReference<ExecutorService> executorRef = new AtomicReference<ExecutorService>();

    /**
     * The {@link SocketChannel} for communicating with the downstream
     * {@link HAReceiveService}.
     */
	final private AtomicReference<SocketChannel> socketChannel = new AtomicReference<SocketChannel>();

	/*
	 * Note: toString() must be thread-safe.
	 */
    public String toString() {

        return super.toString() + "{addrNext=" + addrNext + "}";
        
    }

    /**
     * Return the current address to which this service will send data.
     * 
     * @return The current address -or- <code>null</code> if no address is set
     *         at this time.
     * 
     * @see #start(InetSocketAddress)
     */
    public InetSocketAddress getAddrNext() {
        
        return addrNext.get();
        
    }
    
    
    /**
     * Designated constructor (flyweight initialization).
     * 
     * @see #start(InetSocketAddress)
     */
    public HASendService() {
        
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
     * Return <code>true</code> iff running at the moment this method is
     * evaluated.
     */
    boolean isRunning() {

        return executorRef.get() != null;

    }

//    /**
//     * Return the address of the receiving service (may be <code>null</code>).
//     */
//    InetSocketAddress getAddrNext() {
//
//        return addr.get();
//
//    }
    
    /**
     * Starts a thread which will transfer data to a service listening at the
     * specified {@link InetSocketAddress}. A {@link SocketChannel} will be
     * opened to the specified the connection to the socket specified in the
     * constructor and start the thread pool on which the payloads will be send.
     * <p>
     * Note: This class exposes its synchronization mechanism to
     * {@link HAReceiveService}.
     * 
     * @param addrNext
     *            The Internet socket address of the downstream service.
     * 
     * @see #terminate()
     * 
     * @throws IllegalArgumentException
     *             if the address is <code>null</code>.
     * @throws IllegalStateException
     *             if this service is already running.
     */
    synchronized public void start(final InetSocketAddress addrNext) {

        if (log.isDebugEnabled())
            log.debug(toString() + " : starting.");

        if (addrNext == null)
            throw new IllegalArgumentException();

        if (executorRef.get() != null) {

            // already running.
            log.error("Already running.");

            throw new IllegalStateException("Already running.");

        }

        this.addrNext.set(addrNext);

        /*
         * Note: leave null until send() so we can lazily connect to the
         * downstream service.
         */
        this.socketChannel.set(null);// openChannel(addr)

        this.executorRef.set(Executors.newSingleThreadExecutor());

        if (log.isInfoEnabled())
            log.info(toString() + " : running.");

    }

    /**
     * Immediate shutdown. Any transfer in process will be interrupted. It is
     * safe to invoke this method whether or not the service is running.
     * <p>
     * Note: This class exposes its synchronization mechanism to
     * {@link HAReceiveService}.
     */
    synchronized public void terminate() {
        if (log.isDebugEnabled())
            log.debug(toString() + " : stopping.");
        final ExecutorService tmp = executorRef.getAndSet(null);
        if (tmp == null) {
            // Not running.
            if (log.isInfoEnabled())
                log.info("Service was not running.");
            return;
        }
        try {
            final SocketChannel socketChannel = this.socketChannel.get();
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException ex) {
                    log.error("Ignoring exception during close: " + ex, ex);
                } finally {
                    this.socketChannel.set(null);
                }
            }
        } finally {
            // shutdown executor.
            tmp.shutdownNow();
            // clear address.
            addrNext.set(null);
            if (log.isInfoEnabled())
                log.info(toString() + " : stopped.");
        }
    }

    /**
     * Send the bytes {@link ByteBuffer#remaining()} in the buffer to the
     * configured {@link InetSocketAddress}.
     * <p>
     * Note: This operation DOES NOT have a side effect on the position, limit
     * or mark for the buffer.
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
     * 
     * @todo throws IOException if the {@link SocketChannel} was not open and
     *       could not be opened.
     */
	public Future<Void> send(final ByteBuffer buffer) {
     
	    if (buffer == null)
            throw new IllegalArgumentException();

        if (buffer.remaining() == 0)
            throw new IllegalArgumentException();
	 
        // Note: wrapped as a read-only buffer to prevent side-effects.
        final ExecutorService tmp = executorRef.get();
        
        if (tmp == null)
            throw new IllegalStateException("Service is not running.");

        if (log.isTraceEnabled())
            log.trace("Will send " + buffer.remaining() + " bytes");

//        reopenChannel();
        
        return tmp.submit(newIncSendTask(buffer.asReadOnlyBuffer()));

	}

    /**
     * A series of timeouts used when we need to re-open the
     * {@link SocketChannel}.
     */
    private final static long[] retryMillis = new long[] { 1, 5, 10, 50, 100, 250, 500 };

    /**
     * (Re-)open the {@link SocketChannel} if it is closed and this service is
     * still running.
     * 
     * @return The {@link SocketChannel}.
     */
	private SocketChannel reopenChannel() {

	    /*
         * Synchronize on the socketChannel object to serialize attempts to open
         * the SocketChannel.
         */
        synchronized (socketChannel) {

            int tryno = 0;

            SocketChannel sc = null;
            
            while ((((sc = socketChannel.get()) == null) || !sc.isOpen())
                    && isRunning()) {

                try {

                    /*
                     * (Re-)open the SocketChannel.
                     * 
                     * TODO we may have to retry or play with the timeout for
                     * the socket connect request since the downstream node may
                     * see its pipelineAdd() after the upstream node sees its
                     * pipelineChange() event. For example, given a pipeline
                     * [A], when service B joins the pipeline using
                     * [B.getActor().pipelineAdd()] the following are possible
                     * sequences in which the events could be delivered to A and
                     * B.
                     * 
                     * Option 1:
                     * 
                     * B.pipelineAdd(); A.pipelineChange(null,B);
                     * 
                     * Option 2:
                     * 
                     * A.pipelineChange(null,B); B.pipelineAdd();
                     * 
                     * In option (1), we should be able to connect immediately
                     * since B will have already setup its receive service.
                     * However, in option (2), we can not connect immediately
                     * since B does not setup its receive service until after A
                     * has seen the pipelineChange() event.
                     */

                    socketChannel.set(sc = openChannel(addrNext.get()));

                    if (log.isTraceEnabled())
                    	log.trace("Opened channel on try: " + tryno);

                } catch (IOException e) {

                    if (log.isInfoEnabled())
                    	log.info("Failed to open channel on try: " + tryno);

                    if (tryno < retryMillis.length) {

                        try {
                            // sleep and retry.
                            Thread.sleep(retryMillis[tryno]);
                            tryno++;
                            continue;
                        } catch (InterruptedException e1) {
                            // rethrow original exception.
                            throw new RuntimeException(e);
                        }

                    }

                    // do not wrap.
                    throw new RuntimeException(e);

                } // catch

            } // while

            return socketChannel.get();
            
        } // synchronized(socketChannel)
        
	} // reopenChannel()
	
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

        return new IncSendTask(buffer);
         
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
    static private SocketChannel openChannel(final InetSocketAddress addr)
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
    protected /*static*/ class IncSendTask implements Callable<Void> {

//        private final SocketChannel socketChannel;
        private final ByteBuffer data;

        public IncSendTask(/*final SocketChannel socketChannel, */final ByteBuffer data) {

//            if (socketChannel == null)
//                throw new IllegalArgumentException();

            if (data == null)
                throw new IllegalArgumentException();

//            this.socketChannel = socketChannel;
            
            this.data = data;

        }

        public Void call() throws Exception {

            // defer until we actually run.
            final SocketChannel socketChannel = reopenChannel();

            if (!isRunning())
                throw new RuntimeException("Not Running.");

            if (socketChannel == null)
                throw new AssertionError();
            
            // The #of bytes to transfer.
            final int remaining = data.remaining();

            if (log.isTraceEnabled())
                log.trace("Will send " + remaining + " bytes");

            try {

                int nwritten = 0;
                
                while (nwritten < remaining) {

                    /*
                     * Write the data. Depending on the channel, will either
                     * block or write as many bytes as can be written
                     * immediately (this latter is true for socket channels in a
                     * non-blocking mode). IF it blocks, should block until
                     * finished or until this thread is interrupted, e.g., by
                     * shutting down the thread pool on which it is running.
                     * 
                     * Note: If the SocketChannel is closed by an interrupt,
                     * then the send request for the [data] payload will fail.
                     * However, the SocketChannel will be automatically reopened
                     * for the next request (unless the HASendService has been
                     * terminated).
                     * 
                     * Note: socketChannel.write() returns as soon as the socket
                     * on the remote end point has locally buffered the data.
                     * This is *before* the Selector.select() method returns
                     * control to the application. Thus, the write() method here
                     * can succeed if the payload is transmitted in a single
                     * socket buffer exchange and the send() Future will report
                     * success even through the application code on the receiver
                     * could fail once it gets control back from select(). This
                     * twist can be a bit suprising. Therefore it is useful to
                     * write tests with both small payloads (the data transfer
                     * will succeed at the socket level even if the application
                     * logic then fails the transfer) and for large payloads.
                     * The concept of "large" depends on the size of the socket
                     * buffer.
                     */

                    final int nbytes = socketChannel.write(data);

                    nwritten += nbytes;

                    if (log.isTraceEnabled())
                        log.trace("Sent " + nbytes + " bytes with " + nwritten
                                + " of out " + remaining + " written so far");

                }
                
                /*
                 * The ACK by the receiver divides the HASend requests into
                 * distinct operations. Without this handshaking, the next
                 * available payload would be on the way as soon as the last
                 * byte of the current payload was written.
                 */
//                awaitAck(socketChannel);

            } finally {

                // do not close the socket, leave to explicit closeIncSend
                // socketChannel.close();

            }

            if (log.isTraceEnabled())
                log.trace("Sent total of " + remaining + " bytes");

            // check all data written
            if (data.remaining() != 0)
                throw new IOException("Did not write all data: expected="
                        + remaining + ", actual=" + data.remaining());

            return null;

        }

//        /**
//         * 
//         * @param socketChannel
//         * @throws IOException
//         */
//        private void awaitAck(final SocketChannel socketChannel)
//                throws IOException {
//
//            log.debug("Awaiting (N)ACK");
//
//            // FIXME Optimize.
//            final ByteBuffer b = ByteBuffer.wrap(new byte[] { -1 });
//
//            while (socketChannel.isOpen()) {
//
//                final int nread = socketChannel.read(b);
//
//                if (nread == 1) {
//
//                    final byte ret = b.array()[0];
//
//                    if (ret == ACK) {
//
//                        // Received ACK.
//                        log.debug("ACK");
//                        return;
//
//                    }
//
//                    log.error("NACK");
//                    return;
//
//                }
//
//                throw new IOException("Expecting ACK, not " + nread + " bytes");
//
//            }
//            
//            // channel is closed.
//            throw new AsynchronousCloseException();
//            
////            /*
////             * We should now have parameters ready in the WriteMessage and can
////             * begin transferring data from the stream to the writeCache.
////             */
////            final long begin = System.currentTimeMillis();
////            long mark = begin;
////
////            // #of bytes remaining (to be received).
////            int rem = b.remaining();
////
////            // End of stream flag.
////            boolean EOS = false;
////            
////            // for debug retain number of low level reads
////            int reads = 0;
////            
////            while (rem > 0 && !EOS) {
////
////                // block up to the timeout.
////                final int nkeys = client.clientSelector.select(10000/* ms */);
////            
////                if (nkeys == 0) {
////                
////                    /*
////                     * Nothing available.
////                     */
////                    
////                    // time since last mark.
////                    final long now = System.currentTimeMillis();
////                    final long elapsed = now - mark;
////                    
////                    if (elapsed > 10000) {
////                        // Issue warning if we have been blocked for a while.
////                        log.warn("Blocked: awaiting " + rem + " out of "
////                                + message.getSize() + " bytes.");
////                        mark = now;// reset mark.
////                    }
////
////                    if (!client.client.isOpen()
////                            || !client.clientSelector.isOpen()) {
////                        
////                        /*
////                         * The channel has been closed. The request must be
////                         * failed. TODO Or set EOF:=true? 
////                         * 
////                         * Note: The [callback] is NOT notified. The service
////                         * that issued the RMI request to this service to
////                         * receive the payload over the HAReceivedService will
////                         * see this exception thrown back across the RMI
////                         * request.
////                         * 
////                         * @see HAReceiveService.receiveData().
////                         */
////                        
////                        throw new AsynchronousCloseException();
////                        
////                    }
////                    
////                    // no keys. nothing to read.
////                    continue;
////                    
////                }
////
////                final Set<SelectionKey> keys = client.clientSelector
////                        .selectedKeys();
////                
////                final Iterator<SelectionKey> iter = keys.iterator();
////                
////                while (iter.hasNext()) {
////                
////                    iter.next();
////                    iter.remove();
////
////                    final int rdlen = client.client.read(b);
////
////                    if (log.isTraceEnabled())
////                        log.trace("Read " + rdlen + " bytes of "
////                                + (rdlen > 0 ? rem - rdlen : rem)
////                                + " bytes remaining.");
////
////                    if (rdlen > 0) {
////                        reads++;
////                    }
////
////                    if (rdlen == -1) {
////                        // The stream is closed?
////                        EOS = true;
////                        break;
////                    }
////
////                    rem -= rdlen;
////
////                }
////
////            } // while( rem > 0 )
//
//        }

    }

}
