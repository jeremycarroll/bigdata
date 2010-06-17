/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
package com.bigdata.boot;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.util.logging.Level;

class BaseAgent {

    protected final Logger logger;

    /** Wrapper for sending/receiving messages to the boot launcher. */
    private final MessageTransport messageTransport;

    /**
     * Reference to the background thread receiving messages from the
     * boot launcher.
     */
    private final Thread messageRxThread;

    /**
     * Queue of threads that are waiting for a result from the boot
     * launcher. Each entry contains a field where the result will be
     * stored as well as a method that will block until the result
     * is received.
     */
    private final Queue<LaunchResult> resultWaitQueue =
                                          new LinkedList<LaunchResult>();

    /**
     * Protected constructor used by subclasses to initialize this class.
     *
     * @param logger The logger to use within this utility; must not be
     *               <code>null</code>.
     *
     * @param port   The port on which the boot launcher is listening for
     *               control connections.
     *
     * @throws IOException if a connection to the boot launcher could not
     *         be established.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>logger</code> parameter.
     */
    protected BaseAgent(Logger logger, int port) throws IOException {
        this.logger = logger;
        logger.log(Level.FINE,"connecting to boot launcher [port={0}]", port);
        this.messageTransport = 
            new MessageTransport( new Socket("127.0.0.1", port) );

        logger.log(Level.FINE,"starting background message reception thread");
        this.messageRxThread = new MessageReceptionThread();
        this.messageRxThread.start();
    }

    /**
     * Shuts down all the background threads and closes sockets that
     * have been created by this utility.
     */
    protected void terminate() {
        // Shut down the background thread and the messaging transport
        messageRxThread.interrupt();
        messageTransport.close();

        boolean interrupted = false;
        while(true) {
            try {
                messageRxThread.join();
                break;
            } catch(InterruptedException e) {
                interrupted = true;
            }
        }
        if(interrupted) Thread.currentThread().interrupt();
    }

    /**
     * Sends a message to the boot launcher and waits for a corresponsing
     * RESULT message to be sent back from the launcher.
     *
     * @return The arguments in the RESULT message received from the launcher.
     *
     * @throws IOException if the connection to the launcher is lost.
     */
    protected List<String> sendRecvMessage(List<String> message)
                               throws IOException
    {
        logger.log(Level.FINER, "sending message to launcher {0}", message);

        // Atomically send the request message and add ourself to the
        // result wait queue.
        LaunchResult resultEntry = new LaunchResult();
        synchronized(resultWaitQueue) {
            messageTransport.send(message);
            // Note: If send() throws an exception then there is no cleanup
            //       the entry is added to the queue after the send().
            resultWaitQueue.add(resultEntry);
        }

        // Wait for the result to come back, ignoring interruptions
        boolean interrupted = false;
        while(true) {
            try {
                resultEntry.waitForResult();
                break;
            } catch(InterruptedException e) {
                interrupted = true;
            }
        }

        if(resultEntry.result == null) {
            throw new IOException("lost connection to launcher");
        }

        logger.log(Level.FINER, "Received result from launcher {0}",
                   resultEntry.result);

        // Re-set the interrupt flag
        if(interrupted) Thread.currentThread().interrupt();

        return resultEntry.result;
    }

    /**
     * Processes a message received from the boot launcher. Note that
     * this base class implementation only processes "RESULT" messages.
     * Subclasses should override this method in order to process other
     * types of messages.
     *
     * This method is called from the message reception thread.
     *
     * @return A boolean value indicating if the message was processed or not.
     */
    protected boolean onMessageReceived(List<String> message) {

        String cmd = message.get(0);

        if("RESULT".equals(cmd)) {
            List<String> args = message.subList(1, message.size());

            // Get the next result
            LaunchResult result;
            synchronized(resultWaitQueue) {
                result = resultWaitQueue.poll();
            }
            if(result != null) {
                result.setResult(args);
            } else {
                logger.log(Level.SEVERE, 
                           "result received but no one waiting for it");
            }
            return true;
        }
        return false;
    }

    /**
     * Method called by the message reception thread when that thread
     * determines that the connection to the launcher has been broken.
     * Subclasses can override this method to trigger a desired action
     * when this happens.
     */
    protected void onConnectionBroken() { }

    private class MessageReceptionThread extends Thread {
        public MessageReceptionThread() {
            super("MessageReceptionThread [boot launcher]");
            setDaemon(true);
        }

        public void run() {
            try {
                while(! Thread.interrupted()) {
                    // Wait for and receive a message
                    List<String> message = messageTransport.recv();
                    if(message.isEmpty()) continue;
                    onMessageReceived(message);
                }
            } catch(IOException e) {
                messageTransport.close();
                if( !(e instanceof InterruptedIOException) ) {
                    onConnectionBroken();
                }
            } finally {
                // Notify all parties waiting for a result
                synchronized(resultWaitQueue) {
                    messageTransport.close();
                    while( !resultWaitQueue.isEmpty() ) {
                        resultWaitQueue.poll().setResult(null);
                    }
                }
            }
        }
    }

    private static class LaunchResult {
        private boolean haveResult;

        // The result received from the launcher. If null, the receive
        // process was interrupted.
        public List<String> result;

        synchronized void setResult(List<String> result) {
            if( !haveResult ) {
                this.result = result;
                this.haveResult = true;
                this.notifyAll();
            }
        }

        synchronized void waitForResult() throws InterruptedException {
            while( !haveResult ) {
                this.wait();
            }
        }
    }
}
