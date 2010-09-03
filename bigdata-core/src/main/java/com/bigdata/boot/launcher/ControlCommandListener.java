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
package com.bigdata.boot.launcher;

import com.bigdata.boot.MessageTransport;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.ConcurrentLinkedQueue;

class ControlCommandListener implements Runnable {

    private static final Logger logger =
        Logger.getLogger(ControlCommandListener.class.getName());

    /** Reference to the main launcher. */
    private final LauncherMain launcher;

    /** Thread that waits for new connections and accept them. */
    private final Thread listenerThread;

    /** Socket used to accept new control connections. */
    private final ServerSocket listenSock;

    /** List whose elements are connections that receive event notifications */
    private Collection<Connection> eventReceiverList =
                new ConcurrentLinkedQueue<Connection>();


    public ControlCommandListener(LauncherMain launcher, int port)
               throws IOException
    {
        this.launcher = launcher;
        this.listenSock = new ServerSocket();
        listenSock.setReuseAddress(true);
        listenSock.bind(new InetSocketAddress("127.0.0.1", port));
        this.listenerThread = new Thread(this, "ControlConnectionListener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    /**
     * Sends a message to all connections interested in receiving events.
     */
    public void broadcastEvent(List<String> message) {

        logger.log(Level.FINER, "Broadcasting event: {0}", message);

        for(Connection c : eventReceiverList) {
            try {
                c.sendMessage(message);
            } catch(IOException e) {
                // Ignore IOException because the background receiver thread
                // will eventually get an IOException and shut down the
                // connection.
            }
        }
    }

    /**
     * Shuts down the command listener. New connections will not be accepted
     * but existing connections will remain.
     */
    public void terminate() {

        listenerThread.interrupt();

        // Close the socket to cause the accept() to throw an exception
        try {
            listenSock.close();
        } catch(IOException e) { /* swallow */ }

        // Wait for the listener thread to exit
        boolean interrupted = false;
        while(true) {
            try {
                listenerThread.join();
                break;
            } catch(InterruptedException e) {
                // Postpone interruptions during cleanup
                interrupted = true;
            }
        }
        if(interrupted) Thread.currentThread().interrupt();
    }

    public void run() {

        logger.log(Level.FINE, "started ControlCommandListener thread");

        try {
            while( !Thread.interrupted() ) {
                // Wait for a new conenction
                Socket sock = listenSock.accept();

                try {
                    // Start a new handler thread
                    new Connection(sock).startProcessing();
                } catch(IOException e) {
                    logger.log(Level.WARNING, "Failed to start a new control "
                               +"connection processing thread", e);
                    try {
                        sock.close();
                    } catch(IOException e2) { /* swallow */ }
                }
            }
        } catch(IOException e) {
            // accept() threw an IOException
        } finally {
            try {
                listenSock.close();
            } catch(IOException e) { /* swallow */ }
            logger.log(Level.FINE, "exiting ControlCommandListener thread");
        }
    }


    private class Connection implements Runnable {

        /**
         * The message transport used to send and receive messages to the
         * other side of the connection.
         */
        private final MessageTransport messageTransport;

        /** Thread that receives and processes messages on the connection. */
        private final Thread messageRxThread;

        /**
         * Process associated with this connection; which is determined when
         * the process sends an "IDENTIFY" command.
         */
        private ProcessDescriptor associatedProcess;


        Connection(Socket sock) throws IOException {

            this.messageTransport = new MessageTransport(sock);
            this.messageRxThread = new Thread(this, "ControlMessageRxThread");
            this.messageRxThread.setDaemon(true);
        }

        /**
         * Starts the background thread to receive and process messages
         * on this connection.
         */
        void startProcessing() {
            messageRxThread.start();
        }

        /** Sends a message on this connection. */
        void sendMessage(List<String> message) throws IOException {
            messageTransport.send(message);
        }

        public void run() {

            // Add this connection to the list of connections wishing to 
            // receive events.
            eventReceiverList.add(this);

            try {
                while( !(Thread.interrupted()) ) {

                    List<String> msg = messageTransport.recv();
                    logger.log(Level.FINER, "message received {0}", msg);

                    if( msg.isEmpty() ) continue;

                    List<String> result = processMessage(msg);

                    if(result != null) {
                        result = new LinkedList<String>(result);
                        result.add(0, "RESULT");
                        logger.log(Level.FINER,"send response {0}",result);
                        messageTransport.send(result);
                    }
                }
            } catch(IOException e) {
                logger.log(Level.FINE, "connection closed", e);
            } finally {
                if(associatedProcess != null) {
                    // Disassociate this connection from the process
                    associatedProcess.associateConnection(null);
                    this.associatedProcess = null;
                }

                // Remove this connection from the list of connections wishing
                // to receive events and close the connection.
                eventReceiverList.remove(this);
                messageTransport.close();
            }
        }

        private List<String> processMessage(List<String> msg) 
                    throws IOException
        {
            String cmd = msg.get(0);

            if(cmd.equals("START")) {

                if(msg.size() != 2) {
                    return Arrays.asList("FAIL", "InvalidArgumentCount");
                }

                ProcessDescriptor pd =
                    launcher.findProcessDescriptor(msg.get(1));

                if(pd == null) {
                    return Arrays.asList("FAIL", "NoSuchProcess");
                }
                pd.start();

            } else if(cmd.equals("STOP")) {

                if(msg.size() != 2) {
                    return Arrays.asList("FAIL", "InvalidArgumentCount");
                }

                ProcessDescriptor pd =
                    launcher.findProcessDescriptor(msg.get(1));

                if(pd == null) {
                    return Arrays.asList("FAIL", "NoSuchProcess");
                }
                pd.stop(false);

            } else if(cmd.equals("STATUS")) {

                if(msg.size() != 2) {
                    return Arrays.asList("FAIL", "InvalidArgumentCount");
                }

                ProcessDescriptor pd =
                    launcher.findProcessDescriptor(msg.get(1));

                if(pd == null) {
                    return Arrays.asList("FAIL", "NoSuchProcess");
                }
                return Arrays.asList("OK", pd.getState().toString());

            } else if(cmd.equals("GETTAGS")) {

                List<String> result = new ArrayList<String>();
                result.add("OK");
                result.addAll(launcher.getProcessTagList());
                return result;

            } else if(cmd.equals("IDENTIFY")) {

                if(associatedProcess != null) {
                    return Arrays.asList("FAIL", "ProcessAlreadyIdentified");
                }

                if(msg.size() != 2) {
                    return Arrays.asList("FAIL", "InvalidArgumentCount");
                }

                ProcessDescriptor pd =
                    launcher.findProcessDescriptor(msg.get(1));

                if(pd == null) {
                    return Arrays.asList("FAIL", "NoSuchProcess");
                }

                // Make an association between the process and this connection
                try {
                    pd.associateConnection(messageTransport);
                    this.associatedProcess = pd;
                } catch(IllegalStateException e) {
                    return Arrays.asList("FAIL", "ProcessAlreadyIdentified");
                }

                // As part of the protocol definition, connections no longer
                // receive events after the IDENTIFY command has been issued.
                eventReceiverList.remove(this);

            } else if(cmd.equals("STOPLAUNCHER")) {

                launcher.signalShutdown();

            } else if(cmd.equals("INITCOMPLETE")) {

                if(associatedProcess == null) {
                    return Arrays.asList("FAIL", "UnidentifiedProcess");
                }

                if(msg.size() != 1) {
                    return Arrays.asList("FAIL", "InvalidArgumentCount");
                }

                associatedProcess.initCompleteSignaled();

            } else if(cmd.equals("SHUTTINGDOWN")) {

                if(associatedProcess == null) {
                    return Arrays.asList("FAIL", "UnidentifiedProcess");
                }

                if(msg.size() != 1) {
                    return Arrays.asList("FAIL", "InvalidArgumentCount");
                }

                associatedProcess.shuttingDownSignaled();

            } else {
                logger.log(Level.WARNING, 
                           "received unknown command {0}", cmd);
                return Arrays.asList("FAIL", "UnknownCommand");
            }

            return Arrays.asList("OK");
        }
    }
}
