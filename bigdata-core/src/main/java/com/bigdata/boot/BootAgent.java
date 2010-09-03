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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Utility class instantiated by processes run by the boot launcher
 * in order to communicate status back to the launcher.
 */
public class BootAgent extends BaseAgent {

    private static final Object singletonMutex = new Object();
    private static BootAgent singleton;

    // tag corresponding to the process that instantiated this utility class
    private final String processTag;

    // executor used to execute event notifications in a seperate and
    // dedicated thread.
    private final ExecutorService notifierExecutor =
                                      Executors.newSingleThreadExecutor();

    // list of registered event listeners
    private final List<AgentListener> listenerList =
                      new CopyOnWriteArrayList<AgentListener>();

    private final Object eventTriggerMutex = new Object();

    // boolean value indicating if initComplete() has been called.
    // Access to this field must be synchronized using <code>this</code>.
    private boolean initCompleteCalled;

    // boolean value indicating if shuttingDown() has been called.
    // Access to this field must be synchronized using <code>this</code>.
    private boolean shuttingDownCalled;

    // boolean value indicating if a STOP command has been received or
    // the connection to the launcher has been lost. Access to this field
    // must be synchronized using <code>eventTriggerMutex</code>.
    private boolean beginShutdownTriggered;


    /**
     * Returns a singleton instance of the BootAgent class that is 
     * currently connected to the boot launcher.
     *
     * Retrieves the following system properties:
     *   boot.processtag - The tag representing the process that instantiated
     *                     this utility class
     *   boot.port - The port number on which the boot launcher is listening
     *               for connection requests for control messages
     *
     * @throws IllegalStateException if one of the required system
     *         properties has not been set.
     *
     * @throws IOException if a connection to the boot launcher cannot be
     *         established or the connection is broken.
     */
    public static BootAgent getInstance() throws IOException {
        synchronized(singletonMutex) {

            if(singleton != null) return singleton;

            String processTag = System.getProperty("boot.processtag");
            if(processTag == null) {
                throw new IllegalStateException("required system property not "
                                               +"defined ['boot.processtag']");
            }
            int portNumber = -1;
            String portStr = "-1";
            try {
                portStr = System.getProperty("boot.port");
                portNumber = Integer.parseInt(portStr);
            } catch(NullPointerException e) {
                throw new IllegalStateException
                    ("required system property not defined ['boot.port']");
            } catch(NumberFormatException e) { }
            if( (portNumber < 1) || (portNumber > 65535) ) {
                throw new IllegalStateException
                    ("'boot.port'system property invalid ["+portStr+"]");
            }
            singleton = new BootAgent(processTag, portNumber);
            return singleton;
        }
    }

    /**
     * Constructs an instance of this class.
     *
     * @param processTag The tag representing the process that instantiated
     *                   this utility class
     *
     * @param bootPort   The port number on which the boot launcher is
     *                   listening for connection requests for control
     *                   messages.
     *
     * @throws IOException if a connection to the boot launcher cannot be
     *         established.
     */
    BootAgent(String processTag, int bootPort) throws IOException {

        super(Logger.getLogger(BootAgent.class.getName()), bootPort);

        this.processTag = processTag;

        boolean initSuccess = false;
        try {
            logger.log(Level.FINE, "sending IDENTIFY message to boot launcher "
                       +"[processTag="+processTag+"]");
            List<String> result = 
                sendRecvMessage( Arrays.asList("IDENTIFY", processTag) );

            if( result.isEmpty() ) {
                throw new IOException("boot launcher returned empty result");
            } else if( !result.get(0).equals("OK") ) {
                throw new IOException("boot launcher returned failure ["+
                                      ( (result.size() < 2) ?
                                          "no error code" :
                                          "error code="+result.get(1) )
                                     +"]");
            }
            initSuccess = true;
        } finally {
            if(! initSuccess) {
                notifierExecutor.shutdown();
                super.terminate();
            }
        }
    }

    /**
     * Registers a listener that receives commands from the boot launcher.
     * Any number of listeners may be registered. If the process has already
     * been told to shut down, then the beginShutdown() method of the
     * listener may be invoked immediately.
     *
     * @param listener The listener that will be called when a command from
     *                 the boot launcher is received.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>listener</code> parameter.
     */
    public void registerListener(AgentListener listener) {

        if(listener == null) throw new NullPointerException("null listener");

        // Atomically add the listener to the list of listeners, and
        // determine if events need to be sent to the new listener.
        synchronized(eventTriggerMutex) {
            listenerList.add(listener);
            if( !beginShutdownTriggered) return;
        }
        try {
            listener.beginShutdown();
        } catch(RuntimeException e) { }
    }

    /**
     * Unregisters a previously registered boot command listener.
     *
     * @param listener The listener to unregister. If the listener is not
     *                 currently registered, then this method takes no action.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>listener</code> parameter.
     */
    public void unregisterListener(AgentListener listener) {
        if(listener == null) throw new NullPointerException("null listener");
        listenerList.remove(listener);
    }

    /**
     * Notifies the boot launcher that the process being launched has
     * successfully finished initialization and that process' state can
     * now be changed to RUNNING. This method can only be called once,
     * and must not be called after shuttingDown() is called.
     *
     * @throws IllegalStateException if this method has been invoked 
     *         previously, or is invoked after shuttingDown() has been
     *         called.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken.
     */
    public void initComplete() throws java.io.IOException {
        logger.log(Level.FINE, "entered BootAgent.initComplete()");

        synchronized(this) {
            if(initCompleteCalled) {
                throw new IllegalStateException("BootAgent.initComplete "
					        +"previously invoked");
            }
            if(shuttingDownCalled) {
                throw new IllegalStateException("BootAgent.initComplete "
					        +"invoked after "
                                                +"BootAgent.shuttingDown()");
            }
            List<String> result=sendRecvMessage(Arrays.asList("INITCOMPLETE"));
            if( result.isEmpty() || !"OK".equals(result.get(0)) ) {
                throw new IOException("init completion message to boot "
                                      +"launcher failed");
            }
            this.initCompleteCalled = true;
        }
    }

    /**
     * Notifies the boot launcher that the process that was launched is
     * now shutting down normally. Note that if that process shuts down
     * without calling this method, then the boot launcher will assume
     * that the launched process has crashed instead of shut down.
     *
     * This method will block until an acknowledgment is received from the
     * launcher so the process may exit as soon as this method returns. If
     * the process does not exit within a reasonable amount of time after
     * calling this method then the launcher may forcibly terminate it.
     *
     * This method can only be called once.
     *
     * @throws IllegalStateException if this method has been invoked 
     *         previously.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken.
     */
    public void shuttingDown() throws java.io.IOException {
        logger.log(Level.FINE, "entered BootAgent.shuttingDown()");

        synchronized(this) {
            if(shuttingDownCalled) {
                throw new IllegalStateException("BootAgent.shuttingDown "
					        +"previously invoked");
            }
            List<String> result=sendRecvMessage(Arrays.asList("SHUTTINGDOWN"));
            if( result.isEmpty() || !"OK".equals(result.get(0)) ) {
                throw new IOException("shutdown message to boot launcher "
                                      +"failed");
            }
            this.shuttingDownCalled = true;
        }
    }

    @Override
    protected boolean onMessageReceived(List<String> message) {

        if(super.onMessageReceived(message)) return true;

        String cmd = message.get(0);
        if("STOP".equals(cmd)) {
            logger.log(Level.INFO, "received STOP command from boot launcher");
            notifierExecutor.execute(new StopEventNotifier());
            return true;
        }
        return false;
    }

    @Override
    protected void onConnectionBroken() {
        logger.log(Level.INFO, "connection to boot launcher broken");
        notifierExecutor.execute(new StopEventNotifier());
    }

    private class StopEventNotifier implements Runnable {
        public void run() {
            Iterator<AgentListener> listItr;

            synchronized(eventTriggerMutex) {

                if(beginShutdownTriggered) return; //only want to do this once

                // The list iterator provides a snapshot of the list at this
                // point in time. Such a snapshot is needed because once
                // beginShutdownTriggered is set to true, newly registered
                // listeners are going to have beginShutdown() called
                // immediatly.
                listItr = listenerList.iterator();
                BootAgent.this.beginShutdownTriggered = true;
            }

            while( listItr.hasNext() ) {
                AgentListener l = listItr.next();
                try {
                    l.beginShutdown();
                } catch(RuntimeException e) { }
            }
        }
    }
}
