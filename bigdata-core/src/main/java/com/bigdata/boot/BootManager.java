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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.logging.Level;

public class BootManager extends BaseAgent {

    // list containing all the registered process event listeners
    private final List<ProcessEventListener> listenerList =
                      new CopyOnWriteArrayList<ProcessEventListener>();

    // executor used to execute event notifications in a seperate and
    // dedicated thread.
    private final ExecutorService notifierExecutor =
                                      Executors.newSingleThreadExecutor();


    /**
     * Creates a new instance of this boot manager utility class, connected
     * to the boot launcher that is listening on the default port.
     *
     * @throws IOException if a connection to the boot launcher cannot be
     *         established.
     */
    public BootManager() throws IOException {
        this(MessageTransport.DEFAULT_PORT);
    }

    /**
     * Creates a new instance of this boot manager utility class, connected
     * to the boot launcher that is listening on the specified 
     * <code>bootPort</code>.
     *
     * @param bootPort The port number on which the boot launcher is
     *                 listening for connection requests for control
     *                 messages..
     *
     * @throws IOException if a connection to the boot launcher cannot be
     *         established.
     */
    public BootManager(int bootPort) throws IOException {
	super( Logger.getLogger(BootManager.class.getName()), bootPort);
    }

    /**
     * Shuts down all background threads and sockets created by this
     * instance of the boot manager utility.
     */
    public void terminate() {
        notifierExecutor.shutdown();
        super.terminate();
    }

    /**
     * Returns a list of process tags for the processes that were specified
     * in the boot launcher's configuration file to be launched.
     *
     * @return A list of strings whose values reference the process tags
     *         for the processes that the boot launcher is configured to
     *         launch. Note that the elements of the returned list may not
     *         be in any particular order.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken; or if there is an error in the communication
     *         protocol with the launcher.
     */
    public List<String> getProcessTags() throws IOException {

        List<String> result = sendRecvMessage(Arrays.asList("GETTAGS"));

        if( result.isEmpty() ) {
            throw new IOException("empty result returned by boot launcher");
        }

        if( result.get(0).equals("OK") ) {
            // return a list of everything after the status code
            return result.subList(1, result.size());
        }

        if( result.size() < 2 ) {
            throw new IOException("boot launcher returned failure result "
				  +"with no error code");
        } else {
            throw new IOException("boot launcher returned failure result "
				  +"[error code="+result.get(1)+"]");
        }
    }

    /**
     * Sends a request to the boot launcher, requesting the current status
     * of the specified, previously launched process, and returns the
     * result.
     *
     * @param ptag Process tag of the process whose state should be returned.
     *
     * @return The current state the specified process. If a process
     *         with the given tag does not exist, then an
     *         <code>IllegalArgumentException</code>exception is thrown.
     *
     * @throws IllegalArgumentException if a process with the given
     *         tag does not exist.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken; or if there is an error in the communication
     *         protocol with the launcher.
     */
    public ProcessState getState(String ptag) throws IOException {

        List<String> result = sendRecvMessage(Arrays.asList("STATUS", ptag));

        if( result.isEmpty() ) {
            throw new IOException("empty result returned by boot launcher");
        }

        if( result.get(0).equals("OK") ) {
            if( result.size() < 2 ) {
                throw new IOException("result from boot launcher too short "
                                      +"[result size="+result.size()+"]");
            }
            return ProcessState.valueOf(result.get(1));
        }

        if( result.size() < 2 ) {
            throw new IOException("boot launcher returned failure result "
				  +"with no error code");
        } else if( result.get(1).equals("NoSuchProcess") ) {
            throw new IllegalArgumentException("no such process");
        } else {
            throw new IOException("boot launcher returned failure result "
				  +"[error code="+result.get(1)+"]");
        }
    }

    /**
     * Sends a request to the boot launcher that the specified process be
     * launched (started). If the current state of the specified process is
     * 'starting' or 'running', then this method will take no action. Note
     * that the status of the process that is launched can be monitored
     * by registering a listener using registerListener().
     *
     * @param ptag Process tag of the process that should be launched.
     *
     * @throws IllegalArgumentException if a process with the given
     *         tag does not exist.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken; or if there is an error in the communication
     *         protocol with the launcher.
     */
    public void startProcess(String ptag) throws IOException {

        List<String> result = sendRecvMessage(Arrays.asList("START", ptag));

        if( result.isEmpty() ) {
            throw new IOException("empty result returned by boot launcher");
        }

        if( result.get(0).equals("OK") ) return;

        if( result.size() < 2 ) {
            throw new IOException("boot launcher returned failure result "
				  +"with no error code");
        } else if(result.get(1).equals("NoSuchProcess")) {
            throw new IllegalArgumentException("no such process");
        } else {
            throw new IOException("boot launcher returned failure result "
				  +"[error code="+result.get(1)+"]");
        }
    }

    /**
     * Sends a request to the boot launcher that the specified process be
     * gracefully shutdown ('stopped'). If the specified process does not
     * exit withing a given configurable time limit, then the process will
     * be forcibly shut down ('killed'). If the current state of the
     * specified process is 'stopping' or 'stopped', then this method
     * will take no action.
     *
     * @param ptag Process tag of the process that should be stopped.
     *
     * @throws IllegalArgumentException if a process with the given
     *         tag does not exist.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken; or if there is an error in the communication
     *         protocol with the launcher.
     */
    public void stopProcess(String ptag) throws IOException {

        List<String> result = sendRecvMessage(Arrays.asList("STOP", ptag));

        if( result.isEmpty() ) {
            throw new IOException("empty result returned by boot launcher");
        }

        if( result.get(0).equals("OK") ) return;

        if( result.size() < 2 ) {
            throw new IOException("boot launcher returned failure result "
				  +"with no error code");
        } else if( result.get(1).equals("NoSuchProcess") ) {
            throw new IllegalArgumentException("no such process");
        } else {
            throw new IOException("boot launcher returned failure result "
				  +"[error code="+result.get(1)+"]");
        }
    }

    /**
     * Sends a request to the boot launcher, requesting that the launcher
     * itself be gracefully shutdown; which will cause all processes
     * launched by that launcher with a current state of 'running' to
     * also be shut down. Once all shut down processing is complete, the
     * launcher will exit.
     *
     * @throws IOException if the connection to the boot launcher was lost
     *         or is broken; or if there is an error in the communication
     *         protocol with the launcher.
     */
    public void stopLauncher() throws IOException {

        List<String> result = sendRecvMessage(Arrays.asList("STOPLAUNCHER"));

        if( result.isEmpty() ) {
            throw new IOException("empty result returned by boot launcher");
        }

        if( result.get(0).equals("OK") ) return;

        if( result.size() < 2 ) {
            throw new IOException("boot launcher returned failure result "
				  +"with no error code");
        } else {
            throw new IOException("boot launcher returned failure result "
				  +"[error code="+result.get(1)+"]");
        }
    }

    /**
     * Registers the given <code>listener</code> to receive a notification
     * whenever any launched process changes state. Any number of listeners
     * can be registered.
     *
     * @param listener The listener that will receive an event notificatin
     *                 when a process changes state.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>listener</code> parameter.
     */
    public void registerListener(ProcessEventListener listener) {
        if(listener == null) throw new NullPointerException("null listener");
        listenerList.add(listener);
    }

    /**
     * Unregisters a previously registered event listener.
     *
     * @param listener The listener to unregister. If the listener is not
     *                 currently registered, then this method takes no action.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>listener</code> parameter.
     */
    public void unregisterListener(ProcessEventListener listener) {
        if(listener == null) throw new NullPointerException("null listener");
        listenerList.remove(listener);
    }

    @Override
    protected boolean onMessageReceived(List<String> message) {
        if( super.onMessageReceived(message) ) return true;

        String cmd = message.get(0);
        if( "STATECHANGE".equals(cmd) ) {
            try {
                notifierExecutor.execute
                    ( new StateChangeNotifier(message.get(1),
                      ProcessState.valueOf(message.get(2)),
                      ProcessState.valueOf(message.get(3))) );
            } catch(RuntimeException e) {
                logger.log(Level.WARNING, "invalid STATECHANGE message from "
                           +"launcher {0}", message);
            }
            return true;
        }
        return false;
    }

    private class StateChangeNotifier implements Runnable {
        private final ProcessStateChangeEvent event;

        StateChangeNotifier(String       ptag,
                            ProcessState prevState,
                            ProcessState currState)
        {
            this.event = new ProcessStateChangeEvent(ptag,prevState,currState);
        }

        public void run() {
            // Note: can safely iterate over the list without locking
            //       because it is a CopyOnWriteArrayList
            for(ProcessEventListener l : listenerList) {
                try {
                    l.processStateChangeEvent(event);
                } catch(java.lang.RuntimeException e) { }
            }
        }
    }
}
