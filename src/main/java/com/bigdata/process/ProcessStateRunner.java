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
package com.bigdata.process;

import java.text.MessageFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/** 
 * Task class that executes all state changes for all process state
 * machines.
 */
class ProcessStateRunner implements Runnable {

    private static final Logger logger =
                                Logger.getLogger(ProcessStateRunner.class);

    private final Object gpSync;
    private final List<ProcessManagement> runQueue =
                                          new LinkedList<ProcessManagement>();

    ProcessStateRunner(Object gpSync) {
        this.gpSync = gpSync;
    }

    /**
     * Sends a notificatin indicating that the state machine for the given
     * process has become runnable.
     *
     * This method can be called from any thread.
     */
    void schedule(ProcessManagement pInfo) {
        synchronized(runQueue) {
            if( !runQueue.contains(pInfo) ) runQueue.add(pInfo);
            runQueue.notify();
        }
    }

    public void run() {

        while(true) {
            try {
                ProcessManagement pInfo;

                synchronized(runQueue) {
                    while( runQueue.isEmpty() ) {
                        runQueue.wait();
                    }
                    pInfo = runQueue.remove(0);
                }
                synchronized(gpSync) {
                    pInfo.pump();
                }
            } catch(RuntimeException e) {
                logger.log(Level.ERROR, "unexpected exception", e);
            } catch(InterruptedException e) {
                break;
            }
        }
    }
}
