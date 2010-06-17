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

import com.bigdata.boot.ProcessState;
import java.io.IOException;

/** 
 * The interface for managing a process.
 */
interface ProcessManagement {

    String getTag();// Returns the 'tag' of the process being managed

    boolean isRunning();// Returns true if the process is in RUNNING state

    /**
     * Sets the initial desired state of the process.
     *   true  - the process being managed should be started
     *   false - the process being managed should not be started
     */
    void setAutoStart(boolean value);

    void setAutoRestartRate(int count, int interval);

    void addStartDependency(String pTag);

    void setRestartGroup(RestartGroup rGroup);

    void initialize(Environment env, Object gpSync)
             throws InitializationException, IOException;

    /**
     * Registers the process referenced in the given
     * <code>ProcessManagement</code> parameter as a dependent of
     * the process being managed by the current instance of this
     * interface. Dependent processes will be notified (through a
     * call to the <code>wake</code> method) whenever the process
     * being managed changes state.
     */
    void registerDependent(ProcessManagement pInfo);

    /** Schedules execution of the state machine of the process being managed*/
    void wake();

    void restart_stop();

    void restart_start();

    void onBootStateChange(ProcessState newState);

    void pump();// Processes events and pokes the state machine of the process
}
