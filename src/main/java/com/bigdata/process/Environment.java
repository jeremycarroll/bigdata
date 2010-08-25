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
import java.util.concurrent.Future;

interface Environment {

    /**
     * Retrieves and returns the ProcessManagement object for the
     * process with the given process tag.
     */
    ProcessManagement getProcessInfo(String pTag);

    /**
     * Sends a notification to the boot launcher to cause the launcher
     * to start the process with the given process tag.
     */
    void startProcess(String pTag) throws IOException;

    /**
     * Sends a notification to the boot launcher to cause the launcher
     * to stop the process with the given process tag.
     */
    void stopProcess(String pTag) throws IOException;

    /** Retrieves the current process state from the boot launcher. */
    ProcessState getProcessState(String pTag) throws IOException;

    /** Schedules a ProcessManagement instance to run in the
     *  ProcessStateRunner.
     */
    void schedule(ProcessManagement pInfo);

    /** Schedules a task to execute in another thread. */
    Future<?> schedule(Runnable task);

    /**
     * Schedules a task to run later in another thread.
     *
     * @param the   The task to schedule for execution.
     *
     * @param delay The amount of time (in milliseconds) to delay before
     *              running the given <cod>task</code>.
     */
    Future<?> delayedSchedule(Runnable task, long delay);

    /** Sends an event indicating that a process has crashed. */
    void sendProcessCrashEvent(String pTag, boolean willRestart);
}
