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
import com.bigdata.boot.ProcessState;

/**
 * Interface that defines the operations that can be performed on a
 * process, as well as other inputs into the process state machine.
 */
interface ProcessDescriptor {

    /**
     * Returns the tag of the associated process; where each process on
     * a given node has a unique tag.
     */
    String getTag();

    /** Returns the current state of the associated process. */
    ProcessState getState();

    /**
     * Starts the process if it is not already starting/started.
     */
    void start();

    /**
     * Stops the process if it is not already stopping/stopped.
     *
     * @param abrupt If <code>true</code> is input, then graceful
     *               shut down of the process will be skipped, the
     *               process will be killed. This is necessary to
     *               address situations where the process may be
     *               "hung".
     */
    void stop(boolean abrupt);

    /**
     * Associates a connection with the process. Requests to stop
     * the process can then be communicated to the process through
     * that connection. If <code>null</code> is passed for the
     * <code>msgTransport</code> parameter, then the current
     * connection will be disassociated from the process.
     *
     * Only one connection can be associated with a process. As a
     * result, any attempt to associate another connection with the
     * process will result in an <code>IllegalStateException</code>.
     *
     * @throws IllegalStateException if the process is already associated
     *         with a connection.
     */
    void associateConnection(MessageTransport msgTransport);

    /**
     * Method that is called when the assoicated process signals that
     * it has completed its initialization.
     */
    void initCompleteSignaled();

    /**
     * Method that is called when the process signals that it wishes to
     * shut down on its own.
     */
    void shuttingDownSignaled();
}
