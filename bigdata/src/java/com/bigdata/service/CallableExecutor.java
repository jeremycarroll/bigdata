/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

package com.bigdata.service;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Public interface for a service that can be used to distribute and
 * execute a {@link Callable} task across the resources of a federation.
 */
public interface CallableExecutor extends Service {

    /**
     * Submit a {@link Callable} and return its {@link Future}. The
     * {@link Callable} will execute on each member of the federation.
     * 
     * @return The {@link Future} for the given task to execute.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be accepted for execution.
     * @throws IOException
     *              if there is a communication failure between this
     *              service and the entity that calls this method.
     */
    <T> Future<T> submit(Callable<T> task) throws IOException;
}
