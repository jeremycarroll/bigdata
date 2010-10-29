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

package com.bigdata.resources;

import com.bigdata.counters.CounterSet;
import com.bigdata.event.EventQueueSender;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.service.Service;
import com.bigdata.service.Session;
import com.bigdata.service.ndx.ScaleOutIndexCounters;
import com.bigdata.util.concurrent.TaskCounters;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Interface that specifies information and resources created,
 * managed, and maintained in the local VM; and expected to be
 * accessed only by entities executing in that VM.
 */
public interface ILocalResourceManagement {

    /** 
     * Returns the class type of the entity that is managing the
     * local resources specified by this interface.
     */
    Class getServiceIface();

    /** 
     * Returns the name of the entity that is managing the local
     * resources specified by this interface.
     */
    String getServiceName();

    /** 
     * Returns the <code>UUID</code> of the entity that is managing
     * the local resources specified by this interface.
     */
    UUID getServiceUUID();

    /** 
     * Returns the name of the host on which the entity that is
     * managing the local resources specified by this interface
     * is executing.
     */
    String getHostname();

    /** 
     * Returns an instance of <code>ExecutorService</code>, created
     * in the local VM, that can be used as a thread pool.
     */
    ExecutorService getThreadPool();

    /** 
     * Returns an instance of <code>ScheduledExecutorService</code>,
     * created in the local VM, that can used to execute tasks
     * after a given delay, or on a periodic basis. 
     */
    ScheduledExecutorService getScheduledExecutor();

    /** 
     * Returns an instance of <code>Session</code>, created in the
     * local VM. 
     */
    Session getSession();

    /**
     * Returns the <code>ScaleOutIndexCounters</code> for the
     * specified scale-out index for the entity that instantiated
     * this class.
     *
     * There is only a single instance per scale-out index and all
     * operations by the entity on that index are aggregated by that
     * instance. The counters returned by this method are typically
     * reported by the entity to a load balancer service.
     * 
     * @param name the name of the index for which counters should
     *             be returned.
     *
     * @throws NullPointerException if <code>null</code> is input
     *         for the <code>name</code> parameter.
     */
    ScaleOutIndexCounters getIndexCounters(String name);

    /**
     * Dynamically detach and attach the counters for the named indices
     * underneath the <code>com.bigdata.resources.IndexManager</code>.
     * <p>
     * Note: This method limits the frequency of update to no more
     *       than once every 5 seconds.
     * <p>
     * Note: <code>com.bigdata.resources.OverflowManager#overflow()</code>
     * is responsible for reattaching the counters for the live
     * {@link ManagedJournal} during synchronous overflow.
     */
    void reattachDynamicCounters(ResourceManager resourceManager,
                                 IConcurrencyManager concurrencyManager);

    /**
     * The {@link CounterSet} used to report statistics to
     * the load balancer service.
     */
    CounterSet getCounterSet();
    
    /**
     * The node in {@link #getCounterSet()} corresponding to the 
     * root of the host on which the service is executing.
     */
    CounterSet getHostCounterSet();
    
    /**
     * Returns the node in the currently managed counter set that
     * corresponds to the root of the service; where the set that is
     * returned is created if necessary. If <code>true</code> is input
     * for <code>addCounters</code>, then the service counter set that
     * is returned will also be added/re-added to the basic service
     * (or client) counters currently being managed.
     */
    CounterSet getServiceCounterSet(boolean addCounters);

    /**
     * Returns the node in the currently managed counter set that
     * corresponds to the root of the service; where the set that is
     * returned is created if necessary. This method does <i>not</i>
     * add the service counter set to the basic service (or client)
     * counters currently being managed.
     */
    CounterSet getServiceCounterSet();

    /**
     * Returns the path prefix under which all of the counters
     * being managed by a given instance of this interface are
     * located. The returned path prefix is terminated by an
     * {@link ICounterSet#pathSeparator}.
     */
    String getServiceCounterPathPrefix();

    /**
     * Returns the object used to track events and times occuring in
     * a <code>ThreadPoolExecutorStatisticsTask</code>.
     */
    TaskCounters getTaskCounters();

    /**
     * Returns the currently managed <code>EventQueueSender</code>
     * that can be used to both queue for, and send events to, a
     * load balancer service.
     */
    EventQueueSender getEventQueueSender();

    /**
     * Performs cleanup duties related to the termination of the
     * local resources specified by this interface.
     * <p>
     * Upon completion of the termination process, the semantics of all
     * current and future method invocations on the terminated instance
     * of this interface are undefined.
     * <p>
     * Note that it is important for any creates an instance of this
     * interface to invoke this method when that entity, itself, wishes
     * to perform a "graceful shutdown".
     */
     void terminate(long timeout);
}
