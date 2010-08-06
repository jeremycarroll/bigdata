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
package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Interface for collecting, reporting, and decision-making based on node and
 * service utilization statistics.
 */
public interface LoadBalancer {
    
    /**
     * This method is called by an entity that wishes to send its
     * performance counters to this load balancer. Such an entity
     * should invoke this method no less than once every 60 seconds.
     * 
     * @param serviceId The unique identifier of the entity that calls
     *                  this method.
     *
     * @param data      The serialized performance counter data.
     * 
     * @throws IOException if there is a communication failure between
     *                     this load balancer service and the entity
     *                     that calls this method.
     */
    void notify(UUID serviceId, byte[] data) throws IOException;

    /**
     * This method is called by an entity that wishes to issue a
     * warning to indicate to this load balancer that the entity is in
     * danger of depleting its resources.
     * 
     * @param msg       The message to use to describe the warning.
     * @param serviceId The unique identifier of the entity that calls
     *                  this method.
     * 
     * @throws IOException if there is a communication failure between
     *                     this load balancer service and the entity
     *                     that calls this method.
     */
    void warn(String msg, UUID serviceId) throws IOException;

    /**
     * This method is called by an entity that wishes to issue an urgent
     * warning to indicate to this load balancer that the entity is in
     * immediate danger of depleting its resources with a consequence
     * of immediate service and/or host failure(s).
     * 
     * @param msg       The message to use to describe the urgent warning.
     *
     * @param serviceId The unique identifier of the entity that calls
     *                  this method.
     * 
     * @throws IOException if there is a communication failure between
     *                     this load balancer service and the entity
     *                     that calls this method.
     */
    void urgent(String msg, UUID serviceId) throws IOException;

    /**
     * Returns the unique identifier associated with the data service
     * that is currently considered to be under utilized. If there is no
     * such under utilized data service, then identifier of the data
     * service that currently has the least load is returned.
     * 
     * @throws TimeoutException if there are no data services and a timeout
     *                          occurs while awaiting a service join.
     * 
     * @throws InterruptedException if the request is interrupted.
     */
    UUID getUnderUtilizedDataService()
        throws IOException, TimeoutException, InterruptedException;

    /**
     * Returns an array of <code>UUID</code> instances, where each element
     * of the returned array corresponds to a data service that is
     * currently considered to be under utilized.
     *
     * When the value input for the <code>minCount</code> parameter is
     * positive, the returned array will contain at least <code>minCount</code>
     * elements, however the elements returned may contain duplicates if the
     * load balancer service has a strong preference for allocating load
     * to some data services (or for NOT allocating load to other data
     * services). Additionally, the load balancer may choose (or be forced
     * to choose) to return the ids of data services that are within a
     * nominal utilizsation range; or even the ids of data services that
     * are currently considered highly utilized, if the load balancer could
     * not otherwise satisfy the request.
     * 
     * @param minCount The minimum #of services {@link UUID}s to return
     *                 -or- zero (0) if there is no minimum limit.
     *
     * @param maxCount The maximum #of services {@link UUID}s to return
     *                 -or- zero (0) if there is no maximum limit.
     *
     * @param exclude  The optional {@link UUID} of a data service to be
     *                 excluded from the returned set.
     * 
     * @return Up to <i>maxCount</i> under-utilized services -or-
     *         <code>null</code> IFF no services are recommended at this time
     *         as needing additional load.
     * 
     * @throws TimeoutException if there are no data services, or if there
     *                          is only a single data service and it is
     *                          excluded by the request, and a timeout
     *                          occurs while awaiting a service join.
     * 
     * @throws InterruptedException if the request is interrupted.
     */
    UUID[] getUnderUtilizedDataServices
        (int minCount, int maxCount, UUID exclude)
            throws IOException, TimeoutException, InterruptedException;
    
    /**
     * Return <code>true</code> if the service is considered to be
     * "highly utilized". This method is used to help decide when a
     * service should attempt to shed index partitions. The 
     * implementation of this method should reflect the relative
     * rank of the targeted data service among all data services,
     * as well as that service's absolute load.
     * 
     * @param serviceId The unique identifier of the data service whose 
     *                  state of utilization (highly utilized or not) 
     *                  should be determined.
     * 
     * @return <code>true</code> if the data service corresponding to the
     *         given <code>serviceId</code> parameter is considered to be
     *         "highly utilized"; <code>false</code> otherwise.
     * 
     * @throws IOException if there is a communication failure between
     *                     this load balancer service and the entity
     *                     that calls this method.
     */
    boolean isHighlyUtilizedDataService(UUID serviceId) throws IOException;
    
    /**
     * Return <code>true</code> if the service is considered to be
     * "under utilized".
     * 
     * @param serviceId The unique identifier of the data service whose 
     *                  state of utilization (under utilized or not) 
     *                  should be determined.
     * 
     * @return <code>true</code> if the data service corresponding to the
     *         given <code>serviceId</code> parameter is considered to be
     *         "under utilized"; <code>false</code> otherwise.
     * 
     * @throws IOException if there is a communication failure between
     *                     this load balancer service and the entity
     *                     that calls this method.
     */
    boolean isUnderUtilizedDataService(UUID serviceId) throws IOException;


    /**
     * Special method that instructs the load balancer service to log the
     * counters managed by that service to a temporary file. This method
     * is intended as replacement for the system based sighup mechanism.
     * 
     * @throws IOException if there is a communication failure between
     *                     this load balancer service and the entity
     *                     that calls this method.
     */
    public void sighup() throws IOException;
}
