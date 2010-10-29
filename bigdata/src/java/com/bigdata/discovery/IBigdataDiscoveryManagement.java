/*

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
package com.bigdata.discovery;

import com.bigdata.journal.TransactionService;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardService;

import java.util.UUID;

/**
 * Defines the interface for implementations that manage the discovery
 * of the services in a Bigdata federation.
 */
public interface IBigdataDiscoveryManagement {

    /**
     * Returns a reference to a transaction service; or <code>null</code>
     * if such a service has not been discovered.
     * 
     * @return reference to a transaction service; or <code>null</code>
     *         if such a service has not been discovered.
     */
    TransactionService getTransactionService();

    /**
     * Returns a reference to a load balancer service; or <code>null</code>
     *         if such a service has not been discovered.
     * 
     * @return reference to a load balancer service; or <code>null</code>
     *         if such a service has not been discovered.
     */
    LoadBalancer getLoadBalancerService();

    /**
     * Returns a reference to a shard locator (metadata) service; or
     * <code>null</code> if such a service has not been discovered.
     * 
     * @return reference to a shard locator (metadata) service; or
     *         <code>null</code> if such a service has not been discovered.
     */
    ShardLocator getMetadataService();

    /**
     * Returns an array whose elements are the UUIDs corresponding to
     * a set of discovered shard (data) service(s).
     * 
     * @param maxCount The maximum number of elements to return; where
     *                 the number returned may be less than the value
     *                 specified for <code>maxCount</code>, but will
     *                 not be greater. Note that when zero (0) is 
     *                 input for this parameter, the UUIDs of all
     *                 discovered shard (data) service(s) will be
     *                 returned.
     * 
     * @return An array of {@link UUID}s for data services.
     *
     * @throws IllegalArgumentException when a negative is input for
     *         the <code>maxCount</code> parameter.
     */
    UUID[] getDataServiceUUIDs(int maxCount);
    
    /**
     * Returns an array whose elements are references to shard (data)
     * service(s)  having UUID corresponding to an element of the
     * <code>uuids</code> parameter. If no shard services exist (or can
     * be discovered) that satisfy the given criteria, then an
     * empty array is returned.
     * 
     * @param uuids array whose elements are the UUIDs of the shard (data)
     *              service(s) to discover and return.
     * 
     * @return array whose elements are references to shard (data)
     *         service(s) having UUID corresponding to an element of
     *         the <code>uuid</code> parameter; or <code>null</code>
     *         if no shard services exist (or can be discovered) that
     *         satisfy the given criteria.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>uuids</code> parameter.
     */
    ShardService[] getDataServices(UUID[] uuids);
    
    /**
     * Returns a reference to the shard (data) service whose corresponding
     * UUID equals the <code>uuid</code> parameter. If no shard service
     * exists (or can be discovered) that satisfies the given criteria,
     * then <code>null</code> is returned.
     * 
     * @param uuid the UUID of the shard (data) service to discover and
     *             return.
     * 
     * @return reference to the shard (data) service whose corresponding
     *         UUID equals the <code>uuid</code> parameter; or
     *         <code>null</code> if no shard services exist (or can be
     *         discovered) that satisfy the given criteria.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>uuid</code> parameter.
     */
    ShardService getDataService(UUID uuid);

    /**
     * Returns a reference to the shard (data) service; where the criteria
     * used to choose the service whose reference is returned is
     * implementation-dependent. If no shard service exists (or can be
     * discovered), then <code>null</code> is returned.
     * 
     * @return reference to the shard (data) service; where the criteria
     *         used to choose the service whose reference is returned is
     *         implementation-dependent; or <code>null</code> if no shard
     *         services exist (or can be discovered).
     */
    ShardService getAnyDataService();
    
    /**
     * Returns a reference to the shard (data) service whose corresponding
     * name attribute the value input for the <code>name</code> parameter.
     * If no shard service exists (or can be discovered) that satisfies
     * the given criteria, then <code>null</code> is returned.
     * 
     * @param name the value of the name attribute for the shard (data)
     *             service to discover and return.
     * 
     * @return reference to the shard (data) service whose corresponding
     *         name attribute the value input for the <code>name</code>
     *         parameter; or <code>null</code> if no shard services exist
     *         (or can be discovered) that satisfy the given criteria.
     *
     * @throws NullPointerException if <code>null</code> is input for the
     *         <code>name</code> parameter.
     */
    ShardService getDataServiceByName(String name);
}
