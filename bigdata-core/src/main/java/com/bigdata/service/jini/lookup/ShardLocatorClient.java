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

package com.bigdata.service.jini.lookup;

import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.ShardLocator;

/**
 * Class handles discovery, caching, and local lookup of a shard locator
 * service.
 */
public class ShardLocatorClient extends
        BigdataCachingServiceClient<ShardLocator> {

    /**
     * {@inheritDoc}
     */
    public ShardLocatorClient(final JiniFederation fed, final long timeout)
            throws RemoteException {
        super(fed, ShardLocator.class, new ServiceTemplate(null,
                new Class[] { ShardLocator.class }, null), null/* filter */,
                timeout);
    }

    /**
     * Return the shard locator service from the cache -or-
     * <code>null</code> if there is none in the cache and a remote lookup
     * times out.
     * 
     * @todo handle more than one shard locator service. right now registering
     *       more than one will cause problems since different clients might
     *       discover different shard locators; which are not currently 
     *       arranging themselves into a failover chain or a hash partitioned
     *       service.
     */
    final public ShardLocator getMetadataService() {

        return super.getService();
    }

    /**
     * Return the proxy for a shard locator service from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the shard locator service.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known shard locator service.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     */
    public ShardLocator getMetadataService(final UUID serviceUUID) {

        final ServiceItem serviceItem = getServiceItem(serviceUUID);
        if (serviceItem == null) {
            log.warn("No such service: uuid=" + serviceUUID);
            return null;
        }
        return (ShardLocator) serviceItem.service;
    }
}
