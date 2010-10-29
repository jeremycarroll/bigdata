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
/*
 * Created on Apr 6, 2008
 */

package com.bigdata.service.jini.lookup;

import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceTemplate;

//BTM import com.bigdata.journal.ITransactionService;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.jini.JiniFederation;

//BTM
import com.bigdata.journal.TransactionService;

//BTM - FOR_CLIENT_SERVICE
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceDiscoveryListener;
import java.util.UUID;

/**
 * Client manages discovery of a transaction service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TransactionServiceClient extends
        BigdataCachingServiceClient<TransactionService> {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public TransactionServiceClient(final JiniFederation fed, final long timeout)
//BTM - PRE_CLIENT_SERVICE            throws RemoteException {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        super(fed, TransactionService.class, new ServiceTemplate(null,
//BTM - PRE_CLIENT_SERVICE                new Class[] { TransactionService.class }, null),
//BTM - PRE_CLIENT_SERVICE                null/* filter */, timeout);
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    }
    public TransactionServiceClient(final ServiceDiscoveryManager sdm,
                                    final ServiceDiscoveryListener listener,
                                    final UUID serviceUUID,
                                    final Object serviceRef,
                                    final long timeout)
            throws RemoteException {

        super(sdm, listener, serviceUUID, serviceRef,
              TransactionService.class,
              new ServiceTemplate(null, new Class[] {TransactionService.class}, null),
              null, //filter
              timeout);
    }
//BTM - PRE_CLIENT_SERVICE - END

    /**
     * Return the transaction service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @todo handle multiple service instances for failover but always designate
     *       a primary. secondaries MUST listen to the primary so that time
     *       continues to move forward.
     */
    public TransactionService getTransactionService() {

        return super.getService();

    }

}
