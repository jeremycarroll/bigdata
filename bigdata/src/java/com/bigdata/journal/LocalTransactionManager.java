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

package com.bigdata.journal;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

/**
 * Concrete implementation of <code>AbstractLocalTransactionManager</code>
 * that uses instances of <code>LookupCache</code> to maintain
 * the desired transaction service instance(s); as well as supports
 * testing with an embedded transaction service implementation.
 */
public class LocalTransactionManager
                 extends AbstractLocalTransactionManager
{
    private LookupCache txnServiceCache;//for discovering txn service
    private TransactionService embeddedTxnService;//for embedded fed tests

    public LocalTransactionManager(LookupCache txnServiceCache,
                                   TransactionService embeddedTxnService)
    {
        this.txnServiceCache = txnServiceCache;
        this.embeddedTxnService = embeddedTxnService;
    }

    public TransactionService getTransactionService() {
        if(txnServiceCache != null) {
            ServiceItem txnItem = txnServiceCache.lookup(null);
            if(txnItem != null) return (TransactionService)txnItem.service;
        }
        return embeddedTxnService;
    }

    public void deactivateTx(final Tx localState) {
        super.deactivateTx(localState);
    }
}

