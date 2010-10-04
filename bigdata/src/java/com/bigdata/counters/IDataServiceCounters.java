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

package com.bigdata.counters;

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.resources.ResourceManager;

/**
 * Interface that defines and documents the counters and counter namespaces
 * reported by the shard (data) service and the various services which it
 * uses.
 */
public interface IDataServiceCounters 
                     extends ConcurrencyManager.IConcurrencyManagerCounters,
                             ResourceManager.IResourceManagerCounters
{
    /**
     * The namespace for the counters pertaining to the {@link ConcurrencyManager}.
     */
    String concurrencyManager = "Concurrency Manager";

    /**
     * The namespace for the counters pertaining to the {@link ILocalTransactionService}.
     */
    String transactionManager = "Transaction Manager";

    /**
     * The namespace for the counters pertaining to the {@link ResourceManager}.
     */
    String resourceManager = "Resource Manager";
}