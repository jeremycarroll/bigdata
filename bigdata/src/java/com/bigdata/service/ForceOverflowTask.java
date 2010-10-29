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

import com.bigdata.service.IService;
import com.bigdata.service.OverflowAdmin;
import com.bigdata.service.Service;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;

import net.jini.admin.Administrable;

import org.apache.log4j.Logger;

import java.rmi.RemoteException;
import java.util.concurrent.Callable;

/**
 * Task that forces immediate overflow of the specified data service,
 * returning once both synchronous AND asynchronous overflow are complete.
 * 
 * Note: adapted from AbstractScaleOutFederation#ForceOverflowTask
 */
public class ForceOverflowTask implements Callable<Void> {

    private static final Logger log =
              Logger.getLogger(ForceOverflowTask.class);

    private final ShardService dataService;
    private String serviceName;
    private ShardManagement shardMgr = null;
    private OverflowAdmin overflowAdmin = null;
    private final boolean compactingMerge;
    private final boolean truncateJournal;
        
    public ForceOverflowTask(final ShardService dataService,
                             final boolean compactingMerge,
                             final boolean truncateJournal)
    {
        if (dataService == null) {
            throw new NullPointerException("null dataService");
        }

        this.dataService = dataService;
        this.compactingMerge = compactingMerge;
        this.truncateJournal = truncateJournal;
        this.serviceName = "UNKNOWN";

        if(this.dataService != null) {
            if(this.dataService instanceof IService) {
                try {
                    this.serviceName =
                        ((IService)this.dataService).getServiceName();
                    this.overflowAdmin = (OverflowAdmin)(this.dataService);
                } catch(java.io.IOException e) {
                    log.warn("failed to retrieved serviceName from shard service", e);
                }
            } else {
                this.serviceName = ((Service)this.dataService).getServiceName();
                try {
                    this.overflowAdmin =
                    (OverflowAdmin)( ((Administrable)(this.dataService)).getAdmin() );
                } catch(RemoteException e) {
                    log.warn("failure when retrieving overflow admin", e);
                }
            }
            this.shardMgr = (ShardManagement)(this.dataService);
        }
    }
        
    public Void call() throws Exception {
            
        if(log.isInfoEnabled()) {
            log.info("dataService: " + serviceName);
        }

        // returns once synchronous overflow is complete.
        overflowAdmin.forceOverflow(true/* immediate */, compactingMerge);

        if (log.isInfoEnabled()) {
            log.info("Synchronous overflow is done: " + serviceName);
        }

        // wait until overflow processing is done.
        while (overflowAdmin.isOverflowActive()) {
            Thread.sleep(100/* ms */);
        }

        if (log.isInfoEnabled()) {
            log.info("Asynchronous overflow is done: " + serviceName);
        }

        // Note: Old resources are automatically released as the last step
        //       of asynchronous overflow processing. Therefore all we are really
        //       doing here is issuing a request to truncate the journal. However,
        //       we use the same method to accomplish both ends.

        if (truncateJournal) {
            if (!shardMgr.purgeOldResources(5000/* ms */, true/* truncateJournal */)) {
                log.warn("Could not pause shard service - "
                         +"resources will not be purged.");
            }
        }
        return null;
    }
}

