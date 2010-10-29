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
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;
import com.bigdata.service.Service;

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * Task that directs a shard service to purge any unused
 * resources and to optionally truncate the extent of the
 * live journal.
 * 
 * Note: adapted from AbstractScaleOutFederation#ForceOverflowTask
 */
public class PurgeResourcesTask implements Callable<Void> {

    private static final Logger log =
                Logger.getLogger(PurgeResourcesTask.class);

    private final ShardService dataService;
    private final boolean truncateJournal;
    private final ShardLocator metadataService;

    public PurgeResourcesTask(final ShardLocator metadataService,
                              final boolean truncateJournal)
    {
        if (metadataService == null) {
            throw new NullPointerException("null metadataService");
        }
        this.dataService = null;
        this.metadataService = metadataService;
        this.truncateJournal = truncateJournal;
    }

    public PurgeResourcesTask(final ShardService dataService,
                              final boolean truncateJournal)
    {
        if (dataService == null) {
            throw new NullPointerException("null dataService");
        }
        this.dataService = dataService;
        this.metadataService = null;
        this.truncateJournal = truncateJournal;
    }

    public Void call() throws Exception {

        if(dataService != null) {
            String serviceName = null;
            if(dataService instanceof IService) {
                serviceName = 
                    ((IService)dataService).getServiceName();
            } else {
                serviceName = 
                    ((Service)dataService).getServiceName();
            }
            ShardManagement shardMgr = (ShardManagement)dataService;
            boolean resourcesPurged =
                        shardMgr.purgeOldResources
                                 (5000/* ms */, truncateJournal);
            if (log.isInfoEnabled()) {
                log.info("dataService: " + serviceName);
            }
            if ( !resourcesPurged ) {
                log.warn("Could not pause shard service - "
                         +"resources will not be purged.");
            }
        }//endif (dataService != null)

        if(metadataService != null) {
            String serviceName = null;
            if(metadataService instanceof IService) {
                serviceName = 
                    ((IService)metadataService).getServiceName();
            } else {
                serviceName = 
                    ((Service)metadataService).getServiceName();
            }
            ShardManagement shardMgr = (ShardManagement)metadataService;
            boolean resourcesPurged =
                        shardMgr.purgeOldResources
                                 (5000/* ms */, truncateJournal);

            if (log.isInfoEnabled()) {
                log.info("metadataService: " + serviceName);
            }
            if ( !resourcesPurged ) {
                log.warn("Could not pause shard locator service - "
                         +"resources will not be purged.");
            }
        }//endif (metadataService != null)

        return null;
    }
}
