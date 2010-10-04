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

import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.LocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IService;
import com.bigdata.service.ISession;
import com.bigdata.service.Service;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DataTaskWrapper<T> implements Callable<T> {

    private IIndexManager indexMgr;
    private ResourceManager resourceMgr;
    private IConcurrencyManager concurrencyMgr;
    private Session session;
    private String hostname = "UNKNOWN";;
    private String serviceName = "UNKNOWN";

    private IDataServiceCallable<T> task;

    public DataTaskWrapper(IBigdataFederation federation,
                           ShardManagement dataService,
                           IDataServiceCallable<T> task)
    {
        this.indexMgr = federation;
        if(dataService instanceof DataService) {
            this.resourceMgr =
                ((DataService)dataService).getResourceManager();
            this.concurrencyMgr =
                ((DataService)dataService).getConcurrencyManager();
            this.session = ((DataService)dataService).getSession();
            try {
                this.hostname = ((IService)dataService).getHostname();
                this.serviceName = ((IService)dataService).getServiceName();
            } catch(IOException e) { /* fallback to "UKNOWN" */ }
        } else {//embeddedShardService or embeddedShardLocator
            this.resourceMgr =
                ((LocalResourceManagement)dataService).getResourceManager();
            this.concurrencyMgr = 
             ((LocalResourceManagement)dataService).getConcurrencyManager();
            this.session = ((ISession)dataService).getSession();
            this.hostname = ((Service)dataService).getHostname();
            this.serviceName = ((Service)dataService).getServiceName();
        }
        this.task = task;
    }

    public T call() throws Exception {
        return task.startDataTask
                       (indexMgr, resourceMgr, concurrencyMgr,
                        session, hostname, serviceName);
    }
}

