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

import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IService;
import com.bigdata.service.ISession;
import com.bigdata.service.Service;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DataTaskWrapper<T> implements Callable<T> {

//BTM - POST_CLIENT_SERVICE changes - BEGIN
    private IIndexManager indexMgr;
    private ResourceManager resourceMgr;
    private IConcurrencyManager concurrencyMgr;
    private ILocalResourceManagement localResourceMgr;
    private IBigdataDiscoveryManagement discoveryMgr;

    private IDataServiceCallable<T> task;

    public DataTaskWrapper
               (IIndexManager indexManager,
                ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager,
                ILocalResourceManagement localResourceManager,
                IBigdataDiscoveryManagement discoveryManager,
                IDataServiceCallable<T> task)
    {
        this.indexMgr = indexManager;
        this.resourceMgr = resourceManager;
        this.concurrencyMgr = concurrencyManager;
        this.localResourceMgr = localResourceManager;
        this.discoveryMgr = discoveryManager;
        this.task = task;
    }

    public T call() throws Exception {
        return task.startDataTask
                       (indexMgr, resourceMgr, concurrencyMgr,
                        localResourceMgr, discoveryMgr);
    }
//BTM - POST_CLIENT_SERVICE changes - BEGIN

//BTM - PRE_CLIENT_SERVICE changes - BEGIN
//BTM - PRE_CLIENT_SERVICE    private IIndexManager indexMgr;
//BTM - PRE_CLIENT_SERVICE    private ResourceManager resourceMgr;
//BTM - PRE_CLIENT_SERVICE    private IConcurrencyManager concurrencyMgr;
//BTM - PRE_CLIENT_SERVICE    private Session session;
//BTM - PRE_CLIENT_SERVICE    private String hostname = "UNKNOWN";;
//BTM - PRE_CLIENT_SERVICE    private String serviceName = "UNKNOWN";
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    private IDataServiceCallable<T> task;
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    public DataTaskWrapper(IBigdataFederation federation,
//BTM - PRE_CLIENT_SERVICE                           ShardManagement dataService,
//BTM - PRE_CLIENT_SERVICE                           IDataServiceCallable<T> task)
//BTM - PRE_CLIENT_SERVICE    {
//BTM - PRE_CLIENT_SERVICE        this.indexMgr = federation;
//BTM - PRE_CLIENT_SERVICE        if(dataService instanceof DataService) {
//BTM - PRE_CLIENT_SERVICE            this.resourceMgr =
//BTM - PRE_CLIENT_SERVICE                ((DataService)dataService).getResourceManager();
//BTM - PRE_CLIENT_SERVICE            this.concurrencyMgr =
//BTM - PRE_CLIENT_SERVICE                ((DataService)dataService).getConcurrencyManager();
//BTM - PRE_CLIENT_SERVICE            this.session = ((DataService)dataService).getSession();
//BTM - PRE_CLIENT_SERVICE            try {
//BTM - PRE_CLIENT_SERVICE                this.hostname = ((IService)dataService).getHostname();
//BTM - PRE_CLIENT_SERVICE                this.serviceName = ((IService)dataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE            } catch(IOException e) {
//BTM - PRE_CLIENT_SERVICE                //fallback to "UKNOWN"
//BTM - PRE_CLIENT_SERVICE            }
//BTM - PRE_CLIENT_SERVICE        } else {//embeddedShardService or embeddedShardLocator
//BTM - PRE_CLIENT_SERVICE             this.resourceMgr =
//BTM - PRE_CLIENT_SERVICE                ((ILocalResourceManagement)dataService).getResourceManager();
//BTM - PRE_CLIENT_SERVICE            this.concurrencyMgr = 
//BTM - PRE_CLIENT_SERVICE             ((ILocalResourceManagement)dataService).getConcurrencyManager();
//BTM - PRE_CLIENT_SERVICE            this.session = ((ISession)dataService).getSession();
//BTM - PRE_CLIENT_SERVICE            this.hostname = ((Service)dataService).getHostname();
//BTM - PRE_CLIENT_SERVICE            this.serviceName = ((Service)dataService).getServiceName();
//BTM - PRE_CLIENT_SERVICE        }
//BTM - PRE_CLIENT_SERVICE        this.task = task;
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE    public T call() throws Exception {
//BTM - PRE_CLIENT_SERVICE        return task.startDataTask
//BTM - PRE_CLIENT_SERVICE                       (indexMgr, resourceMgr, concurrencyMgr,
//BTM - PRE_CLIENT_SERVICE                        session, hostname, serviceName);
//BTM - PRE_CLIENT_SERVICE    }
//BTM - PRE_CLIENT_SERVICE changes - END

}

