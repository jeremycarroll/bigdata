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

import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.ILocalResourceManagement;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public class ClientTaskWrapper<T> implements Callable<T> {

    private IIndexManager indexMgr;
    private ILocalResourceManagement localResourceMgr;
    private CallableExecutor embeddedCallableExecutor;
    private IClientServiceCallable<T> task;
    private ZooKeeper zkClient;
    private List<ACL> zkAcl;
    private String zkRoot;

    public ClientTaskWrapper(IIndexManager indexManager,
                             ILocalResourceManagement localResourceManager,
                             CallableExecutor embeddedCallableExecutor,
                             IClientServiceCallable<T> task,
                             ZooKeeper zookeeperClient,
                             List<ACL> zookeeperAcl,
                             String zookeeperRoot)
    {
        this.indexMgr = indexManager;
        this.localResourceMgr = localResourceManager;
        this.embeddedCallableExecutor = embeddedCallableExecutor;
        this.task = task;
        this.zkClient = zookeeperClient;
        this.zkAcl = zookeeperAcl;
        this.zkRoot = zookeeperRoot;
    }

    public T call() throws Exception {
        return task.startClientTask
                       (indexMgr, localResourceMgr,
                        embeddedCallableExecutor,
                        zkClient, zkAcl, zkRoot);
    }
}

