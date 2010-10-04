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
 * Created on Apr 23, 2009
 */

package com.bigdata.service;

//BTM
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.resources.LocalResourceManagement;
import com.bigdata.resources.ResourceManager;

import java.io.IOException;

/**
 * Base class for {@link IDataServiceCallable}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class DataServiceCallable<T> extends FederationCallable<T>
        implements IDataServiceCallable {

//BTM    private transient DataService dataService;
private transient ShardService dataService;

protected transient ConcurrencyManager dsConcurrencyManager;
protected transient ResourceManager dsResourceManager;
protected transient IIndexManager dsIndexManager;
protected transient Session dsSession;
protected transient String dsHost;
protected transient String dsServiceName;

    /**
     * Deserialization ctor.
     */
    public DataServiceCallable() {
    }

    /**
     * Sets the {@link DataService} reference and the {@link IBigdataFederation}
     * reference (if not already set).
     */
//BTM    synchronized public void setDataService(final DataService dataService) {
synchronized public void setDataService(final ShardService dataService) {

        if (dataService == null)
            throw new IllegalArgumentException();

        if (this.dataService != null && this.dataService != dataService)
            throw new IllegalStateException();

        this.dataService = dataService;
//BTM
//BTM - each task should call setFederation separately
//BTM        setFederation(dataService.getFederation());
if(dataService instanceof DataService) {
    this.dsConcurrencyManager = ((DataService)dataService).getConcurrencyManager();
    this.dsResourceManager = ((DataService)dataService).getResourceManager();
    this.dsIndexManager = ((DataService)dataService).getFederation();
    this.dsSession = ((DataService)dataService).getSession();
    try {
        this.dsHost = ((IService)dataService).getHostname();
        this.dsServiceName = ((IService)dataService).getServiceName();
    } catch(IOException e) {
        this.dsHost = "UNKNOWN";
        this.dsServiceName = "UNKNOWN";
    }
} else {//EmbeddedShardService
    this.dsConcurrencyManager = ((LocalResourceManagement)dataService).getConcurrencyManager();
    this.dsResourceManager = ((LocalResourceManagement)dataService).getResourceManager();
    this.dsIndexManager = ((LocalResourceManagement)dataService).getIndexManager();
    this.dsSession = ((ISession)dataService).getSession();
    this.dsHost = ((Service)dataService).getHostname();
    this.dsServiceName = ((Service)dataService).getServiceName();
}
        
    }

//BTM    public DataService getDataService() {
//BTM
//BTM        if (dataService == null)
//BTM            throw new IllegalStateException();
//BTM
//BTM        return dataService;
//BTM
//BTM    }

public ResourceManager getResourceManager() {
    return dsResourceManager;
}

public ConcurrencyManager getConcurrencyManager() {
    return dsConcurrencyManager;
}

public IIndexManager getIndexManager() {
    return dsIndexManager;
}

public Session getSession() {
    return dsSession;
}

public String getHostname() {
    return dsHost;
}

public String getServiceName() {
    return dsServiceName;
}
    
    /**
     * Return <code>true</code> iff the reference to the shard (data)
     * service has been set.
     */
    public boolean isDataService() {
        
        return dataService != null;
        
    }

}
