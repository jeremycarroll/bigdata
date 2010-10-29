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
 * Created on Jul 3, 2008
 */

package com.bigdata.bfs;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;

/**
 * Helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GlobalFileSystemHelper {

    static final public transient String GLOBAL_FILE_SYSTEM_NAMESPACE = "__globalFileSystem";

    private final IIndexManager indexManager;
    
    protected static final transient Logger log = Logger.getLogger(GlobalFileSystemHelper.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    public GlobalFileSystemHelper(IIndexManager indexManager) {
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;
        
    }

    /**
     * The {@link ITx#UNISOLATED} view.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    synchronized public BigdataFileSystem getGlobalFileSystem() {
    synchronized public BigdataFileSystem getGlobalFileSystem
                            (final IConcurrencyManager concurrencyManager,
                             final IBigdataDiscoveryManagement discoveryManager)
    {
//BTM - PRE_CLIENT_SERVICE - END

        if (INFO)
            log.info("");

        if (globalRowStore == null) {

            // setup the repository view.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            globalRowStore = new BigdataFileSystem(indexManager,
//BTM - PRE_CLIENT_SERVICE                    GLOBAL_FILE_SYSTEM_NAMESPACE, ITx.UNISOLATED,
//BTM - PRE_CLIENT_SERVICE                    new Properties());
            globalRowStore = 
                new BigdataFileSystem(indexManager,
                                      concurrencyManager,
                                      discoveryManager,
                                      GLOBAL_FILE_SYSTEM_NAMESPACE,
                                      ITx.UNISOLATED,
                                      new Properties());
//BTM - PRE_CLIENT_SERVICE - END
            
            // register the indices.
            globalRowStore.create();
            
        }
        
        return globalRowStore;

    }
    private transient BigdataFileSystem globalRowStore;

    /**
     * {@link ITx#READ_COMMITTED} view.
     */
    public BigdataFileSystem getReadCommitted() {

        if (INFO)
            log.info("");

        return (BigdataFileSystem) indexManager.getResourceLocator().locate(
                GLOBAL_FILE_SYSTEM_NAMESPACE, ITx.READ_COMMITTED);

    }

}
