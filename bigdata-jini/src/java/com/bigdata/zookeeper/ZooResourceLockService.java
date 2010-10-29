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
 * Created on Jul 10, 2008
 */

package com.bigdata.zookeeper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.IResourceLockService;
//BTM import com.bigdata.service.jini.JiniFederation;
//BTM
import com.bigdata.zookeeper.ZooKeeperAccessor;
import org.apache.zookeeper.data.ACL;
import java.util.List;

/**
 * Implementation using {@link ZLock}s. This is purely a gloss on the
 * {@link ZLock}. The interesting behavior is handled by zookeeper.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class ZooResourceLockService implements IResourceLockService {

    final static protected Logger log = Logger.getLogger(ZooResourceLockService.class);

//BTM - BEGIN
//BTM    private final JiniFederation<?> fed;
//BTM
//BTM    public ZooResourceLockService(final JiniFederation<?> fed) {
//BTM
//BTM        if (fed == null)
//BMT            throw new IllegalArgumentException();
//BTM
//BTM        this.fed = fed;
//BTM        
//BTM    }
    private ZooKeeperAccessor zkAccessor;
    private List<ACL> zkAcl;
    private String zkRoot;
    
    public ZooResourceLockService(ZooKeeperAccessor zookeeperAccessor,
                                  List<ACL> zookeeperAcl,
                                  String zookeeperRoot)
    {
        if (zookeeperAccessor == null) {
            throw new IllegalArgumentException("null zookeeperAccessor");
        }
        if (zookeeperAcl == null) {
            throw new IllegalArgumentException("null zookeeperAcl");
        }
        if (zookeeperRoot == null) {
            throw new IllegalArgumentException("null zookeeperRoot");
        }
        this.zkAccessor = zookeeperAccessor;
        this.zkAcl = zookeeperAcl;
        this.zkRoot = zookeeperRoot;
    }
//BTM - END

    public IResourceLock acquireLock(final String namespace) {

        try {
            
            return acquireLock(namespace, Long.MAX_VALUE);
            
        } catch (InterruptedException t) {
            
            throw new RuntimeException(t);
            
        } catch (TimeoutException t) {

            /*
             * Note: TimeoutException should not be thrown with
             * timeout=MAX_LONG.
             */

            throw new RuntimeException(t);
            
        }
        
    }

    public IResourceLock acquireLock(final String namespace,
            final long timeout) throws InterruptedException, TimeoutException {
    
        try {
            
//BTM - BTM
//BTM            final String zpath = fed.getZooConfig().zroot + "/"
//BTM                    + BigdataZooDefs.LOCKS_RESOURCES + "/" + namespace;
//BTM            
//BTM            final ZLock zlock = ZLockImpl.getLock(fed.getZookeeper(), zpath,
//BTM                    fed.getZooConfig().acl);
//BTM            
            final String zpath = zkRoot + "/" + BigdataZooDefs.LOCKS_RESOURCES + "/" + namespace;
            final ZLock zlock = ZLockImpl.getLock(zkAccessor.getZookeeper(), zpath, zkAcl);
//BTM - END

            if (log.isInfoEnabled())
                log.info("Acquiring zlock: " + zlock);
            
            zlock.lock(timeout, TimeUnit.MILLISECONDS);
            
            if (log.isInfoEnabled())
                log.info("Granted zlock: " + zlock);
            
            return new IResourceLock() {

                public void unlock() {
                    
                    try {
                        
                        zlock.unlock();
                        
                    } catch (Throwable t) {
                        
                        throw new RuntimeException(t);
                        
                    }
                    
                }
                
            };
            
        } catch (InterruptedException t) {
            
            throw t;

        } catch (TimeoutException t) {
            
            throw t;
            
        } catch (Throwable t) {
         
            throw new RuntimeException(namespace, t);
            
        }

    }

}
