/*

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

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.Serializable;
import java.util.List;

/**
 * A task which can be distributed to callable executor service(s).
 *
 * @param <V> type of value returned by a call to the
 *            <code>startClientTask</code> method.
 */
public interface IClientServiceCallable<V> extends Serializable {

    /**
     * Computes a result on a callable executor service, or throws an
     * exception if unable to do so.
     *
     * @param indexMgr the <code>ScaleoutIndexManager</code> currently
     *        being employed by the entity that invokes the
     *        <code>startClientTask</code> method.
     *
     * @param embeddedCallableExecutor reference to the local object
     *        executing this task.
     *
     * @return computed result
     *
     * @throws Exception if unable to compute a result
     */
    V startClientTask(IIndexManager indexManager,
                      IConcurrencyManager concurrencyManager,
                      ILocalResourceManagement localResourceManager,
                      IBigdataDiscoveryManagement discoveryManager,
                      CallableExecutor embeddedCallableExecutor,
                      ZooKeeper zookeeperClient,
                      List<ACL> zookeeperAcl,
                      String zookeeperRoot)
          throws Exception;
}
