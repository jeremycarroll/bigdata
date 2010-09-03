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

import java.io.Serializable;

/**
 * A task which can be distributed to {@link IClientService}s.
 * @param <V> type of value returned by the
 * {@link #startClientTask(IBigdataFederation, ClientService) startClientTask}
 * method.
 */
public interface IClientServiceCallable<V> extends Serializable {

    /**
     * Computes a result on a {@link IClientService}, or throws an
     * exception if unable to do so.
     *
     * @param federation federation to which the ClientService belongs.
     * @param clientService reference to the ClientService executing this task.
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    V startClientTask(IBigdataFederation federation,
                      ClientService clientService) throws Exception;
}
