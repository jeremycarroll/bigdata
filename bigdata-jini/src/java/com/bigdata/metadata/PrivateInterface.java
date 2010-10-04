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
package com.bigdata.metadata;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.mdi.PartitionLocator;

import com.sun.jini.admin.DestroyAdmin;
import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

//BTM - PRE_FRED_3481
import com.bigdata.service.IDataServiceCallable;

interface PrivateInterface extends Remote, Administrable,
                                   DestroyAdmin, JoinAdmin
{
    //Related to ShardLocator

    int nextPartitionId(String name)
            throws RemoteException, IOException,
                   InterruptedException, ExecutionException;

    void splitIndexPartition(String             name,
                             PartitionLocator   oldLocator,
                             PartitionLocator[] newLocators)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    void joinIndexPartition(String             name,
                            PartitionLocator[] oldLocators,
                            PartitionLocator   newLocator)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    void moveIndexPartition(String           name,
                            PartitionLocator oldLocator,
                            PartitionLocator newLocator)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    UUID registerScaleOutIndex(IndexMetadata metadata,
                               byte[][]      separatorKeys,
                               UUID[]        dataServices)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    void dropScaleOutIndex(String name)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    PartitionLocator get(String name, long timestamp, byte[] key)
                         throws RemoteException, IOException,
                                InterruptedException, ExecutionException;

    PartitionLocator find(String name, long timestamp, byte[] key)
                         throws RemoteException, IOException,
                                InterruptedException, ExecutionException;

    //Related to ShardManagement

    IndexMetadata getIndexMetadata(String name, long timestamp)
                      throws RemoteException, IOException,
                             InterruptedException, ExecutionException;

    public ResultSet rangeIterator(long tx,
                                   String name,
                                   byte[] fromKey,
                                   byte[] toKey,
                                   int capacity,
                                   int flags,
                                   IFilterConstructor filter)
                         throws RemoteException, IOException,
                                InterruptedException, ExecutionException;

//BTM - PRE_FRED_3481    Future<? extends Object> submit(Callable<? extends Object> proc)
    <T> Future<T> submit(IDataServiceCallable<T> task)
                                 throws RemoteException;

    Future submit(long tx, String name, IIndexProcedure proc)
               throws RemoteException;

    boolean purgeOldResources(long timeout, boolean truncateJournal)
                throws RemoteException, InterruptedException;

    //Related to ShutdownAdmin

    void shutdown() throws RemoteException;

    void shutdownNow() throws RemoteException;

    void kill(int status) throws RemoteException;
}
