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
package com.bigdata.shard;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;

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
    //Related to ShardService

    void registerIndex(String name, IndexMetadata metadata)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    void dropIndex(String name) 
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    IBlock readBlock(IResourceMetadata resource, long addr)
               throws RemoteException, IOException;

    //Related to ShardManagement

    IndexMetadata getIndexMetadata(String name, long timestamp)
                      throws RemoteException, IOException,
                             InterruptedException, ExecutionException;

    ResultSet rangeIterator(long tx,
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

    //Related to ITxCommitProtocol

    void setReleaseTime(long releaseTime) throws RemoteException, IOException;

    void abort(long tx) throws RemoteException, IOException;

    long singlePhaseCommit(long tx)
             throws RemoteException, IOException,
                    InterruptedException, ExecutionException;

    void prepare(long tx, long revisionTime)
             throws RemoteException, IOException, Throwable;

    //Related to OverflowAdmin (for testing & benchmarking)

    void forceOverflow(boolean immediate, boolean compactingMerge)
            throws RemoteException, IOException,
                   InterruptedException, ExecutionException;

    long getAsynchronousOverflowCounter() throws RemoteException, IOException;

    boolean isOverflowActive() throws RemoteException, IOException;

    //Related to ShutdownAdmin

    void shutdown() throws RemoteException;

    void shutdownNow() throws RemoteException;

    void kill(int status) throws RemoteException;
}
