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
import com.bigdata.mdi.PartitionLocator;

import com.sun.jini.admin.DestroyAdmin;
import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

interface PrivateInterface extends Remote, Administrable,
                                   DestroyAdmin, JoinAdmin
{
    //Related to ShardLocator (metadata)

    int nextPartitionId(String name)
            throws RemoteException, InterruptedException, ExecutionException;

    void splitIndexPartition(String             name,
                             PartitionLocator   oldLocator,
                             PartitionLocator[] newLocators)
             throws RemoteException, InterruptedException, ExecutionException;

    void joinIndexPartition(String             name,
                            PartitionLocator[] oldLocators,
                            PartitionLocator   newLocator)
             throws RemoteException, InterruptedException, ExecutionException;

    void moveIndexPartition(String           name,
                            PartitionLocator oldLocator,
                            PartitionLocator newLocator)
             throws RemoteException, InterruptedException, ExecutionException;

    UUID registerScaleOutIndex(IndexMetadata metadata,
                               byte[][]      separatorKeys,
                               UUID[]        dataServices)
             throws RemoteException, InterruptedException, ExecutionException;

    void dropScaleOutIndex(String name)
             throws RemoteException, InterruptedException, ExecutionException;

    PartitionLocator get(String name, long timestamp, byte[] key)
           throws InterruptedException, ExecutionException, RemoteException;

    PartitionLocator find(String name, long timestamp, byte[] key)
           throws InterruptedException, ExecutionException, RemoteException;

    //Related to TestAdmin

    void kill(int status) throws RemoteException;
}
