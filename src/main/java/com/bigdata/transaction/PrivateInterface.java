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
package com.bigdata.transaction;

import com.bigdata.journal.ValidationError;

import com.sun.jini.admin.DestroyAdmin;
import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

interface PrivateInterface extends Remote, Administrable,
                                   DestroyAdmin, JoinAdmin
{
    //Related to TransactionService

    long newTx(long timestamp) throws RemoteException;
    
    long commit(long tx) throws ValidationError, RemoteException;

    void abort(long tx) throws RemoteException;
    
    void notifyCommit(long commitTime) throws RemoteException;

    long getLastCommitTime() throws RemoteException;

    long getReleaseTime() throws RemoteException;
    
    void declareResources(long tx, UUID shardService, String[] resource)
             throws RemoteException;

    long prepared(long tx, UUID shardService) 
         throws RemoteException, InterruptedException, BrokenBarrierException;

    boolean committed(long tx, UUID shardService)
         throws RemoteException, InterruptedException, BrokenBarrierException;

    long nextTimestamp() throws RemoteException;

    //Related to TestAdmin

    void kill(int status) throws RemoteException;
}
