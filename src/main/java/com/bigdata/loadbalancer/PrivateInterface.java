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
package com.bigdata.loadbalancer;

import com.bigdata.service.Event;

import com.sun.jini.admin.DestroyAdmin;
import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

interface PrivateInterface extends Remote, Administrable,
                                   DestroyAdmin, JoinAdmin
{
    //Related to LoadBalancer

    void notify(UUID serviceId, byte[] data) 
             throws RemoteException, IOException;

    void warn(String msg, UUID serviceId) throws RemoteException;

    void urgent(String msg, UUID serviceId) throws RemoteException;

    UUID getUnderUtilizedDataService()
        throws RemoteException, IOException,
               TimeoutException, InterruptedException;

    UUID[] getUnderUtilizedDataServices
        (int minCount, int maxCount, UUID exclude)
            throws RemoteException, IOException,
                   TimeoutException, InterruptedException;

    boolean isHighlyUtilizedDataService(UUID serviceId) 
                throws RemoteException, IOException;

    boolean isUnderUtilizedDataService(UUID serviceId)
                throws RemoteException, IOException;

    void sighup() throws RemoteException, IOException;

    //Related to EventReceivingService

    void notifyEvent(Event e) throws RemoteException, IOException;


    // Related to ShutdownAdmin

    void shutdown() throws RemoteException;

    void shutdownNow() throws RemoteException;

    void kill(int status) throws RemoteException;
}
