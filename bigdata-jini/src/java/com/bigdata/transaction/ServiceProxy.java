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

import static com.bigdata.transaction.Constants.*;

import com.bigdata.journal.TransactionService;
import com.bigdata.journal.ValidationError;
import com.bigdata.service.Service;

import net.jini.admin.Administrable;

import java.io.InvalidObjectException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.BrokenBarrierException;
import java.util.UUID;

class ServiceProxy implements TransactionService, Service,
                              Administrable, Serializable
{
    private static final long serialVersionUID = 1L;

    final PrivateInterface innerProxy;
    final UUID             proxyId;
    final String           hostname;

    public static ServiceProxy createProxy(PrivateInterface innerProxy,
                                           UUID             proxyId,
                                           String           hostname)
    {
        return new ServiceProxy(innerProxy, proxyId, hostname);
    }

    /** Private constructor, called only by createProxy. */
    ServiceProxy(PrivateInterface innerProxy, UUID proxyId, String hostname) {
        this.innerProxy = innerProxy;
        this.proxyId    = proxyId;
        this.hostname   = hostname;
    }

    // Methods required by the Service interface

    public UUID getServiceUUID() {
        return proxyId;
    }

    public Class getServiceIface() {
        return SERVICE_TYPE;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public String getHostname() {
        return hostname;
    }

    // Remote methods required by the Transaction interface

    public long newTx(long timestamp) throws IOException {
        return innerProxy.newTx(timestamp);
    }
    
    public long commit(long tx) throws ValidationError, IOException {
        return innerProxy.commit(tx);
    }

    public void abort(long tx) throws IOException {
        innerProxy.abort(tx);
    }
    
    public void notifyCommit(long commitTime) throws IOException {
        innerProxy.notifyCommit(commitTime);
    }

    public long getLastCommitTime() throws IOException {
        return innerProxy.getLastCommitTime();
    }

    public long getReleaseTime() throws IOException {
        return innerProxy.getReleaseTime();
    }
    
    public void declareResources(long tx, UUID shardService, String[] resource)
            throws IOException
    {
        innerProxy.declareResources(tx, shardService, resource);
    }

    public long prepared(long tx, UUID shardService) 
             throws IOException, InterruptedException, BrokenBarrierException
    {
        return innerProxy.prepared(tx, shardService);
    }

    public boolean committed(long tx, UUID shardService)
             throws IOException, InterruptedException, BrokenBarrierException
    {
        return innerProxy.committed(tx, shardService);
    }

    public long nextTimestamp() throws IOException {
        return innerProxy.nextTimestamp();
    }

    // Required by net.jini.admin.Administrable

    public Object getAdmin() throws RemoteException {
        return innerProxy.getAdmin();
    }

    //Methods for good proxy behavior: hashCode, equals, readObject, etc.
    public int hashCode() {
        if(proxyId == null) return 0;
        return proxyId.hashCode();
    }

    /** 
     * Proxy equality is defined as <i>reference equality</i>; that is,
     * two proxies are equal if they reference (are proxies to) the
     * same backend server.
     */
    public boolean equals(Object obj) {
        if(proxyId == null) return false;
        return proxyId.equals(obj);
    }

    private void readObject(ObjectInputStream s) throws IOException,
                                                        ClassNotFoundException
    {
        s.defaultReadObject();

        // Verify fields are valid
        String errStr1 = "com.bigdata.transaction.ServiceProxy.readObject "
                         +"failure - ";
        String errStr2 = " field is null";

        if(innerProxy == null) {
            throw new InvalidObjectException(errStr1+"innerProxy"+errStr2);
        }

        if(proxyId == null) {
            throw new InvalidObjectException(errStr1+"proxyId"+errStr2);
        }

        if(hostname == null) {
            throw new InvalidObjectException(errStr1+"hostname"+errStr2);
        }
    }

    private void readObjectNoData() throws InvalidObjectException {
        throw new InvalidObjectException
         ("No data found when attempting to deserialize "
          +"com.bigdata.transaction.ServiceProxy instance");
    }
}
