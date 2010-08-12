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

import static com.bigdata.shard.Constants.*;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.service.ShardService;

import net.jini.admin.Administrable;

import java.io.InvalidObjectException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class ServiceProxy implements ShardService, Administrable, Serializable
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


    // Remote methods required by the ShardService interface

    public void registerIndex(String name, IndexMetadata metadata)
            throws IOException, InterruptedException, ExecutionException
    {
        innerProxy.registerIndex(name, metadata);
    }

    public IndexMetadata getIndexMetadata(String name, long timestamp)
            throws IOException, InterruptedException, ExecutionException
    {
        return innerProxy.getIndexMetadata(name, timestamp);
    }

    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException
    {
        innerProxy.dropIndex(name);
    }

    public ResultSet rangeIterator(long               tx,
                                   String             name,
                                   byte[]             fromKey,
                                   byte[]             toKey,
                                   int                capacity,
                                   int                flags,
                                   IFilterConstructor filter)
            throws InterruptedException, ExecutionException, IOException
    {
        return innerProxy.rangeIterator
                   (tx, name, fromKey, toKey, capacity, flags, filter);
    }

    public IBlock readBlock(IResourceMetadata resource, long addr)
            throws IOException
    {
        return innerProxy.readBlock(resource, addr);
    }

    public Future submit(long tx, String name, IIndexProcedure proc)
            throws IOException
    {
        return innerProxy.submit(tx, name, proc);
    }

    public Future<? extends Object> submit(Callable<? extends Object> proc)
            throws RemoteException
    {
        return innerProxy.submit(proc);
    }


    // Remote methods required by the TxCommitProtocol interface

    // Remote methods required by the RemoteExecutor interface


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
        String errStr1 = "com.bigdata.shard.ServiceProxy.readObject "
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
          +"com.bigdata.shard.ServiceProxy instance");
    }
}
