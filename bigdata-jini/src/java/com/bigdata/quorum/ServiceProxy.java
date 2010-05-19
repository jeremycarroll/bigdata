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

package com.bigdata.quorum;

import static com.bigdata.quorum.Constants.*;

import com.bigdata.service.QuorumPeerService;
import com.bigdata.service.QuorumPeerService.QuorumPeerData;

import net.jini.admin.Administrable;

import java.io.InvalidObjectException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;

class ServiceProxy implements QuorumPeerService, Administrable, Serializable {

    private static final long serialVersionUID = 1L;

    final PrivateInterface innerProxy;
    final UUID             proxyId;
    final QuorumPeerState  peerState;

    public static ServiceProxy createProxy(PrivateInterface innerProxy,
                                           UUID             proxyId,
                                           QuorumPeerState  peerState)
    {
        return new ServiceProxy(innerProxy, proxyId, peerState);
    }

    /** Private constructor, called only by createProxy. */
    ServiceProxy(PrivateInterface innerProxy,
                 UUID             proxyId,
                 QuorumPeerState  peerState)
    {
        this.innerProxy = innerProxy;
        this.proxyId    = proxyId;
        this.peerState  = peerState;
    }

    // Required by the Service interface

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
        return (peerState.getPeerAddress()).getHostAddress();
    }

    // Local methods required by QuorumPeerService

    public InetAddress getPeerAddress() {
        return peerState.getPeerAddress();
    }

    public int getPeerPort() {
        return peerState.getPeerPort();
    }

    public int getElectionPort() {
        return peerState.getElectionPort();
    }

    public int getClientPort() {
        return peerState.getClientPort();
    }

    public String getDataDir() {
        return peerState.getDataDir();
    }

    public String getDataLogDir() {
        return peerState.getDataLogDir();
    }

    public int getTickTime() {
        return peerState.getTickTime();
    }

    public int getInitLimit() {
        return peerState.getInitLimit();
    }

    public int getSyncLimit() {
        return peerState.getSyncLimit();
    }

    public int getElectionAlg() {
        return peerState.getElectionAlg();
    }

    public int getMaxClientCnxns() {
        return peerState.getMaxClientCnxns();
    }

    public int getNQuorumPeers() {
        return peerState.getNQuorumPeers();
    }

    // Potential remote methods required by QuorumPeerService

    public long getPeerId() throws IOException {
        return innerProxy.getPeerId();
    }

    public Map<Long, QuorumPeerData> getPeerDataMap() throws IOException {
        return innerProxy.getPeerDataMap();
    }

    // Required by net.jini.admin.Administrable

    public Object getAdmin() throws java.rmi.RemoteException {
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
        String errStr1 = 
         "com.bigdata.quorum.ServiceProxy.readObject "
         +"failure - ";
        String errStr2 = " field is null";

        if(innerProxy == null) {
            throw new InvalidObjectException(errStr1+"innerProxy"+errStr2);
        }

        if(proxyId == null) {
            throw new InvalidObjectException(errStr1+"proxyId"+errStr2);
        }
    }

    private void readObjectNoData() throws InvalidObjectException {
        throw new InvalidObjectException
         ("No data found when attempting to deserialize "
          +"com.bigdata.quorum.ServiceProxy instance");
    }
}
