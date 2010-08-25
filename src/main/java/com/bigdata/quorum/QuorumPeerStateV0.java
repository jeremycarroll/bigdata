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

import com.bigdata.service.QuorumPeerService.QuorumPeerData;

import net.jini.core.lookup.ServiceID;
import java.net.InetAddress;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class QuorumPeerStateV0 implements QuorumPeerState, Serializable {

    private static final long serialVersionUID = 1L;

    private final UUID proxyId;
    private final ServiceID serviceId;

    private int nQuorumPeers = 1;
    private long peerId = 0;

    private InetAddress peerAddress;
    private int peerPort = 2888;
    private int electionPort = 3888;

    private int clientPort = 2181;
    private String dataDir;
    private String dataLogDir;
    private int tickTime = 2000;
    private int initLimit = 5;
    private int syncLimit = 2;
    private int electionAlg = 3;//0=udp, 3=tcp
    private int maxClientCnxns = 10;

    private Map<Long, QuorumPeerData> peerDataMap =
        new TreeMap<Long, QuorumPeerData>();//order by peerId

    public QuorumPeerStateV0(UUID proxyId, ServiceID serviceId) {
        this.proxyId = proxyId;
        this.serviceId = serviceId;
    }

    public UUID getProxyId() { return proxyId; }
    public ServiceID getServiceId() { return serviceId; }

    public int  getNQuorumPeers() { return nQuorumPeers; }
    public void setNQuorumPeers(int newN) { nQuorumPeers = newN; }

    public long getPeerId() { return peerId; }
    public void setPeerId(long newId) { peerId = newId; }


    //This peer's address
    public InetAddress getPeerAddress() { return peerAddress; }
    public void setPeerAddress(InetAddress newAddr) {
        peerAddress = newAddr;
    }

    public int getPeerPort() { return peerPort; }
    public void setPeerPort(int newPort) {
        peerPort = newPort;
    }

    public int getElectionPort() { return electionPort; }
    public void setElectionPort(int newPort) {
        electionPort = newPort;
    }

    public int  getClientPort() { return clientPort; }
    public void setClientPort(int newPort) { clientPort = newPort; }

    public String getDataDir() { return dataDir; }
    public void   setDataDir(String newDir) { dataDir = newDir; }

    public String getDataLogDir() { return dataLogDir; }
    public void   setDataLogDir(String newDir) { dataLogDir = newDir; }

    public int  getTickTime() { return tickTime; }
    public void setTickTime(int newTime) { tickTime = newTime; }

    public int  getInitLimit() { return initLimit; }
    public void setInitLimit(int newLimit) { initLimit = newLimit; }

    public int  getSyncLimit() { return syncLimit; }
    public void setSyncLimit(int newLimit) { syncLimit = newLimit; }

    public int  getElectionAlg() { return electionAlg; }
    public void setElectionAlg(int newAlg) { electionAlg = newAlg; }

    public int  getMaxClientCnxns() { return maxClientCnxns; }
    public void setMaxClientCnxns(int newMax) {
        maxClientCnxns = newMax;
    }

    public Map<Long, QuorumPeerData> getPeerDataMap() { 
        return peerDataMap;
    }
    public void setPeerDataMap(Map<Long, QuorumPeerData> newDataMap) {
        peerDataMap = newDataMap;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if( !(obj instanceof QuorumPeerStateV0) ) return false;
        if( obj == this ) return true;//same object
        QuorumPeerStateV0 obj1 = this;
        QuorumPeerStateV0 obj2 = (QuorumPeerStateV0)obj;

        if( (obj1.proxyId == null) || (obj2.proxyId == null) ) return false;
        if( !(obj1.proxyId).equals(obj2.proxyId) ) return false;

        if( (obj1.peerAddress == null) || (obj2.peerAddress == null) ) {
            return false;
        }
        if( !(obj1.peerAddress).equals(obj2.peerAddress) ) return false;

        if( obj1.peerPort != obj2.peerPort ) return false;
        if( obj1.electionPort != obj2.electionPort ) return false;
        if( obj1.clientPort != obj2.clientPort ) return false;
        if( obj1.nQuorumPeers != obj2.nQuorumPeers ) return false;
        if( obj1.peerId != obj2.peerId ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        if(proxyId == null) return 0;

        int hc = 11;//prime initial value
        int pm = 37;//prime multiplier

        if(proxyId   != null) hc = (pm*hc) + proxyId.hashCode();
        if(peerAddress != null) hc = (pm*hc) + peerAddress.hashCode();
        hc = (pm*hc) + peerPort;
        hc = (pm*hc) + electionPort;
        hc = (pm*hc) + clientPort;
        hc = (pm*hc) + nQuorumPeers;
        hc = (pm*hc) + (int)peerId;

        return hc;
    }

    @Override
    public String toString() {
        return "["
               + "proxyId="+proxyId
               + ", "
               + "peerId="+peerId
               + ", "
               + "nQuorumPeers="+nQuorumPeers
               + ", "
               + "peerAddress="+peerAddress
               + ", "
               + "peerPort="+peerPort
               + ", "
               + "electionPort="+electionPort
               + ", "
               + "clientPort="+clientPort
               + ", "
               + "dataDir="+dataDir
               + ", "
               + "dataLogDir="+dataLogDir
               + ", "
               + "tickTime="+tickTime
               + ", "
               + "initLimit="+initLimit
               + ", "
               + "syncLimit="+syncLimit
               + ", "
               + "electionAlgorithm="+LEADER_ELECTION_ALGORITHM[electionAlg]
               + ", "
               + "maxConnections="+maxClientCnxns
               + "]";
    }
}
