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
import java.util.Map;
import java.util.UUID;

/**
 * Interface used to define different versions of a data structure that
 * encapsulates the service's configured state; which can be persisted 
 * for recovery on restart/re-initialization or retrieved from the peer
 * service.
 *
 * Note that an interface is used to support evolution of new versions of
 * QuorumPeerState.
 */
public interface QuorumPeerState {

    UUID getProxyId();
    ServiceID getServiceId();

    int  getNQuorumPeers();
    void setNQuorumPeers(int newN);

    long getPeerId();
    void setPeerId(long newId);

    InetAddress getPeerAddress();
    void setPeerAddress(InetAddress newAddr);

    int  getPeerPort();
    void setPeerPort(int newPort);

    int  getElectionPort();
    void setElectionPort(int newPort);

    int  getClientPort();
    void setClientPort(int newPort);

    String getDataDir();
    void   setDataDir(String newDir);

    String getDataLogDir();
    void   setDataLogDir(String newDir);

    int  getTickTime();
    void setTickTime(int newTime);

    int  getInitLimit();
    void setInitLimit(int newLimit);

    int  getSyncLimit();
    void setSyncLimit(int newLimit);

    int  getElectionAlg();
    void setElectionAlg(int newAlg);

    int  getMaxClientCnxns();
    void setMaxClientCnxns(int newMax);

    Map<Long, QuorumPeerData> getPeerDataMap();
    void setPeerDataMap(Map<Long, QuorumPeerData> newInfoMap);

    String[] LEADER_ELECTION_ALGORITHM = 
        new String[] {"UDP", "NON-AUTH-UDP", "AUTH-UDP", "TCP"};
}
