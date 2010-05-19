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

import java.net.InetSocketAddress;
import java.io.Serializable;

public class QuorumPeerDataV0 implements QuorumPeerData, Serializable {

    private static final long serialVersionUID = 1L;

    private long peerId = 0L;
    private InetSocketAddress peerAddr = null;
    private InetSocketAddress electionAddr = null;

    public QuorumPeerDataV0() { }

    public long getPeerId() { return peerId; }
    public void setPeerId(long newId) { peerId = newId; }

    public InetSocketAddress getPeerAddress() { return peerAddr; }
    public void setPeerAddress(InetSocketAddress newAddr) {
        peerAddr = newAddr;
    }

    public InetSocketAddress getElectionAddress() { return electionAddr; }
    public void setElectionAddress(InetSocketAddress newAddr) {
        electionAddr = newAddr;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if( !(obj instanceof QuorumPeerDataV0) ) return false;
        if( obj == this ) return true;//same object
        QuorumPeerDataV0 obj1 = this;
        QuorumPeerDataV0 obj2 = (QuorumPeerDataV0)obj;

        if( obj1.peerId != obj2.peerId ) return false;

        if( (obj1.peerAddr == null) || (obj2.peerAddr == null) ) return false;
        if( !(obj1.peerAddr).equals(obj2.peerAddr) ) return false;

        if( (obj1.electionAddr == null) || (obj2.electionAddr == null) ) {
            return false;
        }
        if( !(obj1.electionAddr).equals(obj2.electionAddr) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int hc = 17;//prime initial value
        int pm = 53;//prime multiplier

        if(peerAddr     != null) hc = (pm*hc) + peerAddr.hashCode();
        if(electionAddr != null) hc = (pm*hc) + electionAddr.hashCode();
        hc = (pm*hc) + (int)peerId;

        return hc;
    }

    @Override
    public String toString() {
        return "["
               + "peerId="+peerId
               + ", "
               + "peerAddress="+peerAddr
               + ", "
               + "electionAddress="+electionAddr
               + "]";
    }
}
