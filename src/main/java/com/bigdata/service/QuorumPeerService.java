/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;

/**
 * Provides an interface to the zookeeper QuorumPeerMain service.
 */
public interface QuorumPeerService extends Service {

    // Local methods

    InetAddress getPeerAddress();

    int getPeerPort();

    int getElectionPort();

    int getClientPort();

    String getDataDir();

    String getDataLogDir();

    int getTickTime();

    int getInitLimit();

    int getSyncLimit();

    int getElectionAlg();

    int getMaxClientCnxns();

    int getNQuorumPeers();

    // Potential remote methods

    long getPeerId() throws IOException;

    Map<Long, QuorumPeerData> getPeerDataMap() throws IOException;

    /**
     * Interface used to define different versions of a data structure that
     * encapsulates information about the servers with which a service is
     * peered; which can be persisted for recovery on restart/re-initialization
     * or retrieved from the peer service.
     *
     * This interface is provided to support evolution of new versions of
     * instances of this interface.
     */
    public interface QuorumPeerData {

        long getPeerId();
        void setPeerId(long newId);

        InetSocketAddress getPeerAddress();
        void setPeerAddress(InetSocketAddress newAddr);

        InetSocketAddress getElectionAddress();
        void setElectionAddress(InetSocketAddress newAddr);
    }
}
