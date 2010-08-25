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

package com.bigdata.attr;

import net.jini.entry.AbstractEntry;
import java.net.InetAddress;
import java.util.UUID;

public class QuorumPeerAttr extends AbstractEntry {

    private static final long serialVersionUID = 1L;

    /**
     * The unique id of the service associated with the given instance of
     * this class.
     *
     * @serial
     */
    public UUID source;

    /**
     * @serial
     */
    public InetAddress address;

    /**
     * @serial
     */
    public Integer peerPort;

    /**
     * @serial
     */
    public Integer electionPort;

    /**
     * @serial
     */
    public Integer clientPort;

    /**
     * @serial
     */
    public Long peerId;

    /**
     * @serial
     */
    public Integer nQuorumPeers;

    /**
     * No-arg constructor requrired by the semantics of <code>Entry</code>.
     */
    public QuorumPeerAttr() {
        super();
    }

    /** 
     * @see <code>net.jini.entry.AbstractEntry#equals</code>.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    /** 
     * @see <code>net.jini.entry.AbstractEntry#hashCode</code>.
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /** 
     * @see <code>net.jini.entry.AbstractEntry#toString</code>.
     */
    @Override
    public String toString() {
        return super.toString();
    }
}
