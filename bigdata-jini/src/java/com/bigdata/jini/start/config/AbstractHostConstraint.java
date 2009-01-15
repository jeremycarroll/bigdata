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
/*
 * Created on Jan 10, 2009
 */

package com.bigdata.jini.start.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.service.jini.JiniFederation;

/**
 * Core impl.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo do a variant based on {@link InetAddress}. This might allow the direct
 *       specification of a subnet.
 */
abstract public class AbstractHostConstraint implements IServiceConstraint {

    protected static final Logger log = Logger.getLogger(AbstractHostConstraint.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    protected final String[] hosts;
    
    public String toString() {
        
        return getClass() + "{hosts=" + Arrays.toString(hosts) + "}";
        
    }

    public AbstractHostConstraint(final String host)
            throws UnknownHostException {

        this(new String[] { host });

    }

    public AbstractHostConstraint(final String[] hosts)
            throws UnknownHostException {

        if (hosts == null)
            throw new IllegalArgumentException();

        if (hosts.length == 0)
            throw new IllegalArgumentException();

        for (String host : hosts) {

            if (host == null)
                throw new IllegalArgumentException();

        }
        
        this.hosts = hosts;

    }

    protected boolean allow(final boolean accept) {

        for (String hostname : hosts) {

            try {
                
                if (isLocalHost(hostname)) {

                    return accept;

                }
                
            } catch (UnknownHostException ex) {

                log.warn("hostname: " + hostname, ex);

            }

        }

        return !accept;
        
// final InetAddress[] localAddresses;
// try {
// localAddresses = InetAddress.getAllByName("localhost");
//        } catch (UnknownHostException e) {
//            // should not be thrown for localhost.
//            throw new AssertionError(e);
//        }
//
//        for (InetAddress tmp : addr) {
//
//            if (DEBUG)
//                log.debug("Considering: addr=" + tmp);
//
//            if (tmp.isLoopbackAddress()) {
//
//                return accept;
//
//            } else {
//
//                for (InetAddress a : localAddresses) {
//
//                    if (tmp.equals(a)) {
//
//                        return accept;
//
//                    }
//
//                }
//
//            }
//
//        }
//
//        return false;

    }

    /**
     * Return <code>true</code> if this is server entry for the local host.
     * 
     * @throws UnknownHostException
     */
    static final public boolean isLocalHost(final String hostname)
            throws UnknownHostException {

        final InetAddress[] localAddrs = InetAddress.getAllByName(InetAddress
                .getLocalHost().getCanonicalHostName());

        final InetAddress[] hostAddrs = InetAddress.getAllByName(hostname);

        if (INFO)
            log.info("Considering: " + hostname + " : localAddrs="
                    + Arrays.toString(localAddrs) + ", hostAddrs="
                    + Arrays.toString(hostAddrs));

        for (InetAddress hostAddr : hostAddrs) {

            if (hostAddr.isLoopbackAddress()) {

                return true;

            } else {

                for (InetAddress localAddr : localAddrs) {

                    if (hostAddr.equals(localAddr)) {

                        return true;

                    }

                }

            }

        }

        return false;

    }

    abstract public boolean allow();
    
    public boolean allow(JiniFederation ignored) {
        
        return allow();
        
    }
    
}
