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
 * Created on Jan 2, 2009
 */

package com.bigdata.jini.start.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import net.jini.config.ConfigurationException;

import com.bigdata.util.config.NicUtil;

/**
 * Unit tests for the {@link ZookeeperServerEntry}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZookeeperServerEntry  {

    /**
     * 
     */
    public TestZookeeperServerEntry() {
    }


    @Test
    public void test_correctRejection() {
        try {
            new ZookeeperServerEntry(0, null);
            Assert.fail("Expecting " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
        }
    }

    /**
     * Test {@link ZookeeperServerEntry} parsing.
     */
    @Test
    public void test001() {
        final int expectedId = 0;
        final String expectedHostname = "192.168.1.2";
        final int expectedPeerPort = 233;
        final int expectedLeaderPort = 1992;

        ZookeeperServerEntry entry = new ZookeeperServerEntry(expectedId,
                expectedHostname + ":" + expectedPeerPort + ":"
                        + expectedLeaderPort);

        Assert.assertEquals(expectedId, entry.id);
        Assert.assertEquals(expectedHostname, entry.hostname);
        Assert.assertEquals(expectedPeerPort, entry.peerPort);
        Assert.assertEquals(expectedLeaderPort, entry.leaderPort);   
    }

    /**
     * Unit test for
     * {@link ZookeeperServerConfiguration#getZookeeperServerEntries(String)}
     * 
     * @throws ConfigurationException
     * @throws SocketException 
     * @throws UnknownHostException 
     */
    @Test
    public void test002() throws ConfigurationException, SocketException, UnknownHostException, IOException {

        final String server = NicUtil.getIpAddress("default.nic", "default", true);
        
        final String[] hosts = new String[] {
                "127.0.0.1",
                "localhost",
                server
        };
        
        final String servers = "1=127.0.0.1:2888:3888, 2=localhost:2888:3888, 3="+ server + ":2888:3888";

        final ZookeeperServerEntry[] a = ZookeeperServerConfiguration.getZookeeperServerEntries(servers);

        for (int i = 0; i < a.length; i++) {
            final ZookeeperServerEntry entry = a[i];

            Assert.assertEquals(i + 1, entry.id);
            Assert.assertEquals(hosts[i], entry.hostname);
            Assert.assertEquals(2888, entry.peerPort);
            Assert.assertEquals(3888, entry.leaderPort);
            
            InetAddress.getByName(entry.hostname);

            Assert.assertTrue(entry.isLocalHost());          
            Assert.assertTrue(new HostAllowConstraint(entry.hostname).allow());          
            Assert.assertFalse(new HostRejectConstraint(entry.hostname).allow());          
        }      
    }
}
