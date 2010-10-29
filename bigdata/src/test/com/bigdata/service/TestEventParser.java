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
 * Created on Mar 15, 2009
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase2;

import com.bigdata.service.TestEventReceiver.MyEvent;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.resources.ILocalResourceManagement;

/**
 * Unit tests for parsing {@link Event}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEventParser extends TestCase2 {

    /**
     * 
     */
    public TestEventParser() {
    }

    /**
     * @param arg0
     */
    public TestEventParser(String arg0) {
        super(arg0);
    }

    private static class MockEventReceivingService implements
            IEventReceivingService {

        public void notifyEvent(Event e) throws IOException {

        }

    }
    
    public void test_parser() throws ClassNotFoundException {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE //BTM        final Event e = new MyEvent(new TestEventReceiver.MockFederation(new MockEventReceivingService()), new EventResource("testIndex"), "testEventType");
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE IBigdataFederation fed = new TestEventReceiver.MockFederation(new MockEventReceivingService());
//BTM - PRE_CLIENT_SERVICE final Event e = new MyEvent(fed,
//BTM - PRE_CLIENT_SERVICE                             fed.getEventQueue(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceIface(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceName(),
//BTM - PRE_CLIENT_SERVICE                             fed.getServiceUUID(),
//BTM - PRE_CLIENT_SERVICE                             new EventResource("testIndex"),
//BTM - PRE_CLIENT_SERVICE                             "testEventType");
        TestEventReceiver.MockFederation fed =
            new TestEventReceiver.MockFederation
                    (new MockEventReceivingService());
        final Event e = new MyEvent( (ILocalResourceManagement)fed,
                                     fed.getService(),
                                     new EventResource("testIndex"),
                                     "testEventType" );
//BTM - PRE_CLIENT_SERVICE - END

        assertSameEvent(e, Event.fromString(e.toString()));
        
    }

    static void assertSameEvent(Event expected, Event actual) {
        
        if (expected == null) {

            assertNull(actual);
        
            return;
            
        }

        assertNotNull(actual);
        
        assertEquals(expected.eventUUID, actual.eventUUID);
        assertEquals(expected.hostname, actual.hostname);
        assertEquals(expected.serviceIface, actual.serviceIface);
        assertEquals(expected.serviceName, actual.serviceName);
        assertEquals(expected.serviceUUID, actual.serviceUUID);
        assertSameEventResource(expected.resource, actual.resource);
        assertEquals(expected.majorEventType, actual.majorEventType);
        assertEquals(expected.minorEventType, actual.minorEventType);
        
        if (expected.getDetails() == null) {
         
            if (actual.getDetails() != null && !actual.getDetails().isEmpty()) {

                fail("Actual has details.");
                
            }
            
        } else {
            
            final Iterator<Map.Entry<String, Object>> itr = expected
                    .getDetails().entrySet().iterator();
            
            while(itr.hasNext()) {
                
                final Map.Entry<String,Object> entry = itr.next();
                
                final String key = entry.getKey();

                assertTrue(actual.getDetails().containsKey(key));
                
                // compare the string representations.
                assertEquals(entry.getValue().toString(), actual.getDetails()
                        .get(key).toString());
                
            }

            assertEquals(expected.getDetails().size(), actual.getDetails()
                    .size());
            
        }
        
    }

    static void assertSameEventResource(EventResource expected,
            EventResource actual) {
        
        if (expected == null) {

            assertNull(actual);
        
            return;
            
        }
        
        assertNotNull(actual);

        assertEquals(expected.indexName, actual.indexName);

        assertEquals(expected.partitionId, actual.partitionId);

        assertEquals(expected.file, actual.file);
        
    }
    
}
