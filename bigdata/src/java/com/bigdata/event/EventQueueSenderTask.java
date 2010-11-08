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

package com.bigdata.event;

import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.service.Event;
import com.bigdata.service.EventReceivingService;
import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

public class EventQueueSenderTask implements EventQueueSender {

    private BlockingQueue<Event> eventQueue;
    private IBigdataDiscoveryManagement discoveryMgr;
    private String serviceName;
    private Logger logger;

    public EventQueueSenderTask(BlockingQueue<Event> eventQueue,
                                IBigdataDiscoveryManagement discoveryMgr,
                                String serviceName,
                                Logger logger)
    {
        this.eventQueue = eventQueue;
        this.discoveryMgr = discoveryMgr;

        this.serviceName = serviceName;//for debug output
        this.logger  = (logger == null ? 
                        LogUtil.getLog4jLogger((this.getClass()).getName()) :
                        logger);
    }

    // Note: EventQueueSender interface sub-classes EventQueue interface
    //       and Runnable interface

    // Required by EventQueue interface

    public void queueEvent(Event e) {
        eventQueue.add(e);
    }

    // Required by Runnable interface

    public void run() {

//BTM - for now, maintain the same logic and functionality as that in
//BTM   the class AbstractFederation#SendEventsTask
        try {
            EventReceivingService serviceRef = 
              (EventReceivingService)(discoveryMgr.getLoadBalancerService());
            if (serviceRef == null) {
                logger.log(Level.WARN, "cannot send events to load "
                       +"balancer from "+serviceName
                       +" - load balancer unavailable");
                return;
            }
            final long begin = System.currentTimeMillis();//for logging

            final LinkedList<Event> queuedEvents = new LinkedList<Event>();
            eventQueue.drainTo(queuedEvents);

            for (Event event : queuedEvents) {
                serviceRef.notifyEvent(event);//don't sync on remote call
            }

            if (logger.isDebugEnabled()) {
                final int nevents = queuedEvents.size();
                if (nevents > 0) {
                    logger.log
                        (Level.DEBUG, nevents+" event(s) sent to load "
                         +"balancer ["+(System.currentTimeMillis()-begin)
                         +" ms]");
                }
            }
        } catch (Throwable t) {
            logger.log(Level.WARN, "while sending events to load "
                       +"balancer from "+serviceName, t);
        }
    }
}
