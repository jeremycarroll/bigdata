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

import com.bigdata.service.Event;
import com.bigdata.service.EventReceivingService;
import com.bigdata.service.LoadBalancer;
import com.bigdata.util.config.LogUtil;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

public class EventQueueSenderTask implements EventQueue, Runnable {

    private BlockingQueue<Event> eventQueue;
    private LookupCache lbsCache;//for discovering lbs
    private LoadBalancer embeddedLbs;//for testing embedded fed
    private String serviceName;
    private Logger logger;

    public EventQueueSenderTask(BlockingQueue<Event> eventQueue,
                                LookupCache loadBalancerCache,
                                LoadBalancer embeddedLoadBalancer,
                                String serviceName,
                                Logger logger)
    {
        this.eventQueue = eventQueue;
        this.lbsCache = loadBalancerCache;
        this.embeddedLbs = embeddedLoadBalancer;//for embedded fed testing

        this.serviceName = serviceName;//for debug output
        this.logger  = (logger == null ? 
                        LogUtil.getLog4jLogger((this.getClass()).getName()) :
                        logger);
    }

    // Required by EventQueue interface

    public void queueEvent(Event e) {
        eventQueue.add(e);
    }

    // Required by Runnable interface

    public void run() {

//BTM - for now, maintain the same logic and functionality as that in
//BTM   the class AbstractFederation#SendEventsTask

        try {
            LoadBalancer lbs = null;
            EventReceivingService serviceRef = null;
            if(embeddedLbs != null) {
                lbs = embeddedLbs;
            } else {
                if(lbsCache != null) {
                    ServiceItem lbsItem = lbsCache.lookup(null);
                    if(lbsItem != null) {
                        lbs = (LoadBalancer)(lbsItem.service);
                    }
                }
            }
            if(lbs == null) return;
            serviceRef = (EventReceivingService)lbs;

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
