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

package com.bigdata.counters;

import com.bigdata.counters.CounterSet;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IFederationDelegate;
import com.bigdata.service.LoadBalancer;
import com.bigdata.util.config.LogUtil;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * <code>Runnable</code> task class that periodically reports 
 * performance counter data to the load balancer service.
 * (Note: see AbstractFederation.ReportTask).
 */
public class LoadBalancerReportingTask implements Runnable {

    private IFederationDelegate embeddedIndexStore;
    private UUID serviceUUID;
    private CounterSet serviceRoot;
    private LookupCache lbsCache;//for discovering lbs
    private LoadBalancer embeddedLbs;//for testing embedded fed
    private Logger logger;

    public LoadBalancerReportingTask
               (IFederationDelegate embeddedIndexStore,
                UUID serviceUUID,
                CounterSet serviceRoot,
                LookupCache loadBalancerCache,
                LoadBalancer embeddedLoadBalancer,
                Logger logger)
    {
        this.embeddedIndexStore = embeddedIndexStore;
        this.serviceUUID = serviceUUID;
        this.serviceRoot = serviceRoot;
        this.lbsCache = loadBalancerCache;
        this.embeddedLbs = embeddedLoadBalancer;//for embedded fed testing
        this.logger  = (logger == null ? 
                        LogUtil.getLog4jLogger((this.getClass()).getName()) :
                        logger);
    }

    public void run() {
        try {
            embeddedIndexStore.reattachDynamicCounters();
        } catch (Throwable t) {
            logger.error
                ("failure on dynamic counter reattachment ["+t+"]", t);
        }
        try {
            // Report the performance counters to the load balancer.
            reportPerformanceCounters();
        } catch (Throwable t) {
            logger.error("failure while reporting performance "
                         +"counters to load balancer ["+t+"]", t);
        }
    }

    private void reportPerformanceCounters() throws IOException {
System.out.println("\n>>>>> LoadBalancerReportingTask.reportPerformanceCounters: serviceUUID = "+serviceUUID);
        LoadBalancer lbs = null;
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
        if(lbs == null) {
            logger.warn
                ("cannot report counters [no load balancer service]");
System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: loadBalancerService = NULL");
            return;
        }

System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: loadBalancerService = "+lbs);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(Bytes.kilobyte32 * 2);
        serviceRoot.asXML(baos, "UTF-8", null/* filter */);

System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: CALLING loadBalancer.notify ...");
        lbs.notify(serviceUUID, baos.toByteArray());
System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: DONE CALLING loadBalancer.notify");

        if (logger.isDebugEnabled()) {
            logger.debug("report counters sent to load balancer");
        }
    }
}
