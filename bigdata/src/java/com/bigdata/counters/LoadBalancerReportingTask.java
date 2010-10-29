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
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.LoadBalancer;
import com.bigdata.util.config.LogUtil;

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

    private ResourceManager resourceMgr;
    private IConcurrencyManager concurrencyMgr;
    private ILocalResourceManagement localResourceMgr;
    private IBigdataDiscoveryManagement discoveryMgr;
    private Logger logger;

    public LoadBalancerReportingTask
               (ResourceManager resourceMgr,
                IConcurrencyManager concurrencyMgr,
                ILocalResourceManagement localResourceMgr,
                IBigdataDiscoveryManagement discoveryMgr,
                Logger logger)
    {
        this.resourceMgr = resourceMgr;
        this.concurrencyMgr = concurrencyMgr;
        this.localResourceMgr = localResourceMgr;
        this.discoveryMgr = discoveryMgr;
        this.logger = (logger == null ? 
                       LogUtil.getLog4jLogger((this.getClass()).getName()) :
                       logger);
    }

    public void run() {
        try {
            localResourceMgr.reattachDynamicCounters
                                 (resourceMgr, concurrencyMgr);
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
//BTM
System.out.println("\n>>>>> LoadBalancerReportingTask.reportPerformanceCounters: serviceUUID = "+localResourceMgr.getServiceUUID());
        LoadBalancer lbs = discoveryMgr.getLoadBalancerService();
        if(lbs == null) {
            logger.warn
                ("cannot report counters [no load balancer service]");
System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: loadBalancerService = NULL");
            return;
        }
System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: loadBalancerService = "+lbs);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Bytes.kilobyte32 * 2);
        (localResourceMgr.getServiceCounterSet()).asXML(baos,
                                                        "UTF-8",
                                                        null);//filter
System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: CALLING loadBalancer.notify ...");
        lbs.notify(localResourceMgr.getServiceUUID(), baos.toByteArray());
System.out.println(">>>>> LoadBalancerReportingTask.reportPerformanceCounters: DONE CALLING loadBalancer.notify");

        if (logger.isDebugEnabled()) {
            logger.debug("report counters sent to load balancer");
        }
    }
}
