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

package com.bigdata.counters.httpd;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.LinkedHashMap;
import java.util.Vector;

/**
 * Overrides the method <code>doGet</code> in 
 * <code>CounterSetHTTPD</code>.
 */
public class HttpReportingServer extends CounterSetHTTPD {

    private ResourceManager resourceMgr;
    private IConcurrencyManager concurrencyMgr;
    private ILocalResourceManagement localResourceMgr;
    private Logger logger;

    public HttpReportingServer
                 (final int port,
                  final ResourceManager resourceMgr,
                  final IConcurrencyManager concurrencyMgr,
                  final ILocalResourceManagement localResourceMgr,
                        Logger logger)
               throws IOException
    {
        super(port, localResourceMgr.getServiceCounterSet());
        this.resourceMgr = resourceMgr;
        this.concurrencyMgr = concurrencyMgr;
        this.localResourceMgr = localResourceMgr;
        this.logger = (logger == null ? 
                       LogUtil.getLog4jLogger((this.getClass()).getName()) :
                       logger);
    }

    @Override
    public Response doGet(String uri,
                          String method,
                          Properties header,
                          LinkedHashMap<String,
                          Vector<String>> parms)
                        throws Exception
    {
        try {
            localResourceMgr.reattachDynamicCounters
                                 (resourceMgr, concurrencyMgr);
        } catch (Exception e) {
            // Usually because the live journal has been
            // concurrently closed during the request.
            logger.error
                ("failure on dynamic counter reattachment ["+e+"]", e);
        }
        return super.doGet(uri, method, header, parms);
    }
}
