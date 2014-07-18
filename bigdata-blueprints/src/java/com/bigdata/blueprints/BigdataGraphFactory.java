/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.blueprints;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailFactory;


/**
 * Helper class to create BigdataGraph instances.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphFactory  {

    protected static final transient Logger log = Logger.getLogger(BigdataGraphFactory.class);
    
    /**
     * Connect to a remote bigdata instance.
     */
    public static BigdataGraph connect(final String host, final int port) {
        return connect("http://"+host+":"+port);
    }
    
    /**
     * Connect to a remote bigdata instance.
     */
    public static BigdataGraph connect(final String serviceEndpoint) {
        if (serviceEndpoint.endsWith("/bigdata/sparql")) {
            return new BigdataGraphClient(serviceEndpoint);
        } else if (serviceEndpoint.endsWith("/bigdata/")) {
            return new BigdataGraphClient(serviceEndpoint + "sparql");
        } else if (serviceEndpoint.endsWith("/bigdata")) {
            return new BigdataGraphClient(serviceEndpoint + "/sparql");
        } else if (serviceEndpoint.endsWith("/")) {
            return new BigdataGraphClient(serviceEndpoint + "bigdata/sparql");
        } else {
            return new BigdataGraphClient(serviceEndpoint + "/bigdata/sparql");
        }
    }

    /**
     * Open an existing persistent local bigdata instance.  If a journal does
     * not exist at the specified location and the boolean create flag is true
     * a journal will be created at that location.
     */
    public static BigdataGraph open(final String file, final boolean create) throws Exception {
        final BigdataSail sail = BigdataSailFactory.openSail(file, create);
        sail.initialize();
        return new BigdataGraphEmbedded(sail);
    }

    /**
     * Create a new local in-memory bigdata instance.
     */
    public static BigdataGraph create() throws Exception {
        final BigdataSail sail = BigdataSailFactory.createSail();
        sail.initialize();
        return new BigdataGraphEmbedded(sail);
    }
    
//    /**
//     * Create a new persistent local bigdata instance.
//     */
//    public static BigdataGraph create(final String file) 
//            throws Exception {
//        final BigdataSail sail = BigdataSailFactory.createSail(file);
//        sail.initialize();
//        return new BigdataGraphEmbedded(sail);
//    }
    
}
