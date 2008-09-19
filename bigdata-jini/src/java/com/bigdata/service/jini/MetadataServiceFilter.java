/*

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
package com.bigdata.service.jini;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

import com.bigdata.service.IMetadataService;

/**
 * Filter only matches a service item where the {@link IMetadataService} is
 * implemented.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetadataServiceFilter implements ServiceItemFilter {

    protected static final transient Logger log = Logger
            .getLogger(MetadataServiceFilter.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    protected static final boolean DEBUG = log.isDebugEnabled();

    public static final transient ServiceItemFilter INSTANCE = new MetadataServiceFilter();
    
    private MetadataServiceFilter(){}
    
    public boolean check(ServiceItem item) {

        if(item.service==null) {
            
            log.warn("Service is null: "+item);

            return false;
            
        }
        
        if (item.service instanceof IMetadataService) {
           
            if (DEBUG)
                log.debug("Matched: " + item);
            
            return true;
            
        }

        if (DEBUG)
            log.debug("Ignoring: " + item);
        
        return false;
        
    }
    
}