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

package com.bigdata.service;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

public class IDataServiceOnlyFilter implements ServiceItemFilter  {

    public boolean check(ServiceItem item) {
        if((item == null) || (item.service == null)) {
            return false;
        }
        Class serviceType = (item.service).getClass();
        boolean isIDataService = 
             (IDataService.class).isAssignableFrom(serviceType);
        if( !isIDataService ) return false;
        boolean isIMetadataService = 
             (IMetadataService.class).isAssignableFrom(serviceType);
        return (isIDataService && !isIMetadataService);
    }
}
