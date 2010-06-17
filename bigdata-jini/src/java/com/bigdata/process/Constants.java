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
package com.bigdata.process;

import com.bigdata.service.ProcessManager;

/**
 * Constants shared by classes in this service implementation's package.
 */
public interface Constants {

    Class  SERVICE_TYPE = ProcessManager.class;
    String SERVICE_NAME = SERVICE_TYPE.getSimpleName();

    /* Name of this component; used in config entry retrieval and the logger.*/
    String COMPONENT_NAME = ((Constants.class).getPackage()).getName();
    String F_SEP          = System.getProperty("file.separator");

    // For wild carding
    String[] NODE_TYPES = 
        new String[] {"storage", "infrastructure", "standalone" };

    String[] NODE_LAYOUTS =
        new String[] { "1-of-1",
                       "1-of-2", "2-of-2",
                       "1-of-3", "2-of-3", "3-of-3",
                       "1-of-5", "2-of-5", "3-of-5", "4-of-5", "5-of-5" };
   String[] NODE_ROLES = new String[] { "bigdata" };
}
