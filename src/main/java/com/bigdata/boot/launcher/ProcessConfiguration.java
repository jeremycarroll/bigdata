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
package com.bigdata.boot.launcher;

import java.util.ArrayList;
import java.util.List;

class ProcessConfiguration {

    public final String tag;
    public final String cls;
    public final ConfigurationPropertyMap properties;

    /**
     * If <code>true</code>, then the associated process supports
     * communication for startup detection and graceful shutdown.
     */
    public final boolean performHandshake;

    /**
     * Maximum amount of time to wait for the associated process to
     * gracefully shut down (in milliseconds).
     */
    public final long stopTimeout;

    public final List<String> args;

    public ProcessConfiguration(String                   tag,
                                String                   cls,
                                ConfigurationPropertyMap properties,
                                boolean                  performHandshake,
                                long                     stopTimeout)
    {
        this.tag = tag;
        this.cls = cls;
        this.properties = properties;
        this.performHandshake = performHandshake;
        this.stopTimeout = stopTimeout;
        this.args = new ArrayList<String>();
    }
}
