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
package com.bigdata.rdf.rio;

import com.bigdata.rdf.inf.ClosureStats;

/**
 * Used to report statistics when loading data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadStats {

    public long toldTriples;
    public long loadTime;
    public long commitTime;
    public long totalTime;
    
    /**
     * Used iff the closure is computed as the data are loaded.
     */
    public final ClosureStats closureStats = new ClosureStats();

    public long triplesPerSecond() {

        return ((long) (((double) toldTriples) / ((double) totalTime) * 1000d));

    }

    public void add(LoadStats stats) {

        toldTriples += stats.toldTriples;

        loadTime += stats.loadTime;

        commitTime += stats.commitTime;

        totalTime += stats.totalTime;

        if (stats.closureStats != null) {

            closureStats.add(stats.closureStats);

        }
        
    }
    
    /**
     * Human readable representation.
     */
    public String toString() {

        return toldTriples
                + " stmts added in "
                + ((double) loadTime)
                / 1000d
                + " secs, rate= "
                + triplesPerSecond()
                + ", commitLatency="
                + commitTime
                + "ms"
                + (closureStats.elapsed!=0L? "\n"+closureStats.toString() : "");

    }
    
}
