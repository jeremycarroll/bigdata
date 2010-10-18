/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
/*
 * Created on Sep 1, 2010
 */

package com.bigdata.bop.engine;

/**
 * An immutable class capturing the evaluation context of an operator against a
 * shard.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BSBundle {

    public final int bopId;

    public final int shardId;

    public String toString() {
        
        return super.toString() + "{bopId=" + bopId + ",shardId=" + shardId
                + "}";
        
    }
    
    public BSBundle(final int bopId, final int shardId) {

        this.bopId = bopId;

        this.shardId = shardId;

    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {

        return (bopId * 31) + shardId;

    }

    public boolean equals(final Object o) {
        
        if (this == o)
            return true;
        
        if (!(o instanceof BSBundle))
            return false;
        
        return bopId == ((BSBundle) o).bopId
                && shardId == ((BSBundle) o).shardId;
        
    }

}
