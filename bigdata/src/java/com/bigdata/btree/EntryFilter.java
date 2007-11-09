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

package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.isolation.IValue;

/**
 * Base class used to filter objects in an {@link EntryIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class EntryFilter implements Serializable {

    protected final Object state;

    public EntryFilter() {
        
        this( null );
        
    }

    /**
     * Constructor initializes a user-defined object that will be available
     * during {@link #isValid()} tests.
     * 
     * @param state
     *            The user defined object.
     */
    public EntryFilter(Object state) {
        
        this.state = state;
        
    }

    /**
     * Return true iff the value should be visited.
     * 
     * @param value
     *            A value that is being considered by the iterator for
     *            visitation. 
     * @return
     */
    abstract public boolean isValid(Object value);
    
    /**
     * Resolve the value that the iterator would visit. This can be used to
     * return an application value encapsulated by an {@link IValue}, to
     * de-serialize application values, etc. The default implementation is a
     * NOP. This method is applied <em>after</em> {@link #isValid(Object)}.
     * 
     * @param value
     *            The value that would be visited.
     * 
     * @return The value that will be visited.
     */
    public Object resolve(Object value) {
        
        return value;
        
    }
    
}