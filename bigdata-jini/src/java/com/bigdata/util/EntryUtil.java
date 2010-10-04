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

package com.bigdata.util;

import net.jini.core.entry.Entry;

/**
 * Class that contains one or more convenient utility methods intended
 * to provide general-purpose functions for processing sets of Jini
 * attributes; where a Jini <i>attribute</i> is an instance of the
 * <code>net.jini.core.entry.Entry</code> marker interface.
 *
 * @see net.jini.core.entry.Entry
 * @see com.sun.jini.lookup.entry.LookupAttributes
 */
public class EntryUtil {

    /** 
     * Examines the given array of entries and returns the first 
     * element encountered that is an instance of the given class
     * type. If no element in the array is an instance of the given
     * type, then <code>null</code> is returned.
     */
    public static <T extends Entry> T getEntryByType(Entry[] attrs,
                                                     Class<T> type)
    {
        if( (attrs == null) || (type == null) ) return null;
        for(int i=0; i<attrs.length; i++) {
            if(type.isInstance(attrs[i]))
                return type.cast(attrs[i]);
        }
        return null;
    }
}
