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
package com.bigdata.test.util;

import java.util.Locale;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;

/**
 * A utility class for utility methods useful to unit and integration tests.
 * 
 * @todo This may need to be moved into the unit test component and a dependency
 *       on that component added to the integration tests.
 *       
 * @author blevine
 *
 */
public class Util
{
    /**
     * Used to unbox an application key (convert it to an unsigned byte[]).
     */
    static private final IKeyBuilder _keyBuilder = KeyBuilder.newUnicodeInstance();
    
    /**
     * Utility method converts an application key to a sort key (an unsigned
     * byte[] that imposes the same sort order).
     * <p>
     * Note: This method is thread-safe.
     * <p>
     * Note: Strings are Unicode safe for the default locale. See
     * {@link Locale#getDefault()}. If you require a specific local or different
     * locals at different times or for different indices then you MUST
     * provision and apply your own {@link KeyBuilder}.
     * <p>
     * Note: This method circumvents explicit configuration of the
     * {@link KeyBuilder} and is used nearly exclusively by unit tests. While
     * explicit configuration is not required for keys which do not include
     * Unicode sort key components, this method also relies on a single global
     * {@link KeyBuilder} instance protected by a lock. That lock is therefore a
     * bottleneck. The correct practice is to use thread-local or per task
     * {@link IKeyBuilder}s to avoid lock contention.
     * 
     * This method is cloned from a method of the same name in <code>TestKeyBuilder</code>.
     * Moving it into a utility class to remove a direct dependency from one test on
     * another.
     * 
     * @param val
     *            An application key.
     * 
     * @return The unsigned byte[] equivalent of that key. This will be
     *         <code>null</code> iff the <i>key</i> is <code>null</code>. If the
     *         <i>key</i> is a byte[], then the byte[] itself will be returned.
     */
    public static final byte[] asSortKey(final Object val)
    {

        if (val == null)
        {
            return null;
        }

        if (val instanceof byte[])
        {
            return (byte[]) val;
        }

        /*
         * Synchronize on the keyBuilder to avoid concurrent modification of its
         * state.
         */

        synchronized (_keyBuilder)
        {
            return _keyBuilder.getSortKey(val);
        }
    }
}
