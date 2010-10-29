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
/*
 * Created on Feb 17, 2007
 */

package com.bigdata.journal;

import com.bigdata.btree.IndexMetadata;
import java.util.UUID;

/**
 * Interface for managing named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexManager extends IIndexStore {

    /**
     * Register a named index; where the name under which the index will
     * registered MUST be specified by the given <code>IndexMetadata</code>.
     * 
     * @param indexMetadata
     *            The metadata describing the index.
     * 
     * @throws NullPointerException
     *             if the <code>null</code> is input for
     *             <code>indexMetadata</code> or if the name under which
     *             to register the index is not specified in the
     *             <code>indexMetadata</code> parameter.
     * 
     * @exception IndexExistsException
     *                if there is an index already registered under the name
     *                returned by <code>IndexMetadata#getName</code>. Note
     *                that <code>IIndexStore#getIndex(String)</code> can be
     *                used to determine whether there is an index already
     *                registered under a given name.
     */
    void registerIndex(IndexMetadata indexMetadata);

    /**
     * Drops the named index.
     * <p>
     * Note: Whether or not and when index resources are reclaimed is
     *       dependent on the store. For example, an immortal store will
     *       retain all historical states for all indices. Likewise, a
     *       store that uses index partitions may be able to delete index
     *       segments immediately.
     * 
     * @param name
     *            The name of the index to be dropped.
     * 
     * @exception NoSuchIndexException
     *                if <i>name</i> does not identify a registered index.
     */
    public void dropIndex(String name);   
}
