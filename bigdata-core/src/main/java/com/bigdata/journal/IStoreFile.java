/*

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

package com.bigdata.journal;

import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IRawStore;

import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.journal.IJournal;

/**
 *  An {@link IRawStore} that exposes additional serializable metadata allowing it to be managed by an
 * {@link IResourceManager}.  Currently this is common interface between the two main store
 *  file types that are used to store btree indices.
 * <b />
 * The methods on this interface have been moved from <code>IRawStore</code> in order to reduce the burden on
 * rawstore implementations which we often needed to stub methods out even though they were never used.
 * It also improves encapsulation by removing direct knowledge of resource management related classes in the rawstore
 * package.
 *
 * {@see IndexSegmentStore}
 * {@see IJournal}  
 */
public interface IStoreFile extends IRawStore {

     /**
     * A description of this store in support of the scale-out architecture.
     */
    public IResourceMetadata getResourceMetadata();
}
