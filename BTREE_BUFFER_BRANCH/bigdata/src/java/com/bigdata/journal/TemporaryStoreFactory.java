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
/*
 * Created on Sep 3, 2008
 */

package com.bigdata.journal;

import java.io.File;
import java.lang.ref.WeakReference;

import com.bigdata.rawstore.Bytes;

/**
 * Helper class for {@link IIndexStore#getTempStore()}. This class is very
 * light weight.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStoreFactory {

    private WeakReference<TemporaryStore> ref = null;
    
    private final long maxExtent;

    /**
     * The default maximum extent ({@value #DEFAULT_MAX_EXTENT}). A new
     * {@link TemporaryStore} will be created by {@link #getTempStore()} when
     * the extent of the current {@link TemporaryStore} reaches this value.
     * However, the temporary store will continue to grow as long as there are
     * execution contexts which retain a reference to that instance.
     * <p>
     * Note: Each file system has its own limits on the maximum size of a file.
     * FAT16 limits the maximum file size to only 2G. FAT32 supports 4G files.
     * NTFS and most un*x file systems support 16G+ files. A safe point for
     * allocating a new temporary store for new requests is therefore LT the
     * smallest maximum file size supported by any of the common file systems.
     * <p>
     * A temporary store that reaches the maximum size allowed for the file
     * system will fail when a request is made to extend that file. How that
     * effects processing depends of course on the purpose to which the
     * temporary store was being applied. E.g., to buffer a transaction, to
     * perform truth maintenance, etc.
     * 
     * @todo If we had more visibility into the file system for a given logical
     *       disk then we could apply that information to dynamically set the
     *       cutover point for the temporary store based on the backing file
     *       system. Unfortunately, {@link File} does not provide us with that
     *       information.
     */
    protected static final long DEFAULT_MAX_EXTENT = 1 * Bytes.gigabyte;
    
    public TemporaryStoreFactory() {

        this(DEFAULT_MAX_EXTENT);
        
    }

    /**
     * 
     * @param maxExtent
     *            The maximum extent of the current {@link TemporaryStore}
     *            before {@link #getTempStore()} will return a new
     *            {@link TemporaryStore}.
     * 
     * @throws IllegalArgumentException
     *             if <i>maxExtent</i> is negative (zero is allowed and will
     *             cause each request to return a distinct
     *             {@link TemporaryStore}).
     */
    public TemporaryStoreFactory(final long maxExtent) {
        
        if (maxExtent < 0L)
            throw new IllegalArgumentException();
        
        this.maxExtent = maxExtent;
        
    }
    
    synchronized public TemporaryStore getTempStore() {

        TemporaryStore t = ref == null ? null : ref.get();

        if (t == null || t.getBufferStrategy().getExtent() > maxExtent) {

            t = new TemporaryStore();

            ref = new WeakReference<TemporaryStore>(t);

        }

        return t;
    }

}
