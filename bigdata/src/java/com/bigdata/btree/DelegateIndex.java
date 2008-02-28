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
 * Created on Feb 20, 2008
 */

package com.bigdata.btree;

import com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor;
import com.bigdata.mdi.IResourceMetadata;

/**
 * An object that delegates its {@link IIndex} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DelegateIndex implements IIndex {

    private final IIndex delegate;
    
    /**
     * @param delegate
     *            The delegate.
     */
    public DelegateIndex(IIndex delegate) {

        if (delegate == null) {

            throw new IllegalArgumentException();
            
        }
        
        this.delegate = delegate;
        
    }

    public boolean contains(byte[] key) {
        return delegate.contains(key);
    }

    public ICounter getCounter() {
        return delegate.getCounter();
    }

    public IndexMetadata getIndexMetadata() {
        return delegate.getIndexMetadata();
    }

    public IResourceMetadata[] getResourceMetadata() {
        return delegate.getResourceMetadata();
    }

    public String getStatistics() {
        return delegate.getStatistics();
    }

    public byte[] insert(byte[] key, byte[] value) {
        return delegate.insert(key, value);
    }

    public byte[] lookup(byte[] key) {
        return delegate.lookup(key);
    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {
        return delegate.rangeCount(fromKey, toKey);
    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey, int capacity, int flags, ITupleFilter filter) {
        return delegate.rangeIterator(fromKey, toKey, capacity, flags, filter);
    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return delegate.rangeIterator(fromKey, toKey);
    }

    public byte[] remove(byte[] key) {
        return delegate.remove(key);
    }

    public void submit(byte[] fromKey, byte[] toKey, IIndexProcedure proc, IResultHandler handler) {
        delegate.submit(fromKey, toKey, proc, handler);
    }

    public Object submit(byte[] key, IIndexProcedure proc) {
        return delegate.submit(key, proc);
    }

    public void submit(int n, byte[][] keys, byte[][] vals, IIndexProcedureConstructor ctor, IResultHandler handler) {
        delegate.submit(n, keys, vals, ctor, handler);
    }

}
