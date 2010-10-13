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
 * Created on Feb 16, 2007
 */

package com.bigdata.btree;

import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.counters.ICounterSet;
import java.util.Iterator;

import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.journal.IResourceManager;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

/**
 * A fly-weight wrapper that does not permit write operations and reads through
 * onto an underlying {@link IIndex}.
 * <p>
 * Note: use this class sparingly. An index loaded from a historical commit
 * point will always be read-only.
 * 
 * @see {@link IResourceManager#getIndex(String, long)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ReadOnlyIndex implements IIndex {

    private IIndex src;
    
    public ReadOnlyIndex(IIndex src) {
        this.src = src;
    }

    /** {@link IndexMetadata} is cloned to disallow modification. */
    final public IndexMetadata getIndexMetadata() {

        return src.getIndexMetadata().clone();
        
    }

    /**
     * {@link IResourceMetadata}[] is cloned to disallow modification (the
     * {@link IResourceMetadata} instances in the array are all dynamically
     * created so changes to them do not propagate back to the index).
     */
    final public IResourceMetadata[] getResourceMetadata() {

        return src.getResourceMetadata().clone();
        
    }

    /**
     * Counter is read-only.
     */
    final public ICounter getCounter() {

        return new ReadOnlyCounter(src.getCounter());
        
    }
    
    /**
     * Disabled.
     */
    final public byte[] insert(byte[] key, byte[] value) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Disabled.
     */
    final public byte[] remove(byte[] key) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * {@link IRangeQuery#REMOVEALL} and {@link Iterator#remove()} are disabled.
     */
    final public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilterConstructor filter) {

        if ((flags & REMOVEALL) != 0) {

            /*
             * Note: Must be explicitly disabled!
             */
            
            throw new UnsupportedOperationException();
            
        }

        /*
         * Must explicitly disable Iterator#remove().
         */
        return new ReadOnlyEntryIterator(src.rangeIterator(fromKey, toKey,
                capacity, flags, filter));
        
    }
    
    /**
     * Overriden to ensure that procedure is applied against read-only view and
     * not the {@link DelegateIndex}.
     */
    final public Object submit(byte[] key, IIndexProcedure proc) {
    
        return proc.apply(this);
        
    }

    /**
     * Overriden to ensure that procedure is applied against read-only view and
     * not the {@link DelegateIndex}.
     */
    @SuppressWarnings("unchecked")
    final public void submit(byte[] fromKey, byte[] toKey,
            final IIndexProcedure proc, final IResultHandler handler) {

        final Object result = proc.apply(this);
        
        if (handler != null) {
            
            handler.aggregate(result, new Split(null,0,0));
            
        }
        
    }
    
    /**
     * Overriden to ensure that procedure is applied against read-only view and
     * not the {@link DelegateIndex}.
     */
    @SuppressWarnings("unchecked")
    final public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractKeyArrayIndexProcedureConstructor ctor, IResultHandler aggregator) {

        final Object result = ctor.newInstance(this,fromIndex, toIndex, keys, vals)
                .apply(this);
        
        if(aggregator != null) {
            
            aggregator.aggregate(result, new Split(null,fromIndex,toIndex));
            
        }
        
    }

    public ICounterSet getCounters() {
        return src.getCounters();
    }

    public Object submit(byte[] key, ISimpleIndexProcedure proc) {
        return src.submit(key, proc);
    }

    public void submit(byte[] fromKey, byte[] toKey,
                       IKeyRangeIndexProcedure proc, IResultHandler handler) {
        src.submit(fromKey, toKey, proc, handler);
    }

    public byte[] lookup(byte[] key) {
        return src.lookup(key);
    }

    public boolean contains(byte[] key) {
        return src.contains(key);
    }

    public Object insert(Object key, Object value) {
        return src.insert(key, value);
    }

    public Object lookup(Object key) {
        return src.lookup(key);
    }

    public boolean contains(Object key) {
        return src.contains(key);
    }

    public Object remove(Object key) {
        return src.remove(key);
    }

    public long rangeCount() {
        return src.rangeCount();
    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {
        return src.rangeCount(fromKey, toKey);
    }

    public long rangeCountExact(byte[] fromKey, byte[] toKey) {
        return src.rangeCountExact(fromKey, toKey);
    }

    public long rangeCountExactWithDeleted(byte[] fromKey, byte[] toKey) {
        return src.rangeCountExactWithDeleted(fromKey, toKey);
    }

    public ITupleIterator rangeIterator() {
        return src.rangeIterator();
    }

    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        return src.rangeIterator(fromKey, toKey);
    }

}
