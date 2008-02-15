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
 * Created on Feb 14, 2007
 */

package com.bigdata.btree;

import com.bigdata.repo.BigdataRepository;
import com.bigdata.service.IDataService;

/**
 * Interface for range count and range query operations (non-batch api).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRangeQuery {

    /**
     * Return the #of entries in a half-open key range. The fromKey and toKey
     * need not be defined in the btree. This method computes the #of entries in
     * the half-open range exactly using {@link AbstractNode#indexOf(Object)}.
     * The cost is equal to the cost of lookup of the both keys.
     * <p>
     * Note: If the index supports transactional isolation then the range count
     * will be the upper bound and will include any deleted index entries that
     * have not been purged through a compacting merge.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return The #of entries in the half-open key range. This will be zero if
     *         <i>toKey</i> is less than or equal to <i>fromKey</i> in the
     *         total ordering.
     */
    public long rangeCount(byte[] fromKey, byte[] toKey);

    /**
     * Flag specifies that keys in the key range will be returned. The keys are
     * guarenteed to be made available via {@link IEntryIterator#getKey()} only
     * when this flag is given.
     */
    public static final int KEYS = 1 << 0;

    /**
     * Flag specifies that values in the key range will be returned. The values
     * are guarenteed to be made available via {@link IEntryIterator#next()} and
     * {@link IEntryIterator#getValue()} only when this flag is given.
     */
    public static final int VALS = 1 << 1;

    /**
     * Flag specifies that deleted index entries for a key are visited by the
     * iterator (by default the iterator will hide deleted index entries).
     */
    public static final int DELETED = 1 << 2;

    /**
     * Shorthand for {@link #KEYS} and {@link #VALS} and {@link #DELETED}.`
     */
    public static final int ALL = (KEYS | VALS | DELETED);
    
    /**
     * Flag specifies that entries visited by the iterator in the key range will
     * be <em>removed</em> from the index. This flag may be combined with
     * {@link #KEYS} or {@link #VALS} in order to return the keys and/or values
     * for the deleted entries.
     * <p>
     * Note: This semantics of this flag require that the entries are atomically
     * removed within the isolation level of the operation. In particular, if
     * the iterator is running against an {@link IDataService} using an
     * unisolated view then the entries MUST be buffered and removed as the
     * {@link ResultSet} is populated.
     * <p>
     * Note: The {@link BigdataRepository#deleteHead(String, int)} relies on
     * this atomic guarentee.
     * 
     * @todo define rangeRemove(fromKey,toKey,filter)? This method would return
     *       the #of items matching the optional filter that were deleted. It
     *       will be a parallelizable operation since it does not specify a
     *       limit on the #of items to be removed and does not return any data
     *       or metadata (other than the count) for the deleted items.
     *       <p>
     *       Note: We still need {@link #REMOVEALL} since it provides an atomic
     *       remove with return of an optionally limited #of matching index
     *       entries. This makes it ideal for creating certain kinds of queue
     *       constructions.
     */
    public static final int REMOVEALL = 1 << 4;

    /**
     * The flags that should be used by default ({@link #KEYS},{@link #VALS})
     * in contexts where the flags are not explicitly specified by the
     * appliction such as {@link #rangeIterator(byte[], byte[])}.
     */
    public static final int DEFAULT = KEYS | VALS;

    /**
     * Return an iterator that visits the entries in a half-open key range.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @see #entryIterator(), which visits all entries in the btree.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see EntryFilter, which may be used to filter the entries visited by the
     *      iterator.
     * 
     * @todo define behavior when the toKey is less than the fromKey.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey);

    /**
     * Designated variant (the one that gets overriden) for an iterator that
     * visits the entries in a half-open key range.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * @param capacity
     *            The #of entries to buffer at a time. This is a hint and MAY be
     *            zero (0) to use an implementation specific <i>default</i>
     *            capacity. The capacity is intended to limit the burden on the
     *            heap imposed by the iterator if it needs to buffer data, e.g.,
     *            before sending it across a network interface.
     * @param flags
     *            A bitwise OR of {@link #KEYS} and/or {@link #VALS} determining
     *            whether the keys or the values or both will be visited by the
     *            iterator.
     * @param filter
     *            An optional filter and/or resolver.
     * 
     * @see #entryIterator(), which visits all entries in the btree.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see EntryFilter, which may be used to filter the entries visited by the
     *      iterator.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IEntryFilter filter);

    // @todo removeAll() could be added, but the problem is that we often want
    // the keys or values of the deleted entries, at which point you have to
    // use the rangeIterator anyway.
//    /**
//     * Removes all entries in the key range from the index. When running on a
//     * scale-out index, this operation is atomic for each index partition. The
//     * operation may be used to build queue-like constructed by atomic delete of
//     * the first item in a key range. This operation is parallelized across
//     * index partitions when no limit is specified and serialized across index
//     * partitions when a limit is specified.
//     * 
//     * @param fromKey
//     *            The first key that will be visited (inclusive). When
//     *            <code>null</code> there is no lower bound.
//     * @param toKey
//     *            The first key that will NOT be visited (exclusive). When
//     *            <code>null</code> there is no upper bound.
//     * @param limit
//     *            When non-zero, this is the maximum #of entries that will be
//     *            removed. When zero (0L) all index entries in the key range
//     *            will be removed.
//     * 
//     * @return The #of index entries that were removed.
//     */
//    public long removeAll(byte[] fromKey,byte[] toKey, long limit);
    
    // /**
    // * Interface
    // *
    // * @todo alternative is to define an interface to recognize change in the
    // * "logical row". This way the sense of the limit/capacity is
    // * unchanged but we only would count logical rows rather than visited
    // * index entries.
    // *
    // * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
    // Thompson</a>
    // * @version $Id$
    // */
    // public static interface IRangeQueryLimit extends Serializable {
    //        
    // public void report(IEntryIterator itr);
    //        
    // public boolean isDone();
    //        
    // }
    //    
    // // @todo Externalizable impl.
    // public static class RangeQueryLimit implements IRangeQueryLimit {
    //
    // /**
    // *
    // */
    // private static final long serialVersionUID = 6047061818958124788L;
    //        
    // private int n = 0;
    //
    // private int limit = 0;
    //        
    // public RangeQueryLimit(int limit) {
    //            
    // if(limit<=0) throw new IllegalArgumentException();
    //            
    // this.limit = limit;
    //            
    // }
    //        
    // public void report(IEntryIterator itr) {
    //
    // n++;
    //            
    // }
    //        
    // public boolean isDone() {
    //
    // return n >= limit;
    //            
    // }
    //
    // }

}
