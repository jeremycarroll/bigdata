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
 * Created on Jun 23, 2008
 */

package com.bigdata.relation;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IKeyOrder;

/**
 * An abstraction corresponding to a set of elements using some schema (think a
 * table containing a set of rows). Each relation is backed by one or more
 * indices and knows how to return the {@link IAccessPath} that is most
 * efficient given an {@link IPredicate} expressing a query against the
 * {@link IRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param E
 *            The generic type for the elements in the relation.
 */
public interface IRelation<E> extends ILocatableResource<IRelation<E>>{

    /**
     * The {@link IIndexManager} for the {@link IRelation}.
     */
    public IIndexManager getIndexManager();
    
    /**
     * The service used to run asynchronous or parallel tasks for the {@link IRelation}.
     */
    public ExecutorService getExecutorService();
    
//    /**
//     * The #of elements in the relation.
//     * 
//     * @param exact
//     *            When <code>true</code> an exact count is reported. An exact
//     *            count will require a key-range scan if delete markers are in
//     *            use, in which case it will be more expensive. See
//     *            {@link IRangeQuery}.
//     */
//    long getElementCount(boolean exact);

    /**
     * Return the best {@link IAccessPath} for a relation given a predicate with
     * zero or more unbound variables.
     * <p>
     * If there is an {@link IIndex} that directly corresponds to the natural
     * order implied by the variable pattern on the predicate then the access
     * path should use that index. Otherwise you should choose the best index
     * given the constraints and make sure that the {@link IAccessPath}
     * incorporates additional filters that will allow you to filter out the
     * irrelevant {@link ITuple}s during the scan - this is very important when
     * the index is remote!
     * <p>
     * If there are any {@link IElementFilter}s then the access path MUST
     * incorporate those constraints such that only elements that satisfy the
     * constraints may be visited.
     * <p>
     * Whether the constraints arise because of the lack of a perfect index for
     * the access path or because they were explicitly specified for the
     * {@link IPredicate}, those constraints should be translated into
     * constraints imposed on the underlying {@link ITupleIterator} and sent
     * with it to be evaluated local to the data.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evaluated on the data service rather than
     * materializing the elements and then filtering then. This can be
     * accomplished by adding the filter as a constraint on the predicate when
     * specifying the access path.
     * 
     * @param predicate
     *            The constraint on the elements to be visited.
     * 
     * @return The best {@link IAccessPath} for that {@link IPredicate}.
     */
    IAccessPath<E> getAccessPath(IPredicate<E> predicate);

    // @todo raise this method into this interface. 
//    IAccessPath<E> getAccessPathForIndexPartition(IIndexManager indexManager, IPredicate<E> predicate);

    /**
     * Return the fully qualified name of each index maintained by this
     * relation.
     * 
     * @return An immutable set of the index names for the relation.
     * 
     * @todo replace with getKeyOrders()?
     */
    Set<String> getIndexNames();

    /*
     * New methods.
     */
    
//    /**
//     * Return the {@link IKeyOrder}s corresponding to the registered indices for
//     * this relation.
//     */
//    Iterator<IKeyOrder<E>> getKeyOrders();
//
//    /**
//     * Return the {@link IKeyOrder} for the predicate corresponding to the
//     * perfect access path. A perfect access path is one where the bound values
//     * in the predicate form a prefix in the key space of the corresponding
//     * index.
//     * 
//     * @param p
//     *            The predicate.
//     * @return The {@link IKeyOrder} for the perfect access path -or-
//     *         <code>null</code> if there is no index which provides a perfect
//     *         access path for that predicate.
//     */
//    IKeyOrder<E> getKeyOrder(IPredicate<E> p);
//    
//    /**
//     * Return the {@link IKeyOrder} for the primary index for the relation.
//     */
//    IKeyOrder<E> getPrimaryKeyOrder();
//    
//    /**
//     * Return the primary index for the relation.
//     * 
//     * @todo how about getIndex(IKeyOrder) instead?
//     */
//    IIndex getPrimaryIndex();

    /*
     * End new methods.
     */
    
    /**
     * Create and return a new element. The element is constructed from the
     * predicate given the bindings. Typically, this is used when generating an
     * {@link ISolution} for an {@link IRule} during either a query or mutation
     * operations. The element is NOT inserted into the relation.
     * 
     * @param predicate
     *            The predicate that is the head of some {@link IRule}.
     * @param bindingSet
     *            A set of bindings for that {@link IRule}.
     * 
     * @return The new element.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     * @throws IllegalStateException
     *             if the predicate is not fully bound given those bindings.
     */
    E newElement(IPredicate<E> predicate, IBindingSet bindingSet);

    /**
     * Return the class for the generic type of this relation. This information
     * is used to dynamically create arrays of that generic type.
     */
    Class<E> getElementClass();
    
}
