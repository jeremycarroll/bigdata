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
 * Created on Aug 2, 2008
 */

package com.bigdata.btree.filter;

import java.io.Serializable;

import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;

import cutthecrap.utils.striterators.Striterator;

/**
 * Interface allows instances of a {@link Striterator} (stackable streaming
 * iterator) to be assembled and executed remotely.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements visited by the source iterator.
 * 
 * @see ITupleIterator
 * @see ITupleCursor
 * @see TupleFilter
 * @see Striterator
 */
public interface IFilterConstructor<E> extends Serializable {

    /**
     * Construct a iterator stack.
     * 
     * @param src
     *            The source iterator.
     * 
     * @return The iterator stack.
     */
    ITupleIterator<E> newInstance(ITupleIterator<E> src);
    
}
