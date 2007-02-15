/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 15, 2006
 */
package com.bigdata.objndx;

import java.io.Serializable;
import java.util.NoSuchElementException;

import com.bigdata.isolation.IValue;

/**
 * Visits the values of a {@link Leaf} in the external key ordering. There is
 * exactly one value per key for a leaf node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EntryIterator implements IEntryIterator {

    private final Leaf leaf;

    private final Tuple tuple;

    private final EntryFilter filter;
    
    private int index = 0;

    private int lastVisited = -1;

//    private final byte[] fromKey;
//    
//    private final byte[] toKey;

    // first index to visit.
    private final int fromIndex;

    // first index to NOT visit.
    private final int toIndex;
    
    public EntryIterator(Leaf leaf) {

        this(leaf, null, null, null, null);

    }

    public EntryIterator(Leaf leaf, Tuple tuple) {

        this(leaf, tuple, null, null, null);

    }

    public EntryIterator(Leaf leaf, Tuple tuple, byte[] fromKey, byte[] toKey) {

        this(leaf, tuple, fromKey, toKey, null);

    }
    
    /**
     * 
     * @param leaf
     *            The leaf whose entries will be traversed.
     * @param tuple
     *            Used to hold the output values.
     * @param fromKey
     *            The first key whose entry will be visited or <code>null</code>
     *            if the lower bound on the key traversal is not constrained.
     * @param toKey
     *            The first key whose entry will NOT be visited or
     *            <code>null</code> if the upper bound on the key traversal is
     *            not constrained.
     * @param filter
     *            An optional filter used to test and exclude elements from the
     *            iteration.
     * 
     * @exception IllegalArgumentException
     *                if fromKey is given and is greater than toKey.
     */
    public EntryIterator(Leaf leaf, Tuple tuple, byte[] fromKey, byte[] toKey,
            EntryFilter filter) {

        assert leaf != null;

        this.leaf = leaf;
        
        this.tuple = tuple; // MAY be null.

//        this.fromKey = fromKey; // may be null (no lower bound).
//        
//        this.toKey = toKey; // may be null (no upper bound).

        this.filter = filter; // MAY be null.
        
        { // figure out the first index to visit.

            int fromIndex;

            if (fromKey != null) {

                fromIndex = leaf.keys.search(fromKey);

                if (fromIndex < 0) {

                    fromIndex = -fromIndex - 1;

                }

            } else {

                fromIndex = 0;

            }

            this.fromIndex = fromIndex;

        }

        { // figure out the first index to NOT visit.

            int toIndex;

            if (toKey != null) {

                toIndex = leaf.keys.search(toKey);

                if (toIndex < 0) {

                    toIndex = -toIndex - 1;

                }

            } else {

                toIndex = leaf.nkeys;

            }

            this.toIndex = toIndex;

        }

        if (fromIndex > toIndex) {
            
            throw new IllegalArgumentException("fromKey > toKey");
            
        }
        
        // starting index is the lower bound.
        index = fromIndex;
        
    }

    public boolean hasNext() {

        if(filter == null) {
            
            return index >= fromIndex && index < toIndex;
            
        }

        while(index >= fromIndex && index < toIndex) {
         
            if (filter.isValid(leaf.values[index])) {
                
                return true;
                
            }
            
            index++;

        }
        
        return false;
        
    }

    public Object next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;
        
        if( tuple != null ) {

            /*
             * eagerly set the key/value on the tuple for a side-effect style
             * return.
             */
            tuple.key = leaf.keys.getKey(lastVisited);
            
            tuple.val = filter == null ? leaf.values[lastVisited] : filter
                    .resolve(leaf.values[lastVisited]);
            
            return tuple.val;
            
        }
        
        return filter == null ? leaf.values[lastVisited] : filter
                .resolve(leaf.values[lastVisited]);
        
    }

    public Object getValue() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return filter == null ? leaf.values[lastVisited] : filter
                .resolve(leaf.values[lastVisited]);
        
    }
    
    public byte[] getKey() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return leaf.keys.getKey(lastVisited);
        
    }
    
    /**
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    /**
     * Base class used to filter objects in an {@link EntryIterator}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract static class EntryFilter implements Serializable {

        protected final Object state;

        public EntryFilter() {
            this.state = null;
        }

        /**
         * Constructor initializes a user-defined object that will be available
         * during {@link #isValid()} tests.
         * 
         * @param state
         *            The user defined object.
         */
        public EntryFilter(Object state) {
            this.state = state;
        }

        /**
         * Return true iff the value should be visited.
         * 
         * @param value
         *            A value that is being considered by the iterator for
         *            visitation. 
         * @return
         */
        abstract public boolean isValid(Object value);
        
        /**
         * Resolve the value that the iterator would visit This can be used to
         * return an application value encapsulated by an {@link IValue}, to
         * de-serialize application values, etc. The default implementation is a
         * NOP. This method is applied <em>after</em> {@link #isValid(Object)}.
         * 
         * @param value
         *            The value that would be visited.
         * 
         * @return The value that will be visited.
         */
        public Object resolve(Object value) {
            
            return value;
            
        }
        
    }

}
