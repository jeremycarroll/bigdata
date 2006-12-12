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

import java.util.NoSuchElementException;

/**
 * Visits the values of a {@link Leaf} in the external key ordering. There is
 * exactly one value per key for a leaf node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class EntryIterator implements KeyValueIterator {

    private final Leaf leaf;

    private final Tuple tuple;
    
    private int index = 0;

    private int lastVisited = -1;
    
    public EntryIterator(Leaf leaf) {

        this( leaf, null );
        
    }

    public EntryIterator(Leaf leaf, Tuple tuple ) {

        assert leaf != null;

        this.leaf = leaf;
        
        this.tuple = tuple; // MAY be null.

    }

    public boolean hasNext() {

        return index < leaf.nkeys;

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
            tuple.key = leaf.getKey(lastVisited);
            
            tuple.val = leaf.values[lastVisited];            
            
            return tuple.val;
            
        }
        
        return leaf.values[lastVisited];
        
    }

    public Object getValue() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return leaf.values[lastVisited];
        
    }
    
    public Object getKey() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return leaf.getKey(lastVisited);
        
    }
    
    /**
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
