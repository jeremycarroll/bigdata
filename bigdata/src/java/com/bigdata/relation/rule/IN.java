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
 * Created on Jun 17, 2008
 */

package com.bigdata.relation.rule;

import java.util.Arrays;
import java.util.HashSet;


/**
 * A constraint that a variable may only take on the bindings enumerated by some
 * set.
 * 
 * @todo This uses binary search, which is thread-safe. It could aslo use a
 *       {@link HashSet}, but the {@link HashSet} needs to be thread-safe since
 *       the filter could be applied concurrently during evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IN<T> implements IConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 5805883429399100605L;

    private final IVariable<T> x;
    
    private final T[] set;

    /**
     * 
     * @param x
     *            Some variable.
     * @param set
     *            A set of legal term identifiers providing a constraint on the
     *            allowable values for that variable.
     */
    public IN(final IVariable<T> x, final IConstant<T>[] set) {

        if (x == null || set == null)
            throw new IllegalArgumentException();

        if (set.length == 0)
            throw new IllegalArgumentException();
        
        this.x = x;
        
        // allocate an array of the correct type.
        this.set = (T[]) java.lang.reflect.Array.newInstance(set[0].getClass(),
                set.length);

        for (int i = 0; i < set.length; i++) {

            this.set[i] = set[i].get();
            
        }
        
        Arrays.sort(this.set);
        
    }
    
    public boolean accept(IBindingSet bindingSet) {
        
        // get binding for "x".
        final IConstant<T> x = bindingSet.get(this.x);

        if (x == null) {

            // not yet bound.
            return true;
            
        }

        final T v = x.get();

        // lookup the bound value in the set of values.
        final int pos = Arrays.binarySearch(set, v);
        
        // true iff the bound value was found in the set.
        return pos >= 0;

   }

}
