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
 * Created on Jun 24, 2008
 */

package com.bigdata.relation.rule.eval;

import java.io.Serializable;

import com.bigdata.btree.ITupleSerializer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;

/**
 * Flyweight implementation.
 * 
 * FIXME smart and compact serialization! Reuse the {@link ITupleSerializer} for
 * the elements and provide compact serialization for the binding sets as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Solution<E> implements ISolution<E>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 5477431033714540993L;
    
    private final E e;
    private final IRule rule;
    private final IBindingSet bindingSet;
    
    /**
     * Constructs the element iff requested, saves the rule reference iff
     * requested, and clones and saves the bindingSet iff requested. The
     * requested behavior depends on {@link IJoinNexus#solutionFlags()}. When
     * requested, the element is created using
     * {@link IJoinNexus#newElement(com.bigdata.relation.rule.IPredicate, IBindingSet)}.
     * 
     * @param e
     *            The element.
     * @param rule
     *            The rule.
     * @param bindingSet
     *            This MUST be a <strong>copy</strong> of the binding set that
     *            will NOT be modified by further execution of the rule.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     */
    @SuppressWarnings("unchecked")
    public Solution(IJoinNexus joinNexus, IRule rule, IBindingSet bindingSet) {
        
        if (joinNexus == null)
            throw new IllegalArgumentException();
        
        if (rule == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();

        final int flags = joinNexus.solutionFlags();

        if ((flags & IJoinNexus.ELEMENT) != 0) {

            this.e = (E) joinNexus.newElement(rule.getHead(), bindingSet);

        } else {

            this.e = null;

        }

        if ((flags & IJoinNexus.BINDINGS) != 0) {

            this.bindingSet = bindingSet.clone();

        } else {

            this.bindingSet = null;

        }

        if ((flags & IJoinNexus.RULE) != 0) {

            this.rule = rule;

        } else {

            this.rule = null;

        }

    }
    
    public E get() {
        
        return e;
        
    }

    public IRule getRule() {

        return rule;
        
    }

    public IBindingSet getBindingSet() {

        return bindingSet;
        
    }

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("Solution");
        
        sb.append("{ element="+e);
        
        sb.append(", rule="+rule);
        
        sb.append(", bindingSet="+bindingSet);
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
}
