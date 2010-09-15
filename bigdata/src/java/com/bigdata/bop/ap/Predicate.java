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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop.ap;

import java.util.Map;

import com.bigdata.bop.AbstractChunkedOrderedIteratorOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * A generic implementation of an immutable {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Predicate<E> extends AbstractChunkedOrderedIteratorOp<E> implements
        IPredicate<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends IPredicate.Annotations {
        
    }

    /**
     * Required shallow copy constructor.
     */
    public Predicate(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public Predicate(final Predicate<E> op) {
        super(op);
    }

    /**
     * Simplified ctor.
     * 
     * @param values
     *            The values (order is important!).
     * @param relationName
     *            Identifies the relation to be queried.
     */
    public Predicate(final IVariableOrConstant<?>[] values,
            final String relationName) {

        this(values, relationName, -1/* partitionId */, false/* optional */,
                null/* constraint */, null/* expander */);

    }

    /**
     * 
     * @param relationName
     *            The namespace of the relation.
     * @param partitionId
     *            The index partition constraint -or- <code>-1</code> if there
     *            is no index partition constraint.
     * @param values
     *            The values (order is important!).
     * @param optional
     *            true iff the predicate is optional when evaluated in a JOIN.
     * @param constraint
     *            An optional constraint.
     * @param expander
     *            Allows selective override of the predicate evaluation.
     */
    public Predicate(final IVariableOrConstant<?>[] values,
            final String relationName, final int partitionId,
            final boolean optional, final IElementFilter<E> constraint,
            final ISolutionExpander<E> expander) {

        this(values, NV.asMap(new NV[] {//
                new NV(Annotations.RELATION_NAME,new String[]{relationName}),//
                new NV(Annotations.PARTITION_ID,partitionId),//
                new NV(Annotations.OPTIONAL,optional),//
                new NV(Annotations.CONSTRAINT,constraint),//
                new NV(Annotations.EXPANDER,expander),//
        }));
        
        if (relationName == null)
            throw new IllegalArgumentException();

        if (partitionId < -1)
            throw new IllegalArgumentException();

        if (values == null)
            throw new IllegalArgumentException();
        
    }
    
    public String getOnlyRelationName() {
        
        final String[] relationName = (String[]) annotations.get(Annotations.RELATION_NAME);
        
        if (relationName.length != 1)
            throw new IllegalStateException();

        return relationName[0];
        
    }
    
    public String getRelationName(final int index) {

        final String[] relationName = (String[]) annotations.get(Annotations.RELATION_NAME);

        return relationName[index];
        
//        throw new UnsupportedOperationException();
        
    }

    public int getRelationCount() {
        
        final String[] relationName = (String[]) annotations.get(Annotations.RELATION_NAME);
      
        return relationName.length;
        
    }

    public Predicate<E> setRelationName(final String[] relationName) {
        
//      throw new UnsupportedOperationException();
      final Predicate<E> tmp = this.clone();

      tmp.annotations.put(Annotations.RELATION_NAME, relationName);

      return tmp;
      
  }

    public int getPartitionId() {
        
        return (Integer)annotations.get(Annotations.PARTITION_ID);
        
    }
    
    @SuppressWarnings("unchecked")
    public IVariableOrConstant get(final int index) {
        
        return (IVariableOrConstant<?>) super.get(index);
        
    }

    @SuppressWarnings("unchecked")
    public IConstant<?> get(final E e, final int index) {

        return new Constant(((IElement) e).get(index));

    }

    final public boolean isOptional() {

        return (Boolean) annotations.get(Annotations.OPTIONAL);
        
    }

    @SuppressWarnings("unchecked")
    final public IElementFilter<E> getConstraint() {

        return (IElementFilter<E>) annotations.get(Annotations.CONSTRAINT);

    }

    @SuppressWarnings("unchecked")
    final public ISolutionExpander<E> getSolutionExpander() {
        
        return (ISolutionExpander<E>) annotations.get(Annotations.EXPANDER);
        
    }

    final public int getVariableCount() {

        int nvars = 0;
        
        final int arity = arity();

        for (int i = 0; i < arity; i++) {

            if (get(i) instanceof IVariable<?>)
                nvars++;
            
        }
        
        return nvars;

    }

    final public boolean isFullyBound(final IKeyOrder<E> keyOrder) {
        
        return getVariableCount(keyOrder) == 0;
        
    }

    final public int getVariableCount(final IKeyOrder<E> keyOrder) {
        int nunbound = 0;
        final int keyArity = keyOrder.getKeyArity();
        for (int keyPos = 0; keyPos < keyArity; keyPos++) {
            final int index = keyOrder.getKeyOrder(keyPos);
            final IVariableOrConstant<?> t = get(index);
            if (t == null || t.isVar()) {
                nunbound++;
            }
        }
        return nunbound;
    }
    
    public Predicate<E> asBound(final IBindingSet bindingSet) {

        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final Predicate<E> tmp = this.clone();

        /*
         * Now override any unbound variables for which we were giving bindings.
         */

        final int arity = arity();
        
        for (int i = 0; i < arity; i++) {

            final IVariableOrConstant<?> t = (IVariableOrConstant<?>) get(i);

            if (t == null) {
                /*
                 * Note: t != null handles the case where the [c] position of an
                 * SPO is allowed to be null.
                 */
                continue;
            }

            if (t.isConstant())
                continue;

            final IVariable<?> var = (IVariable<?>) t;

            final IConstant<?> val = bindingSet.get(var);

            if (val == null) {
                // still unbound.
                continue;
            }

            // bound from the binding set.
//            try {
//                final Field f = tmp.getClass().getField("args");
//                f.setAccessible(true);
//                final BOp[] targs = (BOp[]) f.get(tmp);
//                targs[i] = val.clone();
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            }
            tmp.args[i] = val.clone();

        }
        
        return tmp;
        
    }

    public Object asBound(final int index, final IBindingSet bindingSet) {

        return get(index).get(bindingSet);

//        if (bindingSet == null)
//            throw new IllegalArgumentException();
//        
//        final IVariableOrConstant<?> t = get(index);
//
//        final IConstant<?> c;
//        if (t.isVar()) {
//
//            c = bindingSet.get((IVariable<?>) t);
//
//        } else {
//
//            c = (IConstant<?>) t;
//
//        }
//
//        return c == null ? null : c.get();

    }

    @SuppressWarnings("unchecked")
    public IKeyOrder<E> getKeyOrder() {

        return (IKeyOrder<E>) annotations.get(Annotations.KEY_ORDER);

    }

    public Predicate<E> setKeyOrder(final IKeyOrder<E> keyOrder) {

        final Predicate<E> tmp = this.clone();

        tmp.annotations.put(Annotations.KEY_ORDER, keyOrder);

        return tmp;

    }
    
    @SuppressWarnings("unchecked")
    public Predicate<E> clone() {

        return (Predicate<E>) super.clone();
        
    }

    public Predicate<E> setPartitionId(final int partitionId) {

        final Predicate<E> tmp = this.clone();

        tmp.annotations.put(Annotations.PARTITION_ID, partitionId);

        return tmp;

    }

    public Predicate<E> setBOpId(final int bopId) {

        final Predicate<E> tmp = this.clone();

        tmp.annotations.put(Annotations.BOP_ID, bopId);

        return tmp;

    }

    public String toString() {
        
        return toString(null/* bindingSet */);
        
    }
    
    public String toString(final IBindingSet bindingSet) {

        final int arity = arity();
        
        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());
        
        sb.append("(");

        for (int i = 0; i < arity; i++) {

            if (i > 0)
                sb.append(", ");

            final IVariableOrConstant<?> v = get(i);

            sb.append(v == null ? null : v.isConstant() ? v.toString() : (v
                    + "=" + (bindingSet == null ? null : bindingSet
                    .get((IVariable<?>) v))));

        }

        sb.append(")");

        if (!annotations.isEmpty()) {
            sb.append("[");
            boolean first = true;
            for (Map.Entry<String, Object> e : annotations.entrySet()) {
                if (!first)
                    sb.append(", ");
                sb.append(e.getKey() + "=" + e.getValue());
                first = false;
            }
            sb.append("]");
        }
        
        return sb.toString();

    }

    public boolean equals(final Object other) {
        
        if (this == other)
            return true;

        if(!(other instanceof IPredicate<?>))
            return false;
        
        final IPredicate<?> o = (IPredicate<?>)other;
        
        final int arity = arity();
        
        if(arity != o.arity()) return false;

        for (int i = 0; i < arity; i++) {

            final IVariableOrConstant<?> x = get(i);

            final IVariableOrConstant<?> y = o.get(i);
            
            if (x != y && !(x.equals(y))) {
                
                return false;
            
            }
            
        }
        
        return true;
        
    }
    
    public int hashCode() {
        
        int h = hash;

        if (h == 0) {

            final int n = arity();

            for (int i = 0; i < n; i++) {
        
                h = 31 * h + get(i).hashCode();
                
            }
            
            hash = h;
            
        }
        
        return h;

    }

    /**
     * Caches the hash code.
     */
    private int hash = 0;

    /**
     * @todo This does not allow us to override the iterator behavior based on
     *       the annotations. It also provides expander logic for scaleup and
     *       handles reading on a shard. It ignores the {@link IKeyOrder}
     *       associated with the {@link IPredicate} and there is no way to
     *       specify the {@link IRangeQuery} flags.
     */
    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<E> eval(final IBigdataFederation<?> fed,
            final IJoinNexus joinNexus) {

        // Resolve the relation name to the IRelation object.
        final IRelation<E> relation = (IRelation<E>) joinNexus
                .getTailRelationView(this/* predicate */);

        if (relation == null)
            throw new RuntimeException("Not found: " + getOnlyRelationName());

        return joinNexus.getTailAccessPath(relation, this/* predicate */)
                .iterator();

    }

}
