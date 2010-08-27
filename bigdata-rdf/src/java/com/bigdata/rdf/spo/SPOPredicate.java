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
package com.bigdata.rdf.spo;

import java.util.Map;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.ISolutionExpander;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOPredicate extends Predicate<ISPO> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The arity is 3 unless the context position was given (as either a
     * variable or bound to a constant) in which case it is 4.
     * 
     * @todo rather than having a conditional arity, modify the SPOPredicate
     *       constructor to pass on either args[3] or args[3] depending on
     *       whether we are using triples or quads.
     */
    public final int arity() {
        
        return get(3/*c*/) == null ? 3 : 4;
        
    }

    /**
     * Required shallow copy constructor.
     */
    public SPOPredicate(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public SPOPredicate(final SPOPredicate op) {
        super(op);
    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. The
     * predicate is NOT optional. No constraint is specified. No expander is
     * specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p, final IVariableOrConstant<IV> o) {

        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
                null/* c */, false/* optional */, null/* constraint */, null/* expander */);

    }

    /**
     * Partly specified ctor. The predicate is NOT optional. No constraint is
     * specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param c
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o, final IVariableOrConstant<IV> c) {

        this(new String[] { relationName }, -1/* partitionId */, s, p, o, c,
                false/* optional */, null/* constraint */, null/* expander */);

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. The
     * predicate is NOT optional. No constraint is specified. No expander is
     * specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     */
    public SPOPredicate(final String[] relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p, final IVariableOrConstant<IV> o) {

        this(relationName, -1/* partitionId */, s, p, o,
                null/* c */, false/* optional */, null/* constraint */, null/* expander */);

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o, final boolean optional) {

        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
                null/* c */, optional, null/* constraint */, null/* expander */);

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final ISolutionExpander<ISPO> expander) {

        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
                null/* c */, false/* optional */, null/* constraint */,
                expander);

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o, final boolean optional,
            final ISolutionExpander<ISPO> expander) {

        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
                null/* c */, optional, null/* constraint */, expander);
        
    }
    
    /**
     * Fully specified ctor.
     * 
     * @param relationName
     * @param partitionId
     * @param s
     * @param p
     * @param o
     * @param c MAY be <code>null</code>.
     * @param optional
     * @param constraint
     *            MAY be <code>null</code>.
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(String[] relationName, //
            final int partitionId, //
            final IVariableOrConstant<IV> s,//
            final IVariableOrConstant<IV> p,//
            final IVariableOrConstant<IV> o,//
            final IVariableOrConstant<IV> c,//
            final boolean optional, //
            final IElementFilter<ISPO> constraint,//
            final ISolutionExpander<ISPO> expander//
            ) {
        
        super(
//                (c == null ? new IVariableOrConstant[] { s, p, o }
//                : new IVariableOrConstant[] { s, p, o, c }), 
                
                new IVariableOrConstant[] { s, p, o, c },
                
        relationName[0], partitionId, optional, constraint, expander);

//        if (relationName == null)
//            throw new IllegalArgumentException();
//       
//        for (int i = 0; i < relationName.length; i++) {
//            
//            if (relationName[i] == null)
//                throw new IllegalArgumentException();
//            
//        }
//        
//        if (relationName.length == 0)
//            throw new IllegalArgumentException();
//        
//        if (partitionId < -1)
//            throw new IllegalArgumentException();
//        
//        if (s == null)
//            throw new IllegalArgumentException();
//        
//        if (p == null)
//            throw new IllegalArgumentException();
//        
//        if (o == null)
//            throw new IllegalArgumentException();
//        
//        this.relationName = relationName;
//        
//        this.partitionId = partitionId;
//        
//        this.s = s;
//        this.p = p;
//        this.o = o;
//        this.c = c; // MAY be null.
//
//        this.optional = optional;
//        
//        this.constraint = constraint; /// MAY be null.
//        
//        this.expander = expander;// MAY be null.
        
    }

//    /**
//     * Copy constructor overrides the relation name(s).
//     * 
//     * @param relationName
//     *            The new relation name(s).
//     */
//    protected SPOPredicate(final SPOPredicate src, final String[] relationName) {
//        
//        if (relationName == null)
//            throw new IllegalArgumentException();
//       
//        for(int i=0; i<relationName.length; i++) {
//            
//            if (relationName[i] == null)
//                throw new IllegalArgumentException();
//            
//        }
//        
//        if (relationName.length == 0)
//            throw new IllegalArgumentException();
// 
//        this.partitionId = src.partitionId;
//        
//        this.s = src.s;
//        this.p = src.p;
//        this.o = src.o;
//        this.c = src.c;
//        
//        this.relationName = relationName; // override.
//     
//        this.optional = src.optional;
//        
//        this.constraint = src.constraint;
//        
//        this.expander = src.expander;
//        
//    }

//    /**
//     * Copy constructor sets the index partition identifier.
//     * 
//     * @param partitionId
//     *            The index partition identifier.
//     *            
//     * @throws IllegalArgumentException
//     *             if the index partition identified is a negative integer.
//     * @throws IllegalStateException
//     *             if the index partition identifier was already specified.
//     */
//    protected SPOPredicate(final SPOPredicate src, final int partitionId) {
//
//        //@todo uncomment the other half of this test to make it less paranoid.
//        if (src.partitionId != -1 ) {//&& this.partitionId != partitionId) {
//            
//            throw new IllegalStateException();
//
//        }
//
//        if (partitionId < 0) {
//
//            throw new IllegalArgumentException();
//
//        }
//
//        this.relationName = src.relationName;
//        
//        this.partitionId = partitionId;
//        
//        this.s = src.s;
//        this.p = src.p;
//        this.o = src.o;
//        this.c = src.c;
//        
//        this.optional = src.optional;
//        
//        this.constraint = src.constraint;
//        
//        this.expander = src.expander;
//        
//    }

//    /**
//     * Pure copy constructor.
//     */
//    protected SPOPredicate(final SPOPredicate src) {
//
//        this.relationName = src.relationName;
//        
//        this.partitionId = src.partitionId;
//        
//        this.s = src.s;
//        this.p = src.p;
//        this.o = src.o;
//        this.c = src.c;
//        
//        this.optional = src.optional;
//        
//        this.constraint = src.constraint;
//        
//        this.expander = src.expander;
//        
//    }

    /**
     * Constrain the predicate by setting the context position. If the context
     * position on the {@link SPOPredicate} is non-<code>null</code>, then you
     * must use {@link #asBound(IBindingSet)} to replace all occurrences of the
     * variable appearing in the context position of the predicate with the
     * desired constant. If the context position is already bound the a
     * constant, then you can not modify it (you can only increase the
     * constraint, not change the constraint).
     * 
     * @throws IllegalStateException
     *             unless the context position on the {@link SPOPredicate} is
     *             <code>null</code>.
     */
    public SPOPredicate setC(final IConstant<IV> c) {

        if (c == null)
            throw new IllegalArgumentException();

        final SPOPredicate tmp = this.clone();

        // bound from the binding set.
//        try {
//            final Field f = tmp.getClass().getField("args");
//            f.setAccessible(true);
//            final BOp[] targs = (BOp[]) f.get(tmp);
//            targs[3] = c;
//        } catch (Exception ex) {
//            throw new RuntimeException(ex);
//        }
        tmp.args[3/*c*/] = c;
        
        return tmp;

    }
    
    public SPOPredicate clone() {

        return (SPOPredicate) super.clone();
        
    }

    /**
     * Constrain the predicate by layering on another constraint (the existing
     * constraint, if any, is combined with the new constraint).
     */
    public SPOPredicate setConstraint(final IElementFilter<ISPO> newConstraint) {

        if (newConstraint == null)
            throw new IllegalArgumentException();

        final SPOPredicate tmp = this.clone();

        final IElementFilter<ISPO> wrappedConstraint = getConstraint() == null ? newConstraint
                : new WrappedSPOFilter(newConstraint, getConstraint());

        tmp.annotations.put(Annotations.CONSTRAINT, wrappedConstraint);

        return tmp;
        
//        return new SPOPredicate(//
//                relationName, //
//                partitionId, //
//                s,//
//                p,//
//                o,//
//                c, // 
//                optional, //
//                tmp,// override.
//                expander//
//        );

    }

    /**
     * Wraps two {@link IElementFilter}s as a single logical filter.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class WrappedSPOFilter extends SPOFilter {

        /**
         * 
         */
        private static final long serialVersionUID = 3946650536738814437L;
        
        private final IElementFilter<ISPO> a;
        private final IElementFilter<ISPO> b;
        
        public WrappedSPOFilter(final IElementFilter<ISPO> a,
                final IElementFilter<ISPO> b) {

            this.a = a;
            this.b = b;
            
        }

        public boolean accept(final Object o) {
            
            if (!canAccept(o)) {
                
                return true;
                
            }
            
            final ISPO e = (ISPO) o;
            
            if (a.canAccept(e) && a.accept(e) && b.canAccept(e) && b.accept(e)) {

                return true;
                
            }

            return false;

        }
        
    }
    
//    public final IVariableOrConstant<IV> get(final int index) {
//        switch (index) {
//        case 0:
//            return s;
//        case 1:
//            return p;
//        case 2:
//            return o;
//        case 3:
//            return c;
////            if(c!=null) return c;
//            // fall through
//        default:
//            throw new IndexOutOfBoundsException(""+index);
//        }
//    }
    
//    public final IConstant<IV> get(final ISPO spo, final int index) {
//        switch (index) {
//        case 0:
//            return new Constant<IV>(spo.s());
//        case 1:
//            return new Constant<IV>(spo.p());
//        case 2:
//            return new Constant<IV>(spo.o());
//        case 3:
//            return new Constant<IV>(spo.c());
//        default:
//            throw new IndexOutOfBoundsException("" + index);
//        }
//    }
    
//    /**
//     * Return the index of the variable or constant in the {@link Predicate}.
//     * 
//     * @param t
//     *            The variable or constant.
//     * 
//     * @return The index of that variable or constant. The index will be 0 for
//     *         the subject, 1 for the predicate, or 2 for the object. if the
//     *         variable or constant does not occur in this {@link Predicate} then
//     *         <code>-1</code> will be returned.
//     */
//    public final int indexOf(VarOrConstant t) {
//
//        // variables use a singleton factory.
//        if( s == t ) return 0;
//        if( p == t ) return 1;
//        if( o == t ) return 2;
//        
//        // constants do not give the same guarentee.
//        if(s.equals(t)) return 0;
//        if(p.equals(t)) return 1;
//        if(o.equals(t)) return 2;
//        
//        return -1;
//        
//    }

    final public IVariableOrConstant<IV> s() {
        
        return (IVariableOrConstant<IV>) get(0/* s */);
        
    }
    
    final public IVariableOrConstant<IV> p() {
        
        return (IVariableOrConstant<IV>) get(1/* p */);
        
    }

    final public IVariableOrConstant<IV> o() {
        
        return (IVariableOrConstant<IV>) get(2/* o */);
        
    }
    
    final public IVariableOrConstant<IV> c() {
        
        return (IVariableOrConstant<IV>) get(3/* c */);
        
    }

//    /**
//     * Return true iff the {s,p,o} arguments of the predicate are bound (vs
//     * variables) - the context position is considered iff it is
//     * <code>non-null</code>.
//     * 
//     * @deprecated by {@link #isFullyBound(IKeyOrder)}
//     */
//    final public boolean isFullyBound() {
//
//        return !s.isVar() && !p.isVar() && !o.isVar()
//                && (c == null ? true : (!c.isVar()));
//
//    }

//    /**
//     * The #of arguments in the predicate that are variables (the context
//     * position iff it is non-null).
//     */
//    public int getVariableCount() {
//
//        return (s.isVar() ? 1 : 0) + (p.isVar() ? 1 : 0) + (o.isVar() ? 1 : 0)
//                + (c == null ? 0 : (c.isVar() ? 1 : 0));
//
//    }
    
//    public boolean isFullyBound(final IKeyOrder<ISPO> keyOrder) {
//
//        return getVariableCount(keyOrder) == 0;
//
//    }
//    
//    public int getVariableCount(final IKeyOrder<ISPO> keyOrder) {
//        int nunbound = 0;
//        final int keyArity = keyOrder.getKeyArity();
//        for (int keyPos = 0; keyPos < keyArity; keyPos++) {
//            final int index = keyOrder.getKeyOrder(keyPos);
//            final IVariableOrConstant<?> t = get(index);
//            if (t == null || t.isVar()) {
//                nunbound++;
//            }
//        }
//        return nunbound;
//    }

    /**
     * Return a new instance in which all occurrences of the variable in the
     * predicate have been replaced by the specified constant.
     * 
     * @param var
     *            The variable.
     * @param val
     *            The constant.
     * @return A new instance of the predicate in which all occurrences of the
     *         variable have been replaced by the constant.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    public SPOPredicate asBound(final IVariable<IV> var, final IConstant<IV> val) {

        return asBound(new ArrayBindingSet(new IVariable[]{var}, new IConstant[]{val}));
        
//        if (var == null)
//            throw new IllegalArgumentException();
//        
//        if (val == null)
//            throw new IllegalArgumentException();
//        
////        if(!var.isVar()) {
////            return this;
////        }
//
//        final IVariableOrConstant<Long> s;
//        {
//
//            if (this.s.isVar() && this.s.equals(var)) {
//
//                s = val;
//
//            } else {
//
//                s = this.s;
//
//            }
//        
//        }
//        
//        final IVariableOrConstant<Long> p;
//        {
//
//            if (this.p.isVar() && this.p.equals(var)) {
//
//                p = val;
//
//            } else {
//
//                p = this.p;
//
//            }
//            
//        }
//        
//        final IVariableOrConstant<Long> o;
//        {
//            
//            if (this.o.isVar() && this.o.equals(var)) {
//
//                o = val;
//
//            } else {
//
//               o = this.o;
//
//            }
//            
//        }
//        
//        final IVariableOrConstant<Long> c;
//        {
//
//            if (this.c != null && this.c.isVar() && this.c.equals(var)) {
//
//                c = val;
//
//            } else {
//
//               c = this.c;
//
//            }
//            
//        }
//        
//        return new SPOPredicate(relationName, partitionId, s, p, o, c,
//                optional, constraint, expander);
        
    }

    public SPOPredicate asBound(final IBindingSet bindingSet) {

        return (SPOPredicate) super.asBound(bindingSet);
        
    }

//    public SPOPredicate asBound(final IBindingSet bindingSet) {
//        
//        final IVariableOrConstant<IV> s;
//        {
//            if (this.s.isVar() && bindingSet.isBound((IVariable) this.s)) {
//
//                s = bindingSet.get((IVariable) this.s);
//
//            } else {
//
//                s = this.s;
//
//            }
//        }
//        
//        final IVariableOrConstant<IV> p;
//        {
//            if (this.p.isVar() && bindingSet.isBound((IVariable)this.p)) {
//
//                p = bindingSet.get((IVariable) this.p);
//
//            } else {
//
//                p = this.p;
//
//            }
//        }
//        
//        final IVariableOrConstant<IV> o;
//        {
//            if (this.o.isVar() && bindingSet.isBound((IVariable) this.o)) {
//
//                o = bindingSet.get((IVariable) this.o);
//
//            } else {
//
//                o = this.o;
//
//            }
//        }
//        
//        final IVariableOrConstant<IV> c;
//        {
//            if (this.c != null && this.c.isVar()
//                    && bindingSet.isBound((IVariable) this.c)) {
//
//                c = bindingSet.get((IVariable) this.c);
//
//            } else {
//
//                c = this.c;
//
//            }
//        }
//        
//        return new SPOPredicate(relationName, partitionId, s, p, o, c,
//                optional, constraint, expander);
//        
//    }

//    public SPOPredicate setRelationName(String[] relationName) {
//
//        return new SPOPredicate(this, relationName);
//        
//    }
//    
//    public SPOPredicate setPartitionId(int partitionId) {
//
//        return new SPOPredicate(this, partitionId);
//
//    }

//    public String toString() {
//
//        return toString(null);
//        
//    }
//
//    public String toString(final IBindingSet bindingSet) {
//        
//        return toStringBuilder(bindingSet).toString();
//        
//    }
//    
//    protected StringBuilder toStringBuilder(final IBindingSet bindingSet) {
//        
//        final StringBuilder sb = new StringBuilder();
//
//        sb.append("(");
//
//        sb.append(Arrays.toString(relationName));
//
//        sb.append(", ");
//        
//        sb.append(s.isConstant() || bindingSet == null
//                || !bindingSet.isBound((IVariable) s) ? s.toString()
//                : bindingSet.get((IVariable) s));
//
//        sb.append(", ");
//
//        sb.append(p.isConstant() || bindingSet == null
//                || !bindingSet.isBound((IVariable) p) ? p.toString()
//                : bindingSet.get((IVariable) p));
//
//        sb.append(", ");
//
//        sb.append(o.isConstant() || bindingSet == null
//                || !bindingSet.isBound((IVariable) o) ? o.toString()
//                : bindingSet.get((IVariable) o));
//
//        if (c != null) {
//
//            sb.append(", ");
//
//            sb.append(c.isConstant() || bindingSet == null
//                    || !bindingSet.isBound((IVariable) c) ? c.toString()
//                    : bindingSet.get((IVariable) c));
//
//        }
//        
//        sb.append(")");
//
//        if (optional || constraint != null || expander != null
//                || partitionId != -1) {
//            
//            /*
//             * Something special, so do all this stuff.
//             */
//            
//            boolean first = true;
//            
//            sb.append("[");
//            
//            if(optional) {
//                if(!first) sb.append(", ");
//                sb.append("optional");
//                first = false;
//            }
//
//            if(constraint!=null) {
//                if(!first) sb.append(", ");
//                sb.append(constraint.toString());
//                first = false;
//            }
//            
//            if(expander!=null) {
//                if(!first) sb.append(", ");
//                sb.append(expander.toString());
//                first = false;
//            }
//            
//            if(partitionId!=-1) {
//                if(!first) sb.append(", ");
//                sb.append("partitionId="+partitionId);
//                first = false;
//            }
//            
//            sb.append("]");
//            
//        }
//        
//        return sb;
//
//    }

//    final public boolean isOptional() {
//        
//        return optional;
//        
//    }
//    
//    final public IElementFilter<ISPO> getConstraint() {
//
//        return constraint;
//        
//    }
//
//    final public ISolutionExpander<ISPO> getSolutionExpander() {
//        
//        return expander;
//        
//    }

//    public boolean equals(final Object other) {
//        
//        if (this == other)
//            return true;
//
//        final IPredicate o = (IPredicate)other;
//        
//        final int arity = arity();
//        
//        if(arity != o.arity()) return false;
//        
//        for(int i=0; i<arity; i++) {
//            
//            final IVariableOrConstant x = get(i);
//            
//            final IVariableOrConstant y = o.get(i);
//            
//            if (x != y && !(x.equals(y))) {
//                
//                return false;
//            
//            }
//            
//        }
//        
//        return true;
//        
//    }
//
//    public int hashCode() {
//        
//        int h = hash;
//
//        if (h == 0) {
//
//            final int n = arity();
//
//            for (int i = 0; i < n; i++) {
//        
//                h = 31 * h + get(i).hashCode();
//                
//            }
//            
//            hash = h;
//            
//        }
//        
//        return h;
//
//    }

//    /**
//     * Caches the hash code.
//     */
//    private int hash = 0;

//    public SPOPredicate reBound(final IVariableOrConstant<IV> s, 
//            final IVariableOrConstant<IV> p, final IVariableOrConstant<IV> o, 
//            final IVariableOrConstant<IV> c) {
//
//        final SPOPredicate tmp = this.clone();
//
//        tmp.args[0] = s;
//        tmp.args[1] = p;
//        tmp.args[2] = o;
//        tmp.args[3] = c;
//        
//        return tmp;
//        
//    }

}
