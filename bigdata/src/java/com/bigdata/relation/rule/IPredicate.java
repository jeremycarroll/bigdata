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

package com.bigdata.relation.rule;

import java.io.Serializable;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * An immutable constraint on the elements visited using an {@link IAccessPath}.
 * The slots in the predicate corresponding to variables are named and those
 * names establish binding patterns access {@link IPredicate}s in the context
 * of a {@link IRule}. Access is provided to slots by ordinal index regardless
 * of whether or not they are named variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPredicate<E> extends Cloneable, Serializable {

    /**
     * Resource identifier (aka namespace) identifies the {@link IRelation}
     * associated with this {@link IPredicate}.
     * <p>
     * This is more or less ignored when the {@link IRule} is executed as a
     * query.
     * <p>
     * When the {@link IRule} is executed as an {@link ActionEnum#Insert} or
     * {@link ActionEnum#Delete} then this identifies the target
     * {@link IMutableRelation} on which the computed {@link ISolution}s will
     * be written.
     * 
     * @throws IllegalStateException
     *             if there is more than on element in the view.
     */
    public String getOnlyRelationName();

    /**
     * Return the ith element of the relation view. The view is an ordered array
     * of resource identifiers that describes the view for the relation.
     * 
     * @param index
     *            The index into the array of relation names in the view.
     * 
     * @todo is there any particular reason to use an ordered view? TM does not
     *       appear to require this and we always write on the relation that is
     *       associated with the head of the rule, and that should always be the
     *       "database" (vs the focusStore).
     */
    public String getRelationName(int index);
    
    /**
     * The #of elements in the relation view.
     */
    public int getRelationCount();
    
    /**
     * <code>true</code> iff the predicate is optional when evaluated as the
     * right-hand side of a join. An optional predicate will match once after
     * all matches in the data have been exhausted. By default, the match will
     * NOT bind any variables that have been determined to be bound by the
     * predicate based on the computed {@link IEvaluationPlan}.
     * <p>
     * For mutation, some {@link IRelation}s may require that all variables
     * appearing in the head are bound. This and similar constraints can be
     * enforced using {@link IConstraint}s on the {@link IRule}.
     * <p>
     * More control over the behavior of optionals may be gained through the use
     * of an {@link ISolutionExpander} pattern.
     * 
     * @return <code>true</code> iff this predicate is optional when
     *         evaluating a JOIN.
     */
    public boolean isOptional();

    /**
     * Returns the object that may be used to selectively override the
     * evaluation of the predicate.
     * 
     * @return The {@link ISolutionExpander}.
     * 
     * @todo replace with {@link ISolutionExpander#getAccessPath(IAccessPath)},
     *       which is the only method declared by {@link ISolutionExpander}.
     */
    public ISolutionExpander<E> getSolutionExpander();
    
    /**
     * An optional constraint on the visitable elements.
     */
    public IElementFilter<E> getConstraint();
    
    /**
     * Return true iff all arguments of the predicate are bound (vs
     * variables).
     */
    public boolean isFullyBound();
    
    /**
     * The #of arguments in the predicate that are variables (vs constants).
     */
    public int getVariableCount();
    
    /** The #of slots in the predicate. */
    public int arity();

    /**
     * Return the variable or constant at the specified index.
     * 
     * @param index
     *            The index.
     * 
     * @return The variable or constant at the specified index.
     * 
     * @throws IllegalArgumentException
     *             if the index is less than zero or GTE the {@link #arity()} of
     *             the {@link IPredicate}.
     */
    public IVariableOrConstant get(int index);
    
    /**
     * A copy of this {@link IPredicate} in which zero or more variables have
     * been bound to constants using the given {@link IBindingSet}.
     */
    public IPredicate<E> asBound(IBindingSet bindingSet);

    /**
     * A copy of this {@link IPredicate} in which the <i>relationName</i>(s)
     * replace the existing set of relation name(s).
     * 
     * @param relationName
     *            The relation name(s).
     * 
     * @throws IllegalArgumentException
     *             if <i>relationName</i> is empty.
     * @throws IllegalArgumentException
     *             if <i>relationName</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if any element of <i>relationName</i> is <code>null</code>
     */
    public IPredicate<E> setRelationName(String[] relationName);
    
    /**
     * Representation of the predicate without variable bindings.
     */
    public String toString();
    
    /**
     * Representation of the predicate with variable bindings.
     * 
     * @param bindingSet
     *            The variable bindings
     */
    public String toString(IBindingSet bindingSet);

    /**
     * Compares the bindings of two predicates for equality.
     * 
     * @param other
     *            Another predicate.
     *            
     * @return true iff the predicate have the same arity and their ordered
     *         bindings are the same. when both predicates have a variable at a
     *         given index, the names of the variables must be the same.
     */
    public boolean equals(Object other);

    /**
     * The hash code is defined as
     * 
     * <pre>
     * get(0).hashCode()*31&circ;(n-1) + get(1).hashCode()*31&circ;(n-2) + ... + get(n-1).hashCode()
     * </pre>
     * 
     * using <code>int</code> arithmetic, where <code>n</code> is the
     * {@link #arity()} of the predicate, and <code>^</code> indicates
     * exponentiation.
     * <p>
     * Note: This is similar to how {@link String#hashCode()} is defined.
     */
    public int hashCode();
    
}
