/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
package com.bigdata.rdf.lexicon;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

public class LexPredicate extends Predicate<BigdataValue> {

    /**
     * 
     */
    private static final long serialVersionUID = 6379772624297485704L;

	/**
	 * Simplified forward lookup ctor. Use this ctor to lookup an {@link IV}
	 * from a {@link BigdataValue}.
	 * 
	 * @param relationName
	 *            the namespace of the lexicon relation
	 * @param timestamp
	 *            The timestamp of the view to read on. This should be the same
	 *            as the timestamp associated with the view of the triple store
	 *            except for a full read/write transaction. Since all writes on
	 *            the lexicon are unisolated, a full read/write transaction must
	 *            use the {@link ITx#UNISOLATED} view of the lexicon in order to
	 *            ensure that any writes it performs will be visible.
	 * @param term
	 *            the term to resolve using forward lookup (term2id)
	 */
    public static LexPredicate forwardInstance(final String relationName,
    		final long timestamp,
    		final IVariableOrConstant<BigdataValue> term) {

        return new LexPredicate(
            new IVariableOrConstant[] { 
                term,      // term 
                Var.var(), // iv 
            },
            new NV(Annotations.RELATION_NAME, new String[] { relationName }),
            new NV(Annotations.TIMESTAMP, timestamp) //
        );

    }

	/**
	 * Simplified reverse lookup ctor. Use this ctor to lookup a
	 * {@link BigdataValue} from an {@link IV}.
	 * 
	 * @param relationName
	 *            the namespace of the lexicon relation
	 * @param timestamp
	 *            The timestamp of the view to read on. This should be the same
	 *            as the timestamp associated with the view of the triple store
	 *            except for a full read/write transaction. Since all writes on
	 *            the lexicon are unisolated, a full read/write transaction must
	 *            use the {@link ITx#UNISOLATED} view of the lexicon in order to
	 *            ensure that any writes it performs will be visible.
	 * @param term
	 *            the term to resolve using reverse lookup (id2term)
	 */
    public static LexPredicate reverseInstance(final String relationName,
    		final long timestamp,
    		final IVariableOrConstant<IV> term) {

        return new LexPredicate(
            new IVariableOrConstant[] { 
                Var.var(), // term 
                term,      // iv 
            },
            new NV(Annotations.RELATION_NAME, new String[] { relationName }),
            new NV(Annotations.TIMESTAMP, timestamp) //
        );

    }

    /**
     * Variable argument version of the shallow copy constructor.
     */
    public LexPredicate(final BOp[] args, final NV... anns) {
        super(args, anns);
    }

    /**
     * Required shallow copy constructor.
     */
    public LexPredicate(final BOp[] args, final Map<String, Object> anns) {
        super(args, anns);
    }

    /**
     * Required deep copy constructor.
     */
    public LexPredicate(final LexPredicate op) {
        super(op);
    }

    public LexPredicate clone() {

        return (LexPredicate) super.clone();
        
    }

    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<BigdataValue> term() {
        
        return (IVariableOrConstant<BigdataValue>) get(0/* term */);
        
    }
    
    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<IV> iv() {
        
        return (IVariableOrConstant<IV>) get(1/* iv */);
        
    }
    
    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public LexPredicate asBound(final IBindingSet bindingSet) {

        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final LexPredicate tmp = (LexPredicate) super.asBound(bindingSet);

        return tmp;

    }

}
