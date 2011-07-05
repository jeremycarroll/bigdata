/*

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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDBooleanIV;
import com.bigdata.util.InnerCause;

/**
 * BOpConstraint that wraps a {@link EBVBOp}, which itself computes the 
 * effective boolean value of an IValueExpression.
 */
public class SPARQLConstraint extends com.bigdata.bop.constraint.Constraint {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5796492538735372727L;
	
	protected static final Logger log = Logger.getLogger(SPARQLConstraint.class);
	
	/**
	 * The operand of this operator must evaluate to a boolean. If the operand
	 * is not known to evaluate to a boolean, wrap it with an {@link EBVBOp}.
	 */
	private static XSDBooleanIVValueExpression wrap(
			final IValueExpression<? extends IV> ve) {
		
		return ve instanceof XSDBooleanIVValueExpression ?
			(XSDBooleanIVValueExpression) ve :
				new EBVBOp(ve);
		
	}
	
	
	/**
	 * Construct a SPARQL constraint using the specified value expression.
	 * The value expression will be automatically wrapped inside an
	 * {@link EBVBOp} if it does not itself evaluate to a boolean.
	 */
	public SPARQLConstraint(final IValueExpression<? extends IV> x) {

		this(new BOp[] { wrap(x) }, null/*annocations*/);
		
    }

    /**
     * Required shallow copy constructor.
     */
    public SPARQLConstraint(final BOp[] args, 
    		final Map<String, Object> anns) {
    	
        super(args, anns);
        
        if (args.length != 1 || args[0] == null)
        	throw new IllegalArgumentException();
        
    }

    /**
     * Required deep copy constructor.
     */
    public SPARQLConstraint(final SPARQLConstraint op) {
        super(op);
    }

    @Override
    public IValueExpression<? extends XSDBooleanIV> get(final int i) {
    	return (IValueExpression<? extends XSDBooleanIV>) super.get(i);
    }
    
	public IValueExpression<? extends XSDBooleanIV> getValueExpression() {
		return get(0);
	}
    	
	public boolean accept(final IBindingSet bs) {

		try {

			// evaluate the EBV operator
			final XSDBooleanIV iv = get(0).get(bs);
			
			return iv.booleanValue();

		} catch (Throwable t) {

			if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

				// trap the type error and filter out the solution
				if (log.isInfoEnabled())
					log.info("discarding solution due to type error: " + bs
							+ " : " + t);

				return false;

			}

			throw new RuntimeException(t);

		}

	}

}
