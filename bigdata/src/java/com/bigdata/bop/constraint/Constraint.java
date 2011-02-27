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
package com.bigdata.bop.constraint;

import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.ImmutableBOp;

/**
 * BOpConstraint that wraps a {@link BooleanValueExpression}.
 */
public class Constraint extends ImmutableBOp implements IConstraint {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9144690224260823279L;

	protected static final Logger log = Logger.getLogger(Constraint.class);
	
	/**
	 * Convenience method to generate a constraint from a value expression.
	 */
	public static IConstraint wrap(final BooleanValueExpression ve) {
		return new Constraint(ve);
	}
	
	
	public Constraint(final BooleanValueExpression x) {
		
        this(new BOp[] { x }, null/*annocations*/);

    }

    /**
     * Required shallow copy constructor.
     */
    public Constraint(final BOp[] args, 
    		final Map<String, Object> anns) {
    	
        super(args, anns);
        
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public Constraint(final Constraint op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {
    	
//    	try {
    		
    		// evaluate the BVE operator
    		return ((BooleanValueExpression) get(0)).get(bs);

//		} catch (Throwable t) {
//
//			if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {
//
//				// trap the type error and filter out the solution
//				if (log.isInfoEnabled())
//					log.info("discarding solution due to type error: " + bs
//							+ " : " + t);
//				
//				return false;
//
//			}
//
//			throw new RuntimeException(t);
//
//    	}
    	
    }
    
}
