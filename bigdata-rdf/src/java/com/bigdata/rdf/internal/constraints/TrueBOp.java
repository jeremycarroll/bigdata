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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * Always evaluates to true.
 */
public class TrueBOp extends XSDBooleanIVValueExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6166507977125961015L;
	
	public static final TrueBOp INSTANCE = new TrueBOp();

	private TrueBOp() {
		
		this(BOp.NOARGS, BOp.NOANNS);
		
	}
	
	public TrueBOp(final IValueExpression<? extends IV> x) {
		
		this(new BOp[] { x }, BOp.NOANNS);
		
	}
	
    /**
     * Required shallow copy constructor.
     */
    public TrueBOp(final BOp[] args, final Map<String, Object> anns) {
    	
        super(args, anns);
        
    }

    /**
     * Required deep copy constructor.
     */
    public TrueBOp(final TrueBOp op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {

    	return true;

    }
    
}
