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

import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;

/**
 * Imposes the constraint <code>!x</code>.
 */
public class NOT extends BOpConstraint {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5701967329003122236L;

	/**
     * Required deep copy constructor.
     */
    public NOT(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public NOT(final NOT op) {
        super(op);
    }

    public NOT(final IConstraint x) {

        super(new BOp[] { x }, null/*Annotations*/);

        if (x == null)
            throw new IllegalArgumentException();

    }

    public boolean accept(final IBindingSet s) {

        return !((IConstraint) get(0)).accept(s);

    }
    
}
