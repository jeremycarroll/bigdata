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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;

/**
 * @see http://www.w3.org/2005/xpath-functions#ends-with
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StrendsBOp extends XSDBooleanIVValueExpression implements INeedsMaterialization {

    private static final long serialVersionUID = 5466622630000019821L;

    public StrendsBOp(IValueExpression<? extends IV> x,
            IValueExpression<? extends IV> y, String lex) {
   
        this(new BOp[] { x, y }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));
        
    }

    public StrendsBOp(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);
        
        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();
        
    }

    public StrendsBOp(final StrendsBOp op) {
        super(op);
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }
    
    @Override
    public boolean accept(final IBindingSet bs) throws SparqlTypeErrorException {

        final String v = literalValue(0, bs).getLabel();
        final String c = literalValue(1, bs).getLabel();
        return v.endsWith(c);
        
    }

}
