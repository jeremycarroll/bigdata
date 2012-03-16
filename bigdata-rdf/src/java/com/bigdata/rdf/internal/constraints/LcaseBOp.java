/*

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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

/**
 * <pre>http://www.w3.org/2005/xpath-functions#lower-case</pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LcaseBOp extends AbstractLiteralBOp<IV> {
    
    private static final long serialVersionUID = -6847688419473046477L;

    public LcaseBOp(IValueExpression<? extends IV> x, final String lex) {

        super(x, lex);
        
    }

    /**
     * Required shallow copy constructor.
     * 
     * @param args
     *            The function arguments (value expressions).
     * @param anns
     *            The function annotations.
     */
    public LcaseBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
        if (args.length != 1 || args[0] == null) {
         
            /*
             * There must be exactly one argument for this function.
             */

            throw new IllegalArgumentException();
            
        }
        
    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    public LcaseBOp(final LcaseBOp op) {
        
        super(op);
        
    }

    /**
     * This is a {@link Requirement#SOMETIMES} because it can operate on inline
     * {@link IV}s without materialization but requires materialization of
     * non-inline {@link IV}s.
     */
    public Requirement getRequirement() {
        
        return Requirement.SOMETIMES;
        
    }

    @SuppressWarnings("rawtypes")
    protected IV _get(final IBindingSet bs) {

        final IV iv = getAndCheck(0, bs);

        final BigdataLiteral in = literalValue(iv);

        final BigdataValueFactory vf = getValueFactory();

        final String label = in.getLabel().toLowerCase();

        final BigdataLiteral out;

        if (in.getLanguage() != null) {

            out = vf.createLiteral(label, in.getLanguage());

        } else if (in.getDatatype() != null) {

            out = vf.createLiteral(label, in.getDatatype());

        } else {

            out = vf.createLiteral(label);
            
        }

        return DummyConstantNode.toDummyIV(out);

    }

}
