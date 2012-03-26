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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

public class EncodeForURIBOp extends AbstractLiteralBOp {

    private static final long serialVersionUID = -8448763718374010166L;

    public EncodeForURIBOp(IValueExpression<? extends IV> x, String lex) {
        super(x, lex);
    }

    public EncodeForURIBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();
    }

    public EncodeForURIBOp(EncodeForURIBOp op) {
        super(op);
    }

    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

    public IV _get(final IBindingSet bs) throws SparqlTypeErrorException {
        IV iv = getAndCheckIfMaterializedLiteral(0, bs);
        final BigdataLiteral lit =literalValue(iv);
        try {
            final BigdataLiteral str = getValueFactory().createLiteral(URLEncoder.encode(lit.getLabel(), "UTF-8").replace("+", "%20"));
            return DummyConstantNode.toDummyIV(str);
        } catch (UnsupportedEncodingException uee) {
            throw new SparqlTypeErrorException();
        }
    }
}
