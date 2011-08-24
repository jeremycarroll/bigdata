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
/* Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.BlankNodeVarProcessor;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTFalse;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTRDFValue;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTTrue;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataValueReplacer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Base class for AST visitor impls.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class BigdataASTVisitorBase extends ASTVisitorBase {

    protected final BigdataASTContext context;

    public BigdataASTVisitorBase(final BigdataASTContext context) {

        this.context = context;

    }

    /**
     * Note: The {@link BatchRDFValueResolver} is responsible for annotating the
     * {@link ASTRDFValue}s in the parse tree with {@link BigdataValue}s whose
     * {@link IV}s have been resolved against the database. That processor MUST
     * run before we build the bigdata AST from the jjtree AST nodes.
     */
    @SuppressWarnings("unchecked")
    private IV<BigdataValue, ?> makeIV(final BigdataValue value)
            throws VisitorException {

        if (value == null)
            throw new VisitorException("Value property not set?");
        
        final IV<BigdataValue,?> iv = value.getIV();
        
        if (iv == null)
            throw new VisitorException("IV not resolved : " + value);
        
        return iv;

//        IV iv = context.lexicon.getInlineIV(value);
//
//        if (iv == null) {
//
//            iv = (IV<BigdataValue, ?>) TermId.mockIV(VTE.valueOf(value));
//            
//            iv.setValue(value);
//            
//        }
//
//        return iv;

    }
    
    /*
     * Productions used by different ASTVisitor facets.
     */
    
    /**
     * Note: openrdf uses the {@link BlankNodeVarProcessor} create anonymous
     * variables from blank nodes and then flags those as anonymous variables in
     * this step.
     */
    @Override
    public VarNode visit(final ASTVar node, Object data) throws VisitorException {

        final VarNode var = new VarNode(node.getName());
        
        if (node.isAnonymous())
            var.setAnonymous(true);
        
        return var;
        
    }

    @Override
    public Object visit(ASTQName node, Object data) throws VisitorException {
        throw new VisitorException(
                "QNames must be resolved before building the query model");
    }

    @Override
    public Object visit(ASTBlankNode node, Object data) throws VisitorException {
        throw new VisitorException(
                "Blank nodes must be replaced with variables before building the query model");
    }

    @Override
    public ConstantNode visit(final ASTIRI node, Object data)
            throws VisitorException {
        
//        BigdataURI uri;
//        
//        try {
//        
//            uri = context.valueFactory.createURI(node.getValue());
//            
//        } catch (IllegalArgumentException e) {
//            
//            // invalid URI
//            throw new VisitorException(e.getMessage());
//            
//        }
//
//        return new ConstantNode(makeIV(uri));
        return new ConstantNode(makeIV((BigdataValue)node.getRDFValue()));
        
    }

    @Override
    public ConstantNode visit(final ASTRDFLiteral node, Object data)
            throws VisitorException {
        
//        final String label = (String) node.getLabel().jjtAccept(this, null);
//        final String lang = node.getLang();
//        final ASTIRI datatypeNode = node.getDatatype();
//        final BigdataValueFactory valueFactory = context.valueFactory;
//        
//        final BigdataLiteral literal;
//        if (datatypeNode != null) {
//            final BigdataURI datatype;
//            try {
//                datatype = valueFactory.createURI(datatypeNode.getValue());
//            } catch (IllegalArgumentException e) {
//                // invalid URI
//                throw new VisitorException(e.getMessage());
//            }
//            literal = valueFactory.createLiteral(label, datatype);
//        } else if (lang != null) {
//            literal = valueFactory.createLiteral(label, lang);
//        } else {
//            literal = valueFactory.createLiteral(label);
//        }
//
//        return new ConstantNode(makeIV(literal));

        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));

    }

    @Override
    public ConstantNode visit(ASTNumericLiteral node, Object data)
            throws VisitorException {

//        final BigdataLiteral literal = context.valueFactory.createLiteral(
//                node.getValue(), node.getDatatype());
//        
//        return new ConstantNode(makeIV(literal));

        return new ConstantNode(makeIV((BigdataValue)node.getRDFValue()));
        
    }

    @Override
    public ConstantNode visit(ASTTrue node, Object data)
            throws VisitorException {
        
//        return new ConstantNode(
//                makeIV(context.valueFactory.createLiteral(true)));
        
        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));
        
    }

    @Override
    public ConstantNode visit(ASTFalse node, Object data)
            throws VisitorException {
        
//        return new ConstantNode(
//                makeIV(context.valueFactory.createLiteral(false)));

        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));

    }

    @Override
    public String visit(ASTString node, Object data) throws VisitorException {

        return node.getValue();
        
    }

}
