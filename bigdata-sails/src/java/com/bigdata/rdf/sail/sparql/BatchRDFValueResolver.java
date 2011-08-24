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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.PrefixDeclProcessor;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTDatasetClause;
import org.openrdf.query.parser.sparql.ast.ASTFalse;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTOperationContainer;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTRDFValue;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTTrue;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataValueReplacer;

/**
 * Class performs efficient batch resolution of RDF Values against the database.
 * This efficiency is important on a cluster and when a SPARQL query or update
 * contains a large number of RDF Values.
 * <p>
 * Note: The {@link PrefixDeclProcessor} will rewrite {@link ASTQName} nodes as
 * {@link ASTIRI} nodes. It MUST run before this processor.
 * <p>
 * Note: Any {@link ASTRDFLiteral} or {@link ASTIRI} nodes are annotated by this
 * processor using {@link ASTRDFValue#setRDFValue(Value)}. This includes IRIrefs
 * in the {@link ASTDatasetClause}, which are matched as either {@link ASTIRI}
 * or {@link ASTQName}.
 * <p>
 * Note: This replaces the functionality of the {@link BigdataValueReplacer}.
 * <p>
 * Note: {@link IValueExpression} nodes used in {@link SPARQLConstraint}s are
 * allowed to use values not actually in the database. MP
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchRDFValueResolver extends ASTVisitorBase {

    private final static Logger log = Logger
            .getLogger(BatchRDFValueResolver.class);

    private final BigdataASTContext context;

    private final BigdataValueFactory valueFactory;

    private final LinkedHashMap<ASTRDFValue, BigdataValue> nodes;
    
    /**
     * @param context
     */
    public BatchRDFValueResolver(final BigdataASTContext context) {
        
        this.context = context;

        this.valueFactory = context.valueFactory;
        
        this.nodes = new LinkedHashMap<ASTRDFValue, BigdataValue>();
        
    }

    /**
     * Visit the parse tree, locating and collecting references to all
     * {@link ASTRDFValue} nodes (including blank nodes iff we are in a told
     * bnodes mode). The {@link ASTRDFValue}s are collected in a {@link Map}
     * which associates each one with a {@link BigdataValue} object which is set
     * using {@link ASTRDFValue#setRDFValue(org.openrdf.model.Value)}. The
     * {@link BigdataValue}s are then resolved in a batch against the database,
     * obtaining their {@link IVs}.  This has the side-effect of making their
     * {@link IV}s available in the parse tree.
     * 
     * @param qc
     * 
     * @throws MalformedQueryException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void process(final ASTOperationContainer qc)
            throws MalformedQueryException {
        
        try {

            /*
             * Collect all ASTRDFValue nodes into a map, paired with
             * BigdataValue objects.
             */
            qc.jjtAccept(new RDFValueResolver(), null);
            
        } catch (VisitorException e) {
            
            // Turn the exception into a Query exception.
            throw new MalformedQueryException(e);
            
        }
        
        /*
         * Batch resolve the BigdataValue objects against the database. This
         * sets their IVs as a side-effect.
         */
        
        final BigdataValue[] values = nodes.values().toArray(
                new BigdataValue[0]);
        
        context.lexicon.addTerms(values, values.length, true/* readOnly */);
        
        // cache the BigdataValues on the IVs for later
        for (BigdataValue value : values) {

            final IV iv = value.getIV();

            if (iv == null) {

                /*
                 * Since the term identifier is NULL this value is not known
                 * to the kb.
                 */

                if (log.isInfoEnabled())
                    log.info("Not in knowledge base: " + value);

                /*
                 * Create a dummy iv and cache the unknown value on it so
                 * that it can be used during query evaluation.
                 */
                final IV dummy = TermId.mockIV(VTE.valueOf(value));

                value.setIV(dummy);

                dummy.setValue(value);

            } else {

                iv.setValue(value);

            }

        }

        /*
         * Set the BigdataValue object on each ASTRDFValue node.
         */
        final Iterator<Map.Entry<ASTRDFValue, BigdataValue>> itr = nodes
                .entrySet().iterator();
        
        while(itr.hasNext()) {
            
            final Map.Entry<ASTRDFValue,BigdataValue> e = itr.next();
   
            final ASTRDFValue node = e.getKey();
            
            final BigdataValue value = e.getValue();

            node.setRDFValue(value);
            
        }

    }

    private class RDFValueResolver extends ASTVisitorBase {

        @Override
        public Object visit(ASTQName node, Object data) throws VisitorException {
            throw new VisitorException(
                    "QNames must be resolved before resolving RDF Values");
        }

        /**
         * Note: Blank nodes within a QUERY are treated as anonymous variables,
         * even when we are in a told bnodes mode.
         */
        @Override
        public Object visit(ASTBlankNode node, Object data)
                throws VisitorException {
            throw new VisitorException(
                    "Blank nodes must be replaced with variables before resolving RDF Values");
        }

        @Override
        public Void visit(final ASTIRI node, Object data)
                throws VisitorException {

            try {

                nodes.put(node, context.valueFactory.createURI(node.getValue()));

                return null;

            } catch (IllegalArgumentException e) {

                // invalid URI
                throw new VisitorException(e.getMessage());

            }

        }

        @Override
        public Void visit(final ASTRDFLiteral node, Object data)
                throws VisitorException {

            // Note: This is handled by this ASTVisitor (see below in this
            // class).
            final String label = (String) node.getLabel().jjtAccept(this, null);

            final String lang = node.getLang();

            final ASTIRI datatypeNode = node.getDatatype();

            final BigdataLiteral literal;

            if (datatypeNode != null) {

                final BigdataURI datatype;

                try {

                    datatype = valueFactory.createURI(datatypeNode.getValue());

                } catch (IllegalArgumentException e) {

                    // invalid URI
                    throw new VisitorException(e);

                }

                literal = valueFactory.createLiteral(label, datatype);

            } else if (lang != null) {

                literal = valueFactory.createLiteral(label, lang);

            } else {

                literal = valueFactory.createLiteral(label);

            }

            nodes.put(node, literal);

            return null;

        }

        @Override
        public Void visit(final ASTNumericLiteral node, Object data)
                throws VisitorException {

            nodes.put(
                    node,
                    context.valueFactory.createLiteral(node.getValue(),
                            node.getDatatype()));

            return null;

        }

        @Override
        public Void visit(ASTTrue node, Object data) throws VisitorException {

            nodes.put(node, context.valueFactory.createLiteral(true));

            return null;

        }

        @Override
        public Void visit(ASTFalse node, Object data) throws VisitorException {

            nodes.put(node, context.valueFactory.createLiteral(false));

            return null;

        }

        /**
         * Note: This supports the visitor method for a Literal.
         */
        @Override
        public String visit(ASTString node, Object data)
                throws VisitorException {

            return node.getValue();

        }

    }

}
