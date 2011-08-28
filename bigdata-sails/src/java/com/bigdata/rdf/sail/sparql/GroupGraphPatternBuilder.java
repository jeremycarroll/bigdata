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

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.parser.sparql.ast.ASTBasicGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTBind;
import org.openrdf.query.parser.sparql.ast.ASTConstraint;
import org.openrdf.query.parser.sparql.ast.ASTConstruct;
import org.openrdf.query.parser.sparql.ast.ASTGraphGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTHavingClause;
import org.openrdf.query.parser.sparql.ast.ASTNamedSubqueryInclude;
import org.openrdf.query.parser.sparql.ast.ASTOptionalGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTUnionGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTWhereClause;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Visitor handles the <code>GroupGraphPattern</code> production (aka the
 * "WHERE" clause). This includes <code>SubSelect</code> and
 * <code>GraphPatternNotTriples</code>. ASTWhereClause has GroupGraphPattern
 * child which is a (SelectQuery (aka subquery)), GraphPattern
 * (BasicGraphPattern aka JoinGroup or GraphPatternNotTriples). The
 * <code>TriplesBlock</code> is handled by the {@link TriplePatternExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: GroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 */
public class GroupGraphPatternBuilder extends TriplePatternExprBuilder {

    private static final Logger log = Logger.getLogger(GroupGraphPatternBuilder.class);
    
    public GroupGraphPatternBuilder(final BigdataASTContext context) {

        super(context);

        this.graphPattern = new GroupGraphPattern();

    }

    /**
     * CONSTRUCT (handled as a TriplesBlock).
     */
    @Override
    final public ConstructNode visit(final ASTConstruct node, Object data)
     throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;
        
        graphPattern = new GroupGraphPattern();//parentGP); Note: No parent.

        // visit the children of the node (default behavior).
        super.visit(node, null);

        final ConstructNode group = graphPattern.buildGroup(new ConstructNode());
        
        graphPattern = parentGP;

        return group;
        
    }

    //
    //
    //

    /**
     * <code>( SelectQuery | GraphPattern )</code> - this is the common path for
     * SubSelect and graph patterns.
     * <p>
     * Note: (NOT) EXISTS uses a temporary graph pattern in order to avoid the
     * side-effect on the parent's graph pattern. Other value functions with
     * inner graph patterns should do this as well.
     * <p>
     * Note: Filters are scoped to the graph pattern group and do not affect
     * bindings external to the group.
     * 
     * @return The {@link GroupNodeBase}. This return value is used by the
     *         visitor method for the {@link ASTWhereClause}. If the child was a
     *         SubSelect, then the immediate parent is NOT an
     *         {@link ASTWhereClause} and the return value is ignored.
     */
    @Override
    final public GroupNodeBase<?> visit(final ASTGraphPatternGroup node,
            Object data) throws VisitorException {

        if (log.isInfoEnabled()) {
            log.info("\ndepth=" + depth(node) + ", parentGP(in)="
                    + graphPattern + "\n" + node.dump(indent(node)));
        }

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern(parentGP);

        // visit the children of the node (default behavior).
        final Object ret = super.visit(node, null);
        GroupNodeBase<?> ret2 = null; 

        if (ret instanceof SubqueryRoot) {

            final SubqueryRoot subqueryRoot = (SubqueryRoot) ret;

            final JoinGroupNode joinGroup = new JoinGroupNode();

            joinGroup.addChild(subqueryRoot);

            parentGP.add(joinGroup);

        } else {
            
            final JoinGroupNode joinGroup = new JoinGroupNode();

            if (node.jjtGetParent() instanceof ASTGraphGraphPattern) {

                /*
                 * TODO Should we reach up to the first GRAPH graph pattern in
                 * case it is more than one level above us and pull in its
                 * context? Or should that be handled by an AST Optimizer?
                 */

                joinGroup.setContext(parentGP.getContext());

            }

            @SuppressWarnings("rawtypes")
            final GroupNodeBase group = graphPattern.buildGroup(joinGroup);

            parentGP.add(group);

            ret2 = group;
            
        }

        if (log.isInfoEnabled())
            log.info("\ndepth=" + depth(node) + ", graphPattern(out)="
                    + graphPattern);

        graphPattern = parentGP;

        return ret2;

    }

    /**
     * Note: while openrdf lifts the filters out of the optional, we do not need
     * to do this per MikeP.
     */
    @Override
    final public Void visit(final ASTOptionalGraphPattern node, Object data)
            throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;
        
        graphPattern = new GroupGraphPattern(parentGP);

        // visit the children.
        super.visit(node, null);

        @SuppressWarnings("rawtypes")
        final GroupNodeBase group = graphPattern.buildGroup(new JoinGroupNode(
                true/* optional */));

        parentGP.add(group);

        graphPattern = parentGP;

        return null;
    }

    @Override
    final public Void visit(final ASTGraphGraphPattern node, Object data)
            throws VisitorException {
        
        final TermNode oldContext = graphPattern.getContext();
        final Scope oldScope = graphPattern.getStatementPatternScope();

        final TermNode newContext = (TermNode) node.jjtGetChild(0).jjtAccept(
                this, null);

        graphPattern.setContextVar(newContext);
        graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);

        node.jjtGetChild(1).jjtAccept(this, null);

        graphPattern.setContextVar(oldContext);
        graphPattern.setStatementPatternScope(oldScope);

        return null;
    }

    @Override
    final public Void visit(final ASTUnionGraphPattern node, Object data)
            throws VisitorException {
        
        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern(parentGP);
        
        node.jjtGetChild(0).jjtAccept(this, null);
        
        node.jjtGetChild(1).jjtAccept(this, null);
        
        parentGP.add(graphPattern.buildGroup(new UnionNode()));
        
        graphPattern = parentGP;

        return null;
        
    }

//    @Override // FIXME MinusGraphPattern
//    public Object visit(ASTMinusGraphPattern node, Object data)
//        throws VisitorException
//    {
//        GraphPattern parentGP = graphPattern;
//
//        TupleExpr leftArg = graphPattern.buildTupleExpr();
//
//        graphPattern = new GraphPattern(parentGP);
//        node.jjtGetChild(0).jjtAccept(this, null);
//        TupleExpr rightArg = graphPattern.buildTupleExpr();
//
//        parentGP = new GraphPattern();
//        parentGP.addRequiredTE(new Difference(leftArg, rightArg));
//        graphPattern = parentGP;
//
//        return null;
//    }
//

//    /**
//     * A SubSelect can appear in an {@link ASTGraphPatternGroup}.
//     * <p>
//     * Note: Delegated to an anonymous {@link BigdataExprBuilder}. The
//     * {@link GroupGraphPattern} is passed into the delegate so it can recognize
//     * that the {@link ASTSelectQuery} is appearing as a subquery rather than as
//     * the top-level query.
//     */
//    @Override
//    final public SubqueryRoot visit(final ASTSelectQuery node, Object data)
//            throws VisitorException {
//
//        final SubqueryRoot subSelect = (SubqueryRoot) node.jjtAccept(
//                new BigdataExprBuilder(context), graphPattern/* not-null */);
//
//        graphPattern.add(subSelect);
//
//        return subSelect;
//
//    }

    /**
     * A BIND (or FILTER) can appear in an {@link ASTBasicGraphPattern}.
     * 
     * @return The {@link AssignmentNode} for the BIND.
     */
    @Override
    final public AssignmentNode visit(final ASTBind node, Object data)
            throws VisitorException {

        if (node.jjtGetNumChildren() != 2)
            throw new AssertionError("Expecting two children, not "
                    + node.jjtGetNumChildren() + ", node=" + node.dump(">>>"));

        final ValueExpressionNode ve = (ValueExpressionNode) node
                .jjtGetChild(0).jjtAccept(this, data);

        final Node aliasNode = node.jjtGetChild(1);

        final String alias = ((ASTVar) aliasNode).getName();

        final AssignmentNode bind = new AssignmentNode(new VarNode(alias), ve);

        graphPattern.add(bind);

        return bind;
    }
    
    /**
     * A FILTER. The filter is attached to the {@link #graphPattern}. However,
     * it is also returned from this method. The {@link ASTHavingClause} uses
     * the return value.
     * 
     * @return The constraint.
     */
    @Override
    final public ValueExpressionNode visit(final ASTConstraint node, Object data)
            throws VisitorException {

        final ValueExpressionNode valueExpr = (ValueExpressionNode) node
                .jjtGetChild(0).jjtAccept(this, null);

        graphPattern.addConstraint(valueExpr);

        return valueExpr;

    }

    /**
     * INCLUDE for a named subquery result set.
     */
    @Override
    final public Void visit(final ASTNamedSubqueryInclude node, Object data)
            throws VisitorException {

        graphPattern.add(new NamedSubqueryInclude(node.getName()));

        return null;
        
    }

}
