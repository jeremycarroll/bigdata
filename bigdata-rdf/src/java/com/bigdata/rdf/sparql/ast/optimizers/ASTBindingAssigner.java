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
/*
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Examines the source {@link IBindingSet}[]. If there is a single binding set
 * in the source, then any variable bound in that input is rewritten in the AST
 * to be a {@link ConstantNode}. The rewrite uses the special form of the
 * {@link Constant} constructor which associates the variable with the constant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTBindingAssigner implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTBindingAssigner.class);
    
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSet) {

        if (bindingSet == null || bindingSet.length != 1) {
            /*
             * Used iff there is only one input solution.
             * 
             * TODO We can still apply this when there are multiple solutions if
             * any variable is bound to the same constant in all solutions.
             */
            return queryNode;
        }

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        // consider only the first solution.
        final IBindingSet bset = bindingSet[0];

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        {
            
            @SuppressWarnings("unchecked")
            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                doBindingAssignment(whereClause, bset);

            }

        }

        /*
         * Examine each named subquery. If there is more than one ServiceNode,
         * or if a ServiceNode is embedded in a subquery, then lift it out into
         * its own named subquery root, replacing it with a named subquery
         * include.
         */
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (int i = 0; i < namedSubqueries.size(); i++) {
            
                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                        .get(i);

                @SuppressWarnings("unchecked")
                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause == null)
                    continue;

                doBindingAssignment(whereClause, bset);

            }

        }
        
        return queryNode;

    }
    
    /**
     * Gather the VarNodes for variables which have bindings.
     * <p>
     * Gather variables which are in a FILTER(sameTerm(var,const)) or
     * FILTER(sameTerm(const,var)) expression.
     */
    private void doBindingAssignment(
            final GroupNodeBase<IGroupMemberNode> whereClause,
            final IBindingSet bset) {

        final Map<VarNode, ConstantNode> replacements = new LinkedHashMap<VarNode, ConstantNode>();

        final Iterator<BOp> itr = BOpUtility
                .preOrderIterator((BOp) whereClause);

        while (itr.hasNext()) {

            final BOp node = (BOp) itr.next();

            if (node instanceof FilterNode) {

                /*
                 * Gather variables which are in a FILTER(sameTerm(var,const))
                 * or FILTER(sameTerm(const,var)) expression.
                 */
                tryReplace(replacements, (FilterNode) node);
                continue;
            }

            if (node instanceof VarNode) {

                /*
                 * Gather the VarNodes for variables which have bindings.
                 */

                final VarNode varNode = (VarNode) node;

                if (replacements.containsKey(varNode))
                    continue;

                tryReplace(replacements, varNode, bset);

            }

        }

        int ntotal = 0;

        for (Map.Entry<VarNode, ConstantNode> e : replacements.entrySet()) {

            final VarNode oldVal = e.getKey();

            final ConstantNode newVal = e.getValue();

            int nmods = ((ASTBase) whereClause).replaceAllWith(oldVal, newVal);

            nmods += ((ASTBase) whereClause).replaceAllWith(oldVal, newVal);

            if (log.isInfoEnabled())
                log.info("Replaced " + nmods + " instances of " + oldVal
                        + " with " + newVal);

            assert nmods > 0; // Failed to replace something.

            ntotal += nmods;

        }

        if (log.isInfoEnabled())
            log.info("Replaced " + ntotal + " instances of "
                    + replacements.size() + " bound variables with constants");

    }

    /**
     * Gather the VarNodes for variables which have bindings.
     * 
     * @param replacements
     * @param varNode
     * @param bset
     * 
     */
    private void tryReplace(final Map<VarNode, ConstantNode> replacements,
            final VarNode varNode, final IBindingSet bset) {

        @SuppressWarnings("rawtypes")
        final IVariable<IV> var = varNode.getValueExpression();

        if (bset.isBound(var)) {

            /*
             * Replace the variable with the constant from the binding
             * set, but preserve the reference to the variable on the
             * Constant.
             */

            @SuppressWarnings("rawtypes")
            final IV asBound = (IV) bset.get(var).get();

//            @SuppressWarnings("rawtypes")
//            final ConstantNode constNode = new ConstantNode(
//                    new Constant<IV>(var, asBound));

            willReplace(replacements, varNode, asBound);

        }

    }

    /**
     * Gather variables which are in a FILTER(sameTerm(var,const)) or
     * FILTER(sameTerm(const,var)) expression.
     * 
     * @param replacements
     * @param o
     */
    private void tryReplace(final Map<VarNode, ConstantNode> replacements,
            final FilterNode filter) {
        
        final IValueExpressionNode vexpr = filter.getValueExpressionNode();
        
        if(!(vexpr instanceof FunctionNode))
            return;
        
        final FunctionNode functionNode = (FunctionNode) vexpr;
        
        final URI functionURI = functionNode.getFunctionURI();
        
        if (!functionURI.equals(FunctionRegistry.SAME_TERM) &&
        		!functionURI.equals(FunctionRegistry.EQ))
            return;

        final IValueExpressionNode left = (IValueExpressionNode) functionNode
                .get(0);
        
        final IValueExpressionNode right = (IValueExpressionNode) functionNode
                .get(1);

        if (left instanceof VarNode && right instanceof ConstantNode) {

        	final IV constant = ((ConstantNode) right).getValueExpression().get();
        	
        	// we cannot do the replace for EQ when then constant is a literal
        	if (functionURI.equals(FunctionRegistry.EQ) && constant.isLiteral())
        		return;
        	
            willReplace(replacements, (VarNode) left, constant);
//                    ((ConstantNode) right).getValueExpression().get());

        } else if (left instanceof ConstantNode && right instanceof VarNode) {

        	final IV constant = ((ConstantNode) left).getValueExpression().get();
        	
        	// we cannot do the replace for EQ when then constant is a literal
        	if (functionURI.equals(FunctionRegistry.EQ) && constant.isLiteral())
        		return;
        	
            willReplace(replacements, (VarNode) right, constant);
//                    ((ConstantNode) left).getValueExpression().get());

        }
        
    }

    private void willReplace(final Map<VarNode, ConstantNode> replacements,
            final VarNode varNode, final IV<?, ?> asBound) {

        if (replacements.containsKey(varNode))
            return;

        @SuppressWarnings("rawtypes")
        final ConstantNode constNode = new ConstantNode(new Constant<IV>(
                varNode.getValueExpression(), asBound));

        if (log.isInfoEnabled())
            log.info("Will replace: var=" + varNode + " with (" + asBound + ") as "
                    + constNode);

        replacements.put(varNode, constNode);

    }

}
