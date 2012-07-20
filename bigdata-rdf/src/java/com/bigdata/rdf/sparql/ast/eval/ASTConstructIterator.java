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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.rdf.filter.NativeDistinctFilter;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ICloseable;

import cutthecrap.utils.striterators.IFilterTest;

/**
 * Iterator consumes the solutions from a query and interprets them according to
 * a {@link ConstructNode}. Ground triples in the template are output
 * immediately. Any non-ground triples are output iff they are fully (and
 * validly) bound for a given solution. Blank nodes are scoped to a solution.
 * <p>
 * Note: This supports construct of quads, but the SPARQL grammar does not.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTConstructIterator.java 5131 2011-09-05 20:48:48Z thompsonbry
 *          $
 */
public class ASTConstructIterator implements
        CloseableIteration<BigdataStatement, QueryEvaluationException> {

    private static final Logger log = Logger
            .getLogger(ASTConstructIterator.class);
    
    private final BigdataValueFactory f;

    /**
     * The non-ground statement patterns.
     */
    private final List<StatementPatternNode> templates;
    
    /**
     * Ground triples from the template.
     */
    private final List<BigdataStatement> groundTriples;

    private final CloseableIteration<BindingSet, QueryEvaluationException> src;

    /**
     * A list of {@link Statement}s constructed from the most recently visited
     * solution. Statements are drained from this buffer, sending them to the
     * sink. Once the buffer is empty, we will go back to the {@link #src} to
     * refill it.
     * <p>
     * The buffer is pre-populated with any ground triples in the construct
     * template by the constructor.
     */
    private final List<BigdataStatement> buffer = new LinkedList<BigdataStatement>();

    /**
     * A filter which restricts the emitted statements to the distinct
     * {@link ISPO}. The {@link DistinctFilter} is based on the Java heap. The
     * {@link NativeDistinctFilter} is based on persistence capable data
     * structures and can scale to high cardinality outputs. Unfortunately, a
     * complex CONSTRUCT template can make it impossible to predict the
     * <p>
     * Note: It is possible to disable this filter (in the code) by setting it
     * to <code>null</code>.
     * 
     * @see DistinctFilter
     * @see NativeDistinctFilter
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    private final IFilterTest filter;
    
//    /**
//     * Return <code>true</code>iff {@link LexiconRelation#isStoreBlankNodes()}
//     * is <code>true</code>.
//     */
//    private final boolean toldBNodes;
    
    /**
     * A factory for blank node identifiers, which are scoped to a solution.
     */
    private int bnodeIdFactory = 0;
    
    private boolean open = true;

    /**
     * <code>true</code> until we get the first solution for the WHERE clause.
     * We do not output ground triples in the template until we see that first
     * solution. It is Ok if it is empty, but we need to know that the WHERE
     * clause succeeded before we can output the ground triples.
     */
    private boolean haveFirstSolution = false; 
    
    /**
     * 
     */
    public ASTConstructIterator(//
            final AbstractTripleStore store,//
            final ConstructNode construct,//
            final CloseableIteration<BindingSet, QueryEvaluationException> src) {

        this.f = store.getValueFactory();
        
//        this.toldBNodes = store.getLexiconRelation().isStoreBlankNodes();
        
        templates = new LinkedList<StatementPatternNode>();

        groundTriples = new LinkedList<BigdataStatement>();

        // Blank nodes (scoped to the solution).
        Map<String, BigdataBNode> bnodes = null;

        for (StatementPatternNode pat : construct) {

            if (pat.isGround()) {

                if(bnodes == null)
                    bnodes = new LinkedHashMap<String, BigdataBNode>();
                
                // Create statement from the template.
                final BigdataStatement stmt = makeStatement(pat,
                        EmptyBindingSet.getInstance(), bnodes);
                
//                final BigdataStatement stmt = f.createStatement(//
//                        (Resource) pat.s().getValue(),//
//                        (URI) pat.p().getValue(),//
//                        (Value) pat.o().getValue(),//
//                        pat.c() == null ? null : (Resource) pat.c().getValue()//
//                        );

                if (log.isDebugEnabled())
                    log.debug("Ground statement:\npattern=" + pat + "\nstmt="
                            + stmt);

                groundTriples.add(stmt);

            } else {

                /*
                 * A statement pattern that we will process for each solution.
                 */

                templates.add(pat);

            }

        }

        this.src = src;

        /*
         * Setup the DISTINCT SPO filter.
         */
        
        final boolean nativeDistinct = construct.isNativeDistinct();
        if (nativeDistinct) {
            
            /*
             * Construct a predicate for the first triple template. We will use
             * that as the bias for the scalable DISTINCT SPO filter.
             */
            final IPredicate pred;
            {

                final StatementPatternNode sp = templates.get(0/* index */);

                final IVariableOrConstant<IV> s = sp.s().getValueExpression();
                final IVariableOrConstant<IV> p = sp.p().getValueExpression();
                final IVariableOrConstant<IV> o = sp.o().getValueExpression();

//                // The graph term/variable iff specified by the query.
//                final TermNode cvar = sp.c();
//                final IVariableOrConstant<IV> c = cvar == null ? null : cvar
//                        .getValueExpression();

                final BOp[] vars = new BOp[] { s, p, o /*, c*/ };

                pred = new SPOPredicate(vars, BOp.NOANNS);

            }
            
            /*
             * The index that will be used to read on the B+Tree access path.
             */
            @SuppressWarnings({ "unchecked", "rawtypes" })
            final SPOKeyOrder indexKeyOrder = SPOKeyOrder.getKeyOrder(
                    (IPredicate) pred, 3/* keyArity */);
            
            construct.setProperty(NativeDistinctFilter.Annotations.KEY_ORDER,
                    indexKeyOrder);

            // Native memory based DISTINCT filter.
            filter = new NativeDistinctFilter.DistinctFilterImpl(construct);

        } else {
            
            // JVM Based DISTINCT filter.
            filter = new DistinctFilter.DistinctFilterImpl(construct);
            
        }
        
    }
    
    public boolean hasNext() throws QueryEvaluationException {

        while (true) {

            if (!buffer.isEmpty()) {

                /*
                 * At least one statement is ready in the buffer.
                 */

                return true;
                
            }

            if (!src.hasNext()) {

                /*
                 * Nothing left to visit.
                 */
                
                close();

                return false;

            }

            /*
             * Refill the buffer from the next available solution.
             */
            
            fillBuffer(src.next());

            /*
             * Check to see whether we can assemble any statements from that
             * solution.
             */
            
            continue;
            
        }
        
    }

    @Override
    public BigdataStatement next() throws QueryEvaluationException {

        if (!hasNext())
            throw new NoSuchElementException();

        /*
         * Remove and return the first statement from the buffer.
         */
        
        return buffer.remove(0);
        
    }

    public void close() throws QueryEvaluationException {

        if (open) {
        
            open = false;
            
            src.close();
            
            if (filter instanceof ICloseable) {

                /*
                 * Ensure that we release the backing MemoryManager in a timely
                 * fashion.
                 * 
                 * @see <a
                 * href="https://sourceforge.net/apps/trac/bigdata/ticket/582">
                 * IStriterator does not support close() protocol for IFilter
                 * </a>
                 */
                ((ICloseable) filter).close();

            }

        }

    }

    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();

    }

    /**
     * Refill the buffer from a new solution. This method is responsible for the
     * scope of blank nodes and for discarding statements which are ill-formed
     * (missing slots, bad type for a slot, etc).
     * 
     * @param solution
     */
    private void fillBuffer(final BindingSet solution) {

        // Should only be invoked when the buffer is empty.
        assert buffer.isEmpty();
        
        if (!haveFirstSolution) {

            /*
             * Once we see the first solution (even if it is empty) we can
             * output the ground triples from the template, but not before.
             */

            haveFirstSolution = true;
            
            for(BigdataStatement stmt : groundTriples) {
                
                addStatementToBuffer(stmt);
                
            }
            
        }
        
        // Blank nodes (scoped to the solution).
        final Map<String,BigdataBNode> bnodes = new LinkedHashMap<String, BigdataBNode>();
        
        for(StatementPatternNode pat : templates) {

            /*
             * Attempt to build a statement from this statement pattern and
             * solution.
             */
            
            final BigdataStatement stmt = makeStatement(pat, solution, bnodes);

            if(stmt != null) {
            
                // If successful, then add to the buffer.
                addStatementToBuffer(stmt);
                
            }
            
        }

    }

    /**
     * Add a statement to the output buffer.
     * 
     * @param stmt
     *            The statement.
     */
    private void addStatementToBuffer(final BigdataStatement stmt) {
        
        if(log.isDebugEnabled())
            log.debug(stmt.toString());
        
        if(filter != null) {

            /*
             * Impose a DISTINCT SPO filter on the generated statements in the
             * constructed graph.
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
             * CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
             */
            
            if(filter.isValid(stmt)) {
                
                buffer.add(stmt);
                
            }
            
        } else {

            buffer.add(stmt);

        }
        
    }
    
    /**
     * Return a statement if a valid statement could be constructed for that
     * statement pattern and this solution.
     * 
     * @param pat
     *            A statement pattern from the construct template.
     * @param solution
     *            A solution from the query.
     * @param bnodes
     *            A map used to scope blank nodes to the solution.
     * 
     * @return A statement if a valid statement could be constructed for that
     *         statement pattern and this solution.
     */
    private BigdataStatement makeStatement(final StatementPatternNode pat,
            final BindingSet solution, final Map<String, BigdataBNode> bnodes) {

        // resolve values from template and/or solution.
        final BigdataValue s = getValue(pat.s(), solution, bnodes);
        final BigdataValue p = getValue(pat.p(), solution, bnodes);
        final BigdataValue o = getValue(pat.o(), solution, bnodes);
        final BigdataValue c = pat.c() == null ? null : getValue(pat.c(),
                solution, bnodes);

        // filter out unbound values.
        if (s == null || p == null || o == null)
            return null;

        // filter out bindings which do not produce legal statements.
        if (!(s instanceof Resource))
            return null;
        if (!(p instanceof URI))
            return null;
        if (!(o instanceof Value))
            return null;
        if (c != null && !(c instanceof Resource))
            return null;

        // return the statement
        return f.createStatement((Resource) s, (URI) p, (Value) o, (Resource) c);
        
    }

    /**
     * Return the as-bound value of the variable or constant given the solution.
     * 
     * @param term
     *            Either a variable or a constant from the statement pattern in
     *            the template.
     * @param solution
     *            A solution from the query.
     * @param bnodes
     *            A map used to scope blank nodes to the solution.
     * 
     * @return The as-bound value.
     */
    private BigdataValue getValue(final TermNode term,
            final BindingSet solution, final Map<String, BigdataBNode> bnodes) {

        if (term instanceof ConstantNode) {

            final BigdataValue value = term.getValue();
            
            if(value instanceof BigdataBNode) {

                return getBNode(((BigdataBNode) value).getID(), bnodes);

            }
            
            return value;

        } else if(term instanceof VarNode) {

            /*
             * I can't quite say whether or not this is a hack, so let me
             * explain what is going on instead. When the SPARQL grammar parses
             * a blank node in a query, it is *always* turned into an anonymous
             * variable. So, when we interpret the CONSTRUCT template, we are
             * going to see anonymous variables and we have to recognize them
             * and treat them as if they were really blank nodes.
             * 
             * The code here tests VarNode.isAnonymous() and, if the variable is
             * anonymous, it uses the variable's *name* as a blank node
             * identifier (ID). It then obtains a unique within scope blank node
             * which is correlated with that blank node ID.
             */
            
            final VarNode v = (VarNode) term;

            final String varname = v.getValueExpression().getName();

            if(v.isAnonymous()) {
                
                return getBNode(varname, bnodes); 
                
            }
            
            final BigdataValue val = (BigdataValue) solution.getValue(varname);
            
            /*
             * Note: Doing this will cause several of the DAWG CONSTRUCT tests
             * to fail...
             */
            if (false && val instanceof BigdataBNode) {
 
                return getBNode(((BigdataBNode) val).getID(), bnodes);
            
            }
            
            return val;

        } else {
            
            // TODO Support the BNode() function here?
            throw new UnsupportedOperationException("term: "+term);
            
        }
        
    }
    
    /**
     * Scope the bnode ID to the solution. The same ID in each solution is
     * mapped to the same bnode. The same ID in a new solution is mapped to a
     * new BNode.
     */
    private BigdataBNode getBNode(final String id,
            final Map<String, BigdataBNode> bnodes) {

        final BigdataBNode tmp = bnodes.get(id);

        if (tmp != null) {

            // We've already seen this ID for this solution.
            return tmp;

        }

        /*
         * This is the first time we have seen this ID for this solution. We
         * create a new blank node with an identifier which will be unique
         * across the solutions.
         */

        // new bnode, which will be scoped to this solution.
        final BigdataBNode bnode = f.createBNode("b"
                + Integer.valueOf(bnodeIdFactory++).toString());

        // put into the per-solution cache.
        bnodes.put(id, bnode);

        return bnode;

    }
    
}
