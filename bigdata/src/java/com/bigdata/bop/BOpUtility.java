/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 17, 2010
 */

package com.bigdata.bop;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.bigdata.bop.BOp.Annotations;
import com.bigdata.btree.AbstractNode;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Operator utility class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo In general recursive traversal iterators do not protect against loops
 *       in the operator tree, but see {@link #getIndex(BOp)}.
 */
public class BOpUtility {

    /**
     * Pre-order recursive visitation of the operator tree (arguments only, no
     * annotations).
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> preOrderIterator(final BOp op) {

        return new Striterator(new SingleValueIterator(op))
                .append(preOrderIterator2(op));

    }

    /**
     * Visits the children (recursively) using pre-order traversal, but does
     * NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    static private Iterator<AbstractNode> preOrderIterator2(final BOp op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the pre-order iterator.
         */

        return new Striterator(op.args().iterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final BOp child = (BOp) childObj;

                if (child.arity() > 0) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive pre-order traversal).
                     */

                    final Striterator itr = new Striterator(
                            new SingleValueIterator(child));

                    // append this node in post-order position.
                    itr.append(preOrderIterator2(child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

                    // Visit the leaf itself.
                    return new SingleValueIterator(child);

                }
            }
        });

    }

    /**
     * Post-order recursive visitation of the operator tree (arguments only, no
     * annotations).
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> postOrderIterator(final BOp op) {

        return new Striterator(postOrderIterator2(op))
                .append(new SingleValueIterator(op));

    }

    /**
     * Visits the children (recursively) using post-order traversal, but does
     * NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    static private Iterator<AbstractNode> postOrderIterator2(final BOp op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the post-order iterator.
         */

        return new Striterator(op.args().iterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final BOp child = (BOp) childObj;

                if (child.arity() > 0) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive post-order traversal).
                     */

                    final Striterator itr = new Striterator(
                            postOrderIterator2(child));

                    // append this node in post-order position.
                    itr.append(new SingleValueIterator(child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

                    // Visit the leaf itself.
                    return new SingleValueIterator(child);

                }
            }
        });

    }

    /**
     * Visit all annotations which are {@link BOp}s (non-recursive).
     * 
     * @param op
     *            An operator.
     * 
     * @return An iterator which visits the {@link BOp} annotations in an
     *         arbitrary order.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> annotationOpIterator(final BOp op) {

        return new Striterator(op.annotations().values().iterator())
                .addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean isValid(Object arg0) {
                        return arg0 instanceof BOp;
                    }
                });
        
    }

//    /**
//     * Pre-order traversal of the annotations of the operator which are
//     * themselves operators without recursion through the children of the given
//     * operator (the children of each annotation are visited, but the
//     * annotations of annotations are not).
//     * 
//     * @param op
//     *            An operator.
//     * 
//     * @return An iterator which visits the pre-order traversal or the operator
//     *         annotations.
//     */
//    @SuppressWarnings("unchecked")
//    public static Iterator<BOp> annotationOpPreOrderIterator(final BOp op) {
//        
//        // visit the node's operator annotations.
//        final Striterator itr = new Striterator(annotationOpIterator(op));
//
//        // expand each operator annotation with a pre-order traversal.
//        itr.addFilter(new Expander() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            protected Iterator<?> expand(final Object ann) {
//                return preOrderIterator((BOp) ann);
//            }
//        });
//
//        return (Iterator<BOp>) itr;
//        
//    }
    
    /**
     * Recursive pre-order traversal of the operator tree with visitation of all
     * operator annotations. The annotations for an operator are visited before
     * its children are visited. Only annotations whose values are {@link BOp}s
     * are visited. Annotation {@link BOp}s are also recursively visited with
     * the pre-order traversal.
     * 
     * @param op
     *            An operator.
     *            
     * @return The iterator.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> preOrderIteratorWithAnnotations(final BOp op) {
       
        return new Striterator(preOrderIterator(op)).addFilter(new Expander(){

            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator expand(final Object arg0) {

                final BOp op = (BOp)arg0;

                // visit the node.
                final Striterator itr = new Striterator(
                        new SingleValueIterator(op));

                // visit the node's operator annotations.
                final Striterator itr2 = new Striterator(
                        annotationOpIterator(op));

                // expand each operator annotation with a pre-order traversal.
                itr2.addFilter(new Expander() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Iterator expand(final Object ann) {
                        return preOrderIteratorWithAnnotations((BOp) ann);
                    }
                });
                
                // append the pre-order traveral of each annotation.
                itr.append(itr2);

                return itr;
            }
            
        });
        
    }

    /**
     * Return all variables recursively using a pre-order traversal present
     * whether in the operator tree or on annotations attached to operators.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<IVariable<?>> getSpannedVariables(final BOp op) {

        return new Striterator(preOrderIteratorWithAnnotations(op))
                .addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean isValid(Object arg0) {
                        return arg0 instanceof IVariable<?>;
                    }
                });

    }

    /**
     * Return the variables from the operator's arguments.
     * 
     * @param op
     *            The operator.
     *            
     * @return An iterator visiting its {@link IVariable} arguments.
     */
    @SuppressWarnings("unchecked")
    static public Iterator<IVariable<?>> getArgumentVariables(final BOp op) {

        return new Striterator(op.args().iterator())
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean isValid(final Object arg0) {
                        return arg0 instanceof IVariable<?>;
                    }
                });

    }

    /**
     * The #of arguments to this operation which are variables. This method does
     * not report on variables in child nodes nor on variables in attached
     * {@link IConstraint}, etc.
     */
    static public int getArgumentVariableCount(final BOp op) {
        int nvars = 0;
        final Iterator<BOp> itr = op.args().iterator();
        while(itr.hasNext()) {
            final BOp arg = itr.next();
            if (arg instanceof IVariable<?>)
                nvars++;
        }
        return nvars;
    }

    /**
     * Return an index from the {@link BOp.Annotations#BOP_ID} to the
     * {@link BOp} for each spanned {@link BOp} (including annotations).
     * {@link BOp}s without identifiers are not indexed.
     * 
     * @param op
     *            A {@link BOp}.
     * 
     * @return The index.
     * 
     * @todo define recursive striterator for {@link BOp}s (as top-level method)
     *       and then layer on an expander for the {@link BOp} annotations.
     *       Finally, layer in a filter for the presence of the bopId. The
     *       {@link BOp}s visited by the iterator should be inserted into the
     *       indexed. [it is an error if there is a duplicate bopId.]
     */
    static public Map<Integer,BOp> getIndex(final BOp op) {
        final LinkedHashMap<Integer, BOp> map = new LinkedHashMap<Integer, BOp>();
        final Iterator<BOp> itr = preOrderIteratorWithAnnotations(op);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            final Object x = t.getProperty(Annotations.BOP_ID);
            if (x == null) {
                continue;
            }
            if (!(x instanceof Integer)) {
                throw new BadBOpIdTypeException("Must be Integer, not: "
                        + x.getClass() + ": " + Annotations.BOP_ID);
            }
            final Integer id = (Integer) t.getProperty(Annotations.BOP_ID);
            final BOp conflict = map.put(id, t);
            if (conflict != null)
                throw new DuplicateBOpIdException("duplicate id=" + id + " for "
                        + conflict + " and " + t);
        }
        return map;
    }

}
