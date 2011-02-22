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
 * Created on Feb 20, 2011
 */

package com.bigdata.bop.util;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.constraint.AND;
import com.bigdata.bop.constraint.BooleanValueExpression;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.joinGraph.rto.JoinGraph.JGraph;

/**
 * 
 * This test suite is built around around BSBM Q5. Each test has an existing
 * join path and a new vertex to be added to the join path. The question is
 * whether or not the vertex <em>can join</em> with the join path using one or
 * more shared variable(s). This tests a method used to incrementally grow a
 * join path when it is dynamically decided that an {@link IPredicate} may be
 * added to the join path based on shared variables. Static analysis easily
 * reports those joins which are allowed based on the variables directly given
 * with two {@link IPredicate}s. The purpose of this test suite is to explore
 * when joins (based on shared variables) become permissible through
 * {@link IConstraint}s as the variable(s) used within those constraints become
 * bound.
 * <p>
 * Note: To avoid a dependency on the RDF model layer, this just uses String
 * constants for URIs and Literals.
 * <h2>Analysis of BSBM Q5</h2>
 * The following predicates all join on {@link #product}:
 * <ul>
 * <li>{@link #p0}</li>
 * <li>{@link #p2}</li>
 * <li>{@link #p4}</li>
 * <li>{@link #p5}</li>
 * </ul>
 * The predicates ({@link #p3} and {@link #p5}) do not directly join with any of
 * the other predicates (they do not directly share any variables). In general,
 * a join without shared variables means the cross product of the sources will
 * be materialized and such joins should be run last.
 * <p>
 * However, in this case there are two SPARQL FILTERs ({@link #c1} and
 * {@link #c2}) which (a) use those variables ({@link #origProperty1} and
 * {@link #origProperty2}); and (b) can constrain the query. This means that
 * running the predicates without shared variables and applying the constraints
 * before the tail of the plan can in fact lead to a more efficient join path.
 * <p>
 * This set of unit tests explores various join paths and verifies that the
 * canJoin() and canJoinUsingConstraints() methods correctly recognize edges by
 * which a join path can be extended corresponding to both static and dynamic
 * analysis of the query.
 * 
 * @see BOpUtility#canJoin(IPredicate, IPredicate)
 * @see BOpUtility#canJoinUsingConstraints(IPredicate[], IPredicate,
 *      IConstraint[])
 * @see JGraph
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBOpUtility_canJoinUsingConstraints.java 4211 2011-02-20
 *          21:20:44Z thompsonbry $
 * 
 * @todo These are the full plans generated by the runtime and static
 *       optimizers. One way to test canJoinXXX() is to run out these join plans
 *       and verify that they report "true" in each case. However, the critical
 *       bit to test are join plans where the predicates w/o the shared
 *       variables can be run earlier due to the FILTERs.
 * 
 *       <pre>
 * test_bsbm_q5 : static [0] : : ids=[1, 2, 4, 6, 0, 3, 5]
 * test_bsbm_q5 : runtime[0] : : ids=[1, 2, 0, 4, 6, 3, 5]
 * </pre>
 */
//@SuppressWarnings("unchecked")
public class TestBOpUtility_canJoinUsingConstraints extends TestCase2 {

    /**
     * 
     */
    public TestBOpUtility_canJoinUsingConstraints() {
    }

    /**
     * @param name
     */
    public TestBOpUtility_canJoinUsingConstraints(String name) {
        super(name);
    }

    /**
     * Unit tests to verify that arguments are validated.
     * 
     * @see BOpUtility#canJoinUsingConstraints(IPredicate[], IPredicate,
     *      IConstraint[])
     */
    public void test_canJoinUsingConstraints_illegalArgument() {

        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IPredicate<?> p1 = new Predicate(new BOp[]{x});
        
        final IPredicate<?> p2 = new Predicate(new BOp[]{y});

        // path must not be null.
        try {
            BOpUtility.canJoinUsingConstraints(//
                    null, // path
                    p1,// vertex
                    new IConstraint[0]// constraints
                    );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

        // vertex must not be null.
        try {
            BOpUtility.canJoinUsingConstraints(//
                    new IPredicate[]{p1}, // path
                    null,// vertex
                    new IConstraint[0]// constraints
                    );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

        // path may not be empty.
        try {
            BOpUtility.canJoinUsingConstraints(//
                    new IPredicate[] {}, // path
                    p1,// vertex
                    new IConstraint[0]// constraints
                    );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

        // path elements may not be null.
        try {
            BOpUtility.canJoinUsingConstraints(//
                    new IPredicate[] { p2, null }, // path
                    p1,// vertex
                    new IConstraint[0]// constraints
                    );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

        // vertex must not appear in the path.
        try {
            BOpUtility.canJoinUsingConstraints(//
                    new IPredicate[] { p2, p1 }, // path
                    p1,// vertex
                    new IConstraint[0]// constraints
                    );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

        // constraint array may not contain null elements.
        try {
            BOpUtility.canJoinUsingConstraints(//
                    new IPredicate[] { p2 }, // path
                    p1,// vertex
                    new IConstraint[] { //
                    Constraint.wrap(new NEConstant(x, new Constant<Integer>(12))), //
                    null //
                    }// constraints
                    );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

    }

    // The comparison operators.
    static private final int GT = 0, LT = 1;// , EQ = 2, GTE = 3, LTE = 4;

    // The math operators.
    static private final int PLUS = 0, MINUS = 1;

    // Annotation for the comparison or math operator.
    static private final String OP = "op";

    /**
     * A do-nothing constraint. The constraint is never evaluated. It is only
     * used to test the logic which decides when two predicates can join based
     * on variable(s) shared via a constraint.
     */
    static private final class MyCompareOp extends BooleanValueExpression {

        private static final long serialVersionUID = 1L;

        /**
         * Required deep copy constructor.
         * 
         * @param op
         */
        public MyCompareOp(MyCompareOp op) {
            super(op);
        }

        /**
         * @param args
         * @param annotations
         */
        public MyCompareOp(BOp[] args, Map<String, Object> annotations) {
            super(args, annotations);
        }

        public Boolean get(IBindingSet bindingSet) {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A do-nothing constraint. The constraint is never evaluated. It is only
     * used to test the logic which decides when two predicates can join based
     * on variable(s) shared via a constraint.
     */
    static private final class NEConstant extends BooleanValueExpression {

        private static final long serialVersionUID = 1L;

        /**
         * Required deep copy constructor.
         * 
         * @param op
         */
        public NEConstant(NEConstant op) {
            super(op);
        }

        /**
         * @param args
         * @param annotations
         */
        public NEConstant(BOp[] args, Map<String, Object> annotations) {
            super(args, annotations);
        }

        public NEConstant(IVariable<?> var, IConstant<?> value) {
            this(new BOp[] { var, value }, null/* annotations */);
        }

        public Boolean get(IBindingSet bindingSet) {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A do-nothing value expression. The expression is never evaluated. It is
     * only used to test the logic which decides when two predicates can join
     * based on variable(s) shared via a constraint.
     */
    static private final class MathBOp extends ImmutableBOp implements
            IValueExpression {

        private static final long serialVersionUID = 1L;

        /**
         * Required deep copy constructor.
         * 
         * @param op
         */
        public MathBOp(final MathBOp op) {

            super(op);

        }

        /**
         * Required shallow copy constructor.
         * 
         * @param args
         *            The operands.
         * @param op
         *            The operation.
         */
        public MathBOp(final BOp[] args, Map<String, Object> anns) {

            super(args, anns);

            if (args.length != 2 || args[0] == null || args[1] == null
                    || getProperty(OP) == null) {

                throw new IllegalArgumentException();

            }

        }

        /**
         * 
         * @param left
         *            The left operand.
         * @param right
         *            The right operand.
         * @param op
         *            The annotation specifying the operation to be performed on
         *            those operands.
         */
        public MathBOp(final IValueExpression left,
                final IValueExpression right, final int op) {

            this(new BOp[] { left, right }, NV.asMap(new NV(OP, op)));

        }

        public Object get(IBindingSet bindingSet) {
            throw new UnsupportedOperationException();
        }
    }

    static private final String rdfs = "http://www.w3.org/2000/01/rdf-schema#";

    static private final String bsbm = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";

    static private final String rdfsLabel = rdfs + "label";

    static private final String productFeature = bsbm + "productFeature";

    static private final String productPropertyNumeric1 = "productPropertyNumeric1";

    static private final String productPropertyNumeric2 = bsbm
            + "productPropertyNumeric2";

    static private final String productInstance = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22";

    private int nextId = 0;

    final IVariable<?> product = Var.var("product");

    final IVariable<?> productLabel = Var.var("productLabel");

    final IVariable<?> prodFeature = Var.var("prodFeature");

    final IVariable<?> simProperty1 = Var.var("simProperty1");

    final IVariable<?> simProperty2 = Var.var("simProperty2");

    final IVariable<?> origProperty1 = Var.var("origProperty1");

    final IVariable<?> origProperty2 = Var.var("origProperty2");

    /** ?product rdfs:label ?productLabel . */
    final private IPredicate<?> p0 = new Predicate(new BOp[] {//
            product, new Constant(rdfsLabel), productLabel },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** productInstance bsbm:productFeature ?prodFeature . */
    final private IPredicate<?> p1 = new Predicate(new BOp[] { //
            new Constant(productInstance), new Constant(productFeature),
                    prodFeature },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** ?product bsbm:productFeature ?prodFeature . */
    final private IPredicate<?> p2 = new Predicate(new BOp[] { //
            product, new Constant(productFeature), prodFeature },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** productInstance bsbm:productPropertyNumeric1 ?origProperty1 . */
    final private IPredicate<?> p3 = new Predicate(new BOp[] { //
            new Constant<String>(productInstance),
                    new Constant(productPropertyNumeric1), origProperty1 },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** ?product bsbm:productPropertyNumeric1 ?simProperty1 . */
    final private IPredicate<?> p4 = new Predicate(new BOp[] { //
            product, new Constant(productPropertyNumeric1), simProperty1 },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** productInstance bsbm:productPropertyNumeric2 ?origProperty2 . */
    final private IPredicate<?> p5 = new Predicate(new BOp[] { //
            new Constant(productInstance),
                    new Constant(productPropertyNumeric2), origProperty2 },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** ?product bsbm:productPropertyNumeric2 ?simProperty2 . */
    final private IPredicate<?> p6 = new Predicate(new BOp[] { //
            product, new Constant(productPropertyNumeric2), simProperty2 },//
            new NV(BOp.Annotations.BOP_ID, nextId++)//
    );

    /** The vertices of the join graph (the predicates). */
    final IPredicate<?>[] preds = new IPredicate[] { p0, p1, p2, p3, p4, p5, p6 };

    /**
     * FILTER (productInstance != ?product)
     */
    final IConstraint c0 = Constraint.wrap(new NEConstant(product, new Constant<String>(
            productInstance)));

    /**
     * FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 >
     * (?origProperty1 - 120))
     * <p>
     * Note: The AND in the compound filters is typically optimized out such
     * that each of these is represented as its own IConstraint, but I have
     * combined them for the purposes of these unit tests.
     */
    final IConstraint c1 = Constraint.wrap(new AND(//
            new MyCompareOp(
                    new BOp[] {
                            simProperty1,
                            new MathBOp(origProperty1, new Constant<Integer>(
                                    120), PLUS) }, NV.asMap(new NV[] { new NV(
                            OP, LT) })), //
            new MyCompareOp(new BOp[] {
                    simProperty1,
                    new MathBOp(origProperty1, new Constant<Integer>(120),
                            MINUS) }, NV.asMap(new NV[] { new NV(OP, GT) }))//
    ));

    /**
     * FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 >
     * (?origProperty2 - 170))
     * <p>
     * Note: The AND in the compound filters is typically optimized out such
     * that each of these is represented as its own IConstraint, but I have
     * combined them for the purposes of these unit tests.
     */
    final IConstraint c2 = Constraint.wrap(new AND(//
            new MyCompareOp(
                    new BOp[] {
                            simProperty2,
                            new MathBOp(origProperty2, new Constant<Integer>(
                                    170), PLUS) }, NV.asMap(new NV[] { new NV(
                            OP, LT) })),//
            new MyCompareOp(new BOp[] {
                    simProperty2,
                    new MathBOp(origProperty2, new Constant<Integer>(170),
                            MINUS) }, NV.asMap(new NV[] { new NV(OP, GT) }))//
    ));

    /** The constraints on the join graph. */
    final IConstraint[] constraints = new IConstraint[] { c0, c1, c2 };

    /**
     * Unit test for one-step joins based on the {@link #product} variable.
     */
    public void test_canJoinUsingConstraints_1step_productVar() {

        // share ?product
        final IPredicate<?>[] a = new IPredicate[] { p0, p2, p4, p6 };
        for (int i = 0; i < a.length; i++) {
            for (int j = i; j < a.length; j++) {
                final IPredicate<?> t0 = a[i];
                final IPredicate<?> t1 = a[j];
                assertTrue(BOpUtility.canJoin(t0, t1));
                assertTrue(BOpUtility.canJoin(t1, t0));
                if (t0 != t1) {
                    /*
                     * Test join path extension, but not when the vertex used to
                     * extend the path is already present in the join path.
                     */
                    assertTrue(BOpUtility.canJoinUsingConstraints(//
                            new IPredicate[] { t0 }, // path
                            t1,// vertex
                            new IConstraint[0]// constraints
                            ));
                    assertTrue(BOpUtility.canJoinUsingConstraints(//
                            new IPredicate[] { t1 }, // path
                            t0,// vertex
                            new IConstraint[0]// constraints
                            ));
                }
            }
        }

    }

    /**
     * Unit test for multi-step join paths based on the {@link #product}
     * variable.
     */
    public void test_canJoinUsingConstraints_multiStep_productVar() {

        final Random r = new Random();
        
        // share ?product
        final IPredicate<?>[] a = new IPredicate[] { p0, p2, p4, p6 };
        
        // existing path length [1:3].
        final int existingPathLength = r.nextInt(3)+1;
        
        // generated pre-existing path.
        final IPredicate<?>[] path = new IPredicate[existingPathLength];
        // vertex which will extend that path
        final IPredicate<?> vertex;
        {
            // collection of predicates used so far by the path.
            final Set<Integer> used = new LinkedHashSet<Integer>();
            for (int i = 0; i < path.length; i++) {
                // Locate an unused predicate.
                int index;
                while (true) {
                    index = r.nextInt(a.length);
                    if (!used.contains(index)) {
                        used.add(index);
                        break;
                    }
                }
                // add to the path.
                path[i] = a[index];
            }
            // Locate an unused predicate to serve as the extension vertex.
            {
                // Locate an unused predicate.
                int index;
                while (true) {
                    index = r.nextInt(a.length);
                    if (!used.contains(index)) {
                        used.add(index);
                        break;
                    }
                }
                vertex = a[index];
            }
        }

        // Verify all joins in the path are legal.
        for (int i = 0; i < path.length - 1; i++) {
            assertTrue(BOpUtility.canJoin(path[i], path[i + 1]));
        }
        
        // Verify the extension of the path is legal.
        assertTrue(BOpUtility.canJoinUsingConstraints(//
                path,//
                vertex,//
                new IConstraint[0]// constraints
                ));

    }

    /**
     * Unit test examines the predicates without shared variables and verifies
     * (a) that joins are not permitted when the constraints are not considered;
     * and (b) that joins are permitted when the constraints are considered.
     * <p>
     * This test is identical to {@link #test_canJoinUsingConstraints_p5_p6()()}
     * except that it considers the ({@link #p3} x {@link #p4}) join via the
     * {@link #c1} constraint instead.
     */
    public void test_canJoinUsingConstraints_p3_p4() {

        /*
         * Verify (p3,p4) join is not permitted when we do not consider the
         * constraints (i.e., the join would be an unconstrained cross product
         * if it were executed).
         */
        assertFalse(BOpUtility.canJoin(p3, p4));
        assertFalse(BOpUtility.canJoin(p4, p3));
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p3 }, // path
                p4,// vertex
                new IConstraint[0]// constraints
                ));
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p4 }, // path
                p3,// vertex
                new IConstraint[0]// constraints
                ));

        /*
         * Verify (p3,p4) join is not permitted if we do not consider the
         * constraint which provides the shared variables.
         */
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p3 }, // path
                p4,// vertex
                new IConstraint[] { c2 }// constraints
                ));
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p4 }, // path
                p3,// vertex
                new IConstraint[] { c2 }// constraints
                ));

        /*
         * Verify (p3,p4) join is permitted if we consider the constraint which
         * provides the shared variables.
         */
        assertTrue(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p3 }, // path
                p4,// vertex
                new IConstraint[] { c1 }// constraints
                ));
        assertTrue(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p4 }, // path
                p3,// vertex
                new IConstraint[] { c1 }// constraints
                ));

    }

    /**
     * Unit test examines the predicates without shared variables and verifies
     * (a) that joins are not permitted when the constraints are not considered;
     * and (b) that joins are permitted when the constraints are considered.
     * <p>
     * This test is identical to {@link #test_canJoinUsingConstraints_p3_p4()}
     * except that it considers the ({@link #p5} x {@link #p6}) join via the
     * {@link #c2} constraint instead.
     */
    public void test_canJoinUsingConstraints_p5_p6() {

        /*
         * Verify (p5,p6) join is not permitted when we do not consider the
         * constraints (i.e., the join would be an unconstrained cross product
         * if it were executed).
         */
        assertFalse(BOpUtility.canJoin(p5, p6));
        assertFalse(BOpUtility.canJoin(p6, p5));
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p5 }, // path
                p6,// vertex
                new IConstraint[0]// constraints
                ));
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p6 }, // path
                p5,// vertex
                new IConstraint[0]// constraints
                ));

        /*
         * Verify (p5,p6) join is not permitted if we do not consider the
         * constraint which provides the shared variables.
         */
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p5 }, // path
                p6,// vertex
                new IConstraint[] { c1 }// constraints
                ));
        assertFalse(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p6 }, // path
                p5,// vertex
                new IConstraint[] { c1 }// constraints
                ));

        /*
         * Verify (p5,p6) join is permitted if we consider the constraint which
         * provides the shared variables.
         */
        assertTrue(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p5 }, // path
                p6,// vertex
                new IConstraint[] { c2 }// constraints
                ));
        assertTrue(BOpUtility.canJoinUsingConstraints(//
                new IPredicate[] { p6 }, // path
                p5,// vertex
                new IConstraint[] { c2 }// constraints
                ));

    }

}
