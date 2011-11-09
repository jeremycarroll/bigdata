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
 * Created on Oct 17, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase;

import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for both {@link HTreeHashJoinUtility} and {@link JVMHashJoinUtility}.
 * 
 * TODO Unit test of optional solutions
 * 
 * TODO Verify application of constraints, but only to the non-optional
 * solutions (so this means a test with optionals and also we need a test w/o
 * constraints).
 * 
 * TODO See {@link TestHTreeHashJoinOp} and some of the other join test suites for
 * some of these variations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractHashJoinUtilityTestCase extends TestCase {
    
    public AbstractHashJoinUtilityTestCase() {
    }
    
    public AbstractHashJoinUtilityTestCase(String name) {
        super(name);
    }

    /**
     * Setup for a problem used by many of the join test suites.
     */
    static public class JoinSetup {

        public final String namespace;

        public final IV<?, ?> brad, john, fred, mary, paul, leon;

        public JoinSetup(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();
            
            this.namespace = namespace;

            brad = makeIV(new LiteralImpl("Brad"));
            
            john = makeIV(new LiteralImpl("John"));

            fred = makeIV(new LiteralImpl("Fred"));

            mary = makeIV(new LiteralImpl("Mary"));

            paul = makeIV(new LiteralImpl("Paul"));

            leon = makeIV(new LiteralImpl("Leon"));
        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

        @SuppressWarnings("rawtypes")
        List<IBindingSet> getLeft1() {

            final IVariable<?> x = Var.var("x");
            final IVariable<?> y = Var.var("y");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(brad));
            tmp.set(y, new Constant<IV>(fred));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(mary));
            left.add(tmp);

            return left;
        }

        @SuppressWarnings("rawtypes")
        List<IBindingSet> getRight1() {

            final IVariable<?> a = Var.var("a");
            final IVariable<?> x = Var.var("x");
//            final IVariable<?> y = Var.var("y");

            // The right solutions (the hash index).
            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(paul));
            tmp.set(x, new Constant<IV>(mary));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(paul));
            tmp.set(x, new Constant<IV>(brad));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(john));
            tmp.set(x, new Constant<IV>(mary));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(john));
            tmp.set(x, new Constant<IV>(brad));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(mary));
            tmp.set(x, new Constant<IV>(brad));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(brad));
            tmp.set(x, new Constant<IV>(fred));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(brad));
            tmp.set(x, new Constant<IV>(leon));
            right.add(tmp);

            // new E("Paul", "Mary"),// [0]
            // new E("Paul", "Brad"),// [1]
            //
            // new E("John", "Mary"),// [2]
            // new E("John", "Brad"),// [3]
            //
            // new E("Mary", "Brad"),// [4]
            //
            // new E("Brad", "Fred"),// [5]
            // new E("Brad", "Leon"),// [6]

            return right;

        }

    }

    @SuppressWarnings("deprecation")
    protected static void assertSameSolutionsAnyOrder(
            final IBindingSet[] expected, final Iterator<IBindingSet> actual) {

        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                actual);

    }
    
    /**
     * A buffer which absorbs solutions and let's us replay them via an
     * iterator.
     */
    protected static class TestBuffer<E> implements IBuffer<E> {
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        private final List<E> a = new LinkedList();
        
        public Iterator<E> iterator() {
            return a.iterator();
        }

        public int size() {
            return a.size();
        }

        public boolean isEmpty() {
            return a.isEmpty();
        }

        public void add(E e) {
            a.add(e);
        }

        public long flush() {
            return 0;
        }

        public void reset() {
        }
        
    }

    protected class MockPipelineOp extends PipelineOp {

        public MockPipelineOp(final BOp[] args, final NV... anns) {

            super(args, NV.asMap(anns));

        };

        private static final long serialVersionUID = 1L;

        @Override
        public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Test helper for required or optional join tests.
     * 
     * @param optional
     * @param joinVars
     * @param selectVars
     * @param left
     * @param right
     * @param expected
     */
    protected void doHashJoinTest(//
            final boolean optional,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final List<IBindingSet> left, //
            final List<IBindingSet> right,//
            final IBindingSet[] expected//
            ) {

        // Setup a mock PipelineOp for the test.
        final PipelineOp op = new MockPipelineOp(BOp.NOARGS, 
                new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                        new String[] { getName() }),//
                new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                new NV(JoinAnnotations.SELECT, selectVars),//
                new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                );

        final IHashJoinUtility state = newHashJoinUtility(op, optional, false/* filter */);

        try {

            // Load the right solutions into the HTree.
            {

                final BOpStats stats = new BOpStats();

                state.acceptSolutions(
                        new Chunkerator<IBindingSet>(right.iterator()), stats);

                assertEquals(right.size(), state.getRightSolutionCount());

                assertEquals(right.size(), stats.unitsIn.get());

            }

            /*
             * Run the hash join.
             */

            final ICloseableIterator<IBindingSet> leftItr = new CloseableIteratorWrapper<IBindingSet>(
                    left.iterator());

            // Buffer used to collect the solutions.
            final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();

            // Compute the "required" solutions.
            state.hashJoin(leftItr, outputBuffer);//, true/* leftIsPipeline */);

            if (optional) {

                // Output the optional solutions.
                state.outputOptionals(outputBuffer);

            }

            // Verify the expected solutions.
            assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());

        } finally {

            state.release();

        }

    }
    
    /**
     * Factory for {@link IHashJoinUtility} instances under test.
     * 
     * @param op
     *            The operator from which much of the state will be initialized.
     * @param optional
     *            <code>true</code> if the use case is a JOIN with OPTIONAL
     *            semantics.
     * @param filter
     *            <code>true</code> if the use case is a DISTINCT SOLUTIONS
     *            filter.
     * @return
     */
    abstract protected IHashJoinUtility newHashJoinUtility(PipelineOp op,
            boolean optional, boolean filter);

    /**
     * Empty lhs and rhs with non-optional join.
     */
    public void test_hashJoin01() {

        final boolean optional = false;

        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> left = new LinkedList<IBindingSet>();

        // The right solutions (the hash index).
        final List<IBindingSet> right = new LinkedList<IBindingSet>();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[0];
        
        doHashJoinTest(optional, joinVars, selectVars, constraints, left,
                right, expected);

    }

    /**
     * Empty lhs and rhs with optional join.
     */
    public void test_hashJoin02() {

        final boolean optional = true;

        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> left = new LinkedList<IBindingSet>();

        // The right solutions (the hash index).
        final List<IBindingSet> right = new LinkedList<IBindingSet>();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[0];
        
        doHashJoinTest(optional, joinVars, selectVars, constraints, left,
                right, expected);

    }
    
    /**
     * Non-optional join.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashJoin03() {

        final JoinSetup setup = new JoinSetup(getName());
        
        final boolean optional = false;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{x};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = new IVariable[]{x,y};

        // the join constraints.
        final IConstraint[] constraints = new IConstraint[] { Constraint
                .wrap(new EQConstant(a, new Constant<IV>(setup.john))) };

        // The left solutions (the pipeline).
        final List<IBindingSet> left = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> right = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
                                }//
                ),//
        };

        doHashJoinTest(optional, joinVars, selectVars, constraints, left,
                right, expected);

    }

    /**
     * Variant with no join variables.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashJoin04() {

        final JoinSetup setup = new JoinSetup(getName());
        
        final boolean optional = false;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = new IVariable[]{x,y};

        // the join constraints.
        final IConstraint[] constraints = new IConstraint[] { Constraint
                .wrap(new EQConstant(a, new Constant<IV>(setup.john))) };

        // The left solutions (the pipeline).
        final List<IBindingSet> left = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> right = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
                                }//
                ),//
        };

        doHashJoinTest(optional, joinVars, selectVars, constraints, left,
                right, expected);

    }

    /**
     * Variant without select variables.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashJoin05() {

        final JoinSetup setup = new JoinSetup(getName());

        final boolean optional = false;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;

        // the join constraints.
        final IConstraint[] constraints = new IConstraint[] { Constraint
                .wrap(new EQConstant(a, new Constant<IV>(setup.john))) };

        // The left solutions (the pipeline).
        final List<IBindingSet> left = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> right = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
                                }//
                ),//
        };

        doHashJoinTest(optional, joinVars, selectVars, constraints, left,
                right, expected);

    }

    /**
     * Unit test of a required MERGE JOIN involving two sources. The join
     * variable is <code>?a</code>. One source has <code>(?a,?x)</code>. The
     * other has <code>(?a,?y)</code>. For this test, all solutions having the
     * same value for the join variable will join (there are no join constraints
     * and the solutions can not conflict as they do not bind the same variables
     * for the non-join variables).
     * <p>
     * The following represents the solutions for each source organized by the
     * join variable <code>?a</code>.
     * <pre>
     * (?a,     ?x)   (?a,     ?y)
     * (john, mary)   (john, brad)   // many-to-many join
     * (john, leon)   (john, fred) 
     *                (john, leon) 
     * (mary, john)   (mary, brad)   // 1-1 join.
     * (fred, brad)                  // does not join (would join if OPTIONAL).   
     *                (brad, fred)   // does not join.
     * (leon, john)   (leon, brad)   // many-1 join.
     * (leon, mary)                  // 
     * (paul, leon)   (paul, leon)   // 1-many join.
     *                (paul, brad)
     * </pre>
     */
    public void test_mergeJoin01() {
        
        final JoinSetup setup = new JoinSetup(getName());

        final boolean optional = false;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // The join variables. This must be the same for each source.
        final IVariable<?>[] joinVars = new IVariable[]{a};

        // The variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;
        
        // The join constraints.
        final IConstraint[] constraints = null;

        /**
         * Setup the solutions for [first].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] firstSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.fred),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
        };
        assertEquals(7,firstSolutions.length);

        /**
         * Setup the source solutions for [other].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] otherSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.brad) }//
                ),//
        };
        assertEquals(8,otherSolutions.length);
        
        /**
         * The expected solutions to the join.
         * 
         * The source solutions are:
         * <pre>
         * (?a,     ?x)   (?a,     ?y)
         * (john, mary)   (john, brad)   // many-to-many join
         * (john, leon)   (john, fred) 
         *                (john, leon) 
         * (mary, john)   (mary, brad)   // 1-1 join.
         * (fred, brad)                  // does not join.   
         *                (brad, fred)   // does not join.
         * (leon, john)   (leon, brad)   // many-1 join.
         * (leon, mary)                  // 
         * (paul, leon)   (paul, leon)   // 1-many join.
         *                (paul, brad)
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
            // many-to-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            // 1-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // many-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // 1-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
        };

        IHashJoinUtility first = null;
        IHashJoinUtility other = null;
        try {

            // Setup a mock PipelineOp for the test.
            final PipelineOp firstOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            // Setup a mock PipelineOp for the test.
            final PipelineOp otherOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            first = newHashJoinUtility(firstOp, optional, false/* filter */);

            other = newHashJoinUtility(otherOp, optional, false/* filter */);

            // Load into [first].
            {

                final BOpStats stats = new BOpStats();

                first.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(firstSolutions).iterator()), stats);

                assertEquals(firstSolutions.length, first.getRightSolutionCount());

                assertEquals(firstSolutions.length, stats.unitsIn.get());

            }
            
            // Load into [other].
            {

                final BOpStats stats = new BOpStats();

                other.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(otherSolutions).iterator()), stats);

                assertEquals(otherSolutions.length, other.getRightSolutionCount());

                assertEquals(otherSolutions.length, stats.unitsIn.get());

            }

            // Do the merge join and verify the expected solutions.
            doMergeJoinTest(constraints, expected, optional, first, other);

        } finally {

            if (first != null) {
                first.release();
            }

            if (other != null) {
                other.release();
            }
            
        }
        
    }

    /**
     * Test for optional merge join. This is based on the same data as the
     * non-optional merge join test above.
     */
    public void test_mergeJoin02() {
        final JoinSetup setup = new JoinSetup(getName());

        final boolean optional = false;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // The join variables. This must be the same for each source.
        final IVariable<?>[] joinVars = new IVariable[]{a};

        // The variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;
        
        // The join constraints.
        final IConstraint[] constraints = null;

        /**
         * Setup the solutions for [first].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] firstSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.fred),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
        };
        assertEquals(7,firstSolutions.length);

        /**
         * Setup the source solutions for [other].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] otherSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.brad) }//
                ),//
        };
        assertEquals(8,otherSolutions.length);
        
        /**
         * The expected solutions to the join.
         * 
         * The source solutions are:
         * <pre>
         * (?a,     ?x)   (?a,     ?y)
         * (john, mary)   (john, brad)   // many-to-many join
         * (john, leon)   (john, fred) 
         *                (john, leon) 
         * (mary, john)   (mary, brad)   // 1-1 join.
         * (fred, brad)                  // OPTIONAL JOIN.
         *                (brad, fred)   // does not join.
         * (leon, john)   (leon, brad)   // many-1 join.
         * (leon, mary)                  // 
         * (paul, leon)   (paul, leon)   // 1-many join.
         *                (paul, brad)
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
            // many-to-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            // 1-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // OPTIONAL JOIN
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.fred),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // many-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // 1-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
        };

        IHashJoinUtility first = null;
        IHashJoinUtility other = null;
        try {

            // Setup a mock PipelineOp for the test.
            final PipelineOp firstOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            // Setup a mock PipelineOp for the test.
            final PipelineOp otherOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            first = newHashJoinUtility(firstOp, optional, false/* filter */);

            other = newHashJoinUtility(otherOp, optional, false/* filter */);

            // Load into [first].
            {

                final BOpStats stats = new BOpStats();

                first.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(firstSolutions).iterator()), stats);

                assertEquals(firstSolutions.length, first.getRightSolutionCount());

                assertEquals(firstSolutions.length, stats.unitsIn.get());

            }
            
            // Load into [other].
            {

                final BOpStats stats = new BOpStats();

                other.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(otherSolutions).iterator()), stats);

                assertEquals(otherSolutions.length, other.getRightSolutionCount());

                assertEquals(otherSolutions.length, stats.unitsIn.get());

            }

            // Do the merge join and verify the expected solutions.
            doMergeJoinTest(constraints, expected, optional, first, other);

        } finally {

            if (first != null) {
                first.release();
            }

            if (other != null) {
                other.release();
            }
            
        }
        
    }
    
    /**
     * @param first
     *            The instances from which the required joins will be reported
     *            if the join is OPTIONAL.
     * @param others
     *            The other instances. There must be at least two instances to
     *            be joined when [first] and [others] are considered together.
     * @param constraints
     *            The join constraints.
     * @param expected
     *            The expected solutions.
     */
    protected void doMergeJoinTest(
            final IConstraint[] constraints,//
            final IBindingSet[] expected,//
            final boolean optional,//
            final IHashJoinUtility first,//
            final IHashJoinUtility... others
            ) {

        // Buffer used to collect the solutions.
        final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();

        // Do the merge join,
        first.mergeJoin(others, outputBuffer, constraints, optional);
        
        // Verify the expected solutions.
        assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());
        
    }

}
