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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for {@link DistinctBindingSetOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestDistinctBindingSets.java 4259 2011-02-28 16:24:53Z thompsonbry $
 * 
 * @todo write a unit test in which some variables are unbound.
 */
public class TestDistinctBindingSetsWithHTree extends TestCase2 {

    /**
     * 
     */
    public TestDistinctBindingSetsWithHTree() {
    }

    /**
     * @param name
     */
    public TestDistinctBindingSetsWithHTree(String name) {
        super(name);
    }

//    @Override
//    public Properties getProperties() {
//
//        final Properties p = new Properties(super.getProperties());
//
//        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
//                .toString());
//
//        return p;
//        
//    }

//    Journal jnl = null;

    List<IBindingSet> data = null;

    public void setUp() throws Exception {

//        jnl = new Journal(getProperties());

        setUpData();

    }

    /**
     * Setup the data.
     */
    private void setUpData() {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        data = new LinkedList<IBindingSet>();
            IBindingSet bset = null;
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("John"));
                bset.set(y, new Constant<String>("Mary"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Jane"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("Leon"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("John"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Leon"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
            }

    }

    public void tearDown() throws Exception {

//        if (jnl != null) {
//            jnl.destroy();
//            jnl = null;
//        }
//        
        // clear reference.
        data = null;

    }
    
    public void test_ctor_correctRejection() {
    	
        final Var<?> x = Var.var("x");

        final int distinctId = 1;

        // w/o variables.
        try {
        new DistinctBindingSetsWithHTreeOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(DistinctBindingSetOp.Annotations.BOP_ID,distinctId),//
//                    new NV(DistinctBindingSetOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.SHARED_STATE,
                            true),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

        // w/o evaluation on the query controller.
        try {
        new DistinctBindingSetsWithHTreeOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(DistinctBindingSetOp.Annotations.BOP_ID,distinctId),//
                    new NV(DistinctBindingSetOp.Annotations.VARIABLES,new IVariable[]{x}),//
//                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
//                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.SHARED_STATE,
                            true),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

        // w/o shared state.
        try {
        new DistinctBindingSetsWithHTreeOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(DistinctBindingSetOp.Annotations.BOP_ID,distinctId),//
                    new NV(DistinctBindingSetOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
//                    new NV(PipelineOp.Annotations.SHARED_STATE,
//                            true),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

        // w/o maxParallel := 1.
        try {
        new DistinctBindingSetsWithHTreeOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(DistinctBindingSetOp.Annotations.BOP_ID,distinctId),//
                    new NV(DistinctBindingSetOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.SHARED_STATE,
                            true),//
//                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
//                            1),//
                }));
        fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

    }

    /**
     * Unit test for distinct.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_distinctBindingSets() throws InterruptedException,
            ExecutionException {

        final Var<?> x = Var.var("x");
//        final Var<?> y = Var.var("y");
        
        final int distinctId = 1;
        
        final DistinctBindingSetsWithHTreeOp query = new DistinctBindingSetsWithHTreeOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.BOP_ID,distinctId),//
                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
					new NV(PipelineOp.Annotations.SHARED_STATE, true),//
					new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                }));
        
        // the expected solutions
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
                new IVariable[] { x },//
                new IConstant[] { new Constant<String>("John") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Mary") }//
                ), new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Paul") }//
                ), new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Leon") }//
                ), };

        final UUID queryId = UUID.randomUUID();
        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
        final BOpStats stats = query.newStats(queryContext);

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { data.toArray(new IBindingSet[0]) });

        final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(query, stats);

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, null/* indexManager */),
                -1/* partitionId */, stats, source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);
        
        // execute task.
//        jnl.getExecutorService().execute(ft);
        ft.run();

		TestQueryEngine.assertSameSolutionsAnyOrder("", expected,
				sink.iterator(), ft);

        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

        assertEquals(1L, stats.chunksIn.get());
        assertEquals(6L, stats.unitsIn.get());
        assertEquals(4L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        } finally {
            queryContext.close();
        }

    }
    
}
