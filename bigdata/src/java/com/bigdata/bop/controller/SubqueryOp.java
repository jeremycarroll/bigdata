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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Pipelined join with subquery.
 * <p>
 * For each binding set presented, this operator executes a subquery. Any
 * solutions produced by the subquery are copied to the default sink. If no
 * solutions are produced and {@link Annotations#OPTIONAL} is <code>true</code>,
 * then the original binding set is copied to the default sink (optional join
 * semantics). Each subquery is run as a separate query but will be cancelled if
 * the parent query is cancelled.
 * <p>
 * This operator does not use internal parallelism, but it is thread-safe and
 * multiple instances of this operator may be run in parallel by the query
 * engine for parallel evaluation of different binding set chunks flowing
 * through the pipeline.
 * <h3>Usage Notes</h3>
 * 
 * If there are no shared variables which must already be bound in the caller,
 * then subquery join is (or may be if there are some "might" be bound
 * variables) the full cross product (constraints are still applied and optional
 * solutions must be reported if a constraint fails and the join is optional).
 * Such subqueries should be run as named subqueries instead so they run once,
 * rather than once per binding set.
 * <p>
 * If there are variables in scope in the parent query which are not projected
 * by the subquery but which appear in the subquery as well, then such variables
 * in the subquery are effectively distinct from those having the same name
 * which appear in the parent query. In order to have correct bottom-up
 * evaluation semantics under these conditions, such variables in the subquery
 * MUST be renamed. The easiest way to handle this is to always rename such
 * variables unless they are projected out of the subquery. (If a variable is
 * projected out of the subquery then it should not be renamed in order to have
 * the in scope bindings for that variable in the parent flow into the
 * subquery.)
 * <p>
 * A BIND() used to rename a variable in the projection of the subquery should
 * be interpreted as the projection of both the source and target variable,
 * otherwise the bind is only imposed when leaving the subquery and a binding
 * already available in the parent's context will not be utilized by the
 * subquery, causing it to do more work.
 * 
 * @todo Rename as SubqueryPipelineJoinOp.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see AbstractSubqueryOp
 */
public class SubqueryOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends SubqueryJoinAnnotations {

    }

    /**
     * Deep copy constructor.
     */
    public SubqueryOp(final SubqueryOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public SubqueryOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(Annotations.SUBQUERY);
        
    }

    public SubqueryOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }
    
	/**
	 * Evaluates the subquery for each source binding set. If the controller
	 * operator is interrupted, then the subqueries are cancelled. If a subquery
	 * fails, then all subqueries are cancelled.
	 */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;
        private final boolean optional;
        /** The subquery which is evaluated for each input binding set. */
        private final PipelineOp subquery;
        /** The selected variables for the output binding sets (optional). */
        private final IVariable<?>[] selectVars;
        /** The optional constraints on the join. */
        private final IConstraint[] constraints;
        
        public ControllerTask(final SubqueryOp controllerOp,
                final BOpContext<IBindingSet> context) {

            if (controllerOp == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.optional = controllerOp.getProperty(
                    Annotations.OPTIONAL,
                    Annotations.DEFAULT_OPTIONAL);

            this.subquery = (PipelineOp) controllerOp
                    .getRequiredProperty(Annotations.SUBQUERY);
            
            this.selectVars = (IVariable<?>[]) controllerOp
                    .getProperty(Annotations.SELECT);

            this.constraints = (IConstraint[]) controllerOp
                    .getProperty(Annotations.CONSTRAINTS);
            
        }

        /**
         * Evaluate the subquery.
         */
        public Void call() throws Exception {
            
            try {

                final IAsynchronousIterator<IBindingSet[]> sitr = context
                        .getSource();
                
                while(sitr.hasNext()) {
                    
                    final IBindingSet[] chunk = sitr.next();
                    
                    for(IBindingSet bset : chunk) {

						final IRunningQuery runningSubquery = new SubqueryTask(
								bset, subquery, context).call();

						if (!runningSubquery.isDone()) {

							throw new AssertionError("Future not done: "
									+ runningSubquery.toString());
							
						}
   
                    }
                    
                }
                
                // Now that we know the subqueries ran Ok, flush the sink.
                context.getSink().flush();
                
                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();
                
                if (context.getSink2() != null)
                    context.getSink2().close();

            }
            
        }

        /**
         * Run a subquery.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class SubqueryTask implements Callable<IRunningQuery> {

            /**
             * The evaluation context for the parent query.
             */
            private final BOpContext<IBindingSet> parentContext;

            /**
             * The source binding set. This will be copied to the output if
             * there are no solutions for the subquery (optional join
             * semantics).
             */
            private final IBindingSet bset;
            
            /**
             * The root operator for the subquery.
             */
            private final BOp subQueryOp;

            public SubqueryTask(final IBindingSet bset, final BOp subQuery,
                    final BOpContext<IBindingSet> parentContext) {

                this.bset = bset;
                
                this.subQueryOp = subQuery;

                this.parentContext = parentContext;

            }

            public IRunningQuery call() throws Exception {

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();
                    
                    runningSubquery = queryEngine.eval((PipelineOp) subQueryOp,
                            bset);

					long ncopied = 0L;
					try {
						
						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();

						// Copy solutions from the subquery to the query.
                        ncopied = BOpUtility.copy(subquerySolutionItr,
                                parentContext.getSink(), null/* sink2 */,
                                selectVars, constraints, parentContext
                                        .getStats());

						// wait for the subquery to halt / test for errors.
						runningSubquery.get();
						
					} catch (InterruptedException ex) {

						// this thread was interrupted, so cancel the subquery.
						runningSubquery
								.cancel(true/* mayInterruptIfRunning */);

						// rethrow the exception.
						throw ex;
						
					}
					
                    if (ncopied == 0L && optional) {

                        /*
                         * Since there were no solutions for the subquery, copy
                         * the original binding set to the default sink and do
                         * NOT apply the constraints.
                         */

                        final IBindingSet tmp = selectVars == null ? bset
                                : bset.copy(selectVars);

                        parentContext.getSink().add(new IBindingSet[] { tmp });

                    }
                    
                    // done.
                    return runningSubquery;
                    
                } catch (Throwable t) {

					if (runningSubquery == null
							|| runningSubquery.getCause() != null) {
						/*
						 * If things fail before we start the subquery, or if a
						 * subquery fails (due to abnormal termination), then
						 * propagate the error to the parent and rethrow the
						 * first cause error out of the subquery.
						 * 
						 * Note: IHaltable#getCause() considers exceptions
						 * triggered by an interrupt to be normal termination.
						 * Such exceptions are NOT propagated here and WILL NOT
						 * cause the parent query to terminate.
						 */
                        throw new RuntimeException(ControllerTask.this.context
                                .getRunningQuery().halt(
                                        runningSubquery == null ? t
                                                : runningSubquery.getCause()));
                    }
					
					return runningSubquery;
                    
                } finally {

					try {

						// ensure subquery is halted.
						if (runningSubquery != null)
							runningSubquery
									.cancel(true/* mayInterruptIfRunning */);
						
					} finally {

						// ensure the subquery solution iterator is closed.
						if (subquerySolutionItr != null)
							subquerySolutionItr.close();

					}
					
                }

            }

        } // SubqueryTask

    } // ControllerTask

}
