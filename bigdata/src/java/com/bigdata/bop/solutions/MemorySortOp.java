package com.bigdata.bop.solutions;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.solutions.MemoryGroupByOp.Annotations;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * An in-memory merge sort for binding sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 * 
 *          FIXME ORDER_BY is defined in terms of Expressions, not just Vars.
 *          Either this will need to be an {@link IValueExpression} which is
 *          evaluated during the ordering or we will have to pre-compute a
 *          hidden variable which can be ordered directly. Presumably
 *          BrackettedExpression provides a computed RDF Value while Constraint
 *          orders based on the BEV. Write unit tests for those computed
 *          expressions.
 * 
 *          <pre>
 * [22]  	OrderClause	      ::=  	'ORDER' 'BY' OrderCondition+
 * [23]  	OrderCondition	  ::=  	( ( 'ASC' | 'DESC' ) BrackettedExpression ) | ( Constraint | Var )
 * </pre>
 * 
 *          FIXME ORDER_BY should be written out of a CONSTRUCT or DESCRIBE
 *          query since it will not have any affect on the solutions.
 * 
 * @todo do an external merge sort operator.
 * @todo do a wordsort operator w/ ties broken by the {@link ComparatorOp} after
 *       the main sort.
 * @todo do a radix sort operator (for main memory only?)
 */
public class MemorySortOp extends SortOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public MemorySortOp(final MemorySortOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public MemorySortOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        switch (getEvaluationContext()) {
		case CONTROLLER:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

        assertAtOnceJavaHeapOp();

	}
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new SortTask(this, context));
        
    }

    /**
     * Task executing on the node.
     */
    static private class SortTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The binding set comparator.
         */
        private final Comparator<IBindingSet> comparator;

        SortTask(final MemorySortOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.context = context;

            this.comparator = op.getComparator();

        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final IBindingSet[] all = BOpUtility.toArray(context
                        .getSource(), stats);

                // sort.
                Arrays.sort(all, comparator);

                // write output and flush.
                sink.add(all);
                sink.flush();

                // done.
                return null;

            } finally {

                sink.close();

            }

        }

    }
    
}
