package com.bigdata.bop;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;

/**
 * Sampling operator for an {@link AbstractBTree}.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public class SampleLocalBTree<E> extends AbstractSampleIndex<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public SampleLocalBTree(final IPredicate<E> pred, final int limit) {

        super(pred, limit);

    }

    public Future<Void> eval(final IBigdataFederation<?> fed,
            final IJoinNexus joinNexus, final IBlockingBuffer<E[]> buffer) {

        if (pred().getPartitionId() != -1) {
            // Must not be specific to a shard.
            throw new UnsupportedOperationException();
        }

        final FutureTask<Void> ft = new FutureTask<Void>(new LocalBTreeSampleTask(
                joinNexus, buffer));

        joinNexus.getIndexManager().getExecutorService().execute(ft);

        return ft;

    }

    /**
     * Sample an {@link AbstractBTree}.
     */
    private class LocalBTreeSampleTask implements
            Callable<Void> {
        
        private final IJoinNexus joinNexus;

        private final IBlockingBuffer<E[]> buffer;

        LocalBTreeSampleTask(final IJoinNexus joinNexus,
                final IBlockingBuffer<E[]> buffer) {

            this.joinNexus = joinNexus;

            this.buffer = buffer;
                
        }

        public Void call() throws Exception {

            /*
             * FIXME Decide how we are going to resolve the appropriate index
             * for the predicate. This could go through
             * IJoinNexus.getTailRelationView() and
             * IJoinNexus.getTailAccessPath(). Those are just going through the
             * locator. Review how the actual access path is selected versus the
             * IKeyOrder specified on the IPredicate. If the IKeyOrder of
             * interest is on the IPredicate, then why not just use that?
             */
            
//            final IPredicate<E> pred = pred();
//            
//            final String relationName = pred.getOnlyRelationName();
//
//            final IRelation<E> rel = (IRelation<E>) joinNexus.getIndexManager()
//                    .getResourceLocator().locate(relationName,
//                            joinNexus.getReadTimestamp());
//
//            final IAccessPath<E> accessPath = rel.getAccessPath(pred);

            /*
             * FIXME Sample N randomly chosen indices or evenly selected?
             * 
             * Note: If there are only 100 leaves and we sample evenly, that
             * could result in reading all the leaves. However, when the
             * B+Tree is large we will only touch a few leaves even with
             * uniform sampling.
             */
            throw new UnsupportedOperationException();
        
        }
        
    } // class LocalBTreeSampleTask

}
