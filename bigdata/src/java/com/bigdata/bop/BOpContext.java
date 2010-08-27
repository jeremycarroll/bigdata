/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 26, 2010
 */
package com.bigdata.bop;

import org.apache.log4j.Logger;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;

/**
 * The evaluation context for the operator (NOT serializable).
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 * 
 * @todo Make it easy to obtain another {@link BOpContext} in which the source
 *       or sink are different? E.g., for the evaluation of the right operand in
 *       a join.
 */
public class BOpContext<E> {

    static private final Logger log = Logger.getLogger(BOpContext.class);

    private final IBigdataFederation<?> fed;

    private final IIndexManager indexManager;

    private final long readTimestamp;

    private final long writeTimestamp;

    private final int partitionId;

    private final BOpStats stats;

    private final IAsynchronousIterator<E[]> source;

    private final IBlockingBuffer<E[]> sink;

    private final IBlockingBuffer<E[]> sink2;

    /**
     * The {@link IBigdataFederation} IFF the operator is being evaluated on an
     * {@link IBigdataFederation}. When evaluating operations against an
     * {@link IBigdataFederation}, this reference provides access to the
     * scale-out view of the indices and to other bigdata services.
     */
    public IBigdataFederation<?> getFederation() {
        return fed;
    }

    /**
     * The <strong>local</strong> {@link IIndexManager}. Query evaluation occurs
     * against the local indices. In scale-out, query evaluation proceeds shard
     * wise and this {@link IIndexManager} MUST be able to read on the
     * {@link ILocalBTreeView}.
     */
    public final IIndexManager getIndexManager() {
        return indexManager;
    }

    /**
     * The timestamp or transaction identifier against which the query is
     * reading.
     */
    public final long getReadTimestamp() {
        return readTimestamp;
    }

    /**
     * The timestamp or transaction identifier against which the query is
     * writing.
     */
    public final long getWriteTimestamp() {
        return writeTimestamp;
    }

    /**
     * The index partition identifier -or- <code>-1</code> if the index is not
     * sharded.
     */
    public final int getPartitionId() {
        return partitionId;
    }

    /**
     * The object used to collect statistics about the evaluation of this
     * operator.
     */
    public final BOpStats getStats() {
        return stats;
    }

    /**
     * Where to read the data to be consumed by the operator.
     */
    public final IAsynchronousIterator<E[]> getSource() {
        return source;
    }

    /**
     * Where to write the output of the operator.
     */
    public final IBlockingBuffer<E[]> getSink() {
        return sink;
    }

    /**
     * Optional alternative sink for the output of the operator. This is used by
     * things like SPARQL optional joins to route failed joins outside of the
     * join group.
     */
    public final IBlockingBuffer<E[]> getSink2() {
        return sink2;
    }

    /**
     * 
     * @param fed
     *            The {@link IBigdataFederation} IFF the operator is being
     *            evaluated on an {@link IBigdataFederation}. When evaluating
     *            operations against an {@link IBigdataFederation}, this
     *            reference provides access to the scale-out view of the indices
     *            and to other bigdata services.
     * @param indexManager
     *            The <strong>local</strong> {@link IIndexManager}. Query
     *            evaluation occurs against the local indices. In scale-out,
     *            query evaluation proceeds shard wise and this
     *            {@link IIndexManager} MUST be able to read on the
     *            {@link ILocalBTreeView}.
     * @param readTimestamp
     *            The timestamp or transaction identifier against which the
     *            query is reading.
     * @param writeTimestamp
     *            The timestamp or transaction identifier against which the
     *            query is writing.
     * @param partitionId
     *            The index partition identifier -or- <code>-1</code> if the
     *            index is not sharded.
     * @param stats
     *            The object used to collect statistics about the evaluation of
     *            this operator.
     * @param source
     *            Where to read the data to be consumed by the operator.
     * @param sink
     *            Where to write the output of the operator.
     * @param sink2
     *            Alternative sink for the output of the operator (optional).
     *            This is used by things like SPARQL optional joins to route
     *            failed joins outside of the join group.
     * 
     * @throws IllegalArgumentException
     *             if the <i>indexManager</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if the <i>readTimestamp</i> is {@link ITx#UNISOLATED}
     *             (queries may not read on the unisolated indices).
     * @throws IllegalArgumentException
     *             if the <i>writeTimestamp</i> is neither
     *             {@link ITx#UNISOLATED} nor a read-write transaction
     *             identifier.
     * @throws IllegalArgumentException
     *             if the <i>stats</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if the <i>source</i> is <code>null</code> (use an empty
     *             source if the source will be ignored).
     * @throws IllegalArgumentException
     *             if the <i>sink</i> is <code>null</code>
     */
    public BOpContext(final IBigdataFederation<?> fed,
            final IIndexManager indexManager, final long readTimestamp,
            final long writeTimestamp, final int partitionId,
            final BOpStats stats, final IAsynchronousIterator<E[]> source,
            final IBlockingBuffer<E[]> sink, final IBlockingBuffer<E[]> sink2) {
        if (indexManager == null)
            throw new IllegalArgumentException();
        if (readTimestamp == ITx.UNISOLATED)
            throw new IllegalArgumentException();
        if (TimestampUtility.isReadOnly(writeTimestamp))
            throw new IllegalArgumentException();
        if (stats == null)
            throw new IllegalArgumentException();
        if (source == null)
            throw new IllegalArgumentException();
        if (sink == null)
            throw new IllegalArgumentException();
        this.fed = fed; // may be null
        this.indexManager = indexManager;
        this.readTimestamp = readTimestamp;
        this.writeTimestamp = writeTimestamp;
        this.partitionId = partitionId;
        this.stats = stats;
        this.source = source;
        this.sink = sink;
        this.sink2 = sink2; // may be null
    }

    /**
     * Locate and return the view of the relation(s) identified by the
     * {@link IPredicate}.
     * <p>
     * Note: This method is responsible for returning a fused view when more
     * than one relation name was specified for the {@link IPredicate}. It
     * SHOULD be used whenever the {@link IRelation} is selected based on a
     * predicate in the tail of an {@link IRule} and could therefore be a fused
     * view of more than one relation instance. (The head of the {@link IRule}
     * must be a simple {@link IRelation} and not a view.)
     * <p>
     * Note: The implementation should choose the read timestamp for each
     * relation in the view using {@link #getReadTimestamp(String)}.
     * 
     * @param pred
     *            The {@link IPredicate}, which MUST be a tail from some
     *            {@link IRule}.
     * 
     * @return The {@link IRelation}.
     * 
     * @todo Replaces {@link IJoinNexus#getTailRelationView(IPredicate)}. In
     *       order to support mutation operator we will also have to pass in the
     *       {@link #writeTimestamp} or differentiate this in the method name.
     */
    public IRelation getReadRelation(final IPredicate<?> pred) {

        return (IRelation) getIndexManager().getResourceLocator().locate(
                pred.getOnlyRelationName(), getReadTimestamp());

    }

    /**
    /**
     * Obtain an access path reading from relation for the specified predicate
     * (from the tail of some rule).
     * <p>
     * Note that passing in the {@link IRelation} is important since it
     * otherwise must be discovered using the {@link IResourceLocator}. By
     * requiring the caller to resolve it before hand and pass it into this
     * method the contention and demand on the {@link IResourceLocator} cache is
     * reduced.
     * 
     * @param relation
     *            The relation.
     * @param pred
     *            The predicate. When {@link IPredicate#getPartitionId()} is
     *            set, the returned {@link IAccessPath} MUST read on the
     *            identified local index partition (directly, not via RMI).
     * 
     * @return The access path.
     * 
     * @todo replaces {@link IJoinNexus#getTailAccessPath(IRelation, IPredicate)}.
     */
    @SuppressWarnings("unchecked")
    public IAccessPath<?> getAccessPath(final IRelation<?> relation,
            final IPredicate<?> predicate) {

        if (predicate.getPartitionId() != -1) {

            /*
             * Note: This handles a read against a local index partition. For
             * scale-out, the [indexManager] will be the data service's local
             * index manager.
             * 
             * Note: Expanders ARE NOT applied in this code path. Expanders
             * require a total view of the relation, which is not available
             * during scale-out pipeline joins. Likewise, the [backchain]
             * property will be ignored since it is handled by an expander.
             */

            return ((AbstractRelation<?>) relation)
                    .getAccessPathForIndexPartition(indexManager,
                            (IPredicate) predicate);

        }

        // Find the best access path for the predicate for that relation.
        final IAccessPath<?> accessPath = relation
                .getAccessPath((IPredicate) predicate);

        /*
         * @todo No expander's for bops, at least not right now. They could be
         * added in easily enough, which would support additional features for
         * standalone query evaluation (runtime materialization of some
         * entailments).
         */
        // final ISolutionExpander expander = predicate.getSolutionExpander();
        //            
        // if (expander != null) {
        //                
        // // allow the predicate to wrap the access path
        // accessPath = expander.getAccessPath(accessPath);
        //                
        // }

        // return that access path.
        return accessPath;
    }

    /**
     * Binds variables from a visited element.
     * <p>
     * Note: The bindings are propagated before the constraints are verified so
     * this method will have a side-effect on the bindings even if the
     * constraints were not satisfied. Therefore you should clone the bindings
     * before calling this method.
     * 
     * @param pred
     *            The {@link IPredicate} from which the element was read.
     * @param constraint
     *            A constraint which must be satisfied (optional).
     * @param e
     *            An element materialized by the {@link IAccessPath} for that
     *            {@link IPredicate}.
     * @param bindingSet
     *            the bindings to which new bindings from the element will be
     *            applied.
     * 
     * @return <code>true</code> unless the new bindings would violate any of
     *         the optional {@link IConstraint}.
     * 
     * @throws NullPointerException
     *             if an argument is <code>null</code>.
     */
    final public boolean bind(final IPredicate<?> pred,
            final IConstraint[] constraints, final Object e,
            final IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        copyValues((IElement) e, pred, bindings);

        if (constraints != null) {

            // verify constraint.
            return isConsistent(constraints, bindings);
        
        }
        
        // no constraint.
        return true;
        
    }

    /**
     * Check constraints.
     * 
     * @param constraints
     * @param bindingSet
     * 
     * @return <code>true</code> iff the constraints are satisfied.
     */
    private boolean isConsistent(final IConstraint[] constraints,
            final IBindingSet bindingSet) {

        for (int i = 0; i < constraints.length; i++) {

            final IConstraint constraint = constraints[i];

            if (!constraint.accept(bindingSet)) {

                if (log.isDebugEnabled()) {

                    log.debug("Rejected by "
                            + constraint.getClass().getSimpleName() + " : "
                            + bindingSet);

                }

                return false;

            }

        }

        return true;

    }
    
    @SuppressWarnings("unchecked")
    final private void copyValues(final IElement e, final IPredicate<?> pred,
            final IBindingSet bindingSet) {

        for (int i = 0; i < pred.arity(); i++) {

            final IVariableOrConstant<?> t = pred.get(i);

            if (t.isVar()) {

                final IVariable<?> var = (IVariable<?>) t;

                final Constant<?> newval = new Constant(e.get(i));

                bindingSet.set(var, newval);

            }

        }

    }

}
