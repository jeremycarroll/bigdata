package com.bigdata.rdf.spo;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * {@link IAccessPath} implementation for an {@link SPORelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOAccessPath extends AbstractAccessPath<ISPO> {

    private static transient final long NULL = IRawTripleStore.NULL;

    private SPOTupleSerializer tupleSer;
    
    /** Relation (resolved lazily if not specified to the ctor). */
    private SPORelation relation;

    /**
     * Variant when the {@link SPORelation} has already been materialized.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evaluated on the data service rather
     * than materializing the elements and then filtering them. This can be
     * accomplished by adding the filter as a constraint on the predicate when
     * specifying the access path.
     * 
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     */
    public SPOAccessPath(final SPORelation relation,
            final IPredicate<ISPO> predicate, final IKeyOrder<ISPO> keyOrder,
            final IIndex ndx, final int flags, final int chunkOfChunksCapacity,
            final int chunkCapacity, final int fullyBufferedReadThreshold) {

        this(relation.getIndexManager(), relation.getTimestamp(), predicate,
                keyOrder, ndx, flags, chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);

        this.relation = relation;
        
    }

    /**
     * Variant does not require the {@link SPORelation} to have been
     * materialized. This is useful when you want an {@link IAccessPath} for a
     * specific index partition.
     * 
     * @param indexManager
     * @param timestamp
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     * @param chunkOfChunksCapacity
     * @param chunkCapacity
     * @param fullyBufferedReadThreshold
     */
    public SPOAccessPath(final IIndexManager indexManager,
            final long timestamp, final IPredicate<ISPO> predicate,
            final IKeyOrder<ISPO> keyOrder, final IIndex ndx, final int flags,
            final int chunkOfChunksCapacity, final int chunkCapacity,
            final int fullyBufferedReadThreshold) {

        super(indexManager, timestamp, predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);

    }

    /**
     * Return the constant bound on the {@link #getPredicate() predicate} for
     * this access path at the specified index -or- {@link #NULL} iff the
     * predicate is not bound at that position.
     * 
     * @param index
     *            The index.
     *            
     * @return Either the bound value -or- {@link #NULL} iff the index is
     *         unbound for the predicate for this access path.
     */
    @SuppressWarnings("unchecked")
    public long get(final int index) {

        final IVariableOrConstant<Long> t = predicate.get(index);

        return t.isVar() ? NULL : t.get();

    }
    
    protected SPOTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;
        
    }
    
    public SPOAccessPath init() {

        /*
         * The minimum value that a term identifier may take on.
         */
        final SPOKeyOrder keyOrder = (SPOKeyOrder) this.keyOrder;
        final int keyArity = keyOrder.getKeyArity(); // use the key's "arity".
        final IKeyBuilder keyBuilder = getTupleSerializer().getKeyBuilder();
        
        { // do the from key
            
            keyBuilder.reset();
            boolean noneBound = true;
            for (int i = 0; i < keyArity; i++) {
                IVariableOrConstant<Long> term = 
                    predicate.get(keyOrder.getKeyOrder(i));
                long l;
                // Note: term MAY be null for the context position.
                if (term == null || term.isVar()) {
                    l = Long.MIN_VALUE;
                } else {
                    l = term.get();
                    noneBound = false;
                }
                keyBuilder.append(l);
            }
            final byte[] fromKey = noneBound ? null : keyBuilder.getKey();
            setFromKey(fromKey);
            
        }

        { // do the to key

            keyBuilder.reset();
            boolean noneBound = true;
            boolean foundLastBound = false;
            for (int i = 0; i < keyArity; i++) {
                IVariableOrConstant<Long> term = 
                    predicate.get(keyOrder.getKeyOrder(i));
                long l;
                // Note: term MAY be null for context.
                if (term == null || term.isVar()) {
                    l = Long.MIN_VALUE;
                } else {
                    l = term.get();
                    noneBound = false;
                    if (!foundLastBound) {
                        if (i == keyArity-1) {
                            l++;
                            foundLastBound = true;
                        } else {
                            IVariableOrConstant<Long> next = 
                                predicate.get(keyOrder.getKeyOrder(i+1));
                            // Note: next can be null for quads (context pos).
                            if (next == null || next.isVar()) {
                                l++;
                                foundLastBound = true;
                            }
                        }
                    }
                }
                keyBuilder.append(l);
            }
            final byte[] toKey = noneBound ? null : keyBuilder.getKey();
            setToKey(toKey);
           
        }
        
        super.init();
    
        return this;
        
    }

    /**
     * Resolved lazily if not specified to the ctor.
     */
    synchronized
    public SPORelation getRelation() {
        
        if (relation == null) {
            
            relation = (SPORelation) indexManager.getResourceLocator().locate(
                    predicate.getOnlyRelationName(), timestamp);

        }

        return relation;
            
    }

    /**
     * Overridden to delegate to
     * {@link AbstractTripleStore#removeStatements(IChunkedOrderedIterator)} in
     * order to (a) write on all access paths; (b) handle statement identifiers,
     * including truth maintenance for statement identifiers; and (c) if
     * justifications are being maintained, then retract justifications having
     * no support once the statements visitable by this access path have been
     * retracted.
     */
    @Override
    public long removeAll() {
        
        return getRelation().getContainer().removeStatements(iterator());
        
    }

    @Override
    public SPOPredicate getPredicate() {

        return (SPOPredicate) super.getPredicate();
        
    }
    
}
