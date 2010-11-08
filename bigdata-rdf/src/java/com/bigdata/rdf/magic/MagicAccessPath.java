package com.bigdata.rdf.magic;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOTupleSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;

public class MagicAccessPath extends AbstractAccessPath<IMagicTuple> {
    
    private MagicTupleSerializer tupleSer;
    
    /** Relation (resolved lazily if not specified to the ctor). */
    private MagicRelation relation;

//BTM - FOR_CLIENT_SERVICE - BEGIN
    private IConcurrencyManager concurrencyManager;
    private IBigdataDiscoveryManagement discoveryManager;
//BTM - FOR_CLIENT_SERVICE - END

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
    @SuppressWarnings("unchecked")
    public MagicAccessPath(final MagicRelation relation,
            final IPredicate<IMagicTuple> predicate, final IKeyOrder<IMagicTuple> keyOrder,
            final IIndex ndx, final int flags, final int chunkOfChunksCapacity,
            final int chunkCapacity, final int fullyBufferedReadThreshold) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        this(relation.getIndexManager(), relation.getTimestamp(), predicate,
//BTM - PRE_CLIENT_SERVICE                keyOrder, ndx, flags, chunkOfChunksCapacity, chunkCapacity,
//BTM - PRE_CLIENT_SERVICE                fullyBufferedReadThreshold);
        this(relation.getIndexManager(),
             relation.getConcurrencyManager(),
             relation.getDiscoveryManager(),
             relation.getTimestamp(),
             predicate,
             keyOrder,
             ndx,
             flags,
             chunkOfChunksCapacity,
             chunkCapacity,
             fullyBufferedReadThreshold);
//BTM - PRE_CLIENT_SERVICE - END

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
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public MagicAccessPath(final IIndexManager indexManager,
//BTM - PRE_CLIENT_SERVICE            final long timestamp, final IPredicate<IMagicTuple> predicate,
//BTM - PRE_CLIENT_SERVICE            final IKeyOrder<IMagicTuple> keyOrder, final IIndex ndx, final int flags,
//BTM - PRE_CLIENT_SERVICE            final int chunkOfChunksCapacity, final int chunkCapacity,
//BTM - PRE_CLIENT_SERVICE            final int fullyBufferedReadThreshold) {
    public MagicAccessPath(final IIndexManager indexManager,
                           final IConcurrencyManager concurrencyManager,
                           final IBigdataDiscoveryManagement discoveryManager,
                           final long timestamp,
                           final IPredicate<IMagicTuple> predicate,
                           final IKeyOrder<IMagicTuple> keyOrder,
                           final IIndex ndx,
                           final int flags,
                           final int chunkOfChunksCapacity,
                           final int chunkCapacity,
                           final int fullyBufferedReadThreshold)
    {
//BTM - PRE_CLIENT_SERVICE - END

        super(indexManager, timestamp, predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);
        
//BTM - FOR_CLIENT_SERVICE - BEGIN
        this.concurrencyManager = concurrencyManager;
        this.discoveryManager = discoveryManager;
//BTM - FOR_CLIENT_SERVICE - END
    }
    
    protected MagicTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (MagicTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;
        
    }
    
    public MagicAccessPath init() {

        /*
         * The minimum value that a term identifier may take on.
         */
        long MIN = Long.MIN_VALUE;
        final int arity = predicate.arity();
        MagicKeyOrder keyOrder = (MagicKeyOrder) this.keyOrder;
        int[] keyMap = keyOrder.getKeyMap();
        
        // do the from key
            
        IKeyBuilder keyBuilder =
                getTupleSerializer().getKeyBuilder().reset();
        boolean noneBound = true;
        for (int i = 0; i < arity; i++) {
            IVariableOrConstant<IV> term = predicate.get(keyMap[i]);
            if (term == null || term.isVar())
                break;
            term.get().encode(keyBuilder);
            noneBound = false;
        }
        final byte[] fromKey = noneBound ? null : keyBuilder.getKey();
        setFromKey(fromKey);
            
        // do the to key
        setToKey(noneBound ? null : SuccessorUtil.successor(fromKey.clone()));
        
        super.init();
    
        return this;
        
    }

    /**
     * Resolved lazily if not specified to the ctor.
     */
    synchronized
    public MagicRelation getRelation() {
        
        if (relation == null) {
            
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            relation = (MagicRelation) indexManager.getResourceLocator().locate(
//BTM - PRE_CLIENT_SERVICE                    predicate.getOnlyRelationName(), timestamp);
            relation =
                (MagicRelation) indexManager.getResourceLocator()
                                    .locate(indexManager,
                                            concurrencyManager,
                                            discoveryManager,
                                            predicate.getOnlyRelationName(),
                                            timestamp);
//BTM - PRE_CLIENT_SERVICE - END

        }

        return relation;
            
    }

}
