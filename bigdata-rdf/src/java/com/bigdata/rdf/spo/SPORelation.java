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
 * Created on Jun 21, 2008
 */

package com.bigdata.rdf.spo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchRemove;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.lexicon.ITermIdFilter;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.JustIndexWriteProc.WriteJustificationsProcConstructor;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer.InsertSolutionBuffer;
import com.bigdata.service.DataService;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * The {@link SPORelation} handles all things related to the indices
 * representing the triples stored in the database. Statements are first
 * converted to term identifiers using the {@link LexiconRelation} and then
 * inserted into the statement indices in parallel. There is one statement index
 * for each of the three possible access paths for a triple store. The key is
 * formed from the corresponding permutation of the subject, predicate, and
 * object, e.g., {s,p,o}, {p,o,s}, and {o,s,p}. The statement type (inferred,
 * axiom, or explicit) and the optional statement identifier are stored under the
 * key. All state for a statement is replicated in each of the statement
 * indices.
 * 
 * @todo When materializing a relation, such as the {@link SPORelation} or the
 *       {@link LexiconRelation}, on a {@link DataService} we may not want to
 *       have all indices resolved eager. The {@link AbstractTask} will actually
 *       return <code>null</code> rather than throwing an exception, but eager
 *       resolution of the indices will force {@link IClientIndex}s to spring
 *       into existence when we might only want a single index for the relation.
 * 
 * @todo integration with package providing magic set rewrites of rules in order
 *       to test whether or not a statement is still provable when it is
 *       retracted during TM. this will reduce the cost of loading data, since
 *       much of that is writing the justifications index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME quads : finish code review of entire class.
 */
public class SPORelation extends AbstractRelation<ISPO> {

    protected static final Logger log = Logger.getLogger(SPORelation.class);
    
    private static transient final long NULL = IRawTripleStore.NULL;
    
    private final Set<String> indexNames;

    private final int arity;
    
    public int arity() {
        
        return arity;
        
    }
    

    /** Hard references for the possible indices. */
    private final IIndex[] indices;
//    private IIndex spo;
//    private IIndex pos;
//    private IIndex osp;
    private IIndex just;

    /**
     * Constant for the {@link SPORelation} namespace component.
     * <p>
     * Note: To obtain the fully qualified name of an index in the
     * {@link SPORelation} you need to append a "." to the relation's namespace,
     * then this constant, then a "." and then the local name of the index.
     * 
     * @see AbstractRelation#getFQN(IKeyOrder)
     */
    public static final String NAME_SPO_RELATION = "spo";
    
    private static final transient String NAME_JUST = "JUST";
    
    /**
     * This is used to conditionally enable the logic to retract justifications
     * when the corresponding statements is retracted.
     */
    final public boolean justify;

    /**
     * This is used to conditionally disable all but a single statement index
     * (aka access path).
     */
    final public boolean oneAccessPath;

    /**
     * <code>true</code> iff the SPO index will maintain a bloom filter.
     * 
     * @see Options#BLOOM_FILTER
     */
    final protected boolean bloomFilter;
    
    /**
     * When <code>true</code> the database will support statement identifiers.
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     */
    final public boolean statementIdentifiers;

    /**
     * When <code>true</code> the database will support statement identifiers.
     * <p>
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     * <p>
     * Only explicit statements will have a statement identifier. Statements
     * made about statements using their statement identifiers will
     * automatically be retracted if a statement they describe is retracted (a
     * micro form of truth maintenance that is always enabled when statement
     * identifiers are enabled).
     */
    public boolean getStatementIdentifiers() {
        
        return statementIdentifiers;
        
    }

    public SPORelation(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);
        
        this.arity = Integer.valueOf(getProperty(
                AbstractTripleStore.Options.ARITY,
                AbstractTripleStore.Options.DEFAULT_ARITY));

        /*
         * Reads off the property for the inference engine that tells us whether
         * or not the justification index is being used. This is used to
         * conditionally enable the logic to retract justifications when the
         * corresponding statements is retracted.
         */

        this.justify = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.JUSTIFY,
                AbstractTripleStore.Options.DEFAULT_JUSTIFY));

        this.oneAccessPath = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.ONE_ACCESS_PATH,
                AbstractTripleStore.Options.DEFAULT_ONE_ACCESS_PATH));

        this.statementIdentifiers = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
                AbstractTripleStore.Options.DEFAULT_STATEMENT_IDENTIFIERS));

        this.bloomFilter = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.BLOOM_FILTER,
                AbstractTripleStore.Options.DEFAULT_BLOOM_FILTER));

        // FIXME quads : set of defined indices.
        {
            final Set<String> set = new HashSet<String>();

            if (arity == 3) {

                // three indices for a triple store and the have ids in [0:2].
                this.indices = new IIndex[3];
                
                if (oneAccessPath) {

                    set.add(getFQN(SPOKeyOrder.SPO));

                } else {

                    set.add(getFQN(SPOKeyOrder.SPO));

                    set.add(getFQN(SPOKeyOrder.POS));

                    set.add(getFQN(SPOKeyOrder.OSP));

                }

            } else {
                
                // six indices for a quad store w/ ids in [3:8].
                this.indices = new IIndex[9];

                // FIXME quads : declare indices.
                throw new UnsupportedOperationException();
                
            }

            // @todo removed justifications index from this set. Does this break
            // anything?
//        if(justify) {
//            
//            set.add(getNamespace() + "." + NAME_JUST);
//            
//        }

            this.indexNames = Collections.unmodifiableSet(set);

        }

        lookupIndices();
        
    }
    
    /**
     * Strengthened return type.
     */
    public AbstractTripleStore getContainer() {

        return (AbstractTripleStore) super.getContainer();
        
    }

    /**
     * @todo This should use GRS row scan in the GRS for the SPORelation
     *       namespace. It is only used by the {@link LocalTripleStore}
     *       constructor and a unit test's main() method.  This method
     *       IS NOT part of any public API at this time.
     */
    public boolean exists() {

        for(String name : getIndexNames()) {
            
            if (getIndex(name) == null)
                return false;
            
        }
        
//        if (oneAccessPath && spo == null)
//            return false;
//        
//        if (spo == null || pos == null || osp == null)
//            return false;
//        
//        if (justify && just == null)
//            return false;
        
        return true;
        
    }

    /**
     * Attempt to resolve each index for the {@link SPORelation} and cache a
     * hard reference to that index.
     */
    private void lookupIndices() {

        /*
         * Note: if full transactions are to be used then the statement indices
         * and the justification indices should be assigned the transaction
         * identifier.
         */

        if (arity == 3) {

            if (oneAccessPath) {

                // attempt to resolve the index and set the index reference.
                indices[SPOKeyOrder.SPO.index()] = super.getIndex(SPOKeyOrder.SPO);
                indices[SPOKeyOrder.POS.index()] = null;
                indices[SPOKeyOrder.OSP.index()]= null;

            } else {

                // attempt to resolve the index and set the index reference.
                indices[SPOKeyOrder.SPO.index()] = super.getIndex(SPOKeyOrder.SPO);
                indices[SPOKeyOrder.POS.index()] = super.getIndex(SPOKeyOrder.POS);
                indices[SPOKeyOrder.OSP.index()] = super.getIndex(SPOKeyOrder.OSP);

            }

        } else {

            // FIXME quads.
            throw new UnsupportedOperationException();
            
        }

        if(justify) {

            // attempt to resolve the index and set the index reference.
            just     = super.getIndex(getNamespace()+"."+NAME_JUST);
            
        } else {
            
            just = null;
            
        }

    }

    // FIXME quads : create appropriate indices.
    public void create() {
      
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            // create the relation declaration metadata.
            super.create();

            final IIndexManager indexManager = getIndexManager();

            if (arity == 3) {

                if (oneAccessPath) {

                    indexManager
                            .registerIndex(getStatementIndexMetadata(SPOKeyOrder.SPO));

                    // // resolve the index and set the index reference.
                    // spo = super.getIndex(SPOKeyOrder.SPO);
                    //
                    // assert spo != null;

                } else {

                    indexManager
                            .registerIndex(getStatementIndexMetadata(SPOKeyOrder.SPO));

                    indexManager
                            .registerIndex(getStatementIndexMetadata(SPOKeyOrder.POS));

                    indexManager
                            .registerIndex(getStatementIndexMetadata(SPOKeyOrder.OSP));

                    // // resolve the index and set the index reference.
                    // spo = super.getIndex(SPOKeyOrder.SPO);
                    //
                    // pos = super.getIndex(SPOKeyOrder.POS);
                    //
                    // osp = super.getIndex(SPOKeyOrder.OSP);
                    //                
                    // assert spo != null;
                    // assert pos != null;
                    // assert osp != null;

                }

            } else {

                throw new UnsupportedOperationException();

            }

            if (justify) {

                final String fqn = getNamespace() + "." + NAME_JUST;

                indexManager.registerIndex(getJustIndexMetadata(fqn));

                // resolve the index and set the index reference.
                just = getIndex(fqn);

            }

            lookupIndices();

        } finally {

            unlock(resourceLock);

        }

    }

    /*
     * @todo force drop of all indices rather than throwing an exception if an
     * index does not exist?
     * 
     * FIXME quads : destroy() appropriate indices (all registered in GRS?)
     */
    public void destroy() {

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            final IIndexManager indexManager = getIndexManager();

            // clear hard references.
            for (int i = 0; i < indices.length; i++) {
             
                indices[i] = null;
                
            }

            // drop indices.
            for(String name : getIndexNames()) {

                indexManager.dropIndex(name);
                
            }
            
//            if (oneAccessPath) {
//
//                indexManager.dropIndex(getFQN(SPOKeyOrder.SPO));
//                spo = null;
//
//            } else {
//
//                indexManager.dropIndex(getFQN(SPOKeyOrder.SPO));
//                spo = null;
//
//                indexManager.dropIndex(getFQN(SPOKeyOrder.POS));
//                pos = null;
//
//                indexManager.dropIndex(getFQN(SPOKeyOrder.OSP));
//                osp = null;
//
//            }

            if (justify) {

                indexManager.dropIndex(getNamespace() + "."+ NAME_JUST);
                just = null;

            }

            // destroy the relation declaration metadata.
            super.destroy();

        } finally {

            unlock(resourceLock);

        }
        
    }

    /**
     * Overridden to return the hard reference for the index.
     */
    @Override
    public IIndex getIndex(final IKeyOrder<? extends ISPO> keyOrder) {

        // @todo raise index() onto IKeyOrder?
        
        final int n = ((SPOKeyOrder)keyOrder).index();
        
        if (arity == 3)
            assert n >= 0 && n <= 2;
        else
            assert n >= 3 && n <= 8;
            
        final IIndex ndx = indices[ n ];
        
        if(ndx == null)
            throw new IllegalArgumentException();
        
        return ndx;

//        if (keyOrder == SPOKeyOrder.SPO) {
//     
//            return getSPOIndex();
//            
//        } else if (keyOrder == SPOKeyOrder.POS) {
//            
//            return getPOSIndex();
//            
//        } else if (keyOrder == SPOKeyOrder.OSP) {
//            
//            return getOSPIndex();
//            
//        } else {
//            
//            throw new AssertionError("keyOrder=" + keyOrder);
//            
//        }

    }

    final public SPOKeyOrder getPrimaryKeyOrder() {
        
        return arity == 3 ? SPOKeyOrder.SPO : SPOKeyOrder.SPOC;
        
    }
    
    final public IIndex getPrimaryIndex() {
        
        return getIndex(getPrimaryKeyOrder());
        
    }
    
    /** @deprecated by {@link #getIndex(IKeyOrder)} and #getPrimaryIndex(). */
    final public IIndex getSPOIndex() {

        return getIndex(SPOKeyOrder.SPO);
        
//        if (spo == null)
//            throw new IllegalStateException();
//
//        return spo;

    }

    /** @deprecated by {@link #getIndex(IKeyOrder)}. */
    final public IIndex getPOSIndex() {

        return getIndex(SPOKeyOrder.POS);

//        if (oneAccessPath)
//            return null;
//
//        if (pos == null)
//            throw new IllegalStateException();
//
//        return pos;

    }

    /** @deprecated by {@link #getIndex(IKeyOrder)}. */
    final public IIndex getOSPIndex() {

        return getIndex(SPOKeyOrder.OSP);

//        if (oneAccessPath)
//            return null;
//        
//        if (osp == null)
//            throw new IllegalStateException();
//
//        return osp;

    }

    /**
     * The optional index on which {@link Justification}s are stored.
     * 
     * @todo The Justifications index is not a regular index of the SPORelation.
     *       In fact, it is a relation for proof chains and is not really of the
     *       SPORelation at all and should probably be moved onto its own
     *       JRelation. The presence of the Justification index on the
     *       SPORelation would cause problems for methods which would like to
     *       enumerate the indices, except that we just silently ignore its
     *       presence in those methods (it is not in the index[] for example).
     *       <p>
     *       This would cause the justification index namespace to change to be
     *       a peer of the SPORelation namespace.
     */
    final public IIndex getJustificationIndex() {

        if (!justify)
            return null;

        if (just == null)
            throw new IllegalStateException();

        return just;

    }
    
    /**
     * Overrides for the statement indices.
     */
    protected IndexMetadata getStatementIndexMetadata(final SPOKeyOrder keyOrder) {

        final IndexMetadata metadata = newIndexMetadata(getFQN(keyOrder));

        // leading key compression works great.
        final IRabaCoder leafKeySer = DefaultTupleSerializer
                .getDefaultLeafKeysCoder();

        final IRabaCoder leafValSer;
        if (!statementIdentifiers) {

            /*
             * Note: this value coder does not know about statement identifiers.
             * Therefore it is turned off if statement identifiers are enabled.
             * 
             * @todo Examine some options for value compression for the
             * statement indices when statement identifiers are enabled. Of
             * course, the CanonicalHuffmanRabaCoder can always be used.
             */

            leafValSer = new FastRDFValueCoder2();
//            leafValSer = SimpleRabaCoder.INSTANCE;

        } else {
            
            leafValSer = DefaultTupleSerializer.getDefaultValuesCoder();
            
        }
        
        metadata.setTupleSerializer(new SPOTupleSerializer(keyOrder,
                leafKeySer, leafValSer));

        if (bloomFilter && keyOrder.equals(SPOKeyOrder.SPO)) {
            
            /*
             * Enable the bloom filter for the SPO index only.
             * 
             * Note: This SPO index is used any time we have an access path that
             * is a point test. Therefore this is the only index for which it
             * makes sense to maintain a bloom filter.
             * 
             * Note: The maximum error rate (maxP) applies to the mutable BTree
             * only. For scale-out indices, there is one mutable BTree per index
             * partition and a new (empty) BTree is allocated each time the live
             * journal for the index partitions overflows.
             */

//            // good performance up to ~2M triples.
//            final int n = 1000000; // 1M
//            final double p = 0.01;
//            final double maxP = 0.20;

//            // good performance up to ~20M triples.
//            final int n = 10000000; // 10M
//            final double p = 0.05;
//            final double maxP = 0.20;

//            final BloomFilterFactory factory = new BloomFilterFactory(n, p, maxP);
            
            final BloomFilterFactory factory = BloomFilterFactory.DEFAULT;
            
            if (log.isInfoEnabled())
                log.info("Enabling bloom filter for SPO index: " + factory);
            
            metadata.setBloomFilterFactory( factory );
            
        }
        
        return metadata;

    }

    /**
     * Overrides for the {@link IRawTripleStore#getJustificationIndex()}.
     */
    protected IndexMetadata getJustIndexMetadata(final String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new JustificationTupleSerializer(arity));

        return metadata;

    }

    public Set<String> getIndexNames() {

        return indexNames;
        
    }
    
    /**
     * 
     * @param s
     * @param p
     * @param o
     */
    public IAccessPath<ISPO> getAccessPath(final long s, final long p, final long o) {
     
        return getAccessPath(s, p, o, null/*filter*/);
        
    }

    /**
     * 
     * @param s
     * @param p
     * @param o
     * @param filter
     *            Optional filter to be evaluated close to the data.
     * @return
     */
    @SuppressWarnings("unchecked")
    public IAccessPath<ISPO> getAccessPath(final long s, final long p,
            final long o, final IElementFilter<ISPO> filter) {

        final IVariableOrConstant<Long> S = (s == NULL ? Var.var("s")
                : new Constant<Long>(s));

        final IVariableOrConstant<Long> P = (p == NULL ? Var.var("p")
                : new Constant<Long>(p));

        final IVariableOrConstant<Long> O = (o == NULL ? Var.var("o")
                : new Constant<Long>(o));
        
        return getAccessPath(new SPOPredicate(new String[] { getNamespace() },
                -1, // partitionId
                S, P, O,
                null, // context
                false, // optional
                filter,//
                null // expander
                ));
        
    }

    /**
     * Return the {@link IAccessPath} that is most efficient for the specified
     * predicate based on an analysis of the bound and unbound positions in the
     * predicate.
     * <p>
     * Note: When statement identifiers are enabled, the only way to bind the
     * context position is to already have an {@link SPO} on hand. There is no
     * index which can be used to look up an {@link SPO} by its context and the
     * context is always a blank node.
     * <p>
     * Note: This method is a hot spot, especially when the maximum parallelism
     * for subqueries is large. A variety of caching techniques are being
     * evaluated to address this.
     * 
     * @param pred
     *            The predicate.
     * 
     * @return The best access path for that predicate.
     */
    public IAccessPath<ISPO> getAccessPath(final IPredicate<ISPO> predicate) {

        /*
         * Note: Query is faster w/o cache on all LUBM queries.
         * 
         * @todo Optimization could reuse a caller's SPOAccessPath instance,
         * setting only the changed data on the fromKey/toKey.  That could
         * be done with setS(long), setO(long), setP(long) methods.  The
         * filter could be modified in the same manner.  That could cut down
         * on allocation costs, formatting the from/to keys, etc.
         */
        return _getAccessPath(predicate);
              
    }

    /**
     * Isolates the logic for selecting the {@link SPOKeyOrder} from the
     * {@link SPOPredicate} and then delegates to
     * {@link #getAccessPath(IKeyOrder, IPredicate)}.
     */
    final private SPOAccessPath _getAccessPath(final IPredicate<ISPO> predicate) {

        final SPOKeyOrder keyOrder = getKeyOrder(predicate);
        
        final SPOAccessPath accessPath = getAccessPath(keyOrder, predicate);

        if (log.isDebugEnabled())
            log.debug(accessPath.toString());

        //            System.err.println("new access path: pred="+predicate);

        return accessPath;

    }

    /**
     * Return the {@link SPOKeyOrder} for the given predicate.
     * 
     * @param predicate
     *            The predicate.
     *            
     * @return The {@link SPOKeyOrder}
     */
    static public SPOKeyOrder getKeyOrder(final IPredicate<ISPO> predicate) {

        final long s = predicate.get(0).isVar() ? NULL : (Long) predicate.get(0).get();
        final long p = predicate.get(1).isVar() ? NULL : (Long) predicate.get(1).get();
        final long o = predicate.get(2).isVar() ? NULL : (Long) predicate.get(2).get();
        // Note: Context is ignored!

        if (s != NULL && p != NULL && o != NULL) {

            return SPOKeyOrder.SPO;

        } else if (s != NULL && p != NULL) {

            return SPOKeyOrder.SPO;

        } else if (s != NULL && o != NULL) {

            return SPOKeyOrder.OSP;

        } else if (p != NULL && o != NULL) {

            return SPOKeyOrder.POS;

        } else if (s != NULL) {

            return SPOKeyOrder.SPO;

        } else if (p != NULL) {

            return SPOKeyOrder.POS;

        } else if (o != NULL) {

            return SPOKeyOrder.OSP;

        } else {

            return SPOKeyOrder.SPO;

        }

    }

    /**
     * Core impl.
     * 
     * @param keyOrder
     *            The natural order of the selected index (this identifies the
     *            index).
     * @param predicate
     *            The predicate specifying the query constraint on the access
     *            path.
     * 
     * @return The access path.
     */
    public SPOAccessPath getAccessPath(final IKeyOrder<ISPO> keyOrder,
            final IPredicate<ISPO> predicate) {

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        final IIndex ndx = getIndex(keyOrder);

        if (ndx == null) {
        
            throw new IllegalArgumentException("no index? relation="
                    + getNamespace() + ", timestamp=" + getTimestamp()
                    + ", keyOrder=" + keyOrder + ", pred=" + predicate
                    + ", indexManager=" + getIndexManager());
            
        }
        
        final int flags = IRangeQuery.KEYS
                | IRangeQuery.VALS
                | (TimestampUtility.isReadOnly(getTimestamp()) ? IRangeQuery.READONLY
                        : 0);
        
        final AbstractTripleStore container = getContainer();
        
        final int chunkOfChunksCapacity = container.getChunkOfChunksCapacity();

        final int chunkCapacity = container.getChunkCapacity();

        final int fullyBufferedReadThreshold = container.getFullyBufferedReadThreshold();
        
        return new SPOAccessPath(this, predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold).init();
        
    }
    
//    public long getElementCount(boolean exact) {
//
//        final IIndex ndx = getIndex(SPOKeyOrder.SPO);
//        
//        if (exact) {
//        
//            return ndx.rangeCountExact(null/* fromKey */, null/* toKey */);
//            
//        } else {
//            
//            return ndx.rangeCount(null/* fromKey */, null/* toKey */);
//            
//        }
//        
//    }

    /**
     * Efficient scan of the distinct term identifiers that appear in the first
     * position of the keys for the statement index corresponding to the
     * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS}
     * will give you the term identifiers for the distinct predicates actually
     * in use within statements in the {@link SPORelation}.
     * 
     * @param keyOrder
     *            The selected index order.
     * 
     * @return An iterator visiting the distinct term identifiers.
     */
    public IChunkedIterator<Long> distinctTermScan(final IKeyOrder<ISPO> keyOrder) {

        return distinctTermScan(keyOrder,/* termIdFilter */null);
        
    }
    
    /**
     * Efficient scan of the distinct term identifiers that appear in the first
     * position of the keys for the statement index corresponding to the
     * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS}
     * will give you the term identifiers for the distinct predicates actually
     * in use within statements in the {@link SPORelation}.
     * 
     * @param keyOrder
     *            The selected index order.
     * 
     * @return An iterator visiting the distinct term identifiers.
     */
    public IChunkedIterator<Long> distinctTermScan(
            final IKeyOrder<ISPO> keyOrder, final ITermIdFilter termIdFilter) {

        final FilterConstructor<SPO> filter = new FilterConstructor<SPO>();
        
        /*
         * Layer in the logic to advance to the tuple that will have the
         * next distinct term identifier in the first position of the key.
         */
        filter.addFilter(new DistinctTermAdvancer(arity));

        if (termIdFilter != null) {

            /*
             * Layer in a filter for only the desired term types.
             */
            
            filter.addFilter(new TupleFilter<SPO>() {

                private static final long serialVersionUID = 1L;

                @Override
                protected boolean isValid(final ITuple<SPO> tuple) {

                    final long id = KeyBuilder.decodeLong(tuple
                            .getKeyBuffer().array(), 0);

                    return termIdFilter.isValid(id);

                }

            });

        }

        final Iterator<Long> itr = new Striterator(getIndex(keyOrder)
                .rangeIterator(null/* fromKey */, null/* toKey */,
                        0/* capacity */, IRangeQuery.KEYS | IRangeQuery.CURSOR,
                        filter)).addFilter(new Resolver() {
                    /**
                     * Resolve SPO key to Long.
                     */
                    @Override
                    protected Long resolve(Object obj) {
                        return KeyBuilder.decodeLong(((ITuple) obj)
                                .getKeyBuffer().array(), 0);
                    }
                });

        return new ChunkedWrappedIterator<Long>(itr);
                
    }
    
    public SPO newElement(final IPredicate<ISPO> predicate,
            final IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final long s = asBound(predicate, 0, bindingSet);

        final long p = asBound(predicate, 1, bindingSet);

        final long o = asBound(predicate, 2, bindingSet);

        final SPO spo = new SPO(s, p, o, StatementEnum.Inferred);
        
        if(log.isDebugEnabled())
            log.debug(spo.toString());
        
        return spo;
        
    }

    public Class<ISPO> getElementClass() {

        return ISPO.class;

    }
   
   /**
     * Extract the bound value from the predicate. When the predicate is not
     * bound at that index, then extract its binding from the binding set.
     * 
     * @param pred
     *            The predicate.
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     *            
     * @return The bound value.
     */
    @SuppressWarnings("unchecked")
    private long asBound(final IPredicate<ISPO> pred, final int index,
            final IBindingSet bindingSet) {

        final IVariableOrConstant<Long> t = pred.get(index);

        final IConstant<Long> c;
        if(t.isVar()) {
            
            c = bindingSet.get((IVariable) t);
            
        } else {
            
            c = (IConstant<Long>)t;
            
        }

        return c.get().longValue();

    }

//    /**
//     * Return a buffer onto which a multi-threaded process may write chunks of
//     * elements to be written on the relation asynchronously. Chunks will be
//     * combined by a {@link BlockingBuffer} for greater efficiency. The buffer
//     * should be {@link BlockingBuffer#close() closed} once no more data will be
//     * written. This buffer may be used whether or not statement identifiers are
//     * enabled and will eventually delegate its work to
//     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)}
//     * <p>
//     * The returned {@link BlockingBuffer} is thread-safe and is intended for
//     * high concurrency use cases such as bulk loading data in which multiple
//     * threads need to write on the relation concurrently. The use of this
//     * buffer can substantially increase throughput for such use cases owing to
//     * its ability to combine chunks together before they are scattered to the
//     * indices. The effect is most pronounced for scale-out deployments when
//     * each write would normally be scattered to a large number of index
//     * partitions. By combining the chunks before they are scattered, the writes
//     * against the index partitions can be larger. Increased throughput results
//     * both from issuing fewer RMI requests, each of which must sit in a queue,
//     * and from having more data in each request which results in more efficient
//     * ordered writes on each index partition.
//     * 
//     * @param chunkSize
//     *            The desired chunk size for a write operation (this is an
//     *            explicit parameter since the desirable chunk size for a write
//     *            can be much larger than the desired chunk size for a read).
//     * 
//     * @return A write buffer. The {@link Future} on the blocking buffer is the
//     *         task draining the buffer and writing on the statement indices. It
//     *         may be used to wait until the writes are stable on the federation
//     *         or to cancel any outstanding writes.
//     */
//    synchronized public BlockingBuffer<ISPO[]> newWriteBuffer(final int chunkSize) {
//
//        final BlockingBuffer<ISPO[]> writeBuffer = new BlockingBuffer<ISPO[]>(
//                getChunkOfChunksCapacity(), chunkSize/*getChunkCapacity()*/,
//                getChunkTimeout(), TimeUnit.MILLISECONDS);
//
//        final Future<Void> future = getExecutorService().submit(
//                new ChunkConsumerTask(writeBuffer.iterator()));
//
//        writeBuffer.setFuture(future);
//
//        return writeBuffer;
//
//    }
//
//    /**
//     * Consumes elements from the source iterator, converting them into chunks
//     * on a {@link BlockingBuffer}. The consumer will drain the chunks from the
//     * buffer.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    private class ChunkConsumerTask implements Callable<Void> {
//        
//        /**
//         * The source which this task is draining.
//         * <p>
//         * Note: DO NOT close this iterator from within {@link #call()} - that
//         * would cause the task to interrupt itself!
//         */
//        private final IAsynchronousIterator<ISPO[]> src;
//        
//        public ChunkConsumerTask(final IAsynchronousIterator<ISPO[]> src) {
//
//            if (src == null)
//                throw new IllegalArgumentException();
//            
//            this.src = src;
//
//        }
//            
//        public Void call() throws Exception {
//
//            long nchunks = 0;
//            long nelements = 0;
//
//            while (src.hasNext()) {
//
//                final ISPO[] chunk = src.next();
//
//                nchunks++;
//                nelements += chunk.length;
//
//                if (log.isDebugEnabled())
//                    log.debug("#chunks=" + nchunks + ", chunkSize="
//                            + chunk.length + ", nelements=" + nelements);
//
//                getContainer()
//                        .addStatements(chunk, chunk.length, null/* filter */);
//
//            }
//
//            if (log.isInfoEnabled())
//                log.info("Done: #chunks=" + nchunks + ", #elements="
//                        + nelements);
//
//            return null;
//
//        }
//
//    }

    /**
     * Inserts {@link SPO}s, writing on the statement indices in parallel.
     * <p>
     * Note: This does NOT write on the justifications index. If justifications
     * are being maintained then the {@link ISolution}s MUST report binding
     * sets and an {@link InsertSolutionBuffer} MUST be used that knows how to
     * write on the justifications index AND delegate writes on the statement
     * indices to this method.
     * <p>
     * Note: This does NOT assign statement identifiers. The {@link SPORelation}
     * does not have direct access to the {@link LexiconRelation} and the latter
     * is responsible for assigning term identifiers. Code that writes explicit
     * statements onto the statement indices MUST use
     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)},
     * which knows how to generate the statement identifiers. In turn, that
     * method will delegate each "chunk" to this method.
     */
    public long insert(final IChunkedOrderedIterator<ISPO> itr) {

        try {
            
            long n = 0;
            
            while(itr.hasNext()) {
                
                final ISPO[] a = itr.nextChunk();
                
                n += insert( a, a.length, null/*filter*/ );
                
            }
         
            return n;
            
        } finally {
            
            itr.close();
            
        }
        
    }
    
    /**
     * Deletes {@link SPO}s, writing on the statement indices in parallel.
     * <p>
     * Note: This does NOT write on the justifications index. If justifications
     * are being maintained then the {@link ISolution}s MUST report binding
     * sets and an {@link InsertSolutionBuffer} MUST be used that knows how to
     * write on the justifications index AND delegate writes on the statement
     * indices to this method.
     * <p>
     * Note: This does NOT perform truth maintenance!
     * <p>
     * Note: This does NOT compute the closure for statement identifiers
     * (statements that need to be deleted because they are about a statement
     * that is being deleted).
     * 
     * @see AbstractTripleStore#removeStatements(IChunkedOrderedIterator, boolean)
     * @see SPOAccessPath#removeAll()
     */
    public long delete(final IChunkedOrderedIterator<ISPO> itr) {

        try {
            
            long n = 0;
            
            while(itr.hasNext()) {
                
                final ISPO[] a = itr.nextChunk();
                
                n += delete(a, a.length);
                
            }
         
            return n;
            
        } finally {
            
            itr.close();
            
        }
        
    }

    /**
     * Note: The statements are inserted into each index in parallel. We clone
     * the statement[] and sort and bulk load each statement index in parallel
     * using a thread pool.
     * 
     * @param a
     *            An {@link SPO}[].
     * @param numStmts
     *            The #of elements of that array that will be written.
     * @param filter
     *            An optional filter on the elements to be written.
     * 
     * @return The mutation count.
     */
    public long insert(final ISPO[] a, final int numStmts,
            final IElementFilter<ISPO> filter) {

        if (a == null)
            throw new IllegalArgumentException();
        
        if (numStmts > a.length)
            throw new IllegalArgumentException();
        
        if (numStmts == 0)
            return 0L;

        final long begin = System.currentTimeMillis();

        if(log.isDebugEnabled()) {
            
            log.debug("indexManager="+getIndexManager());
            
        }
        
        // time to sort the statements.
        final AtomicLong sortTime = new AtomicLong(0);

        // time to generate the keys and load the statements into the
        // indices.
        final AtomicLong insertTime = new AtomicLong(0);

        final AtomicLong mutationCount = new AtomicLong(0);
        
        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

        tasks.add(new SPOIndexWriter(this, a, numStmts, false/* clone */,
                SPOKeyOrder.SPO, filter, sortTime, insertTime, mutationCount));

        if (!oneAccessPath) {

            tasks.add(new SPOIndexWriter(this, a, numStmts, true/* clone */,
                    SPOKeyOrder.POS, filter, sortTime, insertTime, mutationCount));

            tasks.add(new SPOIndexWriter(this, a, numStmts, true/* clone */,
                    SPOKeyOrder.OSP, filter, sortTime, insertTime, mutationCount));

        }

        // if(numStmts>1000) {
        //
        // log.info("Writing " + numStmts + " statements...");
        //                    
        // }

        final List<Future<Long>> futures;
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;

        try {

            futures = getExecutorService().invokeAll(tasks);

            elapsed_SPO = futures.get(0).get();
            if (!oneAccessPath) {
                elapsed_POS = futures.get(1).get();
                elapsed_OSP = futures.get(2).get();
            } else {
                elapsed_POS = 0;
                elapsed_OSP = 0;
            }

        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled() && numStmts > 1000) {

            log.info("Wrote " + numStmts + " statements (mutationCount="
                    + mutationCount + ") in " + elapsed + "ms" //
                    + "; sort=" + sortTime + "ms" //
                    + ", keyGen+insert=" + insertTime + "ms" //
                    + "; spo=" + elapsed_SPO + "ms" //
                    + ", pos=" + elapsed_POS + "ms" //
                    + ", osp=" + elapsed_OSP + "ms" //
            );

        }

        return mutationCount.get();
        
    }

    /**
     * Delete the {@link SPO}s from the statement indices. Any justifications
     * for those statements will also be deleted.
     * 
     * @param stmts
     *            The {@link SPO}s.
     * @param numStmts
     *            The #of elements in that array to be processed.
     * 
     * @return The #of statements that were removed (mutationCount).
     * 
     * FIXME This needs to return the mutationCount. Resolve what is actually
     * being reported. I expect that {@link BatchRemove} only removes those
     * statements that it finds and that there is no constraint in place to
     * assure that this method only sees {@link SPO}s known to exist (but
     * perhaps it does since you can only do this safely for explicit
     * statements).
     */
    public long delete(final ISPO[] stmts, final int numStmts) {
        
        final long begin = System.currentTimeMillis();

        // The time to sort the data.
        final AtomicLong sortTime = new AtomicLong(0);

        // The time to delete the statements from the indices.
        final AtomicLong writeTime = new AtomicLong(0);

        // The mutation count.
        final AtomicLong mutationCount = new AtomicLong(0);

        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

        tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                SPOKeyOrder.SPO, false/* clone */, sortTime, writeTime));

        if (!oneAccessPath) {

            tasks
                    .add(new SPOIndexRemover(this, stmts, numStmts,
                            SPOKeyOrder.POS, true/* clone */, sortTime,
                            writeTime));

            tasks
                    .add(new SPOIndexRemover(this, stmts, numStmts,
                            SPOKeyOrder.OSP, true/* clone */, sortTime,
                            writeTime));

        }

        if (justify) {

            /*
             * Also retract the justifications for the statements.
             */

            tasks.add(new JustificationRemover(this, stmts, numStmts,
                    true/* clone */, sortTime, writeTime));

        }

        final List<Future<Long>> futures;
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;
        final long elapsed_JST;

        try {

            futures = getExecutorService().invokeAll(tasks);

            elapsed_SPO = futures.get(0).get();

            if (!oneAccessPath) {

                elapsed_POS = futures.get(1).get();

                elapsed_OSP = futures.get(2).get();

            } else {

                elapsed_POS = 0;

                elapsed_OSP = 0;

            }

            if (justify) {

                elapsed_JST = futures.get(3).get();

            } else {

                elapsed_JST = 0;

            }

        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled() && numStmts > 1000) {

            log.info("Removed " + numStmts + " in " + elapsed
                    + "ms; sort=" + sortTime + "ms, keyGen+delete="
                    + writeTime + "ms; spo=" + elapsed_SPO + "ms, pos="
                    + elapsed_POS + "ms, osp=" + elapsed_OSP
                    + "ms, jst=" + elapsed_JST);

        }

        return numStmts;
        
    }
    
    /**
     * Adds justifications to the store.
     * 
     * @param itr
     *            The iterator from which we will read the {@link Justification}s
     *            to be added. The iterator is closed by this operation.
     * 
     * @return The #of {@link Justification}s written on the justifications
     *         index.
     * 
     * @todo a lot of the cost of loading data is writing the justifications.
     *       SLD/magic sets will relieve us of the need to write the
     *       justifications since we can efficiently prove whether or not the
     *       statements being removed can be entailed from the remaining
     *       statements. Any statement which can still be proven is converted to
     *       an inference. Since writing the justification chains is such a
     *       source of latency, SLD/magic sets will translate into an immediate
     *       performance boost for data load.
     */
    public long addJustifications(IChunkedIterator<Justification> itr) {

        try {

            if (!itr.hasNext())
                return 0;

            final long begin = System.currentTimeMillis();

//            /*
//             * Note: This capacity estimate is based on N longs per SPO, one
//             * head, and 2-3 SPOs in the tail. The capacity will be extended
//             * automatically if necessary.
//             */
//
//            final KeyBuilder keyBuilder = new KeyBuilder(IRawTripleStore.N
//                    * (1 + 3) * Bytes.SIZEOF_LONG);

            long nwritten = 0;

            final IIndex ndx = getJustificationIndex();
            
            final JustificationTupleSerializer tupleSer = (JustificationTupleSerializer) ndx
                    .getIndexMetadata().getTupleSerializer();

            while (itr.hasNext()) {

                final Justification[] a = itr.nextChunk();

                final int n = a.length;

                // sort into their natural order.
                Arrays.sort(a);

                final byte[][] keys = new byte[n][];

                for (int i = 0; i < n; i++) {

//                    final Justification jst = a[i];

                    keys[i] = tupleSer.serializeKey(a[i]);//jst.getKey(keyBuilder);

                }

                /*
                 * sort into their natural order.
                 * 
                 * @todo is it faster to sort the Justification[] or the keys[]?
                 * See above for the alternative.
                 */
                // Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);

                final LongAggregator aggregator = new LongAggregator();

                ndx.submit(0/* fromIndex */, n/* toIndex */, keys,
                                null/* vals */,
                                WriteJustificationsProcConstructor.INSTANCE,
                                aggregator);

                nwritten += aggregator.getResult();

            }

            final long elapsed = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled())
                log.info("Wrote " + nwritten + " justifications in " + elapsed
                        + " ms");

            return nwritten;

        } finally {

            itr.close();

        }

    }

    /**
     * Dumps the specified index.
     */
    public StringBuilder dump(final IKeyOrder<ISPO> keyOrder) {
        
        final StringBuilder sb = new StringBuilder();
        
        {
            
            final IPredicate<ISPO> pred = new SPOPredicate(getNamespace(), Var
                    .var("s"), Var.var("p"), Var.var("o"));

            final IChunkedOrderedIterator<ISPO> itr = getAccessPath(keyOrder,
                    pred).iterator();

            try {

                while (itr.hasNext()) {

                    sb.append(itr.next());

                    sb.append("\n");

                }
                
            } finally {

                itr.close();

            }

        }

        return sb;

    }

}
