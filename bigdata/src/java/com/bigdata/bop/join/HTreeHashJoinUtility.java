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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IndexAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.lexicon.BlobsTupleSerializer;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Utility methods to support hash index builds and hash index joins using a
 * scalable native memory data structures.
 * 
 * <h2>Vectoring and IV encoding</h2>
 * 
 * In order to provide efficient encoding and persistence of solutions on the
 * {@link HTree}, this class is written directly to the RDF data model. Rather
 * than POJO serialization, solutions are encoded as logical {@link IV}[]s in a
 * manner very similar to how we represent the keys of the statement indices.
 * <p>
 * Since this encoding does not persist the {@link IV#getValue() cache}, a
 * separate mapping must be maintained from {@link IV} to {@link BigdataValue}
 * for those {@link IV}s which have a materialized {@link BigdataValue}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: HTreeHashJoinUtility.java 5568 2011-11-07 19:39:12Z thompsonbry
 */
public class HTreeHashJoinUtility implements
        IHashJoinUtility<HTreeHashJoinUtility> {

    static private final transient Logger log = Logger
            .getLogger(HTreeHashJoinUtility.class);

    /**
     * Note: If joinVars is an empty array, then the solutions will all hash to
     * ONE (1).
     */
    private static final int ONE = 1;
    
    /**
     * Return the hash code which will be used as the key given the ordered
     * as-bound values for the join variables.
     * 
     * @param joinVars
     *            The join variables.
     * @param bset
     *            The bindings whose as-bound hash code for the join variables
     *            will be computed.
     * @param ignoreUnboundVariables
     *            If a variable without a binding should be silently ignored.
     * 
     * @return The hash code.
     * 
     * @throws JoinVariableNotBoundException
     *             if there is no binding for a join variable.
     * 
     *             FIXME Does anything actually rely on the
     *             {@link JoinVariableNotBoundException}? It would seem that
     *             this exception could only be thrown if the joinvars[] was
     *             incorrectly formulated as it should only include
     *             "known bound" variables. (I think that this is related to
     *             incorrectly passing along empty solutions for named subquery
     *             hash joins.)
     */
    private static int hashCode(final IVariable<?>[] joinVars,
            final IBindingSet bset, final boolean ignoreUnboundVariables)
            throws JoinVariableNotBoundException {

        int h = ONE;

        for (IVariable<?> v : joinVars) {

            final IConstant<?> c = bset.get(v);

            if (c == null) {

                if(ignoreUnboundVariables)
                    continue;
                
                // Reject any solution which does not have a binding for a join
                // variable.
                throw new JoinVariableNotBoundException(v.getName());
                
            }
            
            // Works Ok.
            h = 31 * h + c.hashCode();
            
//            // Martyn's version.  Also works Ok.
//            final int hc = c.hashCode();
//            h += ~(hc<<15);
//            h ^=  (hc>>10);
//            h +=  (hc<<3);
//            h ^=  (hc>>6);

        }
        
        if (log.isTraceEnabled())
            log.trace("hashCode=" + h + ", joinVars="
                    + Arrays.toString(joinVars) + " : " + bset);

        return h;

    }

    /**
     * <code>true</code> until the state is discarded by {@link #release()}.
     */
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    /**
     * This basically controls the vectoring of the hash join.
     * 
     * TODO parameter from operator annotations. Note that 10k tends to put too
     * much heap pressure on the system if the source chunks happen to be
     * smallish. 1000k or 100 is probably the right value until we improve
     * vectoring of the query engine.
     */
    private final int chunkSize = 1000;//ChunkedWrappedIterator.DEFAULT_CHUNK_SIZE;

    /**
     * The schema provides the order in which the {@link IV}[] for solutions
     * stored in the hash index are encoded in the {@link HTree}. {@link IV}
     * s which are not bound are modeled by a {@link TermId#NullIV}.
     * <p>
     * Note: In order to be able to encode/decode the schema based on the
     * lazy identification of the variables which appear in solutions the
     * {@link HTree} must store variable length {@link IV}[]s since new
     * variables may be discovered at any point.
     */
    private final LinkedHashSet<IVariable<?>> schema;

    /**
     * The set of variables for which materialized {@link IV}s have been
     * observed.
     */
    private final LinkedHashSet<IVariable<?>> ivCacheSchema;

    /**
     * <code>true</code> iff the join is OPTIONAL.
     */
    private final boolean optional;
    
    /**
     * <code>true</code> iff this is a DISTINCT filter.
     */
    private final boolean filter;
    
    /**
     * The join variables.
     */
    private final IVariable<?>[] joinVars;

    /**
     * The variables to be retained (optional, all variables are retained if
     * not specified).
     */
    private final IVariable<?>[] selectVars;

    /**
     * The join constraints (optional).
     */
    private final IConstraint[] constraints;

    /**
     * The namespace of the {@link LexiconRelation} IFF we need to maintain
     * the {@link #ivCache}.
     */
    private final String namespace;

    /**
     * The {@link BigdataValueFactory} for the {@link LexiconRelation} IFF we
     * need to maintain the {@link #ivCache}.
     */
    private final BigdataValueFactory valueFactory;

    /**
     * The backing {@link IRawStore}.
     */
    private final IRawStore store;
    
    /**
     * The hash index. The keys are int32 hash codes built from the join
     * variables. The values are an {@link IV}[], similar to the encoding in
     * the statement indices. The mapping from the index positions in the
     * {@link IV}s to the variables is specified by the {@link #schema}.
     */
    private final AtomicReference<HTree> rightSolutions = new AtomicReference<HTree>();

    /**
     * The set of distinct source solutions which joined. This set is
     * maintained iff the join is optional and is <code>null</code>
     * otherwise.
     */
    private final AtomicReference<HTree> joinSet = new AtomicReference<HTree>();
    
    /**
     * The {@link IV}:{@link BigdataValue} mapping for non-{@link BlobIV}s. This
     * captures any cached BigdataValue references encountered on {@link IV}s.
     * This map does not store duplicate entries for the same {@link IV}.
     * <p>
     * Note: This is precisely the same mapping we use for the ID2TERM index.
     */
    private final AtomicReference<BTree> ivCache = new AtomicReference<BTree>();

    /**
     * The {@link IV}:{@link BigdataValue} mapping for {@link BlobIV}s with
     * cached {@link BigdataValue}s. This captures any cached BigdataValue
     * references encountered on {@link BlobIV}s. This map does not store
     * duplicate entries for the same {@link IV}.
     * <p>
     * Note: This is precisely the same mapping we use for the BLOBS index.
     * 
     * TODO The last committed revision of this class prior to introducing the
     * blobsCache was r5568. I need to verify that there is no performance
     * regression due to the presence of the blobsCache.
     */
    private final AtomicReference<BTree> blobsCache = new AtomicReference<BTree>();

    /**
     * The hash index.
     */
    private HTree getRightSolutions() {
        
        return rightSolutions.get();
        
    }
    
    /**
     * The set of distinct source solutions which joined. This set is
     * maintained iff the join is optional and is <code>null</code>
     * otherwise.
     */
    private HTree getJoinSet() {

        return joinSet.get();
        
    }
    
    @Override
    public boolean isEmpty() {
        
        return getRightSolutions().getEntryCount() == 0;
        
    }
    
    @Override
    public long getRightSolutionCount() {
        
        return getRightSolutions().getEntryCount();
        
    }

    public boolean isOptional() {

        return optional;
        
    }

    public boolean isFilter() {
        
        return filter;
        
    }
    
    public IVariable<?>[] getJoinVars() {

        return joinVars;
        
    }
    
    public IVariable<?>[] getSelectVars() {
        
        return selectVars;
        
    }
    
    public IConstraint[] getConstraints() {
        
        return constraints;
        
    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #rightSolutions} or
     * {@link #joinSet}.
     */
    static private IndexMetadata getIndexMetadata(final PipelineOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int addressBits = op.getProperty(HTreeAnnotations.ADDRESS_BITS,
                HTreeAnnotations.DEFAULT_ADDRESS_BITS);

        final int branchingFactor = 2 ^ addressBits;
        
        final int ratio = 32; // TODO Config/tune.
        
        metadata.setAddressBits(addressBits);

        metadata.setRawRecords(op.getProperty(//
                HTreeAnnotations.RAW_RECORDS,
                HTreeAnnotations.DEFAULT_RAW_RECORDS));

        metadata.setMaxRecLen(op.getProperty(//
                HTreeAnnotations.MAX_RECLEN,
                HTreeAnnotations.DEFAULT_MAX_RECLEN));

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.

        @SuppressWarnings("rawtypes")
        final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                new FrontCodedRabaCoder(ratio),// keys : TODO Optimize for int32!
                new SimpleRabaCoder() // vals
        );

        metadata.setTupleSerializer(tupleSer);
        
        return metadata;

    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #ivCache}.
     * <p>
     * Note: This is basically the same setup as the ID2TERM index.
     */
    private IndexMetadata getIVCacheIndexMetadata(final PipelineOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int branchingFactor = 256;// TODO Config/tune.
        
        final int ratio = 32; // TODO Config/tune.
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        metadata.setTupleSerializer(new Id2TermTupleSerializer(namespace,
                valueFactory, new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG),//
                new FrontCodedRabaCoder(ratio), SimpleRabaCoder.INSTANCE));

        // a bloom filter should help avoid lookups when IVs do not have cached
        // values.
        metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

        // enable raw record support.
        metadata.setRawRecords(true);

        /*
         * Very small RDF values can be inlined into the index, but after that
         * threshold we want to have the values out of line on the backing
         * store.
         * 
         * TODO Tune this and the threshold at which we use the BLOBS index
         * instead.
         */
        metadata.setMaxRecLen(16); 
        
        return metadata;

    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #blobsCache}.
     * <p>
     * Note: This is basically the same setup as the BLOBS index.
     */
    private IndexMetadata getBlobsCacheIndexMetadata(final PipelineOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setTupleSerializer(new BlobsTupleSerializer(namespace,
                valueFactory));

        // enable raw record support.
        metadata.setRawRecords(true);

        /*
         * The presumption is that we are storing large literals (blobs) in this
         * index so we always want to write them on raw records rather than have
         * them be inline in the leaves of the index.
         */
        metadata.setMaxRecLen(0);

        /*
         * TODO The default branching factor for this index should probably be
         * pretty big. All of the values are on raw records, so it is just the
         * keys in the index and the have a fixed width (8 bytes).
         */
        final int branchingFactor = 256;
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        // a bloom filter should help avoid lookups when IVs do not have cached
        // values.
        metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

        return metadata;

    }
    
    /**
     * 
     * @param mmgr
     *            The IMemoryManager which will back the named solution set.
     * @param op
     *            The operator whose annotation will inform construction the
     *            hash index. The {@link HTreeAnnotations} may be specified for
     *            this operator and will control the initialization of the
     *            various {@link HTree} instances.
     * @param optional
     *            <code>true</code> iff the join is optional.
     * @param filter
     *            <code>true</code> iff the hash index is being used as a
     *            DISTINCT filter. Various annotations pertaining to JOIN
     *            processing are ignored when used as a DISTINCT filter.
     * 
     * @see HTreeHashJoinAnnotations
     */
    public HTreeHashJoinUtility(final IMemoryManager mmgr,
            final PipelineOp op, final boolean optional, final boolean filter) {

        if (mmgr == null)
            throw new IllegalArgumentException();

        if (op == null)
            throw new IllegalArgumentException();

        // The join variables (required).
        this.joinVars = (IVariable<?>[]) op
                .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

        // The projected variables (optional and equal to the join variables iff
        // this is a DISTINCT filter).
        this.selectVars = filter ? joinVars : (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT);

        // Initialize the schema with the join variables.
        this.schema = new LinkedHashSet<IVariable<?>>();
        this.schema.addAll(Arrays.asList(joinVars));

        // The set of variables for which materialized values are observed.
        this.ivCacheSchema = filter ? null : new LinkedHashSet<IVariable<?>>();

        // The join constraints (optional).
        this.constraints = (IConstraint[]) op
                .getProperty(JoinAnnotations.CONSTRAINTS);

        // Iff the join has OPTIONAL semantics.
        this.optional = optional;
        
        // Iff this is a DISTINCT filter.
        this.filter = filter;
        
//        // ignore unbound variables when used as a DISTINCT filter.
//        this.ignoreUnboundVariables = filter;

        if (!filter) {

            namespace = ((String[]) op
                    .getRequiredProperty(Predicate.Annotations.RELATION_NAME))[0];

            valueFactory = BigdataValueFactoryImpl.getInstance(namespace);

        } else {

            namespace = null;
            
            valueFactory = null;
            
        }
        
        /*
         * This wraps an efficient raw store interface around a child memory
         * manager created from the IMemoryManager which will back the named
         * solution set.
         */
        store = new MemStore(mmgr.createAllocationContext());

        // Will support incremental eviction and persistence.
        rightSolutions.set(HTree.create(store, getIndexMetadata(op)));

        if (optional) {

            // The join set is used to handle optionals.
            joinSet.set(HTree.create(store, getIndexMetadata(op)));

        }

        /*
         * Setup the IV => BigdataValue mapping. This captures any cached
         * BigdataValue references encountered on IVs. This map does not store
         * duplicate entries for the same IV.
         */
        if (!filter) {

            this.ivCache.set(BTree.create(store, getIVCacheIndexMetadata(op)));

            this.blobsCache.set(BTree
                    .create(store, getBlobsCacheIndexMetadata(op)));

        }

    }

    /**
     * The backing {@link IRawStore}.
     */
    public IRawStore getStore() {
    
        return store;
        
    }
    
    /**
     * Checkpoint the {@link HTree} instance(s) used to buffer the source
     * solutions ({@link #rightSolutions} and {@link #ivCache}) and then
     * re-load the them in a read-only mode from their checkpoint(s). This
     * exposes a view of the {@link HTree} which is safe for concurrent
     * readers.
     */
    public void saveSolutionSet() {

        if (!open.get())
            throw new IllegalStateException();

        checkpointHTree(rightSolutions);

        checkpointBTree(ivCache);
        checkpointBTree(blobsCache);

        /*
         * Note: DO NOT checkpoint the joinSet here. That index is not even
         * written upon until we begin to evaluate the joins, which happens
         * after we checkpoint the source solutions.
         */

    }

    /**
     * Checkpoint the join set (used to buffer the optional solutions).
     * <p>
     * Note: Since we always output the solutions which did not join from a
     * single thread as part of last pass evaluation there is no need to
     * checkpoint the {@link #joinSet}.
     */
    public void checkpointJoinSet() {

        if (!open.get())
            throw new IllegalStateException();

        checkpointHTree(joinSet);

    }

    private void checkpointHTree(final AtomicReference<HTree> ref) {

        final HTree tmp = ref.get();

        if (tmp != null) {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = tmp.writeCheckpoint2();

            final HTree readOnly = HTree.load(store,
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // Get a read-only view of the HTree.
            if (!ref.compareAndSet(tmp/* expect */, readOnly)) {

                throw new IllegalStateException();

            }

        }

    }

    private void checkpointBTree(final AtomicReference<BTree> ref) {

        final BTree tmp = ref.get();

        if (tmp != null) {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = tmp.writeCheckpoint2();

            final BTree readOnly = BTree.load(store,
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // Get a read-only view of the HTree.
            if (!ref.compareAndSet(tmp/* expect */, readOnly)) {

                throw new IllegalStateException();

            }

        }

    }

    @Override
    public void release() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed.
            return;
        }

        schema.clear();

        if (ivCacheSchema != null) {

            ivCacheSchema.clear();
            
        }
        
        HTree tmp = rightSolutions.getAndSet(null/* newValue */);

        if (tmp != null) {

            tmp.close();

        }

        tmp = joinSet.getAndSet(null/* newValue */);
        
        if (tmp != null) {

            tmp.close();

        }

        BTree tmp2 = ivCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }

        tmp2 = blobsCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }

        store.close();

    }

    @Override
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats) {

        if (itr == null)
            throw new IllegalArgumentException();
        
        if (stats == null)
            throw new IllegalArgumentException();

        long naccepted = 0L;
        
        final HTree htree = getRightSolutions();

        final IKeyBuilder keyBuilder = htree.getIndexMetadata().getKeyBuilder();
        
        final Map<IV<?, ?>, BigdataValue> cache = new HashMap<IV<?, ?>, BigdataValue>();
        
        /*
         * Rechunk in order to have a nice fat vector size for ordered inserts.
         */
        final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                new Dechunkerator<IBindingSet>(itr), chunkSize,
                IBindingSet.class);

        final AtomicInteger vectorSize = new AtomicInteger();
        while (it.hasNext()) {

            final BS[] a = vector(it.next(), joinVars, null/* selectVars */,
                    false/* ignoreUnboundVariables */, vectorSize);

            final int n = vectorSize.get();
            
//        while (itr.hasNext()) {
//
//            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

//            for (IBindingSet bset : a) {
//
//                int hashCode = ONE; // default (used iff join is optional).
//                try {
//
//                    hashCode = HTreeHashJoinUtility.hashCode(joinVars, bset,
//                            ignoreUnboundVariables);
//
//                } catch (JoinVariableNotBoundException ex) {
//
//                    if (!optional) {
//                        
//                        // Drop solution;
//
//                        if (log.isTraceEnabled())
//                            log.trace(ex);
//
//                        continue;
//
//                    }
//                    
//                }
            
            for (int i = 0; i < n; i++) {

                final BS tmp = a[i];

                // Update the schema.
                updateSchema(tmp.bset);

                // Encode the key.
                final byte[] key = keyBuilder.reset().append(tmp.hashCode)
                        .getKey();

                // Encode the solution.
                final byte[] val = encodeSolution(keyBuilder, schema, cache,
                        tmp.bset);

                // Insert binding set under hash code for that key.
                htree.insert(key, val);

            }

            naccepted += a.length;

            updateIVCache(cache, ivCache.get(), blobsCache.get());

        }
        
        return naccepted;

    }

    @Override
    public long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink) {

        if (itr == null)
            throw new IllegalArgumentException();
        
        if (stats == null)
            throw new IllegalArgumentException();

        long naccepted = 0L;
        
        final HTree htree = getRightSolutions();

//        final Map<IV<?, ?>, BigdataValue> cache = new HashMap<IV<?, ?>, BigdataValue>();
        
        final IKeyBuilder keyBuilder = htree.getIndexMetadata().getKeyBuilder();

        /*
         * Rechunk in order to have a nice fat vector size for ordered inserts.
         */
        final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                new Dechunkerator<IBindingSet>(itr), chunkSize,
                IBindingSet.class);

        final AtomicInteger vectorSize = new AtomicInteger();
        while (it.hasNext()) {

            final BS[] a = vector(it.next(), joinVars, selectVars,
                    true/* ignoreUnboundVariables */, vectorSize);
  
            final int n = vectorSize.get();

//        while (itr.hasNext()) {
//
//            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

//            for (IBindingSet bset : a) {
//                
//                // Note: Must *only* consider the projected variables.
//                bset = bset.copy(selectVars);
//
//                int hashCode = ONE; // default (used iff join is optional).
//                try {
//
//                    hashCode = HTreeHashJoinUtility.hashCode(joinVars, bset,
//                            ignoreUnboundVariables);
//
//                } catch (JoinVariableNotBoundException ex) {
//
//                    // Exception is not thrown when used as a filter.
//                    throw new AssertionError();
//                    
//                }

            for (int i = 0; i < n; i++) {

                final BS tmp = a[i];
                
                // Update the schema.
                updateSchema(tmp.bset);

                // Encode the key.
                final byte[] key = keyBuilder.reset().append(tmp.hashCode)
                        .getKey();

                // Encode the solution.
                final byte[] val = encodeSolution(keyBuilder, schema,
                        null/* cache */, tmp.bset);

                /*
                 * Search the hash index for a match.
                 */
                boolean found = false;
                
                final ITupleIterator<?> titr = htree.lookupAll(key);

                while(titr.hasNext()) {
                
                    final ITuple<?> t = titr.next();
                    
                    final ByteArrayBuffer tb = t.getValueBuffer();

                    if (0 == BytesUtil.compareBytesWithLenAndOffset(
                            0/* aoff */, val.length/* alen */, val,//
                            0/* boff */, tb.limit()/* blen */, tb.array()/* b */
                    )) {

                        found = true;

                        break;
                        
                    }
                    
                }

                if (!found) {
                
                    // Add to the hash index.
                    htree.insert(key, val);

                    // Write onto the sink.
                    sink.add(tmp.bset);

                    naccepted++;
                    
                }
                
            }

//            updateIVCache(cache, ivCache.get());

        }
        
        return naccepted;

    }
    
    /**
     * Build up the schema. This includes all observed variables, not just those
     * declared in {@link #joinVars}
     * 
     * @param bset An observed binding set.
     */
    private void updateSchema(final IBindingSet bset) {

        @SuppressWarnings("rawtypes")
        final Iterator<IVariable> vitr = bset.vars();
        
        while (vitr.hasNext()) {
        
            schema.add(vitr.next());
            
        }

    }

    /**
     * Transfer any {@link IV} to {@link BigdataValue} mappings to the ivCache.
     * 
     * @param cache
     *            A JVM cache.
     * @param ivCache
     *            A persistence capable cache for non-{@link BlobIV}s {@link IV}
     *            s having a cached {@link BigdataValue} reference.
     * @param blobsCache
     *            A persistence capable cache for {@link BlobIV}s having a
     *            cached {@link BigdataValue} reference.
     */
    private void updateIVCache(final Map<IV<?, ?>, BigdataValue> cache,
            final BTree ivCache, final BTree blobsCache) {

        // Used to serialize RDF {@link Value}s.
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();

        for (Map.Entry<IV<?, ?>, BigdataValue> e : cache.entrySet()) {

            final IV<?, ?> iv = e.getKey();

            final BigdataValue value = e.getValue();

            if (iv instanceof BlobIV<?>) {

                final BlobsIndexHelper h = new BlobsIndexHelper();
                
                final IKeyBuilder keyBuilder = h.newKeyBuilder();

                final byte[] baseKey = h.makePrefixKey(keyBuilder.reset(),
                        value);

                final byte[] val = valueFactory.getValueSerializer().serialize(
                        value);

                h.resolveOrAddValue(blobsCache, false/* readOnly */,
                        keyBuilder, baseKey, val, null/* tmp */, null/* bucketSize */);
                
            } else {

                final byte[] key = tupSer.serializeKey(iv);

                if (!ivCache.contains(key)) {

                    ivCache.insert(key, tupSer.serializeVal(value));

                }

            }

        }

    }

    /**
     * Encode the solution as an IV[].
     * 
     * @param keyBuilder
     *            Where to format the solution.
     * @param schema
     *            The schema, which specifies the order in which the variable
     *            bindings will appear. If a variable declared in the schema is
     *            not bound in the solution, then it will be represented by a
     *            {@link TermId#NullIV}.
     * @param cache
     *            Any cached {@link BigdataValue}s on the {@link IV}s are
     *            inserted into this map (optional since the cache is not 
     *            used when we are filtering DISTINCT solutions).
     * @param bset
     *            The solution to be encoded.
     * @return The encoded solution.
     */
    private byte[] encodeSolution(final IKeyBuilder keyBuilder,
            final LinkedHashSet<IVariable<?>> schema,
            final Map<IV<?, ?>, BigdataValue> cache, final IBindingSet bset) {

        keyBuilder.reset();
        final Iterator<IVariable<?>> vitr = schema.iterator();
        while (vitr.hasNext()) {
            final IVariable<?> v = vitr.next();
            @SuppressWarnings("unchecked")
            final IConstant<IV<?, ?>> c = bset.get(v);
            if (c == null) {
                IVUtility.encode(keyBuilder, TermId.NullIV);
            } else {
                final IV<?, ?> iv = c.get();
                IVUtility.encode(keyBuilder, iv);
                if (iv.hasValue() && !filter) {
                    ivCacheSchema.add(v);
                    cache.put(iv, iv.getValue());
                }
            }
        }
        return keyBuilder.getKey();
        
    }

    /**
     * Decode a solution from an encoded {@link IV}[].
     * <p>
     * Note: The {@link IV#getValue() cached value} is NOT part of the encoded
     * data and will NOT be present in the returned {@link IBindingSet}. The
     * cached {@link BigdataValue} (if any) must be unified by consulting the
     * {@link #ivCache}.
     * 
     * @param t
     *            A tuple whose value is an encoded {@link IV}[].
     *            
     * @return The decoded {@link IBindingSet}.
     */
    private IBindingSet decodeSolution(final ITuple<?> t) {
        
        final ByteArrayBuffer b = t.getValueBuffer();
        
        return decodeSolution(b.array(), 0, b.limit());

    }

    /**
     * Decode a solution from an encoded {@link IV}[].
     * <p>
     * Note: The {@link IV#getValue() cached value} is NOT part of the encoded
     * data and will NOT be present in the returned {@link IBindingSet}. The
     * cached {@link BigdataValue} (if any) must be unified by consulting the
     * {@link #ivCache}.
     * 
     * @param val
     *            The encoded IV[].
     * @param fromOffset
     *            The starting offset.
     * @param toOffset
     *            The byte beyond the end of the encoded data.
     * 
     * @return The decoded {@link IBindingSet}.
     */
    private IBindingSet decodeSolution(final byte[] val, final int fromOffset,
            final int toOffset) {

        final IBindingSet rightSolution = new ListBindingSet();

        final IV<?, ?>[] ivs = IVUtility.decodeAll(val, fromOffset, toOffset);

        int i = 0;

        for (IVariable<?> v : schema) {

            if (i == ivs.length) {
                /*
                 * This solution does not include all variables which were
                 * eventually discovered to be part of the schema.
                 */
                break;
            }

            final IV<?, ?> iv = ivs[i++];
            
            if (iv == null) {
            
                // Not bound.
                continue;
                
            }

            rightSolution.set(v, new Constant<IV<?, ?>>(iv));

        }

        return rightSolution;

    }

    /**
     * Glue class for hash code and binding set used when the hash code is for
     * just the join variables rather than the entire binding set.
     */
    private static class BS implements Comparable<BS> {

        final private int hashCode;

        final private IBindingSet bset;

        BS(final int hashCode, final IBindingSet bset) {
            this.hashCode = hashCode;
            this.bset = bset;
        }

        @Override
        public int compareTo(final BS o) {
            if (this.hashCode < o.hashCode)
                return -1;
            if (this.hashCode > o.hashCode)
                return 1;
            return 0;
        }
        
        public String toString() {
            return getClass().getName() + "{hashCode=" + hashCode + ",bset="
                    + bset + "}";
        }
        
    }
    
    /**
     * Glue class for hash code and encoded binding set used when we already
     * have the binding set encoded.
     */
    private static class BS2 implements Comparable<BS2> {

        final private int hashCode;

        final private byte[] value;

        BS2(final int hashCode, final byte[] value) {
            this.hashCode = hashCode;
            this.value = value;
        }

        @Override
        public int compareTo(final BS2 o) {
            if (this.hashCode < o.hashCode)
                return -1;
            if (this.hashCode > o.hashCode)
                return 1;
            return 0;
        }
        
        public String toString() {
            return getClass().getName() + "{hashCode=" + hashCode + ",value="
                    + BytesUtil.toString(value) + "}";
        }
        
    }
    
    @Override
    public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer//
            ) {

        hashJoin2(leftItr, outputBuffer, constraints);
        
    }

    @Override
    public void hashJoin2(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final IConstraint[] constraints//
            ) {

        try {

            final HTree rightSolutions = this.getRightSolutions();

            if (log.isInfoEnabled()) {
                log.info("rightSolutions: #nnodes="
                        + rightSolutions.getNodeCount() + ",#leaves="
                        + rightSolutions.getLeafCount() + ",#entries="
                        + rightSolutions.getEntryCount());
            }
            
            final IKeyBuilder keyBuilder = rightSolutions.getIndexMetadata()
                    .getKeyBuilder();

            final BTree ivCache = this.ivCache.get();
            
            final BTree blobsCache = this.blobsCache.get();
            
            final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                    leftItr, chunkSize, IBindingSet.class);

            final AtomicInteger vectorSize = new AtomicInteger();
            while (it.hasNext()) {

                final BS[] a = vector(it.next(), joinVars,
                        null/* selectVars */,
                        false/* ignoreUnboundVariables */, vectorSize);
                
                final int n = vectorSize.get();

                int fromIndex = 0;

                while (fromIndex < n) {

                    /*
                     * Figure out how many left solutions in the current chunk
                     * have the same hash code. We will use the same iterator
                     * over the right solutions for that hash code against the
                     * HTree.
                     */
                    
                    // The next hash code to be processed.
                    final int hashCode = a[fromIndex].hashCode;

                    // scan for the first hash code which is different.
                    int toIndex = n; // assume upper bound.
                    for (int i = fromIndex + 1; i < n; i++) {
                        if (a[i].hashCode != hashCode) {
                            toIndex = i;
                            break;
                        }
                    }
                    // #of left solutions having the same hash code.
                    final int bucketSize = toIndex - fromIndex;

                    if (log.isTraceEnabled())
                        log.trace("hashCode=" + hashCode + ": #left="
                                + bucketSize + ", firstLeft=" + a[fromIndex]);

                    /*
                     * Note: all source solutions in [fromIndex:toIndex) have
                     * the same hash code. They will be vectored together.
                     */
                    // All solutions which join for that collision bucket
                    final LinkedList<BS2> joined = optional ? new LinkedList<BS2>()
                            : null;
                    // #of solutions which join for that collision bucket.
                    int njoined = 0;
                    // #of solutions which did not join for that collision bucket.
                    int nrejected = 0;
                    {

                        final byte[] key = keyBuilder.reset().append(hashCode).getKey();
                        
                        // visit all source solutions having the same hash code
                        final ITupleIterator<?> titr = rightSolutions
                                .lookupAll(key);

                        long sameHashCodeCount = 0;
                        
                        while (titr.hasNext()) {

                            sameHashCodeCount++;
                            
                            final ITuple<?> t = titr.next();

                            /*
                             * Note: The map entries must be the full source
                             * binding set, not just the join variables, even
                             * though the key and equality in the key is defined
                             * in terms of just the join variables.
                             * 
                             * Note: Solutions which have the same hash code but
                             * whose bindings are inconsistent will be rejected
                             * by bind() below.
                             */
                            final IBindingSet rightSolution = decodeSolution(t);

                            for (int i = fromIndex; i < toIndex; i++) {

                                final IBindingSet leftSolution = a[i].bset;
                                
                                // Join.
                                final IBindingSet outSolution = BOpContext
                                        .bind(leftSolution, rightSolution,
                                                constraints,
                                                selectVars);

                                if (outSolution == null) {

                                    nrejected++;
                                    
                                    if (log.isTraceEnabled())
                                        log.trace("Does not join: hashCode="
                                                + hashCode
                                                + ", sameHashCodeCount="
                                                + sameHashCodeCount
                                                + ", #left=" + bucketSize
                                                + ", #joined=" + njoined
                                                + ", #rejected=" + nrejected
                                                + ", left=" + leftSolution
                                                + ", right=" + rightSolution);

                                    // Join failed.
                                    continue;

                                }

                                njoined++;

                                resolveCachedValues(ivCache, blobsCache,
                                        outSolution);

                                if (log.isDebugEnabled())
                                    log.debug("JOIN: hashCode=" + hashCode
                                            + ", sameHashCodeCount="
                                            + sameHashCodeCount + ", #left="
                                            + bucketSize + ", #joined="
                                            + njoined + ", #rejected="
                                            + nrejected + ", solution="
                                            + outSolution);

                                // Accept this binding set.
                                outputBuffer.add(outSolution);

                                if (optional) {

                                    joined.add(new BS2(
                                            rightSolution.hashCode(), t
                                                    .getValue()));
                                }

                            } // next left in the same bucket.

                        } // next rightSolution with the same hash code.

                        if (optional) {
                            /*
                             * Vector the inserts into the [joinSet].
                             */
                            final BS2[] a2 = joined.toArray(new BS2[njoined]);
                            Arrays.sort(a2, 0, njoined);
                            for (int i = 0; i < njoined; i++) {
                                final BS2 tmp = a2[i];
                                saveInJoinSet(tmp.hashCode, tmp.value);
                            }
                        }

                    } // end block of leftSolutions having the same hash code.

                    fromIndex = toIndex;
                    
                } // next slice of source solutions with the same hash code.

            } // while(itr.hasNext()

        } finally {

            leftItr.close();

        }

    } // handleJoin

    /**
     * Vector a chunk of solutions.
     * 
     * @param leftSolutions
     *            The solutions.
     * @param joinVars
     *            The variables on which the hash code will be computed.
     * @param selectVars
     *            When non-<code>null</code>, all other variables are dropped.
     *            (This is used when we are modeling a DISTINCT solutions filter
     *            since we need to drop anything which is not part of the
     *            DISTINCT variables list.)
     * @param ignoreUnboundVariables
     *            When <code>true</code>, an unbound variable will not cause a
     *            {@link JoinVariableNotBoundException} to be thrown.
     * @param vectorSize
     *            The vector size (set by side-effect).
     * 
     * @return The vectored chunk of solutions ordered by hash code.
     */
    private BS[] vector(final IBindingSet[] leftSolutions,
            final IVariable<?>[] joinVars,
            final IVariable<?>[] selectVars,
            final boolean ignoreUnboundVariables,
            final AtomicInteger vectorSize) {

        final BS[] a = new BS[leftSolutions.length];

        int n = 0; // The #of non-dropped source solutions.

        for (int i = 0; i < a.length; i++) {

            /*
             * Note: If this is a DISINCT FILTER, then we need to drop the
             * variables which are not being considered immediately. Those
             * variables MUST NOT participate in the computed hash code.
             */

            final IBindingSet bset = selectVars == null ? leftSolutions[i]
                    : leftSolutions[i].copy(selectVars);

            // Compute hash code from bindings on the join vars.
            int hashCode = ONE;
            try {

                hashCode = HTreeHashJoinUtility.hashCode(joinVars,
                        bset, ignoreUnboundVariables);
                
            } catch (JoinVariableNotBoundException ex) {

                if (!optional) {// Drop solution
                
                    if (log.isTraceEnabled())
                        log.trace(ex);
                    
                    continue;
                    
                }
                
            }
            
            a[n++] = new BS(hashCode, bset);
            
        }

        /*
         * Sort by the computed hash code. This not only orders the accesses
         * into the HTree but it also allows us to handle all source solutions
         * which have the same hash code with a single scan of the appropriate
         * collision bucket in the HTree.
         */
        Arrays.sort(a, 0, n);
        
        // Indicate the actual vector size to the caller via a side-effect.
        vectorSize.set(n);
        
        return a;
        
    }

    /**
     * Add to 2nd hash tree of all solutions which join.
     * <p>
     * Note: the hash key is based on the entire solution (not just the join
     * variables). The values are the full encoded {@link IBindingSet}.
     */
    void saveInJoinSet(final int joinSetHashCode, final byte[] val) {

        final HTree joinSet = this.getJoinSet();

        if (true) {
            
            /*
             * Do not insert if there is already an entry for that solution in
             * the join set.
             */

            final IKeyBuilder keyBuilder = joinSet.getIndexMetadata()
                    .getKeyBuilder();

            final byte[] key = keyBuilder.reset().append(joinSetHashCode)
                    .getKey();

            // visit all joinSet solutions having the same hash code
            final ITupleIterator<?> xitr = joinSet.lookupAll(key);

            while (xitr.hasNext()) {

                final ITuple<?> xt = xitr.next();

                final ByteArrayBuffer b = xt.getValueBuffer();

                if (0 == BytesUtil.compareBytesWithLenAndOffset(0/* aoff */,
                        val.length/* alen */, val/* a */, 0/* boff */,
                        b.limit()/* blen */, b.array())) {

                    return;

                }

            }
            
        }

        joinSet.insert(joinSetHashCode, val);

    }
    
    @Override
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer) {

        if (log.isInfoEnabled()) {
            final HTree htree = this.getRightSolutions();
            log.info("rightSolutions: #nnodes=" + htree.getNodeCount()
                    + ",#leaves=" + htree.getLeafCount() + ",#entries="
                    + htree.getEntryCount());
            
            final HTree joinSet = this.getJoinSet();
            log.info("joinSet: #nnodes=" + joinSet.getNodeCount() + ",#leaves="
                    + joinSet.getLeafCount() + ",#entries="
                    + joinSet.getEntryCount());
        }

        final HTree joinSet = getJoinSet();
        
        final IKeyBuilder keyBuilder = joinSet.getIndexMetadata()
                .getKeyBuilder();

        final BTree ivCache = this.ivCache.get();
        
        final BTree blobsCache = this.blobsCache.get();
        
        // Visit all source solutions.
        final ITupleIterator<?> sitr = getRightSolutions().rangeIterator();
        
        while(sitr.hasNext()) {
            
            final ITuple<?> t = sitr.next();
            
            IBindingSet rightSolution = decodeSolution(t);

            // The hash code is based on the entire solution for the joinSet.
            final int hashCode = rightSolution.hashCode();
            
            final byte[] key = keyBuilder.reset().append(hashCode).getKey();
            
            // Probe the join set for this source solution.
            final ITupleIterator<?> jitr = joinSet.lookupAll(key);

            boolean found = false;
            while (jitr.hasNext()) {

                // Note: Compare full solutions, not just the hash code!

                final ITuple<?> xt = jitr.next();

                final ByteArrayBuffer tb = t.getValueBuffer();

                final ByteArrayBuffer xb = xt.getValueBuffer();

                if (0 == BytesUtil.compareBytesWithLenAndOffset(0/* aoff */,
                        tb.limit()/* alen */, tb.array()/* a */, 0/* boff */,
                        xb.limit()/* blen */, xb.array())) {

                    found = true;

                    break;

                }

            }

            if (!found) {
                
                /*
                 * Since the source solution is not in the join set, output it
                 * as an optional solution.
                 */

                if (selectVars != null) {// && selectVars.length > 0) {
                
                    // Only output the projected variables.
                    rightSolution = rightSolution.copy(selectVars);
                    
                }

                resolveCachedValues(ivCache, blobsCache, rightSolution);
                
                outputBuffer.add(rightSolution);

            }

        }

    } // handleOptionals.

    @Override
    public void outputSolutions(final IBuffer<IBindingSet> out) {

        final HTree rightSolutions = getRightSolutions();

        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #nnodes=" + rightSolutions.getNodeCount()
                    + ",#leaves=" + rightSolutions.getLeafCount()
                    + ",#entries=" + rightSolutions.getEntryCount());
        }

        final BTree ivCache = this.ivCache.get();
        
        final BTree blobsCache = this.blobsCache.get();
        
        // source.
        final ITupleIterator<?> solutionsIterator = rightSolutions
                .rangeIterator();

        while (solutionsIterator.hasNext()) {

            final ITuple<?> t = solutionsIterator.next();

            IBindingSet bset = decodeSolution(t);

            if (selectVars != null) {

                // Drop variables which are not projected.
                bset = bset.copy(selectVars);

            }

            resolveCachedValues(ivCache, blobsCache, bset);
            
            out.add(bset);

        }

    }

    /**
     * Resolve any {@link IV}s in the solution for which there are cached
     * {@link BigdataValue}s to those values.
     * 
     * @param bset
     *            A solution having {@link IV}s which need to be reunited with
     *            their cached {@link BigdataValue}s.
     * 
     *            TODO If we vectored this operation it would substantially
     *            reduce its costs. We would have to collect up a bunch of
     *            solutions which needed resolution, then collect up the IVs
     *            which do not have cached values for variables which might have
     *            values in the ivCache. We would then sort the IVs and do a
     *            vectored resolution against the ivCache. Finally, the
     *            solutions could be output in a chunk with their resolved
     *            Values.
     */
    @SuppressWarnings("rawtypes")
    private void resolveCachedValues(final BTree ivCache,
            final BTree blobsCache, final IBindingSet bset) {

        if (ivCache.getEntryCount() == 0L && blobsCache.getEntryCount() == 0L) {
            // Nothing materialized.
            return;
        }
        
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();
        
        final IKeyBuilder keyBuilder = tupSer.getKeyBuilder();

        final Tuple ivCacheTuple = new Tuple(ivCache, IRangeQuery.KEYS
                | IRangeQuery.VALS);

        final Iterator<Map.Entry<IVariable, IConstant>> itr = bset.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            final IVariable<?> v = e.getKey();
            
            if (!ivCacheSchema.contains(v)) {
                // Nothing observed for that variable.
                continue;
            }
            
            final IV iv = (IV) e.getValue().get();

            if (iv.hasValue()) {
                // Already cached.
                continue;
            }

            keyBuilder.reset();

            if (iv instanceof BlobIV<?>) {

                final BlobIV<?> blobIV = (BlobIV<?>)iv; 
            
                // TODO factor out the ctor when we do the resolution of IVs.
                final byte[] val = new BlobsIndexHelper().lookup(blobsCache,
                        blobIV, keyBuilder);

                if (val == null) {

                    continue;

                }

                /*
                 * TODO Factor out the buffers used to do the de-serialization
                 * when we vector the resolution of IVs.
                 */
                final BigdataValue value = valueFactory.getValueSerializer()
                        .deserialize(val);

                iv.setValue(value);

            } else {

                keyBuilder.reset();

                IVUtility.encode(keyBuilder, iv);

                final byte[] key = keyBuilder.getKey();

                if (ivCache.lookup(key, ivCacheTuple) == null) {

                    continue;

                }

                final BigdataValue value = tupSer.deserialize(ivCacheTuple);

                iv.setValue(value);

            }
            
        }

    }

    @Override
    public void mergeJoin(final HTreeHashJoinUtility[] others,
            final IBuffer<IBindingSet> outputBuffer,
            final IConstraint[] constraints) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}
