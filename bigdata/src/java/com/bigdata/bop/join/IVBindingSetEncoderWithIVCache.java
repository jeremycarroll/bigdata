/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 15, 2012
 */

package com.bigdata.bop.join;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.Value;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IndexAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVBindingSetEncoder;
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

/**
 * A concrete implementation using scalable {@link BTree}s to store the mapping
 * from an {@link IV} to the cached RDF {@link Value}. This approach is useful
 * when you will be encoding a LOT of data and you need to get the cached RDF
 * {@link Value} objects off of the JVM heap.
 * <p>
 * Note: Two different {@link BTree} instances are used. One for {@link TermId}s
 * and another for {@link BlobIV}s. These indices use exactly the same schema as
 * the ID2TERM and BLOBS indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IVBindingSetEncoderWithIVCache extends IVBindingSetEncoder {

//    /**
//     * The backing {@link IRawStore}.
//     */
//    private final IRawStore store;
    
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
     */
    private final AtomicReference<BTree> blobsCache = new AtomicReference<BTree>();

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{namespace=" + namespace);
        if (ivCache.get() != null)
            sb.append(",ivCacheSize=" + getIVCacheSize());
        if (blobsCache.get() != null)
            sb.append(",blobCacheSize=" + getBlobsCacheSize());
        sb.append("}");
        return sb.toString();
    }
    
    private long getIVCacheSize() {

        final BTree ndx = ivCache.get();

        if (ndx != null) {

            return ndx.getEntryCount();

        }

        return 0L;
        
    }

    private long getBlobsCacheSize() {

        final BTree ndx = blobsCache.get();

        if (ndx != null) {

            return ndx.getEntryCount();

        }

        return 0L;
        
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
     * @param filter
     *            <code>true</code> iff this is in support of a DISTINCT filter.
     *            <p>
     *            Note: we do not maintain the {@link #ivCacheSchema} for a
     *            DISTINCT filter since the original solutions flow through the
     *            filter.
     */
    public IVBindingSetEncoderWithIVCache(final IRawStore store,
            final boolean filter, final PipelineOp op) {

        super(filter);

        if (!filter) {

            /*
             * Setup the IV => BigdataValue mapping. This captures any cached
             * BigdataValue references encountered on IVs. This map does not store
             * duplicate entries for the same IV.
             */

            namespace = ((String[]) op
                    .getRequiredProperty(Predicate.Annotations.RELATION_NAME))[0];

            valueFactory = BigdataValueFactoryImpl.getInstance(namespace);

            ivCache.set(BTree.create(store, getIVCacheIndexMetadata(op)));

            blobsCache.set(BTree.create(store, getBlobsCacheIndexMetadata(op)));

        } else {

            namespace = null;
            
            valueFactory = null;
            
        }

    }

    /**
     * Checkpoint the {@link BTree} instance(s) used to buffer the cached
     * {@link IV} to RDF {@link Value} mappings and then re-load the them in a
     * read-only mode from their checkpoint(s). This exposes a view of the
     * {@link BTree} instance(s) which is safe for concurrent readers.
     */
    public void saveSolutionSet() {

        checkpointBTree(ivCache);
        
        checkpointBTree(blobsCache);
        
    }
    
    private void checkpointBTree(final AtomicReference<BTree> ref) {

        final BTree tmp = ref.get();

        if (tmp != null) {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = tmp.writeCheckpoint2();

            final BTree readOnly = BTree.load(tmp.getStore(),
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // Get a read-only view of the HTree.
            if (!ref.compareAndSet(tmp/* expect */, readOnly)) {

                throw new IllegalStateException();

            }

        }

    }

    @Override
    public void clear() {

        BTree tmp2 = ivCache.getAndSet(null/* newValue */);

        if (tmp2 != null) {

            tmp2.close();

        }

        tmp2 = blobsCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }

        super.clear();
        
    }
    
    /**
     * Transfer any {@link IV} to {@link BigdataValue} mappings to the ivCache.
     * 
     * @param cache
     *            A JVM cache.
     */
    public void updateIVCache(final Map<IV<?, ?>, BigdataValue> cache) {
        
        updateIVCache(cache, ivCache.get(), blobsCache.get());
        
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
     * Resolve any {@link IV}s in the solution for which there are cached
     * {@link BigdataValue}s to those values.
     * 
     * @param bset
     *            A solution having {@link IV}s which need to be reunited with
     *            their cached {@link BigdataValue}s.
     */
    public void resolveCachedValues(final IBindingSet bset) {
        
        resolveCachedValues(ivCache.get(), blobsCache.get(), bset);
        
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

}
