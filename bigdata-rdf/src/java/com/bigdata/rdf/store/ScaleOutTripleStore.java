/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on May 19, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.ITx;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;

/**
 * Implementation of an {@link ITripleStore} as a client of a
 * {@link BigdataFederation}. The implementation supports a scale-out
 * architecture in which each index may have one or more partitions. Index
 * partitions are multiplexed onto {@link IDataService}s.
 * <p>
 * The client uses unisolated writes against the lexicon (terms and ids indices)
 * and the statement indices. The index writes are automatically broken down
 * into one split per index partition. While each unisolated write on an index
 * partition is ACID, the indices are fully consistent iff the total operation
 * is successfull. For the lexicon, this means that the write on the terms and
 * the ids index must both succeed. For the statement indices, this means that
 * the write on each access path must succeed. If a client fails while adding
 * terms, then it is possible for the ids index to be incomplete with respect to
 * the terms index (i.e., terms are mapped into the lexicon and term identifiers
 * are assigned but the reverse lookup by term identifier will not discover the
 * term). Likewise, if a client fails while adding statements, then it is
 * possible for some of the access paths to be incomplete with respect to the
 * other access paths (i.e., some statements are not present in some access
 * paths).
 * <p>
 * Two additional mechanisms are used in order to guarentee reads from only
 * fully consistent data. First, clients providing query answering should read
 * from a database state that is known to be consistent (by using a read-only
 * transaction whose start time is the globally agreed upon commit time for that
 * database state). Second, if a client operation fails then it must be retried.
 * Such fail-safe retry semantics are available when data load operations are
 * executed as part of a map-reduce job.
 * <p>
 * 
 * @todo test with indices split into more than one partition, preferrably using
 *       dynamic splits.
 * 
 * @todo provide a mechanism to make document loading robust to client failure.
 *       When loads are unisolated, a client failure can result in the
 *       statements being loaded into only a subset of the statement indices.
 *       robust load would require a means for undo or redo of failed loads. a
 *       loaded based on map/reduce would naturally provide a robust mechanism
 *       using a redo model.
 * 
 * @todo Tune up inference for remote data services.
 * 
 * @todo Write a distributed join for inference and high-level query.
 * 
 * @todo tune up SPARQL query (modified LUBM).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ScaleOutTripleStore extends AbstractTripleStore {

    private final IBigdataFederation fed;
    
    /**
     * 
     */
    public ScaleOutTripleStore(IBigdataFederation fed, Properties properties) {

        super( properties );

        if (fed == null)
            throw new IllegalArgumentException();

        // @todo throw ex if client not connected to the federation.
        
        this.fed = fed;

        /*
         * Conditionally register the necessary indices.
         * 
         * @todo right now they are created when the federation is created.
         */
//        registerIndices();
        
    }

    /**
     * Register the indices.
     * 
     * @todo default allocation of the terms, statements, and justifications
     *       index (the latter iff justifications are configured). The default
     *       allocation scheme should be based on expectations of data volume
     *       written or read, the benefits of locality for the indices, and the
     *       concurrency of read or write operations on those indices.
     * 
     * @todo handle exception if the index already exists.
     * 
     * @todo you should not be able to turn off the lexicon for the scale-out
     *       triple store (or for the local triple store). That option only
     *       makes sense for the {@link TempTripleStore}.
     * 
     * @todo registration of indices should be atomic. this can be achieved
     *       using a procedure that runs once it has a lock on the resource
     *       corresponding to each required scale-out index. At that point the
     *       indices either will or will not exist and we can create them or
     *       accept their pre-existence atomically.
     */
    final public void registerIndices() {

        final IBigdataClient client = fed.getClient();
        
        /*
         * Note: Do not use isolation (only deletion markers are required).
         */
        
        final IndexMetadata idTermMetadata;
        {
            IndexMetadata md = new IndexMetadata(name_idTerm, UUID.randomUUID());
            
            md.setDeleteMarkers(true);
            
            idTermMetadata = md;
        }
        
        final IndexMetadata termIdMetadata;
        {
            IndexMetadata md = new IndexMetadata(name_termId, UUID.randomUUID());
            
            md.setDeleteMarkers(true);
            
            termIdMetadata = md;
        }

        final IndexMetadata justMetadata;
        {
            IndexMetadata md = new IndexMetadata(name_just, UUID.randomUUID());
            
            md.setDeleteMarkers(true);
            
            justMetadata = md;
        }

        // all known data service UUIDs.
        final UUID[] uuids = client.getDataServiceUUIDs(0);
    
        if (true && uuids.length == 2 && lexicon && !oneAccessPath) {

            /*
             * Special case for (2) data services attempts to balance the write
             * volume and concurrent write load for the indices.
             * 
             * dataService0: terms, spo
             * 
             * dataService1: ids, pos, osp, just (if used)
             * 
             * @todo This appears to slow things down slightly when loading
             * Thesaurus.owl. Try again with a concurrent load scenario and see
             * what interaction may be occurring with group commit. Also look at
             * the effect of check pointing indices (rather than doing a commit)
             * and of either check pointing nor committing groups (a special
             * procedure could be used to do a commit) in order to simulate the
             * best case scenario for continuous index load. (Both check point
             * and commit may help to keep down GC since they will limit the
             * life span of a mutable btree node, but they will mean more IO
             * unless we retain nodes on a read retention queue for the index
             * and in any case it will mean more conversion of immutable nodes
             * back to mutable nodes.)
             */

            log.warn("Special case allocation for two data services");
            
            fed.registerIndex(termIdMetadata,
                    new byte[][] { new byte[] {} }, new UUID[] { uuids[0] });
            
            fed.registerIndex(idTermMetadata,
                    new byte[][] { new byte[] {} }, new UUID[] { uuids[1] });
            
            /*
             * @todo could pre-partition based on the expected #of statements
             * for the store. If we want on the order of 20M statements per
             * partition and we expect at least 2B statements then we can
             * compute the #of required partitions. Since this is static
             * partitioning it will not be exact. This means that you can have
             * more statements in some partitions than in others - and this will
             * vary across the different access paths. It also means that the
             * last partition will absorb all statements beyond the expected
             * maximum.
             * 
             * The separator keys would be formed from the term identifiers that
             * would be assigned as [id:NULL:NULL]. We can use the same
             * separator keys for each of the statement indices.
             * 
             * Note: The term identifiers will be strictly incremental up to ~30
             * bits per index partition for the term:ids index (the index that
             * assigns the term identifiers). If there are multiple partitions
             * of the terms:ids index then the index partition identifier will
             * be non-zero after the first terms:ids index partition and the
             * logic to compute the ids for forming the statement index
             * separator keys would have to be changed.
             */
            
            fed.registerIndex(newStatementIndexMetadata(name_spo),
                    new byte[][] { new byte[] {} }, new UUID[] { uuids[0] });

            fed.registerIndex(newStatementIndexMetadata(name_pos),
                    new byte[][] { new byte[] {} }, new UUID[] { uuids[1] });

            fed.registerIndex(newStatementIndexMetadata(name_osp),
                    new byte[][] { new byte[] {} }, new UUID[] { uuids[1] });
            
            if(justify) {
                /*
                 * @todo review this decision when tuning the scale-out store
                 * for inference.  also, consider the use of bloom filters for
                 * inference since there appears to be a large number of queries
                 * resulting in small result sets (0 to 5 statements).
                 */
                fed.registerIndex(justMetadata, new byte[][] { new byte[] {} },
                        new UUID[] { uuids[1] });
            }

        } else {

            /*
             * Allocation of index partitions to data services is governed by
             * the metadata service.
             */

            if (lexicon) {

                fed.registerIndex(termIdMetadata);

                fed.registerIndex(idTermMetadata);

            }

            fed.registerIndex(newStatementIndexMetadata(name_spo));

            if(!oneAccessPath) {

                fed.registerIndex(newStatementIndexMetadata(name_pos));

                fed.registerIndex(newStatementIndexMetadata(name_osp));

            }

            if (justify) {

                fed.registerIndex(justMetadata );

            }

        }

    }
    
    private IndexMetadata newStatementIndexMetadata(String name) {
        
        IndexMetadata md = new IndexMetadata(name,UUID.randomUUID());
        
        md.setDeleteMarkers(true);

        // @todo enable key and value compression (API alignment required).
        
//        md.setKeySerializer(new FastRDFKeyCompression(N));
//
//        md.setValueSerializer(new FastRDFValueCompression());
        
        return md;
        
    }
    
    /**
     * @todo this must be an atomic drop/add or concurrent clients will not have
     *       a coherent view of the database during a {@link #clear()}. That
     *       could be achieved using a procedure that runs on the metadata
     *       service and which handles the drop/add while holding a lock on the
     *       resources corresponding to the indices to be dropped/added.
     */
    final public void clear() {

        if(true) {
            
            /*
             * FIXME we need to drop the indices from the federation!
             * 
             * Right now the logic has not been implemented to drop the mdi and
             * partitions for a scale-out index!
             */

            log.warn("request ignored!");
            
            return;
        
        }

        if (lexicon) {
         
            fed.dropIndex(name_idTerm); ids = null;
            
            fed.dropIndex(name_termId); terms = null;
        
        }
        
        if(oneAccessPath) {
            
            fed.dropIndex(name_spo); spo = null;
            
        } else {
            
            fed.dropIndex(name_spo); spo = null;
            
            fed.dropIndex(name_pos); pos = null;
            
            fed.dropIndex(name_osp); osp = null;
            
        }
    
        if(justify) {

            fed.dropIndex(name_just); just = null;
            
        }
        
        registerIndices();
        
    }
    
    /**
     * The terms index.
     */
    private ClientIndexView terms;

    /**
     * The ids index.
     */
    private ClientIndexView ids;

    /**
     * The statement indices for a triple store.
     */
    private ClientIndexView spo, pos, osp;

    private ClientIndexView just;

    final public IIndex getTermIdIndex() {

        if (terms == null) {

            terms = (ClientIndexView) fed.getIndex(
                    ITx.UNISOLATED, name_termId);

        }

        return terms;

    }

    final public IIndex getIdTermIndex() {

        if (ids == null) {

            ids = (ClientIndexView) fed.getIndex(ITx.UNISOLATED,
                    name_idTerm);

        }

        return ids;

    }

    final public IIndex getSPOIndex() {

        if (spo == null) {

            spo = (ClientIndexView) fed.getIndex(ITx.UNISOLATED,
                    name_spo);

        }

        return spo;

    }

    final public IIndex getPOSIndex() {

        if (pos == null) {

            pos = (ClientIndexView) fed.getIndex(ITx.UNISOLATED,
                    name_pos);

        }

        return pos;

    }

    final public IIndex getOSPIndex() {

        if (osp == null) {

            osp = (ClientIndexView) fed.getIndex(ITx.UNISOLATED,
                    name_osp);

        }

        return osp;

    }

    final public IIndex getJustificationIndex() {

        if (just == null) {

            just = (ClientIndexView) fed.getIndex(
                    ITx.UNISOLATED, name_just);

        }

        return just;
        
    }

    /*
     * terms index.
     */
    
// final public _Value getTerm(long id) {
//
// byte[] data = (byte[])getIdTermIndex().lookup(keyBuilder.id2key(id));
//
//        if (data == null)
//            return null;
//
//        return _Value.deserialize(data);
//
//    }
//
//    final public long getTermId(Value value) {
//
//        if(value==null) return IRawTripleStore.NULL;
//        
//        _Value val = (_Value) OptimizedValueFactory.INSTANCE
//                .toNativeValue(value);
//        
//        if( val.termId != IRawTripleStore.NULL ) return val.termId; 
//
//        Object tmp = getTermIdIndex().lookup(keyBuilder.value2Key(value));
//        
//        if (tmp == null)
//            return IRawTripleStore.NULL;
//
//        try {
//
//            val.termId = new DataInputBuffer((byte[]) tmp).unpackLong();
//
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//        return val.termId;
//
//    }
    
    /** TODO Auto-generated method stub */
    public IIndex getFullTextIndex() {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds reporting by data service to the usage summary.
     */
    public String usage(){
        
        StringBuilder sb = new StringBuilder( super.usage() );
        
        sb.append("\nsummary by dataService::\n");
        
        IBigdataClient client = fed.getClient();
        
        UUID[] dataServiceIds = client.getDataServiceUUIDs(0);
        
        for(int i=0; i<dataServiceIds.length; i++) {
            
            UUID serviceId = dataServiceIds[ i ];
            
            IDataService dataService = client.getDataService(serviceId);
            
            sb.append("\n");
            
            try {
            
                sb.append( dataService.getStatistics() );
                
            } catch (IOException e) {
                
                sb.append( "Could not get statistics for data service: uuid="+serviceId);
                
            }
            
        }
        
        return sb.toString();
        
    }

    /**
     * NOP since the client uses unisolated writes which auto-commit.
     */
    final public void commit() {
        
        if(INFO) log.info(usage());
        
    }
    
    /**
     * NOP since the client uses unisolated writes which auto-commit.
     * 
     * @todo verify that no write state is maintained by the
     *       {@link ClientIndexView} and that we therefore do NOT need to
     *       discard those views on abort.
     */
    final public void abort() {
        
    }
    
    /**
     * The federation is considered stable regardless of whether the federation
     * is on stable storage since clients only disconnect when they use
     * {@link #close()}.
     */
    final public boolean isStable() {
        
        return true;
        
    }
    
    final public boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * Disconnects from the {@link IBigdataFederation}.
     */
    final public void close() {
        
        fed.disconnect();
        
        super.close();
        
    }

    /**
     * Drops the indices used by the {@link ScaleOutTripleStore} and disconnects
     * from the {@link IBigdataFederation}.
     */
    final public void closeAndDelete() {
        
        clear();
        
        fed.disconnect();
        
        super.closeAndDelete();
        
    }

//    /**
//     * @todo this is temporarily overriden in order to experiment with buffer
//     *       capacity vs data transfer size for batch operations vs data
//     *       compaction techniques for client-service RPC vs breaking down
//     *       within index partition operations to no more than n megabytes per
//     *       operation.
//     */
//    protected int getDataLoadBufferCapacity() {
//        
//        return 100000;
//        
//    }

    /**
     * This store is safe for concurrent operations (but it only supports
     * read operations).
     */
    public boolean isConcurrent() {

        return true;
        
    }
    
}
