/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on May 18, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.UUID;

import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ICounter;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.rawstore.IRawStore;

/**
 * An instance of this class is used to absorb writes on a {@link DataService}
 * for each index partition mapped onto that data service. The class extends
 * {@link UnisolatedBTree} to carry additional metadata for a specific index
 * partition (the partition identifier, left- and right-separator keys, etc).
 * <p>
 * Note: Having the partition metadata for mapped index partitions available
 * locally means that we do not need to do a network operation in order to
 * validate that a partition was mapped onto the {@link DataService} or to
 * validate that a key lies within the key range of the partition.
 * 
 * @todo Overflow handling: Since the partition metadata is stored locally in
 *       the btree metadata record, then we need to create a new btree each time
 *       we overflow the journal. For this purpose, it would be nice if we did
 *       not have to write out the empty root leaf as well. This means that on
 *       overflow of a journal with 1000 mapped index partitions, that we need
 *       to create 1000 btree metadata records just in case there is a write on
 *       any of those partitions - and to be able to recovered the global
 *       metadata index from the local state. Of couse, writing a 1000 records
 *       on the journal is insanely fast so this could be no problem at all.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write tests for restart safety of the partition metadata.
 */
public class UnisolatedBTreePartition extends UnisolatedBTree {

    private PartitionMetadataWithSeparatorKeys pmd;
    
    /**
     * @param store
     * @param branchingFactor
     * @param indexUUID
     * @param conflictResolver
     */
    public UnisolatedBTreePartition(IRawStore store, UUID indexUUID, Config config) {
        
        super(store, config.branchingFactor, indexUUID, null/*conflictResolver*/);
        
        pmd = config.pmd;
        
    }

    /**
     * @param store
     * @param metadata
     */
    public UnisolatedBTreePartition(IRawStore store, BTreeMetadata metadata) {

        super(store, metadata);
        
    }

    /**
     * Overriden to specify {@link PartitionedUnisolatedBTreeMetadata} as the
     * class for the metadata record.
     */
    protected BTreeMetadata newMetadata() {
        
        return new UnisolatedBTreePartitionMetadata(this);
        
    }
    
    /**
     * The local copy of the metadata for the index partition.
     */
    public PartitionMetadataWithSeparatorKeys getPartitionMetadata() {
        
        return pmd;
        
    }

    /**
     * Overriden to use counters within a namespace defined by the partition
     * identifier.
     */
    public ICounter getCounter() {
        
        return new PartitionedCounter(pmd.getPartitionId(), super.getCounter());
        
    }
    
    
    /**
     * Verify that the key lies within the partition.
     * 
     * @param key
     *            The key.
     * 
     * @exception RuntimeException
     *                if the key does not lie within the partition.
     */
    public void rangeCheck(byte[] key) {

        if(key==null) throw new IllegalArgumentException();

        final byte[] leftSeparatorKey = pmd.getLeftSeparatorKey();

        final byte[] rightSeparatorKey = pmd.getRightSeparatorKey();

        if (BytesUtil.compareBytes(key, leftSeparatorKey) < 0) {

            throw new RuntimeException("KeyBeforePartition");

        }

        if (rightSeparatorKey != null
                && BytesUtil.compareBytes(key, rightSeparatorKey) >= 0) {

            throw new RuntimeException("KeyAfterPartition");

        }
            
    }
    
    /**
     * Places the counter values into a namespace formed by the partition
     * identifier. The partition identifier is found in the high int32 word and
     * the counter value from the underlying {@link BTree} is found in the low
     * int32 word.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class PartitionedCounter implements ICounter {

        private final int partitionId;
        private final ICounter src;
        
        public PartitionedCounter(int partitionId, ICounter src) {
            
            if(src == null) throw new IllegalArgumentException();
            
            this.partitionId = partitionId;
            
            this.src = src;
            
        }
        
        public long get() {
            
            return src.get();
            
        }

        public long inc() {
            
            long tmp = src.inc();
            
            if(tmp>Integer.MAX_VALUE) {
                
                throw new RuntimeException("Counter overflow");
                
            }

            /*
             * Place the partition identifier into the high int32 word and place
             * the truncated counter value into the low int32 word.
             */
            return partitionId<<32 | (int)tmp;
            
        }
        
    }
    
    /**
     * Extends the metadata record to store the per-partition metadata for the
     * {@link UnisolatedBTreePartition}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class UnisolatedBTreePartitionMetadata extends
            UnisolatedBTreeMetadata {
        
        /**
         * 
         */
        private static final long serialVersionUID = 3484277534961805945L;
        
        private PartitionMetadataWithSeparatorKeys pmd;

        public PartitionMetadataWithSeparatorKeys getPartitionMetadata() {
            
            return pmd;
            
        }
        
        /**
         * De-serialization constructor.
         */
        public UnisolatedBTreePartitionMetadata() {
            
        }
        
        protected UnisolatedBTreePartitionMetadata(UnisolatedBTreePartition btree) {
            
            super(btree);
            
            /*
             * Note: this assumes that the partition metadata record is
             * immutable. If mutation operators are added then we need to clone
             * the partition metadata record here.
             */
            this.pmd = btree.pmd;
            
        }

        private static final transient short VERSION0 = 0x0;

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            super.readExternal(in);

            final short version = (short) ShortPacker.unpackShort(in);

            if (version != VERSION0) {

                throw new IOException("Unknown version: version=" + version);

            }

            /*
             * read additional metadata.
             */
            
            this.pmd = (PartitionMetadataWithSeparatorKeys)in.readObject();
            
            assert this.pmd != null;

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            super.writeExternal(out);

            ShortPacker.packShort(out, VERSION0);

            /*
             * write additional metadata.
             */

            assert this.pmd != null;

            out.writeObject(pmd);

        }

    }

    /**
     * Configuration options for the {@link UnisolatedBTreePartition}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo provide config objects for {@link BTree} and derived classes. the
     *       class of the btree implementation can be inferred from the config
     *       object so we can simplify the parameters on
     *       {@link IDataService#registerIndex(String, UUID, String, Object)}.
     */
    public static class Config implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 5758014871939321170L;

        public int branchingFactor = DEFAULT_BRANCHING_FACTOR;
        
//        public IConflictResolver conflictResolver;
        
        public PartitionMetadataWithSeparatorKeys pmd;
        
        public Config(PartitionMetadataWithSeparatorKeys pmd) {
            
            this.pmd = pmd;
            
        }
        
    }
    
}
