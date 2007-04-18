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
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.lang.reflect.Constructor;
import java.util.UUID;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * This class implements a variant of a B+Tree in which all values are stored in
 * leaves, but the leaves are not connected with prior-next links. This
 * constraint arises from the requirement to support a copy-on-write policy.
 * </p>
 * <p>
 * Note: No mechanism is exposed for recovering a node or leaf of the tree other
 * than the root by its key. This is because the parent reference on the node
 * (or leaf) can only be set when it is read from the store in context by its
 * parent node.
 * </p>
 * <p>
 * Note: the leaves can not be stitched together with prior and next references
 * without forming cycles that make it impossible to write out the leaves of the
 * btree. This restriction arises because each time we write out a node or leaf
 * it is assigned a persistent identifier as an unavoidable artifact of
 * providing isolation for the object index.
 * </p>
 * <p>
 * Note: This implementation is NOT thread-safe. The index is intended for use
 * within a single-threaded context.
 * </p>
 * <p>
 * Note: This iterators exposed by this implementation do NOT support concurrent
 * structural modification. Concurrent inserts or removals of keys MAY produce
 * incoherent traversal whether or not they result in addition or removal of
 * nodes in the tree.
 * </p>
 * 
 * @todo create ring buffers to track the serialized size of the last 50 nodes
 *       and leaves so that we can estimate the serialized size of the total
 *       btree based on recent activity.  we could use a moving average and 
 *       persist it as part of the btree metadata.  this could be used when
 *       making a decision to evict a btree vs migrate it onto a new journal
 *       and whether to split or join index segments during a journal overflow
 *       event.
 * 
 * @todo Modify the values in the tree to be variable length byte[]s. This will
 *       get rid of the {@link IValueSerializer}. It will also speed up leaf
 *       de-serialization since we can defer object creation until a specific
 *       value is fetched. Consider introducing an {@link IValueBuffer} to store
 *       the values and compression techniques useful for data that may not be
 *       sorted (in contrast to the keys for those values, which will be
 *       sorted). <br>
 *       Note: This will have the side-effect of increasing the cost of
 *       materializing frequently used values when the btree is used as a local
 *       (in process) data structure. The reason is that the value will need to
 *       be de-serialized each time. That cost can be offset using a weak-value
 *       cache to minimize cost for recently used objects, which is how a btree
 *       would be applied to create a cannonicalizing mapping, e.g., for an
 *       object store. It may also be worth examining the "fast-btree" branch,
 *       which supports primitive data type keys and seeing if that branch is a
 *       good candidiate for an in-process btree. the api for that branch allows
 *       Object keys and Object values, and that might be worth keeping as its
 *       own implementation. (I am not sure if there is an opportunity to merge
 *       those implementations since that gets into indirection about the data
 *       type of the key again.)
 * 
 * @todo reduce the #of argments on the stack in the batch api by introducing an
 *       object that encapsulates the batch parameters. this will reduce stack
 *       depth while permitting concurrent readers on immutable btrees.
 * 
 * @todo indexOf, keyAt, valueAt need batch api compatibility (they use the old
 *       findChild, search, and autobox logic).
 * 
 * @todo keep the non-batch as well as the batch api or simplify to just the
 *       batch api?
 * 
 * @todo Modify to support "stealing" of immutable nodes by wrapping them with a
 *       thin class encapsulating the {parent, btree} references and refactor
 *       the design until it permits an isolated btree to reuse the in memory
 *       nodes and leaves of the base btree in order to minimize the resource
 *       and IO costs of having multiple concurrent transactions running on the
 *       same journal.
 * 
 * @todo drop the jdbm-based btree implementation in favor of this one and test
 *       out a GOM integration. this will also require an extser service /
 *       index. extser support could be handled using an extensible metadata
 *       record for the {@link BTree} or {@link IndexSegment}, at least for an
 *       embedded database scenario. http://xstream.codehaus.org/ is also an
 *       interesting serialization package with somewhat different goals (you do
 *       not have to write serializers, but it is doubtless less compact and
 *       does not have extensible versioning).
 * 
 * @todo Implement an "extser" index that does not use extser itself, but which
 *       could provide the basis for a database that does use extser. The index
 *       needs to map class names to entries. Those entries are a classId and
 *       set of {version : Serializer} entries.
 * 
 * @todo we could defer splits by redistributing keys to left/right siblings
 *       that are under capacity - this makes the tree a b*-tree. however, this
 *       is not critical since the journal is designed to be fully buffered and
 *       the index segments are read-only but it would reduce memory by reducing
 *       the #of nodes -- and we can expect that the siblings will be either
 *       resident or in the direct buffer for the journal
 * 
 * @todo consider using extser an option for serialization so that we can
 *       continually evolve the node and leaf formats. we will also need a node
 *       serializer that does NOT use extser in order to store the persistent
 *       extser mappings themselves, and perhaps for other things such as an
 *       index of the index ranges that are multiplexed on a given journal.
 *       finally, we will need to use extser to simplify reopening an index so
 *       that we can recover its key serializer, value serializer, and key
 *       comparator as well as various configuration values from its metadata
 *       record. that encapsulation will have to be layered over the basic btree
 *       class so that we can use a more parameterized btree instance to support
 *       extser itself. there will also need to be metadata maintained about the
 *       perfect index range segments so that we know how to decompress blocks,
 *       deserialize keys and values, and compare keys.
 * 
 * @todo model out the metadata index design to locate the components of an
 *       index key range. this will include the journal on which writes for the
 *       key range are multiplexed with other key ranges on either the same or
 *       other indices, any frozen snapshot of a journal that is being processed
 *       into index segment files, and those index segment files themselves. if
 *       a key range is always mapped (multiplexed) to a process on a host, then
 *       the historical journal snapshots and index key range files can be
 *       managed by the host rather than showing up in the metadata index
 *       directly.
 * 
 * @todo support column store style indices key := (key, column, timestamp),
 *       locality groups that partition the key space so that we can fully
 *       buffer parts of the index that matter, automated version history
 *       policies that expire old values based on either an external timestamp
 *       or write time on the server.
 * 
 * @todo support key range iterators that allow concurrent structural
 *       modification. structural mutations in a b+tree are relatively limited.
 *       When prior-next references are available, an iterator should be easily
 *       able to adjust for insertion and removal of keys.
 * 
 * @todo maintain prior-next references among leaves (and nodes?) in memory even
 *       if we are not able to write them onto the disk. when reading in a leaf,
 *       always set the prior/next reference iff the corresponding leaf is in
 *       memory - this is easily handled by checking the weak references on the
 *       parent node.
 * 
 * @todo evict subranges by touching the node on the way up so that a node that
 *       is evicted from the hard reference cache will span a subrange that can
 *       be evicted together. this will help to preserve locality on disk. with
 *       this approach leaves might be evicted independently, but also as part
 *       of a node subrange eviction.
 * 
 * @todo since forward scans are much more common, change the post-order
 *       iterator for commit processing to use a reverse traversal so that we
 *       can write the next leaf field whenever we evict a sequence of leaves as
 *       part of a sub-range commit (probably not much of an issue since the
 *       journal is normally fully buffered and the perfect index segments will
 *       not have this problem).
 * 
 * @todo Actually, I could save both prior and next references using a
 *       hand-over-hand chaining in which I pre-serialize the leaf and separate
 *       the allocation step from the write on the store. With just a small
 *       change to the leaf serialization format so that I can write in the
 *       prior and next fields at a known location (which could even be the end
 *       of the buffer), I would then be able to persistent the prior/next
 *       references. <br>
 *       The first step is to start maintaining those references. Also, consider
 *       that it may be useful to maintain them at the node as well as the leaf
 *       level.
 * 
 * @todo pre-fetch leaves for range scans? this really does require asynchronous
 *       IO, which is not available for many platforms (it is starting to show
 *       up in linux 2.6 kernals).
 * 
 * @todo Note that efficient support for large branching factors requires a more
 *       sophisticated approach to maintaining the key order within a node or
 *       leaf. E.g., using a red-black tree or adaptive packed memory array.
 *       However, we can use smaller branching factors for btrees in the journal
 *       and use a separate implementation for bulk generating and reading
 *       "perfect" read-only key range segments.
 * 
 * @todo derive a string index that uses patricia trees in the leaves per
 *       several published papers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTree extends AbstractBTree implements IIndex, IBatchBTree, ICommitter {
    
    /**
     * The default branching factor.
     */
    static public final int DEFAULT_BRANCHING_FACTOR = 32; //256
    
    /**
     * The minimum hard reference queue capacity is two(2) in order to avoid
     * cache evictions of the leaves participating in a split.
     */
    static public final int MINIMUM_LEAF_QUEUE_CAPACITY = 2;
    
    /**
     * The size of the hard reference queue used to defer leaf eviction.
     * 
     * @todo if the journal is fully buffered, then the only IO that we are
     *       talking about is serialization of the leaves onto the buffer with
     *       incremental writes through to disk and NO random reads (since the
     *       entire store is buffered in RAM, even though it writes through to
     *       disk).
     * 
     * @todo The leaf cache capacity is effectively multiplied by the branching
     *       factor so it makes sense that we would use a smaller leaf cache
     *       when the branching factor was larger. This is a good reason for
     *       moving the default for this parameter inside of the btree
     *       implementation.
     * 
     * @todo testing with a large leaf cache and a large branching factor means
     *       that you nearly never evict leaves
     */
    static public final int DEFAULT_HARD_REF_QUEUE_CAPACITY = 500;

    /**
     * The #of entries on the hard reference queue that will be scanned for a
     * match before a new reference is appended to the queue. This trades off
     * the cost of scanning entries on the queue, which is handled by the queue
     * itself, against the cost of queue churn. Note that queue eviction drives
     * IOs required to write the leaves on the store, but incremental writes
     * occurr iff the {@link AbstractNode#referenceCount} is zero and the leaf
     * is dirty.
     */
    static public final int DEFAULT_HARD_REF_QUEUE_SCAN = 20;
    
    public int getBranchingFactor() {
        
        return branchingFactor;
        
    }

    public int getHeight() {
        
        return height;
        
    }

    public int getNodeCount() {
        
        return nnodes;
        
    }

    public int getLeafCount() {
        
        return nleaves;
        
    }

    public int getEntryCount() {
        
        return nentries;
        
    }

    /**
     * The metadata record used to load the last state of the index that was
     * written by {@link #write()}. When an index is loaded this is set to the
     * metadata specified to the constructor. When a new index is created, this
     * is initially <code>null</code>.
     */
    protected BTreeMetadata metadata = null;

//    /**
//     * The root of the btree. This is initially a leaf until the leaf is split,
//     * at which point it is replaced by a node. The root is also replaced each
//     * time copy-on-write triggers a cascade of updates.
//     */
//    protected AbstractNode root;

    /**
     * The height of the btree. The height is the #of leaves minus one. A btree
     * with only a root leaf is said to have <code>height := 0</code>. Note
     * that the height only changes when we split the root node.
     */
    protected int height;

    /**
     * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
     * 
     * @todo this field as well as nleaves and nentries could be taken from the
     *       root node rather than being maintained directly. when the root is a
     *       leaf, then nnodes=0, nleaves=1, and nentries=root.nkeys
     */
    protected int nnodes;

    /**
     * The #of leaf nodes in the btree. This is one (1) for a new btree.
     */
    protected int nleaves;

    /**
     * The #of entries in the btree. This is zero (0) for a new btree.
     */
    protected int nentries;

    /**
     * Constructor for a new B+Tree with a default hard reference queue policy
     * and no record compression.
     * 
     * @param store
     *            The persistence store.
     * @param branchingFactor
     *            The branching factor.
     * @param indexUUID
     *            The unique identifier for the index. All B+Tree objects having
     *            data for the same scale-out index MUST have the same
     *            indexUUID. Otherwise a {@link UUID#randomUUID()} SHOULD be
     *            used.
     * @param valueSer
     *            Object that knows how to (de-)serialize the values in a
     *            {@link Leaf}.
     */
    public BTree(IRawStore store, int branchingFactor, UUID indexUUID, IValueSerializer valSer) {
    
        this(store, branchingFactor, indexUUID, new HardReferenceQueue<PO>(
                new DefaultEvictionListener(),
                BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                BTree.DEFAULT_HARD_REF_QUEUE_SCAN), valSer, null/* recordCompressor */);
        
    }
    
    /**
     * Constructor for a new B+Tree.
     * 
     * @param store
     *            The persistence store.
     * @param branchingFactor
     *            The branching factor.
     * @param indexUUID
     *            The unique identifier for the index. All B+Tree objects having
     *            data for the same scale-out index MUST have the same
     *            indexUUID. Otherwise a {@link UUID#randomUUID()} SHOULD be
     *            used.
     * @param headReferenceQueue
     *            The hard reference queue. The minimum capacity is 2 to avoid
     *            cache evictions of the leaves participating in a split. A
     *            reasonable capacity is specified by
     *            {@link #DEFAULT_HARD_REF_QUEUE_CAPACITY}.
     * @param valueSer
     *            Object that knows how to (de-)serialize the values in a
     *            {@link Leaf}.
     * @param recordCompressor
     *            Object that knows how to (de-)compress a serialized node or
     *            leaf (optional).
     * 
     * @todo change record compressor to an interface.
     */
    public BTree(
            IRawStore store,
            int branchingFactor,
            UUID indexUUID,
            HardReferenceQueue<PO> hardReferenceQueue,
            IValueSerializer valueSer,
            RecordCompressor recordCompressor )
    {

        super(store, 
                branchingFactor,
                0/* initialBufferCapacity will be estimated */,
                hardReferenceQueue,
                PackedAddressSerializer.INSTANCE, valueSer,
                NodeFactory.INSTANCE, //
                recordCompressor, //
                /*
                 * Note: there is less need to use checksum for stores that are
                 * not fully buffered since the data are always read from memory
                 * which we presume is already parity checked. While a checksum
                 * on a fully buffered store could detect an overwrite, the
                 * journal architecture makes that extremely unlikely and one
                 * has never been observed.
                 */
                !store.isFullyBuffered(),/* useChecksum */
                indexUUID
                );

        /*
         * Note: the mutable BTree has a limit here so that split() will always
         * succeed. That limit does not apply for an immutable btree.
         */
        assert hardReferenceQueue.capacity() >= MINIMUM_LEAF_QUEUE_CAPACITY;

        /*
         * Setup the initial root leaf.
         */
        newRootLeaf();
        
    }

    /**
     * Creates and sets new root {@link Leaf} on the B+Tree and (re)sets the
     * various counters to be consistent with that root.  This is used both
     * by the constructor for a new {@link BTree} and by {@link #removeAll()}.
     */
    private void newRootLeaf() {

        this.height = 0;

        this.nnodes = 0;
        
        this.nentries = 0;
        
        this.root = new Leaf(this);
        
        this.nleaves = 1; 

    }
    
    /**
     * Load an existing B+Tree from the store.
     * 
     * @param store
     *            The persistence store.
     * @param metadata
     *            The btree metadata record.
     * @param hardReferenceQueue
     *            The hard reference queue for {@link Leaf}s.
     * 
     * @see #load(IRawStore, long), which will re-load a {@link BTree} or
     *      derived class from the {@link Addr address} of its
     *      {@link BTreeMetadata metadata} record.
     * 
     * @see #newMetadata(), which must be overriden if you subclass
     *      {@link BTreeMetadata}
     */
    protected BTree(IRawStore store, BTreeMetadata metadata,
            HardReferenceQueue<PO> hardReferenceQueue) {

        super(store, metadata.getBranchingFactor(),
                0/* initialBufferCapacity will be estimated */,
                hardReferenceQueue, 
                PackedAddressSerializer.INSTANCE, 
                metadata.getValueSerializer(), NodeFactory.INSTANCE,
                metadata.getRecordCompressor(),//
                metadata.getUseChecksum(), // use checksum iff used on create.
                metadata.getIndexUUID()
                );
        
        // save a reference to the immutable metadata record.
        this.metadata = metadata;
        
        // initialize mutable fields from the immutable metadata record.
        this.height = metadata.getHeight();
        this.nnodes = metadata.getNodeCount();
        this.nleaves = metadata.getLeafCount();
        this.nentries = metadata.getEntryCount();
        
        /*
         * Read the root node of the btree.
         */
//        this.root = readNodeOrLeaf( metadata.addrRoot );
        reopen();

    }

    /**
     * Load from the store (required de-serialization constructor).
     * 
     * @param store
     *            The backing store.
     * 
     * @param metadata
     *            The metadata record.
     */
    public BTree(IRawStore store, BTreeMetadata metadata) {

        this(store, metadata, new HardReferenceQueue<PO>(
                new DefaultEvictionListener(), DEFAULT_HARD_REF_QUEUE_CAPACITY,
                DEFAULT_HARD_REF_QUEUE_SCAN));
        
    }
    
    
    /**
     * Uses {@link #handleCommit()} to flush any dirty nodes to the store and
     * update the metadata so we can clear the hard reference queue and release
     * the hard reference to the root node. {@link #reopen()} is responsible for
     * reloading the root node.
     */
    public void close() {

        /*
         * flush any dirty records, noting the address of the metadata record
         * so that we can reload the store.
         */
        handleCommit();
        
        /*
         * this will clear the hard reference cache, release the node serializer
         * buffers, and release the hard reference to the root node.
         */
        super.close();
        
    }
    
    /**
     * Reloads the root node iff it is <code>null</code> (indicating a closed
     * index).
     * 
     * @see #close()
     */
    protected void reopen() {

        if (root == null) {

            /*
             * reload the root node.
             */

            root = readNodeOrLeaf(metadata.getRootAddr());

        }

    }
    
    /**
     * Writes dirty nodes using a post-order traversal that first writes any
     * dirty leaves and then (recursively) their parent nodes. The parent nodes
     * are guarenteed to be dirty if there is a dirty child so the commit never
     * triggers copy-on-write. This is basically a checkpoint -- it is NOT an
     * atomic commit. The commit protocol is at the store level and involves the
     * use of alternating root blocks and (for transactions) validating and
     * merging down onto the corresponding global index.
     * 
     * @return The persistent identity of the metadata record for the btree. The
     *         btree can be reloaded from this metadata record. When used as
     *         part of an atomic commit protocol, the metadata record address
     *         must be written into a slot on the root block or a named root
     *         object.
     */
    public long write() {
        
        assert root != null; // i.e., isOpen().

        if (root.dirty) {

            writeNodeRecursive( root );
            
        }

        /*
         * Note: In order to give users the ability to derive and use subclasses
         * of the BTreeMetadata class we have to wait until the constructor
         * chain has finished initialization before writing out the metadata
         * record. Therefore, the BTree is responsible for writing out the
         * metadata record. This has a few implications: first, the
         * [addrMetadata] field on the metadata record is not itself persistent
         * since we do not have its value until we have written out the record;
         * second, the [addrMetadata] field is not [final] since we can not
         * assign its value until we are outside of the constructor.
         */
        final BTreeMetadata metadata = newMetadata();
        
        metadata.addrMetadata = metadata.write(store);
        
        this.metadata = metadata;
        
//        this.metadata = newMetadata();
        
        return metadata.addrMetadata;

    }
    
    /**
     * Returns the most recent metadata record for this btree.
     * 
     * @return The most recent metadata record for this btree and
     *         <code>null</code> if the btree has never written a metadata
     *         record on the store.
     */
    public BTreeMetadata getMetadata() {

        return metadata;
        
    }
    
    /**
     * Method returns the metadata record persisted by {@link #write()}.
     * <p>
     * Note: In order to persist additional metadata with the btree you MUST
     * override this method to return a subclass of {@link BTreeMetadata}.
     * 
     * @return A new metadata object that can be used to restore the btree.
     */
    protected BTreeMetadata newMetadata() {
        
        assert root != null; // i.e., isOpen().
        
        return new BTreeMetadata(this);
        
    }

    /**
     * Handle request for a commit by {@link #write()}ing dirty nodes and
     * leaves onto the store, writing a new metadata record, and returning the
     * address of that metadata record.
     * <p>
     * Note: In order to avoid needless writes the existing metadata record is
     * always returned if:
     * <ol>
     * <li> the metadata record is defined (it is not defined when a btree is
     * first created) -AND- </li>
     * <li> the root of the btree is NOT dirty and the persistent address of the
     * root of the btree is the same as the address record in the metadata
     * record -OR- </li>
     * <li> the root is <code>null</code>, indicating that the index is
     * closed (flushing the index to disk and updating the metadata record is
     * part of the close protocol so we know that the metadata address is
     * current in this case).</li>
     * </ol>
     * 
     * @return The {@link Addr address} of a metadata record from which the
     *         btree may be reloaded.
     */
    public long handleCommit() {

        if (metadata != null
                && (root == null || !root.dirty
                        && metadata.getRootAddr() == root.getIdentity())) {

            /*
             * There have not been any writes on this btree.
             */
            return metadata.addrMetadata;
            
        }
        
        /*
         * Flush the btree, write its metadata record, and return the address of
         * that metadata record.
         */
        return write();
        
    }
    
    /**
     * Remove all entries in the B+Tree.
     * <p>
     * This implementation simply replaces the root with a new root leaf and
     * resets the counters (height, #of nodes, #of leaves, etc). to be
     * consistent with that new root. If the btree is then made restart-safe by
     * the commit protocol of the backing store then the effect is as if all
     * entries had been deleted. Old nodes and leaves will be swept from the
     * store eventually when the journal overflows.
     * <p>
     * Note: This implementation simply replaces the root node with a new root
     * leaf and does not release storage for nodes and leaves in the B+Tree.
     * This is because the overall design for bigdata combines an append only
     * {@link IJournal} with overflow onto read-only {@link IndexSegment}s. Old
     * journals are simply deleted, thereby reclaiming the storage on the file
     * system.
     * <p>
     * Note: This implementation is overriden in the
     * <code>com.btree.isolation</code> package since isolation requires
     * writing explicit delete markers for each entry in the B+Tree.
     * <p>
     * Note: The {@link IIndexManager} defines methods for registering (adding)
     * and dropping indices. The {@link IIndexManager#dropIndex(String)} method
     * should be used to remove a scale-out partitioned index.
     */
    public void removeAll() {

        /*
         * Replace the root with a new root leaf.
         */
 
        newRootLeaf();
        
    }

    /**
     * Re-load the {@link BTree} or derived class from the store. The
     * {@link BTree} or derived class MUST provide a public constructor with the
     * following signature: <code>
     * 
     * <i>className</i>(IRawStore store, BTreeMetadata metadata)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * @param addr
     *            The address of the {@link BTreeMetadata} record for that
     *            class.
     * 
     * @return The {@link BTree} or derived class loaded from that
     *         {@link BTreeMetadata} record.
     * 
     * @see BTree#newMetadata(), which MUST be overloaded if you subclass
     *      {@link BTreeMetadata} in order to store extended metadata.
     */
    public static BTree load(IRawStore store, long addr) {
        
        BTreeMetadata metadata = BTreeMetadata.read(store, addr);
        
        try {
            
            Class cl = Class.forName(metadata.getClassName());
            
            Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class, BTreeMetadata.class });
            
            BTree btree = (BTree) ctor.newInstance(new Object[] { store,
                    metadata });
            
            return btree;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /**
     * Factory for mutable nodes and leaves used by the {@link NodeSerializer}.
     */
    protected static class NodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new NodeFactory();

        private NodeFactory() {
        }

        public ILeafData allocLeaf(IIndex btree, long addr,
                int branchingFactor, IKeyBuffer keys, Object[] values) {

            return new Leaf((BTree) btree, addr, branchingFactor, keys,
                    values);

        }

        public INodeData allocNode(IIndex btree, long addr,
                int branchingFactor, int nentries, IKeyBuffer keys,
                long[] childAddr, int[] childEntryCounts) {

            return new Node((BTree) btree, addr, branchingFactor, nentries,
                    keys, childAddr, childEntryCounts);

        }

    }

}
