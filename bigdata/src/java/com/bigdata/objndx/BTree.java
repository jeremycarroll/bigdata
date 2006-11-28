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
package com.bigdata.objndx;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.Bytes;
import com.bigdata.journal.ContiguousSlotAllocation;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;

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
 * FIXME add counters, nano timers, and track storage used by the object index.
 * The goal is to know how much of the time of the server is consumed by the
 * object index, what percentage of the store is dedicated to the object index,
 * how expensive it is to do some scan-based operations (merged down, delete of
 * transactional isolated persistent index), and evaluate the buffer strategy by
 * comparing accesses with IOs.
 * 
 * @todo refactor to support long keys (vs int).
 * 
 * @todo refactor to support generic key types.
 * 
 * @todo refactor object index implementation into two variants providing
 *       isolation. In one the objects are referenced but exist outside of the
 *       index. This is the current design. In the other, then objects are
 *       stored as part of the values in the index. This is a clustered object
 *       index design and is identicaly to the design where non-int64 keys are
 *       used for the index. The latter design requires that we replace the
 *       currentSlots reference with the current version data. The advantage of
 *       this design is two fold. First, the index may be split into multiple
 *       segments and therefore scales out. Second, objects with key locality
 *       have retrieval locality as well. The former design has an advantage iff
 *       the objects are migrated to a read-optimized segment in which the int64
 *       identifier directly addresses the object in a slot on a page. Making
 *       clustering work effectively with the int64 object index by reference
 *       design probably requires mechanisms for assigning persistent
 *       identifiers based on expected locality of reference, e.g., late
 *       assignment of identifiers to persistence capable transient objects or
 *       an explicit notion of clustering rules or "local" objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTree implements IBTree {
    
    /**
     * Log for btree opeations.
     */
    public static final Logger log = Logger.getLogger(BTree.class);
    
    /**
     * Log for {@link BTree#dump(PrintStream)} and friends. 
     */
    public static final Logger dumpLog = Logger.getLogger(BTree.class.getName()+"#dump");
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG.toInt();

    /**
     * The minimum allowed branching factor (3).
     */
    static public final int MIN_BRANCHING_FACTOR = 3;
    
    /**
     * The minimum hard reference queue capacity is two(2) in order to avoid
     * cache evictions of the leaves participating in a split.
     */
    static public final int DEFAULT_LEAF_QUEUE_CAPACITY = 2;
    
    /**
     * The size of the hard reference queue used to defer leaf eviction.
     */
    static public final int DEFAULT_LEAF_CACHE_CAPACITY = 1000;

    /**
     * The #of entries on the hard reference queue that will be scanned for a
     * match before a new reference is appended to the queue. This trades off
     * the cost of scanning entries on the queue, which is handled by the queue
     * itself, against the cost of queue churn. Note that queue eviction drives
     * IOs required to write the leaves on the store, but incremental writes
     * occurr iff the {@link AbstractNode#referenceCount} is zero and the leaf
     * is dirty.
     */
    static public final int DEFAULT_LEAF_QUEUE_SCAN = 10;
    
    /**
     * The persistence store.
     */
    final protected IRawStore store;

    /**
     * The branching factor for the btree.
     */
    protected int branchingFactor;

    /**
     * Computes the split index when splitting a non-leaf {@link Node}.
     */
    final public INodeSplitRule nodeSplitRule;

    /**
     * Computes the split index when splitting a non-leaf {@link Node}.
     */
    final public ILeafSplitRule leafSplitRule;
    
    public IRawStore getStore() {
        
        return store;
        
    }
    
    public int getBrachingFactor() {
        
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

    public int size() {
        
        return nentries;
        
    }

    public NodeSerializer getNodeSerializer() {
        
        return nodeSer;
        
    }
    
    /**
     * A hard reference hash map for nodes in the btree is used to ensure that
     * nodes remain wired into memory. Dirty nodes are written to disk during
     * commit using a pre-order traversal that first writes any dirty leaves and
     * then (recursively) their parent nodes.
     * 
     * @todo Make sure that nodes are eventually removed from this set. There
     *       are two ways to make that happen. One is to just use a ring buffer
     *       with a large capacity. This will serve a bit like an MRU. The other
     *       is to remove nodes from this set explicitly on certain conditions.
     *       For example, when a copy is made of an immutable node the immutable
     *       node might be removed from this set. Also, if we support
     *       incremental writes of nodes, then newly written nodes could be
     *       removed from this set as well - they would remain accessible via
     *       {@link Node#childRefs} for their parent as long as the node
     *       remained on the hard reference queue. I.e., we could just use
     *       another hard reference queue for nodes in addition to the one that
     *       we have for leaves. The thing is that we may want to defer all node
     *       writes until the commit.
     * 
     * @todo replace with the use of a hard reference queue and incremental
     *       eviction of nodes.
     * 
     * @see AbstractNode#AbstractNode(AbstractNode)
     */
    final Set<Node> nodes = new HashSet<Node>();

    /**
     * Leaves are added to a hard reference queue when they are created or read
     * from the store. On eviction from the queue the leaf is serialized by
     * {@link #listener} against the {@link #store}. Once the leaf is no longer
     * strongly reachable its weak references may be cleared by the VM. Note
     * that leaves are evicted as new leaves are added to the hard reference
     * queue. This occurs in two situations: (1) when a new leaf is created
     * during a split of an existing leaf; and (2) when a leaf is read in from
     * the store. The minimum capacity for the hard reference queue is two (2)
     * so that a split may occur without forcing eviction of either leaf in the
     * split. Incremental writes basically make it impossible for the commit IO
     * to get "too large" where too large is defined by the size of the hard
     * reference cache.
     * 
     * @todo Only leaves are incrementally flushed, but it would be easy enough
     *       to also incrementally flush nodes using
     *       {@link #writeNodeRecursive(AbstractNode)}.
     */
    final HardReferenceQueue<PO> leafQueue;

    /**
     * Touch the node or leaf, causing its {@link AbstractNode#referenceCount}
     * to be incremented and the node or leaf to be appended to the appropriate
     * {@link HardReferenceQueue}. If the queue is full, then this will cause a
     * reference to be evicted from the queue. If the reference counter for the
     * evicted node or leaf is zero, then the node or leaf will be written onto
     * the store and made immutable. A subsequent attempt to modify the node or
     * leaf will force copy-on-write for that node or leaf.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @todo Touch nodes on read and write as well and write tests for
     *       incremental node eviction.
     * 
     * @todo Refactor to use a separate hard reference queue for nodes. This
     *       will allow incremental writes of parts of the tree that are not
     *       being accessed.
     */
    protected void touch(AbstractNode node) {

        assert node != null;

        node.referenceCount++;
  
        assert node.isLeaf();
        
//        if (node.isLeaf()) {

            leafQueue.append(node);

//        } else {
//
//            leafQueue.append(node);
//            
//        }

    }
    
    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     */
    final NodeSerializer nodeSer;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree. This
     * is pre-allocated to the maximum size of any node or leaf and the single
     * buffer is then reused every time we read or write a node or a leaf.
     */
    final ByteBuffer buf;
    
    /**
     * The root of the btree. This is initially a leaf until the leaf is
     * split, at which point it is replaced by a node. The root is also
     * replaced each time copy-on-write triggers a cascade of updates.
     */
    AbstractNode root;

    /**
     * The height of the btree. The height is the #of leaves minus one. A
     * btree with only a root leaf is said to have <code>height := 0</code>.
     * Note that the height only changes when we split the root node.
     */
    int height;

    /**
     * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
     */
    int nnodes;

    /**
     * The #of leaf nodes in the btree.  This is one (1) for a new btree.
     */
    int nleaves;

    /**
     * The #of entries in the btree.  This is zero (0) for a new btree.
     */
    int nentries;

    public AbstractNode getRoot() {

        return root;

    }

    /**
     * Constructor for a new btree.
     * 
     * @param store
     *            The persistence store.
     * @param branchingFactor
     *            The branching factor.
     * @param headReferenceQueueCapacity
     *            The capacity of the hard reference queue (minimum of 2 to
     *            avoid cache evictions of the leaves participating in a split).
     * @param valueSer
     *            Object that knows how to (de-)serializes the values in a
     *            {@link Leaf}.
     */
    public BTree(IRawStore store, int branchingFactor,
            HardReferenceQueue<PO> hardReferenceQueue, IValueSerializer valueSer) {

        assert store != null;

        assert branchingFactor >= MIN_BRANCHING_FACTOR;

        assert hardReferenceQueue.capacity() >= DEFAULT_LEAF_QUEUE_CAPACITY;
        
        assert valueSer != null;
        
        this.store = store;

        this.branchingFactor = branchingFactor;

        this.leafQueue = hardReferenceQueue;

        // @todo debug and substitute the FastLeafSplitRule.
        this.nodeSplitRule = new SlowNodeSplitRule(branchingFactor-1);
        this.leafSplitRule = new SlowLeafSplitRule(branchingFactor);

        this.nodeSer = new NodeSerializer(valueSer);

        int maxNodeOrLeafSize = Math.max(
                // max size for a leaf.
                nodeSer.getSize(true, branchingFactor),
                // max size for a node.
                nodeSer.getSize(false, branchingFactor - 1));
        
        log.info("maxNodeOrLeafSize="+maxNodeOrLeafSize);
        
        this.buf = ByteBuffer.allocate(maxNodeOrLeafSize);

        this.height = 0;

        this.nnodes = nleaves = nentries = 0;
        
        this.root = new Leaf(this);
        
    }

    /**
     * Constructor for an existing btree.
     * 
     * @param store
     *            The persistence store.
     * @param metadataId
     *            The persistent identifier of btree metadata.
     * @param leafQueue
     *            The hard reference queue for {@link Leaf}s.
     * @param valueSer
     *            Object that knows how to (de-)serialize the values in a
     *            {@link Leaf}.
     */
    public BTree(IRawStore store, long metadataId,
            HardReferenceQueue<PO> leafQueue, IValueSerializer valueSer) {

        assert store != null;

        assert leafQueue != null;
        
        assert valueSer != null;
        
        assert height >= 0;

        assert nnodes >= 0;

        assert nleaves >= 0;

        assert nentries >= 0;

        this.store = store;

        this.leafQueue = leafQueue;
        
        /*
         * read the btree metadata record. this tells us the branchingFactor and
         * the root node identifier, both of which we need below to initialize
         * the btree.
         */
        final long rootId = read(metadataId);

        // @todo debug and substitute the FastLeafSplitRule.
        this.nodeSplitRule = new SlowNodeSplitRule(branchingFactor-1);
        this.leafSplitRule = new SlowLeafSplitRule(branchingFactor);

        this.nodeSer = new NodeSerializer(valueSer);
        
        int maxNodeOrLeafSize = Math.max(
                // max size for a leaf.
                nodeSer.getSize(true, branchingFactor),
                // max size for a node.
                nodeSer.getSize(false, branchingFactor - 1));

        log.info("maxNodeOrLeafSize="+maxNodeOrLeafSize);
        
        this.buf = ByteBuffer.allocate(maxNodeOrLeafSize);

        /*
         * Read the root node of the btree.
         * 
         * Note: We could optionally run a variant of the post-order
         * iterator to suck in the entire node structure of the btree. If we
         * do nothing, then the nodes will be read in incrementally on
         * demand. Since we always place non-leaf nodes into a hard
         * reference cache, tree operations will speed up over time until
         * the entire non-leaf node structure is loaded.
         */
        this.root = readNodeOrLeaf( rootId );

    }

    /**
     * Write a dirty node and its children using a post-order traversal that
     * first writes any dirty leaves and then (recursively) their parent nodes.
     * The parent nodes are guarenteed to be dirty if there is a dirty child so
     * this never triggers copy-on-write. This is used as part of the commit
     * protocol where it is invoked with the root of the tree, but it may also
     * be used to incrementally flush dirty non-root {@link Node}s.
     * 
     * @param root
     *            The root of the hierarchy of nodes to be written. The node
     *            MUST be dirty. The node this does NOT have to be the root of
     *            the tree and it does NOT have to be a {@link Node}.
     */
    protected void writeNodeRecursive( AbstractNode root ) {

        assert root != null;
        
        assert root.isDirty();
        
        assert ! root.isDeleted();

        assert ! root.isPersistent();
        
        /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway.  If we were to evict everything from the
         * hard reference queue before a commit then the counters would be zero
         * but the queue would no longer be holding our nodes and leaves and
         * they would be GC'd soon as since they would no longer be strongly
         * reachable.
         */
        assert root.referenceCount >= 0;
        
        // #of dirty nodes written (nodes or leaves)
        int ndirty = 0;

        // #of dirty leaves written.
        int nleaves = 0;

        /*
         * Post-order traversal of children and this node itself.  Dirty
         * nodes get written onto the store.
         * 
         * Note: This iterator only visits dirty nodes.
         */
        Iterator itr = root.postOrderIterator(true);

        while (itr.hasNext()) {

            AbstractNode node = (AbstractNode) itr.next();

            assert node.isDirty();

            if (node != this.root) {

                /*
                 * The parent MUST be defined unless this is the root node.
                 */

                assert node.parent != null;
                assert node.parent.get() != null;

            }

            // write the dirty node on the store.
            writeNodeOrLeaf(node);

            ndirty++;

            if (node instanceof Leaf)
                nleaves++;

        }

        log.info("write: " + ndirty + " dirty nodes (" + nleaves
                + " leaves), rootId=" + root.getIdentity());
        
    }
    
    /**
     * Writes the node on the store. The node MUST be dirty. If the node has
     * a parent, then the parent is notified of the persistent identity
     * assigned to the node by the store.
     * 
     * @return The persistent identity assigned by the store.
     */
    protected long writeNodeOrLeaf( AbstractNode node ) {

        assert node != null;
        assert node.btree == this;
        assert node.isDirty();
        assert !node.isDeleted();
        assert !node.isPersistent();

        /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway.  If we were to evict everything from the
         * hard reference queue before a commit then the counters would be zero
         * but the queue would no longer be holding our nodes and leaves and
         * they would be GC'd soon as since they would no longer be strongly
         * reachable.
         */
        assert node.referenceCount >= 0;

        /*
         * Note: The parent should be defined unless this is the root node.
         * 
         * Note: A parent CAN NOT be serialized before all of its children have
         * persistent identity since it needs to write the identity of each
         * child in its serialization record.
         */
        Node parent = node.getParent();

        if (parent == null) {
            
            assert node == root;

        } else {

            // parent must be dirty if child is dirty.
            assert parent.isDirty();

            // parent must not be persistent if it is dirty.
            assert !parent.isPersistent();
            
        }
        
        /*
         * Serialize the node or leaf onto a shared buffer.
         */
        
        buf.clear();
        
        if( node.isLeaf() ) {
        
            nodeSer.putLeaf(buf, (Leaf)node);

        } else {

            nodeSer.putNode(buf, (Node) node);
            
        }
        
        /*
         * Flip the buffer and write the serialized node or leaf onto the store.
         */
        
        buf.flip();
        
        final long id = store.write(buf).toLong();

        /*
         * The node or leaf now has a persistent identity and is marked as
         * clean. At this point is MUST be treated as being immutable. Any
         * changes directed to this node or leaf MUST trigger copy-on-write.
         */

        node.setIdentity(id);
        
        node.setDirty(false);

        /*
         * Set the persistent identity of the child on the parent.
         */

        if( parent != null ) {
            
            parent.setChildKey(node);

        }

        return id;

    }

    /**
     * Convert the persistent identifier into an {@link ISlotAllocation}.
     * 
     * @param id
     *            The persistent identifier.
     * 
     * @return The {@link ISlotAllocation}
     */
    protected ISlotAllocation asSlots(long id) {
        
        final int firstSlot = SlotMath.getFirstSlot(id);
        
        final int byteCount = SlotMath.getByteCount(id);

        final int slotCount = store.getSlotMath().getSlotCount(byteCount);
        
        return new ContiguousSlotAllocation(byteCount, slotCount, firstSlot);

    }
    
    protected AbstractNode readNodeOrLeaf( long id ) {
        
        buf.clear();
        
        ByteBuffer tmp = store.read(asSlots(id),buf);
        
        AbstractNode node = nodeSer.getNodeOrLeaf(this, id, tmp);
        
        node.setDirty(false);
        
        if (node instanceof Leaf) {

            /*
             * Leaves are added to a hard reference queue. On eviction
             * from the queue the leaf is serialized. Once the leaf is
             * no longer strongly reachable its weak references may be
             * cleared by the VM.
             * 
             * @todo also place nodes onto a queue.
             */

            touch(node);

        }

        return node;
        
    }
    
    public Object insert(int key, Object entry) {

        assert key > NEGINF && key < POSINF;
        
        assert entry != null;
        
        return root.insert(key, entry);

    }

    public Object lookup(int key) {

        assert key > IBTree.NEGINF && key < IBTree.POSINF;

        return root.lookup(key);

    }

    public Object remove(int key) {

        assert key > IBTree.NEGINF && key < IBTree.POSINF;

        return root.remove(key);

    }

    /**
     * Recursive dump of the tree.
     * 
     * @param out
     *            The dump is written on this stream.
     * 
     * @return true unless an inconsistency is detected.
     * 
     * @todo modify to write on log vs PrintStream.
     */
    boolean dump(PrintStream out) {

            return dump(BTree.dumpLog.getEffectiveLevel(), out );

    }
        
    public boolean dump(Level level, PrintStream out) {
            
        // True iff we will write out the node structure.
        final boolean debug = level.toInt() <= Level.DEBUG.toInt();

        int[] utils = getUtilization();
        
        out.println("height=" + height + ", branchingFactor=" + branchingFactor
                + ", #nodes=" + nnodes + ", #leaves=" + nleaves + ", #entries="
                + nentries + ", nodeUtil=" + utils[0] + "%, leafUtil="
                + utils[1] + "%, utilization=" + utils[2] + "%");

        boolean ok = root.dump(level, out, 0, true);

        return ok;

    }

    /**
     * Computes and returns the utilization of the tree. The utilization figures
     * do not factor in the space requirements of nodes and leaves.
     * 
     * @return An array whose elements are:
     *         <ul>
     *         <li>0 - the leaf utilization percentage [0:100]. The leaf
     *         utilization is computed as the #of values stored in the tree
     *         divided by the #of values that could be stored in the #of
     *         allocated leaves.</li>
     *         <li>1 - the node utilization percentage [0:100]. The node
     *         utilization is computed as the #of non-root nodes divided by the
     *         #of non-root nodes that could be addressed by the tree.</li>
     *         <li>2 - the total utilization percentage [0:100]. This is the
     *         average of the leaf utilization and the node utilization.</li>
     *         </ul>
     */
    public int[] getUtilization() {
        
        int numNonRootNodes = nnodes + nleaves - 1;
        
        int nodeUtilization = nnodes == 0 ? 100 : (100 * numNonRootNodes )
                / (nnodes * branchingFactor);
        
        int leafUtilization = ( 100 * nentries ) / (nleaves * branchingFactor);
        
        int utilization = (nodeUtilization + leafUtilization) / 2;

        return new int[]{nodeUtilization,leafUtilization,utilization};
        
    }

    /**
     * Write out the persistent metadata for the btree on the store and
     * return the persistent identifier for that metadata. The metadata
     * include the persistent identifier of the root of the btree and the
     * height, #of nodes, #of leaves, and #of entries in the btree.
     * 
     * @param rootId
     *            The persistent identifier of the root of the btree.
     * 
     * @return The persistent identifier for the metadata.
     */
    long write() {

        long rootId = root.getIdentity();

        ByteBuffer buf = ByteBuffer.allocate(SIZEOF_METADATA);

        buf.putLong(rootId);
        buf.putInt(branchingFactor);
        buf.putInt(height);
        buf.putInt(nnodes);
        buf.putInt(nleaves);
        buf.putInt(nentries);

        return store.write(buf).toLong();

    }

    /**
     * Read the persistent metadata record for the btree.  Sets the height,
     * #of nodes, #of leavs, and #of entries from the metadata record as a
     * side effect.
     * 
     * @param metadataId
     *            The persistent identifier of the btree metadata record.
     *            
     * @return The persistent identifier of the root of the btree.
     */
    long read(long metadataId) {

        ByteBuffer buf = store.read(asSlots(metadataId),null);

        final long rootId = buf.getLong();
        log.info("rootId=" + rootId);
        branchingFactor = buf.getInt();
        assert branchingFactor >= MIN_BRANCHING_FACTOR;
        height = buf.getInt();
        assert height >= 0;
        nnodes = buf.getInt();
        assert nnodes >= 0;
        nleaves = buf.getInt();
        assert nleaves >= 0;
        nentries = buf.getInt();
        assert nentries >= 0;

        return rootId;

    }

    /**
     * The #of bytes in the metadata record written by {@link #writeNodeOrLeaf(int)}.
     * 
     * @see #writeNodeOrLeaf(int)
     */
    public static final int SIZEOF_METADATA = Bytes.SIZEOF_LONG
            + Bytes.SIZEOF_INT * 5;

    public long commit() {

        if (root.isDirty()) {

            writeNodeRecursive( root );
            
        }

        return write();

    }
    
    /**
     * Delete all nodes and leaves in this btree.
     * 
     * FIXME We need an iterator that visits only the nodes since we do not need
     * to fetch the leaves in order to delete them. The order does not matter if
     * we delete all at once. However, this might introduce latency when the
     * object index for a historical transaction is reclaimed.
     */
    public void delete() {
        
        throw new UnsupportedOperationException();
        
    }

}
