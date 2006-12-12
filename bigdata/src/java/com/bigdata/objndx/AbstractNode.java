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
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * Abstract node.
 * </p>
 * <p>
 * Note: For nodes in the index, the attributes dirty and persistent are 100%
 * correlated. Since only transient nodes may be dirty and only persistent nodes
 * are clean any time one is true the other is false.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractNode extends PO {

    /**
     * Log for node and leaf operations.
     * <dl>
     * <dt>info</dt>
     * <dd> A high level trace of insert, split, joint, and remove operations.
     * You MUST test on {@link #INFO} before generating log messages at this
     * level to avoid string concatenation operations would otherwise kill
     * performance.</dd>
     * <dt></dt>
     * <dd> A low level trace including a lot of dumps of leaf and node state. *
     * You MUST test on {@link #DEBUG} before generating log messages at this
     * level to avoid string concatenation operations would otherwise kill
     * performance.</dd>
     * </dl>
     * 
     * @see BTree#log
     * @see BTree#dumpLog
     */
    protected static final Logger log = Logger.getLogger(AbstractNode.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG.toInt();

    /**
     * The BTree.
     * 
     * Note: This field MUST be patched when the node is read from the store.
     * This requires a custom method to read the node with the btree reference
     * on hand so that we can set this field.
     */
    final transient protected BTree btree;

    /**
     * The branching factor (#of slots for keys or values).
     */
    final transient protected int branchingFactor;
    
    /**
     * The minimum #of keys. For a {@link Node}, the minimum #of children is
     * <code>minKeys + 1</code>. For a {@link Leaf}, the minimum #of values
     * is <code>minKeys</code>.
     */
    final transient protected int minKeys;
    
    /**
     * The maximum #of keys.  For a {@link Node}, the maximum #of children is
     * <code>maxKeys + 1</code>.  For a {@link Leaf}, the maximum #of values is
     * <code>maxKeys</code>.
     */
    final transient protected int maxKeys;

    /**
     * The #of valid keys for this node or leaf.  For a {@link Node}, the #of
     * children is always <code>nkeys+1</code>.  For a {@link Leaf}, the #of
     * values is always the same as the #of keys.
     */
    protected int nkeys = 0;

    /**
     * The external keys for the B+Tree. The #of keys depends on whether this is
     * a {@link Node} or a {@link Leaf}. A leaf has one key per value - that
     * is, the maximum #of keys for a leaf is specified by the branching factor.
     * In contrast a node has m-1 keys where m is the maximum #of children (aka
     * the branching factor). Therefore this field is initialized by the
     * {@link Leaf} or {@link Node} - NOT by the {@link AbstractNode}.
     * 
     * The interpretation of the key index for a leaf is one to one - key[0]
     * corresponds to value[0].
     * 
     * For both a {@link Node} and a {@link Leaf}, this array is dimensioned to
     * one more than the maximum capacity so that the key that causes overflow
     * and forces the split may be inserted.  This greatly simplifies the logic
     * for computing the split point and performing the split.
     * 
     * @see #findChild(int key)
     * @see Search#search(int, int[], int)
     */
//    protected int[] keys;
//    protected Integer[] keys;
//    protected Object[] keys;
    protected Object keys;

    /**
     * The parent of this node. This is null for the root node. The parent is
     * required in order to set the persistent identity of a newly persisted
     * child node on its parent. The reference to the parent will remain
     * strongly reachable as long as the parent is either a root (held by the
     * {@link BTree}) or a dirty child (held by the {@link Node}). The parent
     * reference is set when a node is attached as the child of another node.
     * 
     * Note: When a node is cloned by {@link #copyOnWrite()} the parent
     * references for its <em>clean</em> children are set to the new copy of
     * the node. This is refered to in several places as "stealing" the children
     * since they are no longer linked back to their old parents via their
     * parent reference.
     */
    protected WeakReference<Node> parent = null;

    /**
     * The #of times that this node is present on the {@link HardReferenceQueue} .
     * This value is incremented each time the node is added to the queue and is
     * decremented each time the node is evicted from the queue. On eviction, if
     * the counter is zero(0) after it is decremented then the node is written
     * on the store. This mechanism is critical because it prevents a node
     * entering the queue from forcing IO for the same node in the edge case
     * where the node is also on the tail on the queue. Since the counter is
     * incremented before it is added to the queue, it is guarenteed to be
     * non-zero when the node forces its own eviction from the tail of the
     * queue. Preventing this edge case is important since the node can
     * otherwise become immutable at the very moment that it is touched to
     * indicate that we are going to update its state, e.g., during an insert,
     * split, or remove operation. This mechanism also helps to defer IOs since
     * IO can not occur until the last reference to the node is evicted from the
     * queue.
     */
    protected int referenceCount = 0;

    public void delete() {
        
        if( deleted ) {
            
            throw new IllegalStateException();
            
        }

        if( identity != NULL ) {
            
            /*
             * Deallocate the object on the store.
             */

            btree.store.delete(btree.asSlots(identity));
            
        }
        
        deleted = true;
        
    }

    /**
     * The parent iff the node has been added as the child of another node
     * and the parent reference has not been cleared.
     * 
     * @return The parent.
     */
    public Node getParent() {

        Node p = null;

        if (parent != null) {

            /*
             * Note: Will be null if the parent reference has been cleared.
             */
            p = parent.get();

        }

        /*
         * The parent is allowed to be null iff this is the root of the
         * btree.
         */
        assert (this == btree.root && p == null) || p != null;

        return p;

    }

    /**
     * Disallowed.
     */
    private AbstractNode() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * All constructors delegate to this constructor to set the btree and
     * branching factor and to compute the minimum and maximum #of keys for the
     * node. This isolates the logic required for computing the minimum and
     * maximum capacity and encapsulates it as <code>final</code> data fields
     * rather than permitting that logic to be replicated throughout the code
     * with the corresponding difficulty in ensuring that the logic is correct
     * throughout.
     * 
     * @param btree
     *            The btree to which the node belongs.
     * @param branchingFactor
     *            The branching factor for the node. By passing the branching
     *            factor rather than using the branching factor declared on the
     *            btree we are able to support different branching factors at
     *            different levels of the tree.
     */
    protected AbstractNode(BTree btree, int branchingFactor) {

        assert btree != null;

        assert branchingFactor>=BTree.MIN_BRANCHING_FACTOR;
        
        this.btree = btree;

        this.branchingFactor = branchingFactor;

        /*
         * Compute the minimum #of children/values. this is the same whether
         * this is a Node or a Leaf.
         */
        final int minChildren = (branchingFactor+1)>>1;
        
        this.minKeys = isLeaf() ? minChildren : minChildren - 1;
        
        // The maximum #of keys is easy to compute.
        this.maxKeys = isLeaf() ? branchingFactor : branchingFactor - 1;

        // Add to the hard reference queue.
        btree.touch(this);
        
//        /*
//         * If this is a {@link Node} then ensures that the btree holds a hard
//         * reference to the node.
//         */
//        if( !isLeaf() ) {
//                
//            btree.nodes.add((Node)this);
//
//        }

    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node.
     */
    protected AbstractNode(AbstractNode src) {

        /*
         * Note: We do NOT clone the base class since this is a new
         * persistence capable object, but it is not yet persistent and we
         * do not want to copy the persistent identity of the source object.
         */
        this(src.btree,src.branchingFactor);

        // This node must be mutable (it is a new node).
        assert isDirty();
        assert !isPersistent();
        
        /* The source must not be dirty.  We are cloning it so that we can
         * make changes on it.
         */
        assert src != null;
        assert !src.isDirty();
        assert src.isPersistent();

        /*
         * Copy the parent reference. The parent must be defined unless the
         * source is the current root.
         * 
         * Note that we reuse the weak reference since it is immutable (it state
         * is only changed by the VM, not by the application).
         */

        assert src == btree.getRoot()
                || (src.parent != null && src.parent.get() != null);
        
        this.parent = src.parent;
        
    }

    /**
     * <p>
     * Return this leaf iff it is dirty (aka mutable) and otherwise return a
     * copy of this leaf. If a copy is made of the leaf, then a copy will also
     * be made of each immutable parent up to the first mutable parent or the
     * root of the tree, which ever comes first. If the root is copied, then the
     * new root will be set on the {@link BTree}. This method must MUST be
     * invoked any time an mutative operation is requested for the leaf.
     * </p>
     * <p>
     * Note: You can not modify a node that has been written onto the store.
     * Instead, you have to clone the node causing it and all nodes up to the
     * root to be dirty and transient. This method handles that cloning process,
     * but the caller MUST test whether or not the node was copied by this
     * method, MUST delegate the mutation operation to the copy iff a copy was
     * made, and MUST result in an awareness in the caller that the copy exists
     * and needs to be used in place of the immutable version of the node.
     * </p>
     * 
     * @return Either this leaf or a copy of this leaf.
     */
    protected AbstractNode copyOnWrite() {
        
        // Always invoked first for a leaf and thereafter in its other form.
        assert isLeaf();
        
        return copyOnWrite(null);
        
    }
    
    /**
     * <p>
     * Return this node or leaf iff it is dirty (aka mutable) and otherwise
     * return a copy of this node or leaf. If a copy is made of the node, then a
     * copy will also be made of each immutable parent up to the first mutable
     * parent or the root of the tree, which ever comes first. If the root is
     * copied, then the new root will be set on the {@link BTree}. This method
     * must MUST be invoked any time an mutative operation is requested for the
     * leaf.
     * </p>
     * <p>
     * Note: You can not modify a node that has been written onto the store.
     * Instead, you have to clone the node causing it and all nodes up to the
     * root to be dirty and transient. This method handles that cloning process,
     * but the caller MUST test whether or not the node was copied by this
     * method, MUST delegate the mutation operation to the copy iff a copy was
     * made, and MUST result in an awareness in the caller that the copy exists
     * and needs to be used in place of the immutable version of the node.
     * </p>
     * 
     * @param triggeredByChild
     *            The child that triggered this event if any.
     * 
     * @return Either this node or a copy of this node.
     */
    protected AbstractNode copyOnWrite(AbstractNode triggeredByChild) {

        if (isPersistent()) {

            if(INFO) {
                log.info("this="+this+", trigger="+triggeredByChild);
                if( DEBUG ) {
                    System.err.println("this"); dump(Level.DEBUG,System.err);
                    if( triggeredByChild != null ) {
                        System.err.println("trigger"); triggeredByChild.dump(Level.DEBUG,System.err);
                    }
                }
            }
            
            AbstractNode newNode;

            if (this instanceof Node) {

                newNode = new Node((Node) this, triggeredByChild );
                
                btree.counters.nodesCopyOnWrite++;

            } else {

                newNode = new Leaf((Leaf) this);

                btree.counters.leavesCopyOnWrite++;

            }

            Node parent = this.getParent();

            if (btree.root == this) {

                assert parent == null;

                // Update the root node on the btree.
                log.info("Copy-on-write : replaced root node on btree.");

                btree.root = newNode;

            } else {

                /*
                 * Recursive copy-on-write up the tree. This operations stops as
                 * soon as we reach a parent node that is already dirty and
                 * grounds out at the root in any case.
                 */
                assert parent != null;

                if (!parent.isDirty()) {

                    /*
                     * Note: pass up the old child since we want to avoid having
                     * its parent reference reset.
                     */
                    parent = (Node) parent.copyOnWrite(this);

                }

                /*
                 * Replace the reference to this child with the reference to the
                 * new child. This makes the old child inaccessible via
                 * navigation. It will be GCd once it falls off of the hard
                 * reference queue.
                 */
                parent.replaceChildRef(this.getIdentity(), newNode);

            }

            return newNode;

        } else {

            /*
             * Since a clone was not required, we use this as an opportunity to
             * touch the hard reference queue. This helps us to ensure that
             * nodes which have been touched recently will remain strongly
             * reachable.
             */
            btree.touch(this);
            
            return this;

        }

    }

    /**
     * Post-order traveral of nodes and leaves in the tree. For any given
     * node, its children are always visited before the node itself (hence
     * the node occurs in the post-order position in the traveral). The
     * iterator is NOT safe for concurrent modification.
     */
    public Iterator postOrderIterator() {

        return postOrderIterator(false);

    }

    /**
     * Post-order traveral of nodes and leaves in the tree. For any given
     * node, its children are always visited before the node itself (hence
     * the node occurs in the post-order position in the traveral). The
     * iterator is NOT safe for concurrent modification.
     * 
     * @param dirtyNodesOnly
     *            When true, only dirty nodes and leaves will be visited
     */
    abstract public Iterator postOrderIterator(boolean dirtyNodesOnly);

    /**
     * Traversal of index values in key order.
     */
    public Iterator entryIterator() {

        /*
         * Begin with a post-order iterator.
         */
        return new Striterator(postOrderIterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand the value objects for each leaf visited in the post-order
             * traversal.
             */
            protected Iterator expand(Object childObj) {
                /*
                 * A child of this node.
                 */
                AbstractNode child = (AbstractNode) childObj;

                if (child instanceof Leaf) {

                    return ((Leaf) child).entryIterator();

                } else {

                    return EmptyIterator.DEFAULT;

                }
            }
        });

    }

    /**
     * <p>
     * Invariants:
     * <ul>
     * <li>A node with nkeys + 1 children.</li>
     * <li>A node must have between [m/2:m] children (alternatively, between
     * [m/2-1:m-1] keys since nkeys + 1 == nchildren for a node).</li>
     * <li>A leaf has no children and has between [m/2:m] key-value pairs (the
     * same as the #of children on a node).</li>
     * <li>The root leaf may be deficient (may have less than m/2 key-value
     * pairs).</li>
     * </ul>
     * where <code>m</code> is the branching factor and a node is understood
     * to be a non-leaf node in the tree.
     * </p>
     * <p>
     * In addition, all leaves are at the same level (not tested by this
     * assertion).
     * </p>
     */
    protected final void assertInvariants() {

        try {

            /*
             * either the root or the parent is reachable.
             */
            assert btree.root == this
                    || (this.parent != null && this.parent.get() != null);

            if (btree.root != this) {

                // not the root, so the min #of keys must be observed.
                assert nkeys >= minKeys;

            }

            // max #of keys.
            assert nkeys <= maxKeys;

        } catch (AssertionError ex) {

            log.fatal("Invariants failed\n"
                    + ex.getStackTrace()[0].toString());
            
            dump(Level.FATAL, System.err);
            
            throw ex;
            
        }

    }

    static protected final void assertKeyTypeAndLength(BTree btree, Object keys,int length) {
        
        assert btree != null;
        assert keys != null;
        ArrayType keyType = ArrayType.getArrayType(keys);
        assert btree.keyType == keyType;
        assert getLength(keys) == length;
    }
    
    /**
     * Return the length of the key array (its dimension). This method works for
     * both primitive and object array types.
     * 
     * @param keys
     *            The key array.
     * 
     * @return Its length.
     */
    final static protected int getLength(Object keys) {
        switch(ArrayType.getArrayType(keys)) {
        case BYTE: return ((byte[])keys).length;
        case SHORT: return ((short[])keys).length;
        case CHAR: return ((char[])keys).length;
        case INT: return ((int[])keys).length;
        case LONG: return ((long[])keys).length;
        case FLOAT: return ((float[])keys).length;
        case DOUBLE: return ((double[])keys).length;
        case OBJECT: return ((Object[])keys).length;
        default: throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Verify keys are monotonically increasing.
     */
    protected final void assertKeysMonotonic() {

        Object lastKey = btree.NEGINF;

        int limit = getLength(keys);
        
        for (int i = 0; i < limit; i++) {

            Object key = getKey(i);

            if (i < nkeys) {
                
                if (compare(key,lastKey) <= 0) {

                    throw new AssertionError("Keys out of order at index=" + i
                            + ", lastKey=" + lastKey + ", keys[" + i + "]="
                            + key);

                }

            } else if (((Integer)key) != btree.NEGINF) {
                
                // undefined keys.
                throw new AssertionError("Expecting NEGINF(" + btree.NEGINF
                        + ") at index=" + i + ", nkeys=" + nkeys
                        + ", but found keys[" + i + "]=" + key);
                
            }

            lastKey = key;

        }

    }
    
    /**
     * Generic key comparison.
     * 
     * Note: This is used only for ease in debugging routines. All key
     * comparison for search is performed within data type specific methods.
     * 
     * @param k1
     *            A key (non-null).
     * @param k2
     *            Another key (non-null).
     * 
     * @return a negative integer, zero, or a positive integer as the first
     *         argument is less than, equal to, or greater than the second.
     * 
     * @see #search(Object)
     */
    final protected int compare(Object k1,Object k2) {
        switch(btree.keyType) {
        case BYTE: return ((Byte)k1).compareTo((Byte)k2);
        case SHORT: return ((Short)k1).compareTo((Short)k2);
        case CHAR: return ((Character)k1).compareTo((Character)k2);
        case INT: return ((Integer)k1).compareTo((Integer)k2);
        case LONG: return ((Long)k1).compareTo((Long)k2);
        case FLOAT: return ((Float)k1).compareTo((Float)k2);
        case DOUBLE: return ((Double)k1).compareTo((Double)k2);
        case OBJECT: return btree.comparator.compare(k1,k2);
        default: throw new UnsupportedOperationException();
        }        
    }
    
    /**
     * A type agnostic wrapper directs the request to a type appropriate search
     * implementation based on the key type as defined for the btree.
     * 
     * @param key
     *            The key.
     * 
     * @return index of the search key, if it is contained in the array;
     *         otherwise, <code>(-(insertion point) - 1)</code>. The
     *         insertion point is defined as the point at which the key would be
     *         inserted into the array. Note that this guarantees that the
     *         return value will be >= 0 if and only if the key is found.
     */
    final protected int search(Object key) {
        switch(btree.keyType) {
        // @todo handle at least float, double here and in Search.java
        case INT: return Search.search(((Integer)key).intValue(), (int[])keys, nkeys);
        case LONG: return Search.search(((Long)key).longValue(), (long[])keys, nkeys);
        case OBJECT: return Search.search(key, (Object[])keys, nkeys,btree.comparator);
        default: throw new UnsupportedOperationException();
        }
    }

    /**
     * The value of the key at the specified index.
     * 
     * @param index
     *            The key index in [0:maxKeys-1].
     *            
     * @return The value of the key at that index. If the key is a primitive
     *         data type then it will be autoboxed as an instance of its
     *         corresponding Class.
     *         
     * @see #setKey(int, Object)
     * @see #copyKey(int, AbstractNode, int)
     */
    final protected Object getKey(int index) {
//        assert index >=0 && index < maxKeys;
        switch(btree.keyType) {
        case BYTE: return ((byte[])keys)[index];
        case SHORT: return ((short[])keys)[index];
        case CHAR: return ((char[])keys)[index];
        case INT: return ((int[])keys)[index];
        case LONG: return ((long[])keys)[index];
        case FLOAT: return ((float[])keys)[index];
        case DOUBLE: return ((double[])keys)[index];
        case OBJECT: return ((Object[])keys)[index];
        default: throw new UnsupportedOperationException();
        }        
    }

    /**
     * Allocate an array of the indicated key type and capacity.
     * 
     * @param keyType
     *            The key type.
     * @param capacity
     *            The array capacity.
     * @return The array.
     * 
     * @todo consider the use of an allocation pool for this, leaves, and nodes.
     */
    static public final Object allocKeys(ArrayType keyType, int capacity) {
        switch (keyType) {
        case BYTE:
            return new byte[capacity];
        case SHORT:
            return new short[capacity];
        case CHAR:
            return new char[capacity];
        case INT:
            return new int[capacity];
        case LONG:
            return new long[capacity];
        case FLOAT:
            return new float[capacity];
        case DOUBLE:
            return new double[capacity];
        case OBJECT:
            return new Object[capacity];
        default:
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Update the value of the key at the specified index.
     * 
     * @param index
     *            The key index in [0:maxKeys-1];
     * @param key
     *            The value to be assigned to the key at that index. If the keys
     *            are represented as a primitive data type then the
     *            corresponding primitive value for the key will be extracted.
     * 
     * @see #copyKey(int, AbstractNode, int)
     * @see #getKey(int)
     */
    final protected void setKey(int index,Object key) {
//        assert index >=0 && index < maxKeys;
        switch(btree.keyType) {
        case BYTE: ((byte[])keys)[index] = (Byte)key; break;
        case SHORT: ((short[])keys)[index] = (Short)key; break;
        case CHAR: ((char[])keys)[index] = (Character)key; break;
        case INT: ((int[])keys)[index] = (Integer)key; break;
        case LONG: ((long[])keys)[index] = (Long)key; break;
        case FLOAT: ((float[])keys)[index] = (Float)key; break;
        case DOUBLE: ((double[])keys)[index] = (Double)key; break;
        case OBJECT: ((Object[])keys)[index] = key; break;
        default: throw new UnsupportedOperationException();
        }        
    }
    
    /**
     * Copy a key from the source node into this node. This method does not
     * modify the source node. This method does not update the #of keys in this
     * node. This method has the substantial advantage that primitive keys are
     * not boxed and unboxed solely to perform the cop.
     * 
     * @param dstpos
     *            The index position to which the key will be copied on this
     *            node.
     * @param src
     *            The source node from which the key will be copied.
     * @param srcpos
     *            The index position from which the key will be copied.
     */
    final protected void copyKey(int dstpos,AbstractNode src,int srcpos) {
//          assert index >=0 && index < maxKeys;
          switch(btree.keyType) {
          case BYTE: ((byte[])keys)[dstpos] = ((byte[])src.keys)[srcpos]; break;
          case SHORT: ((short[])keys)[dstpos] = ((short[])src.keys)[srcpos]; break;
          case CHAR: ((char[])keys)[dstpos] = ((char[])src.keys)[srcpos]; break;
          case INT: ((int[])keys)[dstpos] = ((int[])src.keys)[srcpos]; break;
          case LONG: ((long[])keys)[dstpos] = ((long[])src.keys)[srcpos]; break;
          case FLOAT: ((float[])keys)[dstpos] = ((float[])src.keys)[srcpos]; break;
          case DOUBLE: ((double[])keys)[dstpos] = ((double[])src.keys)[srcpos]; break;
          case OBJECT: ((Object[])keys)[dstpos] = ((Object[])src.keys)[srcpos]; break;
          default: throw new UnsupportedOperationException();
          }     
    }
    
    /**
     * Return a representation of the keys.
     */
    static protected String keysAsString(Object keys) {
        switch(ArrayType.getArrayType(keys)) {
        case BYTE: return Arrays.toString((byte[])keys);
        case SHORT: return Arrays.toString((short[])keys);
        case CHAR: return Arrays.toString((char[])keys);
        case INT: return Arrays.toString((int[])keys);
        case LONG: return Arrays.toString((long[])keys);
        case FLOAT: return Arrays.toString((float[])keys);
        case DOUBLE: return Arrays.toString((double[])keys);
        case OBJECT: return Arrays.toString((Object[])keys);
        default: throw new UnsupportedOperationException();
        }
    }
    
    /**
     * True iff this is a leaf node.
     */
    abstract public boolean isLeaf();

    /**
     * <p>
     * Split a node or leaf that is over capacity (by one).
     * </p>
     * 
     * @return The high node (or leaf) created by the split.
     */
    abstract protected AbstractNode split();
    
    /**
     * <p>
     * Join this node (must be deficient) with either its left or right sibling.
     * A join will either cause a single key and value (child) to be
     * redistributed from a sibling to this leaf (node) or it will cause a
     * sibling leaf (node) to be merged into this leaf (node). Both situations
     * also cause the separator key in the parent to be adjusted.
     * </p>
     * <p>
     * Join is invoked when a leaf has become deficient (too few keys/values).
     * This method is never invoked for the root leaf therefore the parent of
     * this leaf must be defined. Further, since the minimum #of children is two
     * (2) for the smallest branching factor three (3), there is always a
     * sibling to consider.
     * </p>
     * <p>
     * Join first considers the immediate siblings. if either is materialized
     * and has more than the minimum #of values, then it redistributes one key
     * and value (child) from the sibling into this leaf (node). If either
     * sibling is materialized and has only the minimum #of values, then it
     * merges this leaf (node) with that sibling.
     * </p>
     * <p>
     * If no materialized immediate sibling meets these criteria, then first
     * materialize and test the right sibling. if the right sibling does not
     * meet these criteria, then materialize and test the left sibling.
     * </p>
     * <p>
     * Note that (a) we prefer to merge a materialized sibling with this leaf to
     * materializing a sibling; and (b) merging siblings is the only way that a
     * separator key is removed from a parent. If the parent becomes deficient
     * through merging then join is invoked on the parent as well. Note that
     * join is never invoked on the root node (or leaf) since it by definition
     * has no siblings.
     * </p>
     * <p>
     * Note that we must invoked copy-on-write before modifying a sibling.
     * However, the parent of the leaf MUST already be mutable (aka dirty) since
     * that is a precondition for removing a key from the leaf. This means that
     * copy-on-write will not force the parent to be cloned.
     * </p>
     */
    protected void join() {
        
        /*
         * copyOnWrite() wants to know the child that triggered the action when
         * that information is available. However we do not have that
         * information in this case so we use a [null] trigger.
         */
        final AbstractNode t = null; // a [null] trigger node.

        // verify that this node is deficient.
        assert nkeys < minKeys;
        // verify that this leaf is under minimum capacity by one key.
        assert nkeys == minKeys - 1;
        // verify that the node is mutable.
        assert isDirty();
        assert !isPersistent();
        // verify that the leaf is not the root.
        assert btree.root != this;
        
        final Node parent = getParent();

        if (INFO) {
            log.info("this="+this);
            if(DEBUG) {
                System.err.println("this"); dump(Level.DEBUG,System.err);
            }
        }
        
        if( this instanceof Leaf ) {

            btree.counters.leavesJoined++;

        } else {
            
            btree.counters.nodesJoined++;

        }

        /*
         * Look for, but do not materialize, the left and right siblings.
         * 
         * Note that we defer invoking copy-on-write for the left/right sibling
         * until we are sure which sibling we will use.
         */
        
        AbstractNode rightSibling = (AbstractNode) parent.getRightSibling(this,
                false);

        AbstractNode leftSibling = (AbstractNode) parent.getLeftSibling(this,
                false);
        
        /*
         * prefer a sibling that is already materialized.
         */
        if (rightSibling != null && rightSibling.nkeys > rightSibling.minKeys) {

            redistributeKeys(rightSibling.copyOnWrite(t), true);

            return;

        }

        if (leftSibling != null && leftSibling.nkeys > leftSibling.minKeys) {

            redistributeKeys(leftSibling.copyOnWrite(t), false);

            return;

        }
        
        /*
         * if either sibling was not materialized, then materialize and test
         * that sibling.
         */
        if( rightSibling == null ) {
            
            rightSibling = parent.getRightSibling(this,true);
            
            if( rightSibling != null && rightSibling.nkeys>rightSibling.minKeys  ) {

                redistributeKeys(rightSibling.copyOnWrite(t),true);
                
                return;
                
            }

        }

        if( leftSibling == null ) {
            
            leftSibling = parent.getLeftSibling(this,true);
            
            if( leftSibling != null && leftSibling.nkeys>leftSibling.minKeys ) {

                redistributeKeys(leftSibling.copyOnWrite(t),false);
                
                return;
                
            }

        }

        /*
         * by now the left and right siblings have both been materialized. At
         * least one sibling must be non-null. Since neither sibling was over
         * the minimum, we now merge this node with a sibling and remove the
         * separator key from the parent.
         */
        
        if( rightSibling != null ) {
            
            merge(rightSibling,true);
            
            return;
            
        } else if( leftSibling != null ) {
            
            merge(leftSibling,false);
            
            return;
            
        } else {
            
            throw new AssertionError();
            
        }
        
    }
    
    /**
     * Redistribute the one key from the sibling into this node.
     * 
     * @param sibling
     *            The sibling.
     * @param isRightSibling
     *            True iff the sibling is the rightSibling of this node.
     * 
     * @todo redistribution should proceed until the node and the sibling have
     *       an equal #of keys (or perhaps more exactly until the node would
     *       have more keys than the sibling if another key was redistributed
     *       into the node from the sibling). this takes advantage of the fact
     *       that the node and the sibling are known to be in memory to bring
     *       them to the point where they are equally full. along the same lines
     *       when both siblings are resident we could actually redistribute keys
     *       from both siblings into the node until the keys were equally
     *       distributed among the node and its siblings.
     * 
     * @todo a b*-tree variant simply uses redistribution of keys among siblings
     *       during insert to defer a split until the node and its siblings are
     *       all full.
     */
    abstract protected void redistributeKeys(AbstractNode sibling,
            boolean isRightSibling);

    /**
     * Merge the sibling into this node.
     * 
     * @param sibling
     *            The sibling.
     * @param isRightSibling
     *            True iff the sibling is the rightSibling of this node.
     */
    abstract protected void merge(AbstractNode sibling, boolean isRightSibling);

    /**
     * Recursive search locates the approprate leaf and inserts the entry under
     * the key. The leaf is split iff necessary. Splitting the leaf can cause
     * splits to cascade up towards the root. If the root is split then the
     * total depth of the tree is inceased by one.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     * 
     * @return The previous value or <code>null</code> if the key was not
     *         found.
     */
    abstract public Object insert(Object key, Object entry);

    /**
     * Recursive search locates the appropriate leaf and removes and returns
     * the pre-existing value stored under the key (if any).
     * 
     * @param key
     *            The external key.
     *            
     * @return The value or null if there was no entry for that key.
     */
    abstract public Object remove(Object key);

    /**
     * Recursive search locates the entry for the probe key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The entry or <code>null</code> iff there is no entry for
     *         that key.
     */
    abstract public Object lookup(Object key);

    /**
     * Dump the data onto the {@link PrintStream} (non-recursive).
     * 
     * @param out
     *            Where to write the dump.
     * 
     * @return True unless an inconsistency was detected.
     */
    public boolean dump(PrintStream out) {

        return dump(BTree.dumpLog.getEffectiveLevel(), out);

    }

    /**
     * Dump the data onto the {@link PrintStream}.
     * 
     * @param level
     *            The logging level.
     * @param out
     *            Where to write the dump.
     *            
     * @return True unless an inconsistency was detected.
     */
    public boolean dump(Level level,PrintStream out) {

        return dump(level, out, -1, false);

    }

    /**
     * Dump the data onto the {@link PrintStream}.
     * 
     * @param level
     *            The logging level.
     * @param out
     *            Where to write the dump.
     * @param height
     *            The height of this node in the tree or -1 iff you need to
     *            invoke this method on a node or leaf whose height in the tree
     *            is not known.
     * @param recursive
     *            When true, the node will be dumped recursively using a
     *            pre-order traversal.
     * 
     * @return True unless an inconsistency was detected.
     */
    abstract public boolean dump(Level level, PrintStream out, int height, boolean recursive);

    /**
     * Returns a string that may be used to indent a dump of the nodes in
     * the tree.
     * 
     * @param height
     *            The height.
     *            
     * @return A string suitable for indent at that height.
     */
    protected static String indent(int height) {

        if( height == -1 ) {
        
            // The height is not defined.
            
            return "";
            
        }
        
        return ws.substring(0, height * 4);

    }

    private static final String ws = "                                                                                                                                                                                                                  ";

}
