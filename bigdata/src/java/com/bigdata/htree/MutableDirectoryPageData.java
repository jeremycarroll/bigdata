package com.bigdata.htree;

import com.bigdata.btree.Node;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;

/**
 * Implementation maintains Java objects corresponding to the persistent data
 * and defines methods for a variety of mutations on the {@link IDirectoryData}
 * record which operate by direct manipulation of the Java objects.
 * <p>
 * Note: package private fields are used so that they may be directly accessed
 * by the {@link DirectoryPage} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: MutableNodeData.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public class MutableDirectoryPageData implements IDirectoryData {
	
	/**
	 * <p>
	 * The persistent address of each child node (may be nodes or leaves). The
	 * capacity of this array is m, where m is the {@link #branchingFactor}.
	 * Valid indices are in [0:nkeys+1] since nchildren := nkeys+1 for a
	 * {@link Node}. The key is {@link #NULL} until the child has been
	 * persisted. The protocol for persisting child nodes requires that we use a
	 * pre-order traversal (the general case is a directed graph) so that we can
	 * update the keys on the parent before the parent itself is persisted.
	 * </p>
	 * <p>
	 * Note: It is an error if there is an attempt to serialize a node having a
	 * null entry in this array and a non-null entry in the {@link #keys} array.
	 * </p>
	 * <p>
	 * This array is dimensioned to one more than the maximum capacity so that
	 * the child reference corresponding to the key that causes overflow and
	 * forces the split may be inserted. This greatly simplifies the logic for
	 * computing the split point and performing the split.
	 * </p>
	 */
	final long[] childAddr;

//	/**
//	 * The #of entries spanned by this node. This value should always be equal
//	 * to the sum of the defined values in {@link #childEntryCounts}.
//	 * <p>
//	 * When a node is split, the value is updated by subtracting off the counts
//	 * for the children that are being moved to the new sibling.
//	 * <p>
//	 * When a node is joined, the value is updated by adding in the counts for
//	 * the children that are being moved to the new sibling.
//	 * <p>
//	 * When a key is redistributed from a node to a sibling, the value is
//	 * updated by subtracting off the count for the child from the source
//	 * sibling and adding it in to this node.
//	 * <p>
//	 * This field is initialized by the various {@link Node} constructors.
//	 */
//	int nentries;

//	/**
//	 * The #of entries spanned by each direct child of this node.
//	 * <p>
//	 * The appropriate element in this array is incremented on all ancestor
//	 * nodes by {@link Leaf#insert(Object, Object)} and decremented on all
//	 * ancestors nodes by {@link Leaf#remove(Object)}. Since the ancestors are
//	 * guaranteed to be mutable as preconditions for those operations we are
//	 * able to traverse the {@link AbstractNode#parent} reference in a straight
//	 * forward manner.
//	 */
//	final int[] childEntryCounts;

	/**
	 * <code>true</code> iff the B+Tree is maintaining per tuple revision
	 * timestamps.
	 */
	final boolean hasVersionTimestamps;

	/**
	 * The minimum tuple revision timestamp for any leaf spanned by this node
	 * IFF the B+Tree is maintaining tuple revision timestamps.
	 */
	long minimumVersionTimestamp;

	/**
	 * The maximum tuple revision timestamp for any leaf spanned by this node
	 * IFF the B+Tree is maintaining tuple revision timestamps.
	 */
	long maximumVersionTimestamp;

	/**
	 * Create an empty mutable data record.
	 * 
	 * @param addressBits
	 *            The #of address bits.
	 * @param hasVersionTimestamps
	 *            <code>true</code> iff the HTree is maintaining per tuple
	 *            version timestamps.
	 */
	public MutableDirectoryPageData(final int addressBits, 
			final boolean hasVersionTimestamps) {

//		nentries = 0;

		childAddr = new long[1 << addressBits];

//		childEntryCounts = new int[branchingFactor + 1];

		this.hasVersionTimestamps = hasVersionTimestamps;

		minimumVersionTimestamp = maximumVersionTimestamp = 0L;

	}

	/**
	 * Makes a mutable copy of the source data record.
	 * 
	 * @param branchingFactor
	 *            The branching factor for the owning B+Tree. This is used to
	 *            initialize the various arrays to the correct capacity.
	 * @param src
	 *            The source data record.
	 */
	public MutableDirectoryPageData(final int addressBits, final IDirectoryData src) {

		if (src == null)
			throw new IllegalArgumentException();

//		nentries = src.getSpannedTupleCount();

		childAddr = new long[1 << addressBits];

//		childEntryCounts = new int[branchingFactor + 1];

		final int nchildren = src.getChildCount();

//		int sum = 0;

		for (int i = 0; i < nchildren; i++) {

			childAddr[i] = src.getChildAddr(i);

//			final int tmp = childEntryCounts[i] = src.getChildEntryCount(i);

//			sum += tmp;

		}

		this.hasVersionTimestamps = src.hasVersionTimestamps();

		if (src.hasVersionTimestamps()) {

			minimumVersionTimestamp = src.getMinimumVersionTimestamp();

			maximumVersionTimestamp = src.getMaximumVersionTimestamp();

		}

//		assert sum == nentries;

	}

	/**
	 * Ctor based on just the "data" -- used by unit tests.
	 * 
	 * @param nentries
	 * @param keys
	 * @param childAddr
	 * @param childEntryCounts
	 */
	public MutableDirectoryPageData(//final int nentries, final IRaba keys,
			final long[] childAddr, //final int[] childEntryCounts,
			final boolean hasVersionTimestamps,
			final long minimumVersionTimestamp,
			final long maximumVersionTimestamp) {

		assert childAddr != null;
//		assert childEntryCounts != null;
//		assert keys.capacity() + 1 == childAddr.length;
//		assert childAddr.length == childEntryCounts.length;

//		this.nentries = nentries;

		this.childAddr = childAddr;

//		this.childEntryCounts = childEntryCounts;

		this.hasVersionTimestamps = hasVersionTimestamps;

		this.minimumVersionTimestamp = minimumVersionTimestamp;

		this.maximumVersionTimestamp = maximumVersionTimestamp;

	}

	/**
	 * No - this is a mutable data record.
	 */
	final public boolean isReadOnly() {

		return false;

	}

	/**
	 * No.
	 */
	final public boolean isCoded() {

		return false;

	}

	final public AbstractFixedByteArrayBuffer data() {

		throw new UnsupportedOperationException();

	}

	public final long getChildAddr(final int index) {

		return childAddr[index];

	}

	/**
	 * Overridden to report the size of the address space.
	 * 
	 * TODO Alternatively, could report the #of non-null child addresses.
	 */
	@Override
	final public int getChildCount() {

		return childAddr.length;

	}

	final public boolean isLeaf() {

		return false;

	}

	final public boolean hasVersionTimestamps() {

		return hasVersionTimestamps;

	}

	final public long getMaximumVersionTimestamp() {

		if (!hasVersionTimestamps)
			throw new UnsupportedOperationException();

		return maximumVersionTimestamp;

	}

	final public long getMinimumVersionTimestamp() {

		if (!hasVersionTimestamps)
			throw new UnsupportedOperationException();

		return minimumVersionTimestamp;

	}

}
