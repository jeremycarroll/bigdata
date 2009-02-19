package com.bigdata.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexMetadata.Options;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.resources.SplitIndexPartitionTask.AtomicUpdateSplitIndexPartitionTask;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.Split;

/**
 * A configurable default policy for deciding when and where to split an index
 * partition into 2 or more index partitions.
 * <p>
 * Note: There is probably no single value for {@link #getEntryCountPerSplit()}
 * that is going to be "right" across applications. The space requirements for
 * keys is very difficult to estimate since leading key compression will often
 * provide a good win. Likewise, indices are free to use compression on their
 * values as well so the size of the byte[] values is not a good estimate of
 * their size in the index.
 * <p>
 * Note: The #of index entries is a good proxy for the space requirements of
 * most indices. The {@link BigdataFileSystem} is one case where the space
 * requirements could be quite different since 64M blocks may be stored along
 * with the index entries, however in that case you can also test for the size
 * of the index segment that is part of the view and decide that it's time to
 * split the view.
 * 
 * @todo Perhaps I could do something to estimate the size of the nodes and the
 *       leaves in the index. or the percent of the data on the journal that
 *       belongs to the mutable {@link BTree} and then count the #of bytes in
 *       the index segments (which is only accurate after a compacting merge).
 * 
 * @todo Make the twiddling of the split point to respect application
 *       constraints on atomic logical row operations separable from the
 *       {@link ISplitHandler} so it can be just another property on the
 *       {@link DefaultSplitHandler}.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultSplitHandler implements ISplitHandler {

    /**
     * 
     */
    private static final long serialVersionUID = 1675517991163473445L;

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(DefaultSplitHandler.class);

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    private int minimumEntryCount;
    
    private int entryCountPerSplit;

    private int sampleRate;

    private double overCapacityMultiplier;

    private double underCapacityMultiplier;

    public String toString() {
    
        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getName());
        
        sb.append("{ minimumEntryCount=" + minimumEntryCount);

        sb.append(", entryCountPerSplit=" + entryCountPerSplit);
        
        sb.append(", sampleRate=" + sampleRate);
        
        sb.append(", overCapacityMultiplier=" + overCapacityMultiplier);
        
        sb.append(", underCapacityMultiplier=" + underCapacityMultiplier);
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * De-serialization ctor.
     */
    public DefaultSplitHandler() {

    }

    /**
     * Setup a split handler.
     * 
     * @param minimumEntryCount
     *            An index partition which has no more than this many tuples
     *            should be joined with its rightSibling (if any).
     * @param entryCountPerSplit
     *            The target #of tuples for an index partition.
     * @param overCapacityMultiplier
     *            The index partition will be split when its actual entry count
     *            is GTE to
     *            <code>overCapacityMultiplier * entryCountPerSplit</code>
     * @param underCapacityMultiplier
     *            When an index partition will be split, the #of new index
     *            partitions will be choosen such that each index partition is
     *            approximately <i>underCapacityMultiple</i> full.
     * @param sampleRate
     *            #of samples to take per estimated split.
     * 
     * @throws IllegalArgumentException
     *             if any argument, or combination or arguments, is out of
     *             range.
     */
    public DefaultSplitHandler(final int minimumEntryCount,
            final int entryCountPerSplit, final double overCapacityMultiplier,
            final double underCapacityMultiplier, final int sampleRate) {

        /*
         * Bootstap parameter settings. 
         * 
         * First, verify combination of parameters is legal.
         */
        assertSplitJoinStable(minimumEntryCount, entryCountPerSplit,
                underCapacityMultiplier);

        /*
         * Now that we know the combination is legal, set individual parameters
         * that have dependencies in their legal range. This will let us set the
         * individual parameters with their settor methods below.
         */
        this.minimumEntryCount = minimumEntryCount;
        this.entryCountPerSplit = entryCountPerSplit;
        this.underCapacityMultiplier = underCapacityMultiplier;
        
        /*
         * Use individual set methods to validate each parameter by itself.
         */

        setMinimumEntryCount(minimumEntryCount);

        setEntryCountPerSplit(entryCountPerSplit);

        setOverCapacityMultiplier(overCapacityMultiplier);

        setUnderCapacityMultiplier(underCapacityMultiplier);

        setSampleRate(sampleRate);
        
    }

    /**
     * Return <code>true</code> iff the range count of the index is less than
     * the {@link #getMinimumEntryCount()}.
     * <p>
     * Note: This relies on the fast range count, which is the upper bound on
     * the #of index entries. For this reason an index partition which has
     * undergone a lot of deletes will not underflow until it has gone through a
     * build to purge the deleted index entries. This is true even when all
     * index entries in the index partition have been deleted!
     */
    public boolean shouldJoin(final long rangeCount) {

        final boolean shouldJoin = rangeCount <= getMinimumEntryCount();
        
        if (INFO)
            log.info("shouldJoin=" + shouldJoin + " : rangeCount=" + rangeCount
                    + ", minimumEntryCount=" + getMinimumEntryCount());
        
        return shouldJoin;
        
    }
    
    /**
     * 
     * 
     */
    static void assertSplitJoinStable(final int minimumEntryCount,
            final int entryCountPerSplit, final double underCapacityMultiplier) {

        if (minimumEntryCount > underCapacityMultiplier * entryCountPerSplit) {
            
            throw new IllegalArgumentException("minimumEntryCount("
                    + minimumEntryCount + ") exceeds underCapacityMultiplier("
                    + underCapacityMultiplier + ") * entryCountPerSplit("
                    + entryCountPerSplit + ")");
            
        }

    }

    /**
     * The minimum #of index entries before the index partition becomes eligible
     * to be joined.
     */
    public int getMinimumEntryCount() {

        return minimumEntryCount;
        
    }

    public void setMinimumEntryCount(final int minimumEntryCount) {

        assertSplitJoinStable(minimumEntryCount, getEntryCountPerSplit(),
                getUnderCapacityMultiplier());

        this.minimumEntryCount = minimumEntryCount;
        
    }

    /**
     * The target maximum #of index entries in an index partition.
     */
    public int getEntryCountPerSplit() {

        return entryCountPerSplit;

    }

    public void setEntryCountPerSplit(final int entryCountPerSplit) {

        if (entryCountPerSplit < Options.MIN_BRANCHING_FACTOR) {

            throw new IllegalArgumentException(
                    "entryCountPerSplit must be GTE the minimum branching factor: entryCountPerSplit="
                            + entryCountPerSplit
                            + ", minBranchingFactor="
                            + Options.MIN_BRANCHING_FACTOR);

        }

        assertSplitJoinStable(getMinimumEntryCount(), entryCountPerSplit,
                getUnderCapacityMultiplier());
        
        this.entryCountPerSplit = entryCountPerSplit;
        
    }

    /**
     * The #of samples per estimated #of splits.
     */
    public int getSampleRate() {

        return sampleRate;

    }

    public void setSampleRate(int sampleRate) {

        this.sampleRate = sampleRate;

    }

    /**
     * The threshold for splitting an index is the
     * {@link #getOverCapacityMultiplier()} times
     * {@link #getEntryCountPerSplit()}. If there are fewer than this many
     * entries in the index then it will not be split.
     */
    public double getOverCapacityMultiplier() {

        return overCapacityMultiplier;

    }

    /**
     * 
     * @param overCapacityMultiplier
     *            A value in [1.0:2.0].
     */
    public void setOverCapacityMultiplier(final double overCapacityMultiplier) {

        final double min = 1.0;
        final double max = 2.0;

        if (overCapacityMultiplier < min || overCapacityMultiplier > max) {

            throw new IllegalArgumentException("Must be in [" + min + ":" + max
                    + "], but was " + overCapacityMultiplier);

        }

        this.overCapacityMultiplier = overCapacityMultiplier;

    }

    /**
     * This is the target under capacity rate for a new index partition. For
     * example, if the {@link #getEntryCountPerSplit()} is 5M and this
     * property is <code>.75</code> then an attempt will be made to divide
     * the index partition into N splits such that each split is at 75% of
     * the {@link #getEntryCountPerSplit()} capacity.
     */
    public double getUnderCapacityMultiplier() {

        return underCapacityMultiplier;

    }

    /**
     * 
     * @param underCapacityMultiplier
     *            A value in [0.5,1.0).
     */
    public void setUnderCapacityMultiplier(final double underCapacityMultiplier) {

        final double min = 0.5;
        final double max = 1.0;

        if (underCapacityMultiplier < min || underCapacityMultiplier >= max) {

            throw new IllegalArgumentException("Must be in [" + min + ":" + max
                    + "), but was " + underCapacityMultiplier);

        }

        assertSplitJoinStable(getMinimumEntryCount(), getEntryCountPerSplit(),
                underCapacityMultiplier);
        
        this.underCapacityMultiplier = underCapacityMultiplier;

    }

    public boolean shouldSplit(final long rangeCount) {

        /*
         * Recommend split if the range count equals or exceeds the overcapacity
         * multiplier.
         */

        if (rangeCount >= (getOverCapacityMultiplier() * entryCountPerSplit)) {

            if(INFO)
            log.info("Recommending split: rangeCount(" + rangeCount
                    + ") >= (entryCountPerSplit(" + entryCountPerSplit
                    + ") * overCapacityMultiplier("
                    + getOverCapacityMultiplier() + "))");

            return true;

        }

        return false;

    }

    public double percentOfSplit(final long rangeCount) {

        final double percentOfSplit = (double) rangeCount
                / (double) entryCountPerSplit;

        if (INFO)
            log.info("percentOfSplit=" + percentOfSplit + " = rangeCount("
                    + rangeCount + ") / entryCountPerSplit("
                    + entryCountPerSplit + ")");

        return percentOfSplit;
        
    }
    
    /**
     * A sample collected from a key-range scan.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class Sample {

        /**
         * A key from the index.
         */
        final byte[] key;

        /**
         * The origin zero (0) offset at which that key was found
         * (interpretation is that the key was visited by the Nth
         * {@link ITuple}).
         */
        final int offset;

        public Sample(byte[] key, int offset) {

            assert key != null;

            assert offset >= 0;

            this.key = key;

            this.offset = offset;

        }

        public String toString() {

            return super.toString() + "{offset=" + offset + ", key="
                    + Arrays.toString(key) + "}";

        }

    }

    /**
     * Sample index using a range scan choosing ({@link #getSampleRate()} x N)
     * {@link Sample}s. The key range scan will filter out both duplicates and
     * deleted index entries. The scan will halt if the index entry offsets
     * would exceed an int32 value.
     * 
     * @param ndx
     *            The index partition.
     * @param nvisited
     *            Used to return the actual #of tuples in the view as measured
     *            by a scan of the index partition.
     * 
     * @return An ordered array of {@link Sample}s as an aid to choosen the
     *         split points for the view.
     */
    public Sample[] sampleIndex(final ILocalBTreeView ndx,
            final AtomicLong nvisited) {

        final int rangeCount = (int) Math.min(ndx.rangeCount(null, null),
                Integer.MAX_VALUE);

        final ITupleIterator itr = ndx.rangeIterator(null, null,
                0/* capacity */, IRangeQuery.KEYS, null/* filter */);

        ITuple tuple = null;

        // The estimated #of splits based on the range count.
        final int numSplitsEstimate = Math.max(1,rangeCount / getEntryCountPerSplit());
        
        // Compute the #of samples to take (the sample rate is the #of samples per split).
        final int numSamplesEstimate = numSplitsEstimate * getSampleRate();
        
        // Note: Minimum value is to sample every tuple.
        final int sampleEveryNTuples = Math.max(1, rangeCount
                / numSamplesEstimate);
        
        if(INFO)
        log.info("Estimating " + numSplitsEstimate + " splits with sampleRate="
                + getSampleRate() + " yeilding ~ " + numSamplesEstimate
                + " samples with one sample every " + sampleEveryNTuples
                + " tuples");
        
        final List<Sample> samples = new ArrayList<Sample>(numSamplesEstimate);

        while (itr.hasNext()) {

            tuple = itr.next();

            final long offset = tuple.getVisitCount() - 1;

            if (offset == Integer.MAX_VALUE) {

                /*
                 * This covers an extreme condition. If the split offsets
                 * would exceed an int32 value then we do not continue. Such
                 * views can be broken down by multiple passes, e.g., on
                 * subsequent overflows of a journal.
                 */

                log.warn("Aborting sample - offsets would exceed int32.");

                break;

            }

            if ((offset % sampleEveryNTuples) == 0) {

                // take a sample.

                final Sample sample = new Sample(tuple.getKey(), (int) offset);

                if (DEBUG)
                    log.debug("samples[" + samples.size() + "] = " + sample);

                samples.add(sample);

            }

        }

        assert samples.size() > 0;

        assert samples.get(0).offset == 0 : "Expecting offset := 0 for 1st sample, not "
                + samples.get(0).offset;

        // the actual #of index entries in the view.
        nvisited.set(tuple == null ? 0L : tuple.getVisitCount());

        if(INFO)
        log.info("Collected " + samples.size() + " samples from " + nvisited
                + " index entries; estimatedSplitCount=" + numSplitsEstimate
                + ", sampleRate=" + getSampleRate() + ", sampling every "
                + sampleEveryNTuples);

        return samples.toArray(new Sample[samples.size()]);

    }

    /**
     * Note: There are configuation parameters so that you can choose to let the
     * index partition grow until it reaches e.g., 150-200% of its maximum entry
     * count and then split it into N index partitions each of which is e.g.,
     * 50-75% full.
     * <p>
     * Note: If the index partition has more than int32 index entries then the
     * last split will have a zero (0) toIndex since we don't know how many
     * index entries will go into that split.
     * 
     * @param ndx
     *            The source index partition.
     *            
     * @return A {@link Split}[] array contains everything that we need to
     *         define the new index partitions <em>except</em> the partition
     *         identifiers.
     * 
     * @see #getSplits(IIndex, int, Sample[])
     * 
     * FIXME Can't we use an {@link ITupleCursor} or {@link ILinearList} here
     * and do less work by advancing to a desired #of tuples into the
     * {@link BTree}? E.g., scanning forward or backward and using keyAt() and
     * indexOf().
     * 
     * @todo Subclasses which impose constraints on where the index partition
     *       can be split need to use {@link ITupleCursor}. They can simply
     *       scan forward or backward looking for an acceptable separator key.
     *       If none is found, then they will have to delete the {@link Split}
     *       from the set of recommended splits but that is hugely unlikely
     *       except when the target index partition size is quite small.
     */
//    * @param btreeCounters
//    *            The performance counters (optional, but tail splits will not
//    *            be choosen when <code>null</code>).
//    * 
    public Split[] getSplits(final IPartitionIdFactory partitionIdFactory,
            final ILocalBTreeView ndx) {//, final BTreeCounters btreeCounters) {

        // Sample the index for tuples used to split into key-ranges.
        final AtomicLong nvisited = new AtomicLong();
        final Sample[] samples = sampleIndex(ndx, nvisited);
        // range count back from sampleIndex.
        final long rangeCount = nvisited.get();

//        // percentage of leaf splits that occur in the head of the BTree.
//        final double percentHeadSplits;
//
//        // percentage of leaf splits that occur in the tail of the BTree.
//        final double percentTailSplits;
//        
//        if (btreeCounters != null) {
//
//            // Note: +1 in the denominator to avoid divide by zero.
//            percentHeadSplits = btreeCounters.headSplit
//                    / (btreeCounters.leavesSplit + 1d);
//
//            // Note: +1 in the denominator to avoid divide by zero.
//            percentTailSplits = btreeCounters.tailSplit
//                    / (btreeCounters.leavesSplit + 1d);
//
//        } else {
//
//            percentHeadSplits = 0d;
//
//            percentTailSplits = 0d;
//
//        }
//
//        // true iff this is a good candidate for a tail split.
//        final boolean tailSplit = percentTailSplits > resourceManager.tailSplitThreshold;

        if (rangeCount < overCapacityMultiplier * getEntryCountPerSplit()) {

            /*
             * The index is too small to split.
             */
            
//            if (tailSplit
//                    && rangeCount >= underCapacityMultiplier
//                            * getEntryCountPerSplit()) {
//
//                /*
//                 * There are heavy writes on the tail of the index and the total
//                 * index GTE the undercapacity threshold so we can do a
//                 * tailSplit and wind up with a head that is near the
//                 * undercapacity threshold and a tail that is getting a lot of
//                 * writes.
//                 */
//                
//                return SplitUtility.tailSplit(resourceManager, ndx.getMutableBTree());
//                
//            }
            
            if (INFO)
                log.info("Will not split : nvisited=" + rangeCount
                        + " is less than " + overCapacityMultiplier
                        + " * entryCountPerSplit(" + entryCountPerSplit + ")");

            return null;

        }

        /*
         * Compute the actual #of splits
         */
        final int nsplits = (int) Math
                .floor((rangeCount / getUnderCapacityMultiplier())
                        / getEntryCountPerSplit());

        if (nsplits < 2) {

            /*
             * Split is rejected based on insufficient capacity in the computed
             * Split[] for the configured undercapacity multiplier.
             */

            if(INFO)
            log.info("Will not split : nsplits(" + nsplits
                    + ") := floor(nvisited(" + rangeCount
                    + ") / underCapacityMultiplier("
                    + getUnderCapacityMultiplier() + ") / entryCountPerSplit("
                    + +entryCountPerSplit + ")");

            return null;

        }

        return getSplits(partitionIdFactory, ndx, nsplits, samples, rangeCount);

    }

    /**
     * Examine the {@link Sample}s choosing {@link Split}s that best capture
     * the target #of splits to be made.
     * <p>
     * Note: If you are trying to write a custom split rule then consider
     * subclassing this method and adjust the split points so as to obey any
     * application constraint, such as not splitting a primary key across index
     * partitions. In general, the split rule can scan forward or backward until
     * it finds a suitable adjusted split point.
     * <p>
     * Note: The caller MUST disregard the {@link IResourceMetadata}[] attached
     * to {@link Split#pmd} since we do not have that information on hand here.
     * The correct {@link IResourceMetadata}[] is available locally to
     * {@link AtomicUpdateSplitIndexPartitionTask}.
     * 
     * @param ndx
     *            The source index partition.
     * @param nsplits
     *            The target #of splits. If necessary or desired, the #of splits
     *            MAY be changed simply by returning an array with a different
     *            #of splits -or- <code>null</code> iff you decide that you do
     *            not want the index partition to be split at this time.
     * @param samples
     *            An ordered array of samples from the index partition. See
     *            {@link #sampleIndex(IIndex, AtomicLong)}.
     * @param nvisited
     *            The #of index entries that were visited when generating those
     *            samples. This is capped at {@link Integer#MAX_VALUE} by
     *            {@link #sampleIndex(IIndex, AtomicLong)}.
     * 
     * @return A {@link Split}[] array containing everything that we need to
     *         define the new index partitions (including the new partition
     *         identifiers assigned by the {@link IMetadataService}) -or-
     *         <code>null</code> if a more detailed examination reveals that
     *         the index SHOULD NOT be split at this time.
     * 
     * @see #getEntryCountPerSplit()
     * @see #getUnderCapacityMultiplier()
     * 
     * @todo there are a lot of edge conditions in this -- write tests!
     */
    protected Split[] getSplits(final IPartitionIdFactory partitionIdFactory,
            final IIndex ndx, final int nsplits, final Sample[] samples,
            final long nvisited) {

        // The source index partition metadata.
        final IndexMetadata indexMetadata = ndx.getIndexMetadata();

        // The target #of index entries per split.
        final int targetCapacity = (int) (getEntryCountPerSplit() * getUnderCapacityMultiplier());

        // The splits that we will generate.
        final List<Split> splits = new ArrayList<Split>(nsplits);

        // The metadata for the index partition that is being split. 
        final LocalPartitionMetadata oldpmd = ndx.getIndexMetadata().getPartitionMetadata();

        if (oldpmd.getSourcePartitionId() != -1) {
            
            throw new IllegalStateException(
                    "Split not allowed during move: sourcePartitionId="
                            + oldpmd.getSourcePartitionId());
            
        }
        
        // index into the samples[].
        int j = 0;
        // #of index entries assigned into splits so far.
        int nused = 0;
        // begin at index zero into the source index partition.
        int fromIndex = 0;
        // begin with the leftSeparator for the source index partition.
        byte[] fromKey = oldpmd.getLeftSeparatorKey();

        for (int i = 0; i < nsplits; i++) {

            Sample sample = null;

            // consider remaining samples.
            for (; j < samples.length; j++) {

                sample = samples[j];

                final int count = sample.offset - nused;

                if (count >= targetCapacity) {

                    if (INFO)
                        log.info("Filled split[" + i + "] with " + count
                                + " entries: targetCapacity=" + targetCapacity
                                + ", samples[j]=" + sample);

                    j++; // consumed.

                    nused += count;

                    break;

                }

            }

            final int toIndex;
            if (sample == null) {

                assert j == samples.length : "j="+j+", samples.length="+samples.length;

                toIndex = 0;

            } else {

                toIndex = sample.offset;

            }

            if (fromIndex == toIndex) {

                /*
                 * Note: I've seen what appears to be an empty Split, which is
                 * illegal, so I added this to get some more information on when
                 * that occurs and added detail to the asserts in the Split()
                 * ctor. If you see this warning look into it a bit further and
                 * see what the fence post conditions are.
                 */
                
                log
                        .warn("Skipping over an empty split: fromIndex="
                                + fromIndex + ", toIndex=" + toIndex + ", j="
                                + j + ", nused=" + nused + ", fromKey="
                                + fromKey + ", sample=" + sample);

                continue;
                
            }
            
            final byte[] toKey;
            if (i == nsplits - 1) {

                // Note: always assign the rightSeparator to the last split.

                toKey = ndx.getIndexMetadata().getPartitionMetadata()
                        .getRightSeparatorKey();

            } else {

                assert sample != null;

                toKey = sample.key;

            }

            // Get the next partition identifier for the named scale-out index.
            final int partitionId = partitionIdFactory
                    .nextPartitionId(indexMetadata.getName());

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    partitionId,//
                    -1, // Note: split not allowed during move.
                    fromKey,//
                    toKey,//
                    /*
                     * Note: no resources for an index segment
                     */
                    null,//
                    oldpmd.getHistory()+
                    "chooseSplitPoint(oldPartitionId="
                            + oldpmd.getPartitionId() + ",nsplits=" + nsplits
                            + ",newPartitionId=" + partitionId + ") "
                    );

            splits.add( new Split(pmd, fromIndex, toIndex) );

            fromKey = toKey;

            fromIndex = toIndex;

        } // next split.

        final int splitCount = splits.size();

        if (splitCount <= 1) {

            log.warn("No splits! splitCount=" + splitCount);

            return null;

        }

        return splits.toArray(new Split[splitCount]);

    }

    /**
     * Return an adjusted split handler. The split handler will be adjusted to
     * be heavily biased in favor of splitting an index partition when the #of
     * index partitions for a scale-out index is small. This adjustment is
     * designed to very rapidly (re-)distribute a new scale-out index until
     * there are at least 10 index partitions and rapidly (re-)distribute a
     * scale-out index until they are at least 100 index partitions. Thereafter
     * the as configured behavior will be observed.
     * <p>
     * Note: The potential parallelism of a data service is limited by the #of
     * index partitions on that data service as well as by the workload of the
     * application. By partitioning new and young indices aggressively we ensure
     * that the index is broken into multiple index partitions on the initial
     * data service. Those index partitions will be re-distributed across the
     * available data services based on recommendations made by the load
     * balancer. Both breaking an index into multiple partitions on a single
     * data service and re-distributing those index partitions among the hosts
     * of a cluster will increase the resources (CPU, DISK, RAM) which can be
     * brought to bear on the index. In particular, there is a constraint of a
     * single core per index partition (for writes). Therefore breaking a new
     * index into 2 pieces doubles the potential concurrency for a data service
     * on a host with at least 2 cores. This can be readily extrapolated to a
     * cluster with 8 cores x 16 machines, etc.
     * <p>
     * Note: The adjustment is proportional to the #of existing index partitions
     * and is adjusted using a floating point discount factor. This should
     * prevent a split triggering a subsequent join on the next overflow.
     * 
     * @param accelerateSplitThreshold
     *            The #of index partitions below which we will accelerate the
     *            decision to split an index partition.
     * @param npartitions
     *            The #of index partitions (pre-split) for the scale-out index.
     * 
     * @return The adjusted split handler.
     * 
     * @see OverflowManager.Options#ACCELERATE_SPLIT_THRESHOLD
     */
    public DefaultSplitHandler getAdjustedSplitHandler(
            final int accelerateSplitThreshold, final long npartitions) {

        if (npartitions >= accelerateSplitThreshold) {

            /*
             * There are plenty of index partitions. Use the original split
             * handler.
             * 
             * Note: This also prevents our "discount" from becoming an
             * "inflation" factor!
             */

            return this;

        }

        // the split handler as configured.
        final DefaultSplitHandler s = (DefaultSplitHandler) this;

        // discount: given T=100, will be 1 when N=100; 10 when N=10, and
        // 100 when N=1.
        final double d = (double) npartitions / accelerateSplitThreshold;

        try {

            // adjusted split handler.
            final DefaultSplitHandler t = new DefaultSplitHandler(//
                    (int) (s.getMinimumEntryCount() * d), //
                    (int) (s.getEntryCountPerSplit() * d), //
                    s.getOverCapacityMultiplier(), // unchanged 
                    s.getUnderCapacityMultiplier(), // unchanged
                    s.getSampleRate() // unchanged
            );

            if (INFO)
                log.info("npartitions=" + npartitions + ", discount=" + d
                        + ", threshold=" + accelerateSplitThreshold
                        + ", adjustedSplitHandler=" + t);

            return t;

        } catch (IllegalArgumentException ex) {

            /*
             * The adjustment violated some constraint. Log a warning and
             * use the original splitHandler since it was at least valid.
             */

            log.warn("Adjustment failed" + ": npartitions=" + npartitions
                    + ", discount=" + d + ", splitHandler=" + this, ex);

            return this;

        }

    }

    /**
     * Tweaks the split handler so that it will generate N more or less equal
     * splits given an index with the specified rangeCount.
     * 
     * @param nsplits
     *            The desired number of splits.
     * @param rangeCount
     *            The range count of the index to be split.
     * 
     * @return
     */
    public DefaultSplitHandler getAdjustedSplitHandlerForEqualSplits(
            final int nsplits, final long rangeCount) {

        final DefaultSplitHandler s = this;

        final int adjustedEntryCountPerSplit = (int) (rangeCount / nsplits);

        final double ratio = adjustedEntryCountPerSplit
                / s.getEntryCountPerSplit();

        final int adjustedMinEntryCount = (int) (s.getMinimumEntryCount() * ratio);
        
        // adjusted split handler.
        final DefaultSplitHandler t = new DefaultSplitHandler(//
                adjustedMinEntryCount,//
                adjustedEntryCountPerSplit,//
                s.getOverCapacityMultiplier(), // unchanged 
//                s.getUnderCapacityMultiplier(), // unchanged
                .99, // we want each split to be full.
                s.getSampleRate() // unchanged
        );

        if (INFO)
            log.info("nsplits=" + nsplits + ", rangeCount=" + rangeCount
                    + ", unadjustedSplitHandler=" + this
                    +", adjustedSplitHandler=" + t);

        return t;

    }

}
